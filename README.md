# KafKatch: Sniffer de Microflows & Cluster Kafka

Este projeto é um testbed de observabilidade de rede focado na captura de tráfego de rede em tempo real, geração de datasets e exportação assíncrona para um cluster Apache Kafka. O sistema é composto por um sniffer de alto desempenho escrito em C e um cluster Kafka operando em modo KRaft via Docker.

## 1. Dependências e Instalação

O projeto foi desenvolvido para sistemas Linux (Debian/Ubuntu). Certifique-se de ter os pacotes de desenvolvimento instalados.

### Bibliotecas de Sistema
sudo apt update
sudo apt install libpcap-dev librdkafka-dev

---

## 2. Configuração da Infraestrutura

### 2.1. Ponto de Acesso (WiFi Hotspot)
O sniffer captura tráfego de dispositivos conectados a um ponto de acesso gerenciado. Use o script `setup-ap.sh` para configurar a interface e as regras de encaminhamento (NAT).

1. Dê permissão de execução: chmod +x setup-ap.sh
2. Execute o script: sudo ./setup-ap.sh

**O que o script faz:**
* Configura uma interface WiFi em modo AP (Access Point).
* Ativa o roteamento IPv4 e desativa IPv6 na interface.
* Configura regras de iptables (MASQUERADE) para que os dispositivos conectados tenham acesso à internet através da sua conexão principal.

### 2.2. Cluster Kafka (Modo KRaft)
O cluster utiliza 3 brokers em modo misto (controller e broker) para garantir alta disponibilidade e tolerância a falhas, eliminando a dependência do Zookeeper.

**Subindo o cluster:**
docker compose up -d

**Inicializando os Tópicos:**
O sniffer exige o tópico `network-microflows-raw`. Você pode criá-lo com 3 partições e fator de replicação 3:
  kafka-topics.sh --create --topic network-microflows-raw \
  --bootstrap-server localhost:9092 --partitions 3 --replication-factor 3

---

## 3. Estrutura do Projeto, Compilação e Execução

O código-fonte em C está organizado na pasta `src/`, enquanto os binários compilados são direcionados para `obj/`.

### Compilação
O projeto utiliza um Makefile padronizado. Os seguintes comandos estão disponíveis:

* `make`: Compila o binário de produção (`sniffer`) com otimizações padrão.
* `make debug`: Compila o binário com a flag `-g` e sem otimizações (`-O0`), ideal para análise com GDB ou Valgrind.
* `make clean`: Remove o binário final e a pasta `obj/`.

### Execução
O programa exige privilégios de superusuário para colocar a interface de rede em modo promíscuo.

sudo ./sniffer <interface_do_hotspot>
Exemplo: sudo ./sniffer wlxe894f6281fb9

---

## 4. Detalhes de Performance e Resiliência

### Configuração do Produtor (librdkafka)
* acks = 1: O produtor aguarda a confirmação de escrita apenas do broker líder. Isso garante que o dado foi persistido no cluster sem a latência de esperar a replicação em todos os nós.
* queue.buffering.max.ms = 10: Força o envio de lotes de mensagens a cada 10ms. Isso evita que os dados fiquem presos no buffer do cliente por muito tempo, garantindo que o "tick" de 1 segundo do host seja refletido quase instantaneamente no Kafka.
* rd_kafka_flush(): Invocado ao final de cada janela de 1 segundo para garantir que o socket TCP seja esvaziado e as mensagens sejam transmitidas antes da próxima janela de captura.

### Arquitetura do Cluster
* Multi-Broker: Com 3 instâncias.
* KRaft: Utiliza o protocolo Raft para consenso de metadados, reduzindo o tempo de recuperação em caso de falha de um controlador.

---

## 5. Data Model (Event Schema)

Cada evento enviado ao Kafka representa um Microflow agregado em uma janela de 1 segundo.

### Exemplo de JSON:
{
  "src_ip": "10.42.1.228",
  "src_port": 45764,
  "dst_ip": "216.239.32.223",
  "dst_port": 443,
  "protocol": "TCP",
  "dir": "OUTBOUND",
  "packets": 14,
  "bytes": 826,
  "min_size": 0,
  "max_size": 517,
  "mean_iat_us": 52285.3,
  "tcp": {
    "syn": 1,
    "ack": 13,
    "fin": 0,
    "rst": 0
  }
}

### Descrição dos Campos:

| Campo | Tipo | Descrição |
| :--- | :--- | :--- |
| src_ip | string | Endereço IPv4 de origem. |
| src_port | integer | Porta de origem (Transport Layer). |
| dst_ip | string | Endereço IPv4 de destino. |
| dst_port | integer | Porta de destino (Transport Layer). |
| protocol | string | Protocolo de transporte (TCP, UDP ou OTHER). |
| dir | string | Direção do fluxo (INBOUND, OUTBOUND, LATERAL). |
| packets | integer | Total de pacotes capturados no intervalo de 1s. |
| bytes | integer | Total de bytes de payload (camada de aplicação). |
| min_size | integer | Tamanho do menor pacote (payload) no intervalo. |
| max_size | integer | Tamanho do maior pacote (payload) no intervalo. |
| mean_iat_us| float | Média do tempo entre chegadas (IAT) em microssegundos. |
| tcp.syn | integer | Contagem de pacotes com flag SYN ativa. |
| tcp.ack | integer | Contagem de pacotes com flag ACK ativa. |
| tcp.fin | integer | Contagem de pacotes com flag FIN ativa. |
| tcp.rst | integer | Contagem de pacotes com flag RST ativa. |

---

### Visualização
A configuração do cluster e os tópicos/mensagens podem ser observados de forma gráfica em http://localhost:8080