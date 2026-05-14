/*
 * kafka_client.c - Implementação do produtor Kafka (librdkafka)
 *
 * Autor: Vitor Soprani
 *
 * Este módulo gerencia o ciclo de vida do cliente produtor Kafka.
 * Ele inicializa as configurações do broker, registra callbacks de
 * entrega, e realiza a serialização da tabela de fluxos (microflows)
 * para o formato JSON antes de injetá-los no tópico correspondente.
 */

#include "kafka_client.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <arpa/inet.h>

#define LOG(fmt, ...) \
	fprintf(stderr, "[%s:%d] " fmt "\n", __FILE__, __LINE__, ##__VA_ARGS__)

/**
 * dir_to_str() - Converte a enumeração de direção para string legível
 * @dir: Enumerador flow_direction_t
 *
 * Retorna o ponteiro para a string literal correspondente.
 */
static const char *dir_to_str(flow_direction_t dir)
{
	switch (dir) {
	case DIR_OUTBOUND:
		return "OUTBOUND";
	case DIR_INBOUND:
		return "INBOUND";
	case DIR_LATERAL:
		return "LATERAL";
	default:
		return "UNKNOWN";
	}
}

/**
 * dr_msg_cb() - Delivery Report Callback (Callback de Entrega)
 * @rk:        Ponteiro para a instância do produtor Kafka
 * @rkmessage: Estrutura contendo o status da mensagem enviada
 * @opaque:    Ponteiro de contexto opcional (não utilizado)
 *
 * Função invocada assincronamente pela librdkafka para cada mensagem
 * produzida. Confirma se o broker recebeu e comitou o dado na partição.
 * Vital para observar perda de pacotes de telemetria em caso de falha de rede.
 */
static void dr_msg_cb(rd_kafka_t *rk, const rd_kafka_message_t *rkmessage, void *opaque)
{
	if (rkmessage->err)
		LOG("Falha na entrega da mensagem: %s", rd_kafka_err2str(rkmessage->err));
}

/**
 * kafka_client_init() - Inicializa e configura o produtor Kafka
 * @brokers: String CSV com endereços (ex: "localhost:9092,localhost:9094")
 *
 * Instancia o objeto de configuração, injeta os parâmetros de bootstrap,
 * otimiza o buffer de envio e aloca o handle do produtor principal.
 */
rd_kafka_t *kafka_client_init(const char *brokers)
{
	char errstr[512];
	rd_kafka_conf_t *conf;
	rd_kafka_t *rk;

	conf = rd_kafka_conf_new();

	/*
	 * CONFIGURAÇÃO 1: Bootstrap Servers
	 * Informa à biblioteca a lista inicial de brokers para descobrir o cluster.
	 * A conexão real aos nós líderes de cada partição é feita dinamicamente
	 * pelos metadados recebidos após este contato inicial.
	 */
	if (rd_kafka_conf_set(conf, "bootstrap.servers", brokers, errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK) {
		LOG("Erro de configuração do Kafka: %s", errstr);
		rd_kafka_conf_destroy(conf);
		return NULL;
	}

	/*
	 * CONFIGURAÇÃO 2: Max Buffering Time (queue.buffering.max.ms)
	 * Alterando para "10", instruímos o cliente a agrupar (batch) as mensagens em
	 * janelas máximas de 10 milissegundos antes de dispará-las para a rede TCP.
	 */
	rd_kafka_conf_set(conf, "queue.buffering.max.ms", "10", errstr, sizeof(errstr));

	/* * CONFIGURAÇÃO 3: Nível de Confirmação (Acks)
	 * Define o nível de garantia de entrega exigido pelo produtor.
	 * "1" = Requer apenas a confirmação do broker Líder da partição.
	 */
	rd_kafka_conf_set(conf, "acks", "1", errstr, sizeof(errstr));

	/* Registro do callback de verificação de entrega (Delivery Report) */
	rd_kafka_conf_set_dr_msg_cb(conf, dr_msg_cb);

	/* Criação do produtor. Em caso de sucesso, 'conf' é consumido (não precisamos liberar) */
	rk = rd_kafka_new(RD_KAFKA_PRODUCER, conf, errstr, sizeof(errstr));
	if (!rk) {
		LOG("Falha ao criar o produtor Kafka: %s", errstr);
		rd_kafka_conf_destroy(conf);
		return NULL;
	}

	return rk;
}

/**
 * kafka_client_send_flows() - Serializa e despacha os fluxos ativos
 * @producer:       Handle do produtor Kafka previamente inicializado
 * @topic:          Nome do tópico de destino ("network-microflows-raw")
 * @flow_table_ptr: Ponteiro duplo para a tabela hash do período isolado
 *
 * Itera sobre a tabela de fluxos offline passada pela thread de flush,
 * formatando cada entrada em JSON. Realiza a deleção concomitante da
 * memória para garantir que o snapshot da janela seja esvaziado.
 */
void kafka_client_send_flows(rd_kafka_t *producer, const char *topic, flow_t **flow_table_ptr)
{
	flow_t *current_flow;
	flow_t *tmp;
	char src_ip_str[INET_ADDRSTRLEN];
	char dst_ip_str[INET_ADDRSTRLEN];
	char json_payload[1024];
	const char *proto_str;
	double mean_iat;

	if (!flow_table_ptr || !*flow_table_ptr || !producer)
		return;

	/*
	 * O uso de HASH_ITER permite iteração segura (safe iteration).
	 * Ao contrário de um 'for' simples, o ponteiro 'tmp' armazena o
	 * próximo elemento da lista encadeada da uthash, permitindo que
	 * o 'current_flow' seja deletado (HASH_DEL) e liberado (free)
	 * dentro do próprio laço sem causar falhas de segmentação (Segfault).
	 */
	HASH_ITER(hh, *flow_table_ptr, current_flow, tmp) {
		inet_ntop(AF_INET, &(current_flow->key.src_ip), src_ip_str, INET_ADDRSTRLEN);
		inet_ntop(AF_INET, &(current_flow->key.dst_ip), dst_ip_str, INET_ADDRSTRLEN);

		mean_iat = 0.0;
		if (current_flow->packet_count > 1)
			mean_iat = current_flow->total_iat_usec / (current_flow->packet_count - 1);

		proto_str = "OTHER";
		if (current_flow->key.protocol == IPPROTO_TCP)
			proto_str = "TCP";
		else if (current_flow->key.protocol == IPPROTO_UDP)
			proto_str = "UDP";

		/* Serialização bruta para JSON sem bibliotecas externas pesadas */
		snprintf(json_payload, sizeof(json_payload),
			 "{\"src_ip\":\"%s\",\"src_port\":%u,\"dst_ip\":\"%s\",\"dst_port\":%u,"
			 "\"protocol\":\"%s\",\"dir\":\"%s\",\"packets\":%u,\"bytes\":%u,"
			 "\"min_size\":%u,\"max_size\":%u,\"mean_iat_us\":%.1f,"
			 "\"tcp\":{\"syn\":%u,\"ack\":%u,\"fin\":%u,\"rst\":%u}}",
			 src_ip_str, ntohs(current_flow->key.src_port),
			 dst_ip_str, ntohs(current_flow->key.dst_port),
			 proto_str, dir_to_str(current_flow->direction),
			 current_flow->packet_count, current_flow->byte_count,
			 current_flow->min_packet_size, current_flow->max_packet_size, mean_iat,
			 current_flow->tcp_syn_count, current_flow->tcp_ack_count,
			 current_flow->tcp_fin_count, current_flow->tcp_rst_count);

		/*
		 * PRODUÇÃO DA MENSAGEM (rd_kafka_producev)
		 * Emprega o método vetorizado para passagem de argumentos limpa.
		 * A flag RD_KAFKA_MSG_F_COPY é crucial: instrui a librdkafka a fazer
		 * uma cópia do conteúdo de 'json_payload' para sua própria memória
		 * interna, permitindo que a variável de escopo local seja sobrescrita
		 * na próxima iteração do laço de forma segura.
		 */
		rd_kafka_producev(
			producer,
			RD_KAFKA_V_TOPIC(topic),
			RD_KAFKA_V_MSGFLAGS(RD_KAFKA_MSG_F_COPY),
			RD_KAFKA_V_VALUE(json_payload, strlen(json_payload)),
			RD_KAFKA_V_OPAQUE(NULL),
			RD_KAFKA_V_END
		);

		/* Limpeza imediata da estrutura de dados para não acumular lixo térmico */
		HASH_DEL(*flow_table_ptr, current_flow);
		free(current_flow);
	}

	/*
	 * I/O SÍNCRONO FINAL DA JANELA
	 * 1. rd_kafka_poll(0): Aciona a verificação de callbacks (como o dr_msg_cb)
	 * não bloqueante. Atende os retornos das mensagens da janela anterior.
	 * 2. rd_kafka_flush(500): Tendo em vista o requisito de fixidez da janela
	 * temporal (1 segundo host-time), este flush força que todas as mensagens
	 * produzidas no laço acima desçam da RAM para o Socket TCP do Kafka no
	 * momento exato, garantindo a fidelidade cronológica dos dados. O tempo
	 * limite de espera (timeout) é de 500ms.
	 */
	rd_kafka_poll(producer, 0);
	rd_kafka_flush(producer, 500);
}

/**
 * kafka_client_cleanup() - Encerramento pacífico do cliente Kafka
 * @producer: Handle do produtor a ser destruído
 *
 * Drena as filas internas e desmobiliza threads secundárias em background
 * geridas pela librdkafka para evitar segmentation fault durante a finalização.
 */
void kafka_client_cleanup(rd_kafka_t *producer)
{
	if (producer) {
		LOG("Limpando a fila final do Kafka (aguarde)...");
		
		/* Dá ao sistema 5 segundos para transmitir buffers não despachados */
		rd_kafka_flush(producer, 5000);
		rd_kafka_destroy(producer);
	}
}
