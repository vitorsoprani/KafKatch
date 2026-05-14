/*
 * parser.h - Definições e estruturas para o processamento de pacotes
 *
 * Autor: Vitor Soprani
 *
 * Contém a definição do contexto principal do sniffer (usado para o
 * double buffering e sincronização) e a assinatura da função de
 * extração de dados (callback do pcap).
 */

#ifndef _PARSER_H
#define _PARSER_H

#include <pcap/pcap.h>
#include <pthread.h>
#include <librdkafka/rdkafka.h>

#include "flow.h"

/*
 * struct sniffer_context - Contexto global injetado no loop de captura
 * * Mantém o estado do double buffering, chaves de sincronização,
 * informações da rede local para cálculo de direção (inbound/outbound)
 * e o produtor Kafka.
 */
struct sniffer_context {
	flow_t		*tables[2];	/* Tabelas ping-pong (captura/flush) */
	int		active_idx;	/* Índice da tabela ativa (0 ou 1) */
	pthread_mutex_t	mutex;		/* Protege a troca do active_idx */
	uint32_t	net_ip;		/* IP da rede (Network Byte Order) */
	uint32_t	net_mask;	/* Máscara (Network Byte Order) */
	int		is_running;	/* Flag de controle da thread de flush */
	rd_kafka_t	*kafka_producer;/* Handler do produtor Kafka */
};

/*
 * pkt_handler() - Callback invocado pelo libpcap a cada pacote capturado.
 * user:  Ponteiro genérico (contexto) injetado pelo pcap_loop.
 * h:     Cabeçalho gerado pelo pcap (contém timestamp e tamanhos).
 * bytes: Ponteiro bruto para os dados do frame Ethernet.
 */
void pkt_handler(u_char *user, const struct pcap_pkthdr *h, const u_char *bytes);

#endif /* _PARSER_H */
