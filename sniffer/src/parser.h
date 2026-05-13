#ifndef _PARSER_H
#define _PARSER_H

#include <pcap/pcap.h>
#include <pthread.h>
#include <librdkafka/rdkafka.h>
#include "flow.h"

/* O contexto que será injetado no loop de captura */
struct sniffer_context {
    flow_t *tables[2];		/* uma tabela para captura e outra para o flush */
    int active_idx;		/* indica qual tabela está na captura */
    pthread_mutex_t mutex;	/* protege a troca dos índices */
    uint32_t net_ip;		/* Endereço da rede local (Network Byte Order) */
    uint32_t net_mask;		/* Máscara de sub-rede (Network Byte Order) */
    int is_running;		/* controla a execução da thread de flush */
    rd_kafka_t *kafka_producer;	/* envia os dados para o cluster */
};

/* Callback para o pcap_loop */
void pkt_handler(u_char *user, const struct pcap_pkthdr *h, const u_char *bytes);

#endif
