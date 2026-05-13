#ifndef _KAFKA_CLIENT_H
#define _KAFKA_CLIENT_H

#include <librdkafka/rdkafka.h>
#include "flow.h"

/* Inicializa o produtor Kafka. Retorna NULL em caso de erro. */
rd_kafka_t *kafka_client_init(const char *brokers);

/* Formata a tabela hash para JSON e a envia para o tópico */
void kafka_client_send_flows(rd_kafka_t *producer, const char *topic, flow_t **flow_table);

/* Força a entrega das últimas mensagens e limpa o produtor da memória */
void kafka_client_cleanup(rd_kafka_t *producer);

#endif
