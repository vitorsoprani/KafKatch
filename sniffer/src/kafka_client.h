/*
 * kafka_client.h - Definições e protótipos para o produtor Kafka
 *
 * Autor: Vitor Soprani
 *
 * Interface responsável pela inicialização do produtor na librdkafka,
 * serialização dos microflows e envio assíncrono para o cluster.
 */

#ifndef _KAFKA_CLIENT_H
#define _KAFKA_CLIENT_H

#include <librdkafka/rdkafka.h>

#include "flow.h"

rd_kafka_t *kafka_client_init(const char *brokers);
void kafka_client_send_flows(rd_kafka_t *producer, const char *topic, flow_t **flow_table);
void kafka_client_cleanup(rd_kafka_t *producer);

#endif /* _KAFKA_CLIENT_H */
