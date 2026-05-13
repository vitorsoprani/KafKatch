#include "kafka_client.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <arpa/inet.h>

#define LOG(fmt, ...) \
	fprintf(stderr, "[%s:%d] " fmt "\n", __FILE__, __LINE__, ##__VA_ARGS__)

/* Função auxiliar para traduzir a direção */
static const char* dir_to_str(flow_direction_t dir) {
	switch(dir) {
		case DIR_OUTBOUND: return "OUTBOUND";
		case DIR_INBOUND:  return "INBOUND";
		case DIR_LATERAL:  return "LATERAL";
		default:		   return "UNKNOWN";
	}
}

/* Callback de verificação de entrega */
static void dr_msg_cb(rd_kafka_t *rk, const rd_kafka_message_t *rkmessage, void *opaque) {
	if (rkmessage->err) {
		LOG("Falha na entrega da mensagem: %s", rd_kafka_err2str(rkmessage->err));
	}
}

rd_kafka_t *kafka_client_init(const char *brokers) {
	char errstr[512];
	rd_kafka_conf_t *conf = rd_kafka_conf_new();

	if (rd_kafka_conf_set(conf, "bootstrap.servers", brokers, errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK) {
		LOG("Erro de configuração do Kafka: %s", errstr);
		rd_kafka_conf_destroy(conf);
		return NULL;
	}

	/* SOLUÇÃO DE "PERFORMANCE": Forçamos o buffer a não reter mensagens por muito tempo.
	 * Tempo máximo de agrupamento em ms. */
	rd_kafka_conf_set(conf, "queue.buffering.max.ms", "10", errstr, sizeof(errstr));

	rd_kafka_conf_set_dr_msg_cb(conf, dr_msg_cb);

	rd_kafka_t *rk = rd_kafka_new(RD_KAFKA_PRODUCER, conf, errstr, sizeof(errstr));
	if (!rk) {
		LOG("Falha ao criar o produtor Kafka: %s", errstr);
		rd_kafka_conf_destroy(conf); /* Só limpamos se falhar; se der sucesso o rd_kafka_new consome o ponteiro */
		return NULL;
	}

	return rk;
}

void kafka_client_send_flows(rd_kafka_t *producer, const char *topic, flow_t **flow_table_ptr) {
	if (flow_table_ptr == NULL || *flow_table_ptr == NULL || producer == NULL) return;

	flow_t *current_flow, *tmp;
	HASH_ITER(hh, *flow_table_ptr, current_flow, tmp) {
		char src_ip_str[INET_ADDRSTRLEN];
		char dst_ip_str[INET_ADDRSTRLEN];
		inet_ntop(AF_INET, &(current_flow->key.src_ip), src_ip_str, INET_ADDRSTRLEN);
		inet_ntop(AF_INET, &(current_flow->key.dst_ip), dst_ip_str, INET_ADDRSTRLEN);

		/* Cálculo de IAT Médio igual ao debug_print_flow_table */
		double mean_iat = 0.0;
		if (current_flow->packet_count > 1) {
			mean_iat = current_flow->total_iat_usec / (current_flow->packet_count - 1);
		}

		const char *proto_str = "OTHER";
		if (current_flow->key.protocol == IPPROTO_TCP) proto_str = "TCP";
		else if (current_flow->key.protocol == IPPROTO_UDP) proto_str = "UDP";

		/* Adicionando as métricas e flags exigidas na string JSON */
		char json_payload[1024];
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

		rd_kafka_producev(
			producer,
			RD_KAFKA_V_TOPIC(topic),
			RD_KAFKA_V_MSGFLAGS(RD_KAFKA_MSG_F_COPY),
			RD_KAFKA_V_VALUE(json_payload, strlen(json_payload)),
			RD_KAFKA_V_OPAQUE(NULL),
			RD_KAFKA_V_END
		);

		HASH_DEL(*flow_table_ptr, current_flow); /* Remove da tabela (gerenciamento uthash) */
		free(current_flow);                      /* Libera a memória alocada via calloc() */
	}

	/* Processa os callbacks e FORÇA o envio síncrono da janela inteira agora.
	   Isso garante que cada tick de 1 segundo envie os dados imediatamente,
	   resolvendo a sua "falta de performance" nas entregas. */
	rd_kafka_poll(producer, 0);
	rd_kafka_flush(producer, 500);
}

void kafka_client_cleanup(rd_kafka_t *producer) {
	if (producer) {
		LOG("Limpando a fila final do Kafka (aguarde)...");
		rd_kafka_flush(producer, 5000);
		rd_kafka_destroy(producer);
	}
}
