#ifndef _FLOW_H
#define _FLOW_H

#include "stats.h"
#include "uthash.h"

#include <sys/time.h>

struct sniffer_context;
typedef struct sniffer_context sniffer_context_t;

typedef enum {
    DIR_OUTBOUND = 0, /* Local para Externo */
    DIR_INBOUND  = 1, /* Externo para Local */
    DIR_LATERAL  = 2, /* Local para Local */
    DIR_UNKNOWN  = 3
} flow_direction_t;

typedef struct {
	flow_key_t key;

	flow_direction_t direction;

	/* volume: */
	uint32_t packet_count;
	uint32_t byte_count;
	uint16_t min_packet_size;
	uint16_t max_packet_size;

	/* tempo */
	struct timeval start_time;
	struct timeval last_seen;
	double total_iat_usec;

	/* Agregadores TCP */
	uint32_t tcp_syn_count;
	uint32_t tcp_fin_count;
	uint32_t tcp_rst_count;
	uint32_t tcp_ack_count;

	UT_hash_handle hh;         /* Motor da uthash */
} flow_t;

/* Cria um novo flow apenas com a chave, demais elementos são inicializados 0 */
flow_t *flow_create(flow_key_t key);

/* Atualiza as inforamções agregadas com as informações de um novo pacote */
void flow_update_stats(flow_t *flow, const pkt_stats_t *stats);

/* implementa a lógica de inserção/busca na tabela hash */
void flow_process_packet(sniffer_context_t *ctx, const pkt_stats_t *stats);

void flow_table_clear(flow_t **table_ptr);

void debug_print_flow_table(flow_t *table);

#endif
