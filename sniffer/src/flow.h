/*
 * flow.h - Estruturas e protótipos para o gerenciamento de estado de fluxos
 *
 * Autor: Vitor Soprani
 *
 * Define o nó de fluxo usado na tabela hash e as funções de ciclo de vida
 * e atualização das métricas agregadas (microflows).
 */

#ifndef _FLOW_H
#define _FLOW_H

#include "stats.h"
#include "uthash.h"

#include <sys/time.h>

struct sniffer_context;
typedef struct sniffer_context sniffer_context_t;

/**
 * enum flow_direction_t - Direção do tráfego em relação à rede local
 * @DIR_OUTBOUND: Tráfego originado na rede local com destino externo
 * @DIR_INBOUND:  Tráfego originado externamente com destino à rede local
 * @DIR_LATERAL:  Tráfego interno (origem e destino na rede local)
 * @DIR_UNKNOWN:  Direção não identificada ou rede local não configurada
 */
typedef enum {
	DIR_OUTBOUND = 0,
	DIR_INBOUND  = 1,
	DIR_LATERAL  = 2,
	DIR_UNKNOWN  = 3
} flow_direction_t;

/**
 * struct flow_t - Nó da tabela hash representando um microflow ativo
 * @key:		Chave binária de busca e identificação
 * @direction:		Direção inferida do fluxo (In/Out/Lateral)
 * @packet_count:	Volume total de pacotes capturados
 * @byte_count:		Volume total de bytes de payload (camada de aplicação)
 * @min_packet_size:	Tamanho do menor payload observado
 * @max_packet_size:	Tamanho do maior payload observado
 * @start_time:		Timestamp de início do fluxo
 * @last_seen:		Timestamp do último pacote processado
 * @total_iat_usec:	Soma dos tempos entre chegadas de pacotes (IAT) em us
 * @tcp_syn_count:	Frequência da flag TCP SYN
 * @tcp_fin_count:	Frequência da flag TCP FIN
 * @tcp_rst_count:	Frequência da flag TCP RST
 * @tcp_ack_count:	Frequência da flag TCP ACK
 * @hh:			Metadados internos para o funcionamento da uthash
 */
typedef struct {
	flow_key_t		key;
	flow_direction_t	direction;

	/* Volume */
	uint32_t		packet_count;
	uint32_t		byte_count;
	uint16_t		min_packet_size;
	uint16_t		max_packet_size;

	/* Tempo */
	struct timeval		start_time;
	struct timeval		last_seen;
	double			total_iat_usec;

	/* Agregadores TCP */
	uint32_t		tcp_syn_count;
	uint32_t		tcp_fin_count;
	uint32_t		tcp_rst_count;
	uint32_t		tcp_ack_count;

	UT_hash_handle		hh;
} flow_t;

flow_t *flow_create(flow_key_t key);
void flow_update_stats(flow_t *flow, const pkt_stats_t *stats);
void flow_process_packet(sniffer_context_t *ctx, const pkt_stats_t *stats);
const char *get_dir_string(flow_direction_t dir);
void flow_table_clear(flow_t **table_ptr);
void debug_print_flow_table(flow_t *table);

#endif /* _FLOW_H */
