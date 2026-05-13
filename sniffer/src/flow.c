#include "flow.h"
#include "parser.h"

#include <stdlib.h>
#include <stdio.h>
#include <netinet/tcp.h> /* macros TH_SYN, TH_ACK, etc. */
#include <netinet/in.h>  /* IPPROTO_TCP */
#include <arpa/inet.h>

flow_t *flow_create(flow_key_t key)
{
	flow_t *flow = (flow_t *)calloc(1, sizeof(flow_t));
	if (flow == NULL) {
		fprintf(stderr, "Erro ao alocar memória\n");
		exit(EXIT_FAILURE);
	}

	flow->key = key;

	return flow;
}

void flow_update_stats(flow_t *flow, const pkt_stats_t *stats)
{
	if (flow->packet_count == 0) {
		/* primeiro pacote do fluxo */
		flow->start_time = stats->timestamp;
		flow->min_packet_size = stats->payload_size;
		flow->max_packet_size = stats->payload_size;
		flow->total_iat_usec = 0.0;
	} else {
		if (stats->payload_size < flow->min_packet_size)
			flow->min_packet_size = stats->payload_size;
		if (stats->payload_size > flow->max_packet_size)
			flow->max_packet_size = stats->payload_size;

		/* Calcula IAT: diferença entre o pacote atual e o last_seen em microssegundos */
		double delta_usec = (stats->timestamp.tv_sec - flow->last_seen.tv_sec) * 1000000.0 +
		                    (stats->timestamp.tv_usec - flow->last_seen.tv_usec);
		flow->total_iat_usec += delta_usec;
	}

	/* Atualizações contínuas */
	flow->last_seen = stats->timestamp;
	flow->packet_count++;
	flow->byte_count += stats->payload_size;

	/* Extração categórica das flags TCP */
	if (stats->key.protocol == IPPROTO_TCP) {
		if (stats->tcp_flags & TH_SYN) flow->tcp_syn_count++;
		if (stats->tcp_flags & TH_FIN) flow->tcp_fin_count++;
		if (stats->tcp_flags & TH_RST) flow->tcp_rst_count++;
		if (stats->tcp_flags & TH_ACK) flow->tcp_ack_count++;
	}
}

void flow_process_packet(sniffer_context_t *ctx, const pkt_stats_t *stats)
{
	flow_t *flow_entry = NULL;
	flow_t **table_ptr = &ctx->tables[ctx->active_idx];
	HASH_FIND(hh, *table_ptr, &(stats->key), sizeof(flow_key_t), flow_entry);
	if (flow_entry == NULL) {
		/* miss na tabela. criação e inserção */
		flow_entry = flow_create(stats->key);

		if (ctx->net_mask != 0) {
			int src_is_local = (stats->key.src_ip & ctx->net_mask) == ctx->net_ip;
			int dst_is_local = (stats->key.dst_ip & ctx->net_mask) == ctx->net_ip;

			if (src_is_local && !dst_is_local) {
				flow_entry->direction = DIR_OUTBOUND;
			} else if (!src_is_local && dst_is_local) {
				flow_entry->direction = DIR_INBOUND;
			} else if (src_is_local && dst_is_local) {
				flow_entry->direction = DIR_LATERAL;
			} else {
				flow_entry->direction = DIR_UNKNOWN;
			}
		} else {
			flow_entry->direction = DIR_UNKNOWN;
		}

		HASH_ADD(hh, *table_ptr, key, sizeof(flow_key_t), flow_entry);
	}

	flow_update_stats(flow_entry, stats);
}

const char* get_dir_string(flow_direction_t dir)
{
	switch(dir) {
		case DIR_OUTBOUND: return "OUT";
		case DIR_INBOUND:  return "IN";
		case DIR_LATERAL:  return "LAT";
		default:	   return "UNK";
	}
}

void flow_table_clear(flow_t **table_ptr)
{
	flow_t *current_flow, *tmp;

	/* HASH_ITER é a macro segura para deletar enquanto itera */
	HASH_ITER(hh, *table_ptr, current_flow, tmp) {
		HASH_DEL(*table_ptr, current_flow);
		free(current_flow);
	}
	*table_ptr = NULL; /* Garante que a tabela volte ao estado inicial vazio */
}

void debug_print_flow_table(flow_t *table)
{
	flow_t *current_flow, *tmp;
	unsigned int num_flows = HASH_COUNT(table);

	char title[128];
	snprintf(title, sizeof(title), "DUMP DA TABELA DE FLUXOS (UTHASH) - Total de Fluxos Ativos: %u", num_flows);

	/* Linhas ajustadas para exatos 126 caracteres internos + 2 bordas laterais */
	printf("\n┌──────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┐\n");
	printf("│ %-124s │\n", title);
	printf("├────────────────────────────────────────────────┬─────┬───────┬────────┬──────────┬───────────┬──────────────┬────────────────┤\n");
	printf("│ %-46s │ %-3s │ %-5s │ %-6s │ %-8s │ %-9s │ %-12s │ %-14s │\n", 
		   "Fluxo (Origem -> Destino)", "Dir", "Proto", "Pkts", "Bytes", "Min/Max", "Mean IAT", "TCP (S/A/F/R)");
	printf("├────────────────────────────────────────────────┼─────┼───────┼────────┼──────────┼───────────┼──────────────┼────────────────┤\n");

	if (num_flows == 0) {
		printf("│ %-124s │\n", "TABELA VAZIA");
	}

	HASH_ITER(hh, table, current_flow, tmp) {
		char src_ip[INET_ADDRSTRLEN];
		char dst_ip[INET_ADDRSTRLEN];
		inet_ntop(AF_INET, &(current_flow->key.src_ip), src_ip, INET_ADDRSTRLEN);
		inet_ntop(AF_INET, &(current_flow->key.dst_ip), dst_ip, INET_ADDRSTRLEN);

		const char *proto_str = "OTHER";
		if (current_flow->key.protocol == IPPROTO_TCP) proto_str = "TCP";
		else if (current_flow->key.protocol == IPPROTO_UDP) proto_str = "UDP";

		double mean_iat = 0.0;
		if (current_flow->packet_count > 1) {
			mean_iat = current_flow->total_iat_usec / (current_flow->packet_count - 1);
		}

		char flow_str[64];
		snprintf(flow_str, sizeof(flow_str), "%s:%u -> %s:%u",
				 src_ip, ntohs(current_flow->key.src_port),
				 dst_ip, ntohs(current_flow->key.dst_port));

		char min_max_str[32];
		snprintf(min_max_str, sizeof(min_max_str), "%u/%u",
				 current_flow->min_packet_size, current_flow->max_packet_size);

		char iat_str[32];
		snprintf(iat_str, sizeof(iat_str), "%.1f us", mean_iat);

		char tcp_str[32] = "-";
		if (current_flow->key.protocol == IPPROTO_TCP) {
			snprintf(tcp_str, sizeof(tcp_str), "%u/%u/%u/%u",
					 current_flow->tcp_syn_count,
					 current_flow->tcp_ack_count,
					 current_flow->tcp_fin_count,
					 current_flow->tcp_rst_count);
		}

		printf("│ %-46s │ %-3s │ %-5s │ %-6u │ %-8u │ %-9s │ %-12s │ %-14s │\n",
			   flow_str,
			   get_dir_string(current_flow->direction),
			   proto_str,
			   current_flow->packet_count,
			   current_flow->byte_count,
			   min_max_str,
			   iat_str,
			   tcp_str);
	}
	printf("└────────────────────────────────────────────────┴─────┴───────┴────────┴──────────┴───────────┴──────────────┴────────────────┘\n");
}
