#ifndef _STATS_H
#define _STATS_H

#include <stdint.h>
#include <sys/time.h>

typedef struct {
	/* ATENÇÃO: DADOS ESTÃO EM NETWORK ORDER */
	uint32_t src_ip;
	uint32_t dst_ip;
	uint16_t src_port;
	uint16_t dst_port;
	uint8_t protocol;
} flow_key_t;

typedef struct {
	flow_key_t key;

	/* métricas: */
	int payload_size;       /* apenas dados da camada de aplicação */
	struct timeval timestamp;
	uint8_t tcp_flags;
} pkt_stats_t;

#endif
