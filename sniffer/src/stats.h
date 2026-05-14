/*
 * stats.h - Estruturas de dados para chaves de fluxo e métricas de pacotes
 *
 * Autor: Vitor Soprani
 *
 * Define as tuplas de identificação de fluxos e os contentores de estatísticas
 * intermediárias usados entre o parser e o motor de agregação.
 */

#ifndef _STATS_H
#define _STATS_H

#include <stdint.h>
#include <sys/time.h>

/**
 * struct flow_key_t - Tupla de 5 elementos que identifica um fluxo único
 * @src_ip:   Endereço IP de origem (Network Byte Order)
 * @dst_ip:   Endereço IP de destino (Network Byte Order)
 * @src_port: Porta de transporte de origem (Network Byte Order)
 * @dst_port: Porta de transporte de destino (Network Byte Order)
 * @protocol: Protocolo IP (ex: IPPROTO_TCP, IPPROTO_UDP)
 *
 * Nota: Todos os campos são mantidos em Network Byte Order para otimizar
 * a performance da busca na tabela hash, evitando conversões desnecessárias.
 */
typedef struct {
	uint32_t	src_ip;
	uint32_t	dst_ip;
	uint16_t	src_port;
	uint16_t	dst_port;
	uint8_t		protocol;
} flow_key_t;

/**
 * struct pkt_stats_t - Estatísticas extraídas de um único pacote
 * @key:	  Chave de identificação do fluxo associado
 * @payload_size: Tamanho dos dados (exclui cabeçalhos L2/L3/L4)
 * @timestamp:	  Momento exato da captura (fornecido pelo libpcap)
 * @tcp_flags:	  Flags do cabeçalho TCP (apenas para pacotes TCP)
 */
typedef struct {
	flow_key_t	key;
	int		payload_size;
	struct timeval	timestamp;
	uint8_t		tcp_flags;
} pkt_stats_t;

#endif /* _STATS_H */
