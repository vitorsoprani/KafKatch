#include "parser.h"
#include "stats.h"
#include "flow.h"

#include <net/ethernet.h>
#include <netinet/in.h>
#include <netinet/ip.h>
#include <netinet/tcp.h>
#include <netinet/udp.h>
#include <arpa/inet.h>
#include <stdio.h>
#include <time.h>
#include <string.h>

void debug_print_stats(const pkt_stats_t *stats)
{
	/* função feita pelo gemini */
	/* 1. Formatação de Tempo (Local) */
	struct tm *timeinfo;
	char time_str[20];
	timeinfo = localtime(&stats->timestamp.tv_sec);
	strftime(time_str, sizeof(time_str), "%H:%M:%S", timeinfo);

	/* 2. Conversão de IP (Seguro e limpo) */
	char src_ip[INET_ADDRSTRLEN];
	char dst_ip[INET_ADDRSTRLEN];
	inet_ntop(AF_INET, &(stats->key.src_ip), src_ip, INET_ADDRSTRLEN);
	inet_ntop(AF_INET, &(stats->key.dst_ip), dst_ip, INET_ADDRSTRLEN);

	/* 3. Identificação do Protocolo */
	const char *proto_str = "OTHER";
	if (stats->key.protocol == IPPROTO_TCP) proto_str = "TCP";
	else if (stats->key.protocol == IPPROTO_UDP) proto_str = "UDP";

	/* =========================================
	* PRINT FORMATADO PARA O TERMINAL (LARGURA FIXA: 66 CHARS)
	* ========================================= */
	printf("\n┌────────────────────────────────────────────────────────────────┐\n");

	/* Linha 1: Tempo e Payload travados no tamanho máximo previsto */
	printf("│ %s.%06ld | Proto: %-5s | Payload: %-5d bytes          │\n",
		time_str, stats->timestamp.tv_usec, proto_str, stats->payload_size);

	printf("├────────────────────────────────────────────────────────────────┤\n");

	/* Linha 2: IPs travados em 15 caracteres (%15s) e portas em 5 (%-5u) */
	printf("│ Flow: %15s:%-5u -> %15s:%-5u           │\n", 
		src_ip, ntohs(stats->key.src_port), 
		dst_ip, ntohs(stats->key.dst_port));

	/* Linha 3: Flags TCP dinâmicas, mas com preenchimento fixo */
	if (stats->key.protocol == IPPROTO_TCP) {
		char flags_str[32] = ""; // Buffer limpo para acumular as flags

		if (stats->tcp_flags & TH_SYN)
			strcat(flags_str, "SYN ");
		if (stats->tcp_flags & TH_ACK)
			strcat(flags_str, "ACK ");
		if (stats->tcp_flags & TH_FIN)
			strcat(flags_str, "FIN ");
		if (stats->tcp_flags & TH_RST)
			strcat(flags_str, "RST ");
		if (stats->tcp_flags & TH_PUSH)
			strcat(flags_str, "PSH ");
		if (stats->tcp_flags & TH_URG)
			strcat(flags_str, "URG ");

		/*
		 * %-24s garante que a string ocupará exatos 24 espaços,
		 * mesmo que tenha apenas "SYN ". O sinal de menos (-) alinha à
		 * esquerda.
		 */
		printf("│ TCP Flags: [%-24s] (0x%02x)                   │\n",
			flags_str, stats->tcp_flags);
	}

	printf("└────────────────────────────────────────────────────────────────┘\n");
}

/*
 * typedef void (*pcap_handler)(u_char *user, const struct pcap_pkthdr *h, const u_char *bytes);
 * u_char *user: ponteiro passado no argumento "user" em int pcap_loop(pcap_t *p, int cnt, pcap_handler callback, u_char *user);
 * struct pcap_pkthdr *h: header do pacote com timestamp e tamanhos;
 * u_char *bytes: pacote cru (truncado em CAPLEN bytes), começa com um cabeçalho no padrão retornado por pcap_datalink
 */
void pkt_handler(u_char *user, const struct pcap_pkthdr *h, const u_char *bytes)
{
	sniffer_context_t *ctx = (sniffer_context_t *)user;

	struct ether_header *ether_hdr = (struct ether_header *) bytes;
	uint16_t ether_type = ntohs(ether_hdr->ether_type);
	int offset = sizeof(struct ether_header);

	if (ether_type == ETHERTYPE_VLAN) {
		offset += 4;
		ether_type = ntohs(*((uint16_t *)(bytes + offset - 2)));
	}

	if (ether_type != ETHERTYPE_IP)
		return;
	if (h->caplen < offset + sizeof(struct ip))
		return;

	struct ip *ip_hdr = (struct ip *)(bytes + offset);
	if (ip_hdr->ip_v != 4) /*TODO: adicionar suporte a outros protocolos */
		return;

	/* multiplica por 4 para obter o valor em bytes */
	int ip_hdrlen = ip_hdr->ip_hl * 4;
	/* pega o tamanho total do pacote do ponto de vista da camada 3 */
	uint16_t ip_tot_len = ntohs(ip_hdr->ip_len);

	pkt_stats_t stats = {0};
	stats.timestamp = h->ts;

	/* Preenchendo os dados na chave binária (NETWORK ORDER) */
	stats.key.protocol = ip_hdr->ip_p;
	stats.key.src_ip = ip_hdr->ip_src.s_addr;
	stats.key.dst_ip = ip_hdr->ip_dst.s_addr;

	if (stats.key.protocol == IPPROTO_TCP) {
		if (h->caplen < offset + ip_hdrlen + sizeof(struct tcphdr))
			return;
		struct tcphdr *tcp_hdr = (struct tcphdr *)(bytes + offset + ip_hdrlen);

		stats.key.src_port = tcp_hdr->th_sport;
		stats.key.dst_port = tcp_hdr->th_dport;
		stats.tcp_flags = tcp_hdr->th_flags;

		/* Cálculo exato do payload TCP */
		int tcp_hdrlen = tcp_hdr->th_off * 4;
		stats.payload_size = ip_tot_len - ip_hdrlen - tcp_hdrlen;

	} else if (stats.key.protocol == IPPROTO_UDP) {
		if (h->caplen < offset + ip_hdrlen + sizeof(struct udphdr))
			return;
		struct udphdr *udp_hdr = (struct udphdr *)(bytes + offset + ip_hdrlen);

		stats.key.src_port = udp_hdr->uh_sport;
		stats.key.dst_port = udp_hdr->uh_dport;

		/* Cálculo do payload UDP (Cabeçalho UDP tem tamanho fixo); */
		stats.payload_size = ip_tot_len - ip_hdrlen - 8;
	} else {
		return; /* Ignora ICMP, etc */
	}

	/* prevenção contra pacotes corrompidos (underflow) */
	if (stats.payload_size < 0) {
		stats.payload_size = 0;
	}
	pthread_mutex_lock(&ctx->mutex);

	flow_process_packet(ctx, &stats);

	pthread_mutex_unlock(&ctx->mutex);
	flow_process_packet(ctx, &stats);

	//debug_print_stats(&stats);
}
