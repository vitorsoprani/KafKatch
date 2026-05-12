#include <net/ethernet.h>
#include <netinet/ether.h>	/* Protocolos padrão */
#include <netinet/in.h>		/* Protocolos padrão */
#include <netinet/ip.h>		/* Structs do IPv4 */
#include <netinet/tcp.h>	/* Structs do TCP */
#include <netinet/udp.h>	/* Structs do UDP */
#include <arpa/inet.h>		/* Funções de conversão de IP (inet_ntoa) */

#include <stdio.h>
#include <stdlib.h>
#include <pcap/pcap.h>

#define LOG(fmt, ...) \
	fprintf(stderr, "[%s:%d] " fmt "\n", __FILE__, __LINE__, ##__VA_ARGS__)

#define CAPLEN		128 /* Estamos interessados apenas nos (cabeçalhos). */
#define CAPBUFFER_SIZE	32000000 /* 32MB, evita que o kernel "drope" pacotes */


char errbuf[PCAP_ERRBUF_SIZE]; /* Mensagens de erro são escritas aqui */

void list_devs(void);

void pkt_handler(u_char *user, const struct pcap_pkthdr *h, const u_char *bytes);

int main(int argc, char **argv)
{
	if (argc < 2) {
		LOG("Erro: Indique a interface a ser monitorada!");
		return EXIT_FAILURE;
	}

	if (pcap_init(PCAP_CHAR_ENC_LOCAL, errbuf) == PCAP_ERROR) {
		LOG("Erro ao iniciar libpcap: %s\n", errbuf);
		return EXIT_FAILURE;
	}

	/* ===== CONFIGURANDO O HANDLER PARA A CAPTURA ===== */
	int err_code = 0;
	pcap_t *pcap = pcap_create(argv[1], errbuf);


	if (pcap == NULL) {
		LOG("Erro ao criar o handler de captura: %s\n", errbuf);
		return EXIT_FAILURE;
	}

	err_code = pcap_set_snaplen(pcap, CAPLEN);
	if (err_code != 0) {
		LOG("Erro ao configurar snaplen\n");
		pcap_close(pcap);
		return EXIT_FAILURE;
	}

	err_code = pcap_set_promisc(pcap, 1);
	if (err_code != 0) {
		LOG("Erro ao configurar a interface em modo promiscuo\n");
		pcap_close(pcap);
		return EXIT_FAILURE;
	}

	err_code = pcap_set_immediate_mode(pcap, 1);
	if (err_code != 0) {
		LOG("Erro ao configurar a captura em modo imediato\n");
		pcap_close(pcap);
		return EXIT_FAILURE;
	}

	err_code = pcap_set_buffer_size(pcap, CAPBUFFER_SIZE);
	if (err_code != 0) {
		LOG("Erro ao configurar tamanho do buffer da captura\n");
		pcap_close(pcap);
		return EXIT_FAILURE;
	}

	err_code = pcap_activate(pcap);
	if (err_code < 0) {
		LOG("Erro ao ativar a captura: %s\n", pcap_geterr(pcap));
		pcap_close(pcap);
		return EXIT_FAILURE;
	} else if (err_code > 0) {
		LOG("Aviso ao ativar a captura: %s\n", pcap_geterr(pcap));
	}


	/* ===== INICIANDO CAPTURA ===== */
	if (pcap_datalink(pcap) != DLT_EN10MB) {
		LOG("Fatal: linklayer header fornecido pela interface não é suportado");
		pcap_close(pcap);
		return EXIT_FAILURE;
	}
	printf("Captura iniciada: monitorando a interface %s\n", argv[1]);

	pcap_loop(pcap, -1, pkt_handler, NULL);


	pcap_close(pcap);
	return EXIT_SUCCESS;
}

void list_devs(void)
{
	pcap_if_t *devs = NULL;

	if (pcap_findalldevs(&devs, errbuf) == PCAP_ERROR) {
		LOG("Erro ao listar devices: %s\n", errbuf);
		return;
	}

	printf("Interfaces dispiniveis para o programa com suas permissoes:\n");

	if (devs == NULL) {
		printf("Nenhuma interface disponível");
	}

	pcap_if_t *curr_dev = devs;
	while (curr_dev != NULL) {
		printf("%s\n", curr_dev->name);
		curr_dev = curr_dev->next;
	}

	pcap_freealldevs(devs);
}

/*
 * typedef void (*pcap_handler)(u_char *user, const struct pcap_pkthdr *h, const u_char *bytes);
 * u_char *user: ponteiro passado no argumento "user" em int pcap_loop(pcap_t *p, int cnt, pcap_handler callback, u_char *user);
 * struct pcap_pkthdr *h: header do pacote com timestamp e tamanhos;
 * u_char *bytes: pacote cru (truncado em CAPLEN bytes), começa com um cabeçalho no padrão retornado por pcap_datalink
 */
void pkt_handler(u_char *user, const struct pcap_pkthdr *h, const u_char *bytes)
{
	struct ether_header *ether_hdr = (struct ether_header *) bytes;
	uint16_t ether_type = ntohs(ether_hdr->ether_type); /* convert endianness */
	int offset = sizeof(struct ether_header);

/*
	printf("================\n");
	printf("pacote detectado\n"
	       "\tts: %ld\n"
	       "\tlen: %d\n",
	       h->ts.tv_sec, h->len);

	printf("src_mac: %s\n",
		ether_ntoa((struct ether_addr *)ether_hdr->ether_shost));
	printf("dst_mac: %s\n",
		ether_ntoa((struct ether_addr *)ether_hdr->ether_dhost));
*/

	/* Verifica se há tag vlan (4 bytes extras no header) */
	if (ether_type == ETHERTYPE_VLAN) {
		offset += 4;

		/* Agora lê o veradeiro tipo (ultimos 2 bytes do header) */
		ether_type = ntohs(*((uint16_t *)(bytes + offset - 2)));
	}

	if (ether_type != ETHERTYPE_IP) {
		/*
		 * Ignora pacotes que não são ip.
		 * TODO: implementar suporte a outros protocolos:
		 *	arp, ipv6, etc... [?]
		 */
		return;
	}

	struct ip *ip_hdr = (struct ip *)(bytes + offset);

	if (ip_hdr->ip_v != 4)
		return;

	/* o tamanho é dinamico. multiplicado por 4 para ter em bytes */
	int ip_hdrlen = ip_hdr->ip_hl * 4;

	uint32_t src_ip = ntohs(ip_hdr->ip_src.s_addr);
	uint32_t dst_ip = ntohs(ip_hdr->ip_dst.s_addr);
	uint8_t protocol = ip_hdr->ip_p;

	uint16_t src_port = 0;
	uint16_t dst_port = 0;
	uint8_t tcp_flags = 0;

	if (protocol == IPPROTO_TCP) {
		if (h->caplen < offset + ip_hdrlen + sizeof(struct tcphdr))
			return;
		struct tcphdr *tcp_hdr = (struct tcphdr *)(bytes + offset + ip_hdrlen);

		src_port = ntohs(tcp_hdr->th_sport);
		dst_port = ntohs(tcp_hdr->th_dport);
		tcp_flags = tcp_hdr->th_flags;

	} else if (protocol == IPPROTO_UDP) {
		if (h->caplen < offset + ip_hdrlen + sizeof(struct udphdr))
			return;

		struct udphdr *udp_hdr = (struct udphdr *)(bytes + offset + ip_hdrlen);

		src_port = ntohs(udp_hdr->uh_sport);
		dst_port = ntohs(udp_hdr->uh_dport);
	} else {
		/* Ignora ICMP, IGMP, etc. se o foco for apenas fluxos L4 */
		return;
	}

	char src_ip_str[INET_ADDRSTRLEN];
	char dst_ip_str[INET_ADDRSTRLEN];
	inet_ntop(AF_INET, &(ip_hdr->ip_src), src_ip_str, INET_ADDRSTRLEN);
	inet_ntop(AF_INET, &(ip_hdr->ip_dst), dst_ip_str, INET_ADDRSTRLEN);

	printf("================\n");
	printf("Fluxo Detectado: %s:%d -> %s:%d (Proto: %d)\n",
		src_ip_str, src_port, dst_ip_str, dst_port, protocol);
	printf("Bytes reais na rede: %d\n", h->len);

	if (protocol == IPPROTO_TCP) {
		printf("Flags TCP: [SYN:%d ACK:%d FIN:%d RST:%d]\n",
			(tcp_flags & TH_SYN) ? 1 : 0,
			(tcp_flags & TH_ACK) ? 1 : 0,
			(tcp_flags & TH_FIN) ? 1 : 0,
			(tcp_flags & TH_RST) ? 1 : 0);
	}
}
