#include <stdio.h>
#include <stdlib.h>
#include <pcap/pcap.h>
#include <unistd.h>
#include <pthread.h>

#include "parser.h"
#include "flow.h"

#define LOG(fmt, ...) \
	fprintf(stderr, "[%s:%d] " fmt "\n", __FILE__, __LINE__, ##__VA_ARGS__)

#define CAPLEN		128 /* Estamos interessados apenas nos (cabeçalhos). */
#define CAPBUFFER_SIZE	32000000 /* 32MB, evita que o kernel "drope" pacotes */

char errbuf[PCAP_ERRBUF_SIZE]; /* Mensagens de erro são escritas aqui */

void *flush_worker(void *arg) {
	sniffer_context_t *ctx = (sniffer_context_t *)arg;

	while (ctx->is_running) {
		sleep(1); /* Janela de 1 segundo (tempo de host) */

		flow_t *table_to_export = NULL;

		/* SEÇÃO CRÍTICA: Apenas trocamos os ponteiros.
		 * O lock dura nanossegundos, não afeta a captura. */
		pthread_mutex_lock(&ctx->mutex);

		table_to_export = ctx->tables[ctx->active_idx]; // Pega a tabela cheia
		ctx->active_idx = !ctx->active_idx;			 // Inverte o índice (0->1 ou 1->0)
		ctx->tables[ctx->active_idx] = NULL;			// Prepara a próxima tabela limpa

		pthread_mutex_unlock(&ctx->mutex);

		/* PROCESSAMENTO OFFLINE: Agora podemos imprimir/enviar sem travar o pcap */
		if (table_to_export != NULL) {
			printf("\n[FLUSH THREAD] Processando janela de tempo terminada...\n");
			debug_print_flow_table(table_to_export);

			/* TODO: Enviar para Kafka aqui */

			/* LIMPEZA: Fundamental para o próximo ciclo */
			flow_table_clear(&table_to_export);
		}
	}
	return NULL;
}

void list_devs(void)
{
	pcap_if_t *devs = NULL;

	if (pcap_findalldevs(&devs, errbuf) == PCAP_ERROR) {
		LOG("Erro ao listar devices: %s\n", errbuf);
		return;
	}

	printf("Interfaces dispiniveis:\n");
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

int main(int argc, char **argv)
{
	if (argc < 2) {
		LOG("Erro: Indique a interface a ser monitorada!");
		return EXIT_FAILURE;
	}
	char errbuf[PCAP_ERRBUF_SIZE];
	uint32_t net_ip, net_mask;

	/* Extrai a rede e a máscara da interface Wi-Fi (ex: wlxe894...) */
	if (pcap_lookupnet(argv[1], &net_ip, &net_mask, errbuf) == -1) {
		LOG("Aviso: Não foi possível obter IP/Mascara para %s: %s", argv[1], errbuf);
		net_ip = 0;
		net_mask = 0;
	}

	/* Instancia o contexto com a tabela vazia e os dados da rede */
	sniffer_context_t ctx = {
		.tables = {NULL, NULL},
		.active_idx = 0,
		.net_ip = net_ip,
		.net_mask = net_mask,
		.is_running = 1
	};
	pthread_mutex_init(&ctx.mutex, NULL);

	pthread_t thread_id;
	if (pthread_create(&thread_id, NULL, flush_worker, &ctx) != 0) {
		LOG("Erro ao criar thread de flush");
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
		LOG("Fatal: linklayer header fornecido não é suportado (Ethernet requerido)");
		pcap_close(pcap);
		return EXIT_FAILURE;
	}

	printf("Captura iniciada: monitorando a interface %s\n", argv[1]);

	pcap_loop(pcap, 1000, pkt_handler, (u_char *)&ctx);

	ctx.is_running = 0;
	pthread_join(thread_id, NULL);
	pthread_mutex_destroy(&ctx.mutex);
	pcap_close(pcap);
	return EXIT_SUCCESS;
}

