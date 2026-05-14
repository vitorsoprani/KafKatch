/*
 * main.c - Ponto de entrada do sniffer de microflows
 *
 * Autor: Vitor Soprani
 *
 * Responsável por inicializar a interface de captura (libpcap), instanciar
 * o contexto de double buffering e coordenar as threads de processamento
 * de rede e de exportação para o cluster Kafka.
 */

#include <stdio.h>
#include <stdlib.h>
#include <pcap/pcap.h>
#include <unistd.h>
#include <pthread.h>
#include <arpa/inet.h>
#include <signal.h>
#include <time.h>

#include "parser.h"
#include "flow.h"
#include "kafka_client.h"

#define LOG(fmt, ...) \
	fprintf(stderr, "[%s:%d] " fmt "\n", __FILE__, __LINE__, ##__VA_ARGS__)

#define CAPLEN			128
#define CAPBUFFER_SIZE		32000000 /* 32MB */

static pcap_t *global_pcap_handle = NULL;

void handle_sigint(int sig)
{
	LOG("[!] Sinal %d (Ctrl+C) recebido. Encerrando captura pacificamente...", sig);
	if (global_pcap_handle != NULL)
		pcap_breakloop(global_pcap_handle);
}

void *flush_worker(void *arg)
{
	sniffer_context_t *ctx = (sniffer_context_t *)arg;
	const char *topic_name = "network-microflows-raw";
	static uint32_t count_flushes = 0;
	struct timespec next_flush;
	flow_t *table_to_export;

	clock_gettime(CLOCK_MONOTONIC, &next_flush);

	while (ctx->is_running) {
		next_flush.tv_sec += 1;
		table_to_export = NULL;

		pthread_mutex_lock(&ctx->mutex);
		table_to_export = ctx->tables[ctx->active_idx];
		ctx->active_idx = !ctx->active_idx;
		ctx->tables[ctx->active_idx] = NULL;
		pthread_mutex_unlock(&ctx->mutex);

		printf("\n[FLUSH THREAD] Tick %d ", ++count_flushes);
		if (table_to_export != NULL) {
			printf("- Exportando fluxos capturados!\n");
			kafka_client_send_flows(ctx->kafka_producer, topic_name, &table_to_export);
		} else {
			printf("- Silêncio na rede (Tabela vazia).\n");
		}

		clock_nanosleep(CLOCK_MONOTONIC, TIMER_ABSTIME, &next_flush, NULL);
	}

	return NULL;
}

static void list_devs(void)
{
	char errbuf[PCAP_ERRBUF_SIZE];
	pcap_if_t *devs = NULL;
	pcap_if_t *curr_dev;

	if (pcap_findalldevs(&devs, errbuf) == PCAP_ERROR) {
		LOG("Erro ao listar devices: %s\n", errbuf);
		return;
	}

	printf("Interfaces disponiveis:\n");
	if (!devs)
		printf("Nenhuma interface disponível\n");

	for (curr_dev = devs; curr_dev != NULL; curr_dev = curr_dev->next)
		printf("%s\n", curr_dev->name);

	pcap_freealldevs(devs);
}

static pcap_t *setup_pcap(const char *iface)
{
	char errbuf[PCAP_ERRBUF_SIZE];
	pcap_t *pcap;
	int err;

	pcap = pcap_create(iface, errbuf);
	if (!pcap) {
		LOG("Erro ao criar o handler de captura: %s", errbuf);
		return NULL;
	}

	err = pcap_set_snaplen(pcap, CAPLEN);
	if (err)
		goto err_close;

	err = pcap_set_promisc(pcap, 1);
	if (err)
		goto err_close;

	err = pcap_set_immediate_mode(pcap, 0);
	if (err)
		goto err_close;

	err = pcap_set_timeout(pcap, 10);
	if (err)
		goto err_close;

	err = pcap_set_buffer_size(pcap, CAPBUFFER_SIZE);
	if (err)
		goto err_close;

	err = pcap_activate(pcap);
	if (err < 0) {
		LOG("Erro ao ativar a captura: %s", pcap_geterr(pcap));
		goto err_close;
	} else if (err > 0) {
		LOG("Aviso ao ativar a captura: %s", pcap_geterr(pcap));
	}

	if (pcap_datalink(pcap) != DLT_EN10MB) {
		LOG("Fatal: linklayer header fornecido não é suportado");
		goto err_close;
	}

	return pcap;

err_close:
	pcap_close(pcap);
	return NULL;
}

int main(int argc, char **argv)
{
	char errbuf[PCAP_ERRBUF_SIZE];
	uint32_t net_ip = 0;
	uint32_t net_mask = 0;
	rd_kafka_t *rk;
	sniffer_context_t ctx;
	pthread_t thread_id;
	struct pcap_stat stats;

	if (argc < 2) {
		LOG("Erro: Indique a interface a ser monitorada!");
		list_devs();
		return EXIT_FAILURE;
	}

	if (pcap_lookupnet(argv[1], &net_ip, &net_mask, errbuf) == -1) {
		LOG("Aviso: Não foi possível obter IP/Mascara: %s", errbuf);
		net_ip = 0;
		net_mask = 0;
	}

	rk = kafka_client_init("localhost:9092,localhost:9094,localhost:9096");
	if (!rk)
		return EXIT_FAILURE;

	ctx.tables[0] = NULL;
	ctx.tables[1] = NULL;
	ctx.active_idx = 0;
	ctx.net_ip = net_ip;
	ctx.net_mask = net_mask;
	ctx.is_running = 1;
	ctx.kafka_producer = rk;

	pthread_mutex_init(&ctx.mutex, NULL);

	if (pthread_create(&thread_id, NULL, flush_worker, &ctx) != 0) {
		LOG("Erro ao criar thread de flush");
		goto err_kafka;
	}

	if (pcap_init(PCAP_CHAR_ENC_LOCAL, errbuf) == PCAP_ERROR) {
		LOG("Erro ao iniciar libpcap: %s", errbuf);
		goto err_thread;
	}

	global_pcap_handle = setup_pcap(argv[1]);
	if (!global_pcap_handle)
		goto err_thread;

	signal(SIGINT, handle_sigint);

	printf("Captura iniciada: monitorando a interface %s\n", argv[1]);
	pcap_loop(global_pcap_handle, -1, pkt_handler, (u_char *)&ctx);

	if (pcap_stats(global_pcap_handle, &stats) >= 0) {
		printf("\n=== ESTATÍSTICAS DA CAPTURA ===\n");
		printf("Pacotes recebidos pelo filtro: %d\n", stats.ps_recv);
		printf("Pacotes dropados pelo kernel:  %d\n", stats.ps_drop);
		printf("Pacotes dropados pela placa:   %d\n", stats.ps_ifdrop);
		printf("===============================\n");
	}

	LOG("Captura finalizada. Iniciando encerramento gracioso...");

	ctx.is_running = 0;
	pthread_join(thread_id, NULL);
	pthread_mutex_destroy(&ctx.mutex);
	pcap_close(global_pcap_handle);

	kafka_client_cleanup(rk);
	rd_kafka_wait_destroyed(5000);

	LOG("Programa encerrado com sucesso.");
	return EXIT_SUCCESS;

err_thread:
	ctx.is_running = 0;
	pthread_join(thread_id, NULL);
err_kafka:
	pthread_mutex_destroy(&ctx.mutex);
	kafka_client_cleanup(rk);
	return EXIT_FAILURE;
}

