#ifndef RDMA_AGENT_H
#define RDMA_AGENT_H

#include "globals.h"
#include "common.h"
#include "connection.h"
#include "verbs.h"

static app_conn_cb_fn app_conn_event;
static app_disc_cb_fn app_disc_event;
static app_recv_cb_fn app_recv_event;

static pthread_t comm_thread;

extern int rdma_initialized;
extern char port[10];

void init_rdma_agent(char *listen_port, struct mr_context *regions,
		int region_count, uint16_t buffer_size,
		app_conn_cb_fn conn_callback,
		app_disc_cb_fn disc_callback,
		app_recv_cb_fn recv_callback);

void shutdown_rdma_agent();

int add_connection(char* ip, char *port, int app_type, int polling_loop, int flags); 

static void on_pre_conn(struct rdma_cm_id *id);
static void on_connection(struct rdma_cm_id *id);
static void on_disconnect(struct rdma_cm_id *id);
static void on_completion(struct ibv_wc *wc);

static void* client_loop();
static void* server_loop();

#endif
