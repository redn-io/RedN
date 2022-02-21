#include <sys/syscall.h>
#include <pthread.h>
#include <stdatomic.h>

#include "verbs.h"
#include "messaging.h"
#include "mr.h"
#include "agent.h"

int rdma_initialized = 0;
char port[10];

//initialize memory region information
void init_rdma_agent(char *listen_port, struct mr_context *regions,
		int region_count, uint16_t buffer_size,
		app_conn_cb_fn app_connect,
		app_disc_cb_fn app_disconnect,
		app_recv_cb_fn app_receive)
{
	//pthread_mutex_lock(&global_mutex);
	if(rdma_initialized)
		return;

	if(region_count > MAX_MR)
		rc_die("region count is greater than MAX_MR");

	mrs = regions;
	num_mrs = region_count;
	msg_size = buffer_size;

	app_conn_event = app_connect;
	app_disc_event = app_disconnect;
	app_recv_event = app_receive;

	set_seed(5);

	if(listen_port)
		snprintf(port, sizeof(port), "%s", listen_port);

#if 0
	cq_lock = (pthread_mutex_t *)malloc(sizeof(pthread_mutex_t));
	pthread_mutexattr_init(&attr);
	pthread_mutexattr_setpshared(&attr, PTHREAD_PROCESS_SHARED);
	pthread_mutex_init(cq_lock, &attr);
	wr_lock = (pthread_mutex_t *)malloc(sizeof(pthread_mutex_t));
	pthread_mutexattr_init(&attr);
	pthread_mutexattr_setpshared(&attr, PTHREAD_PROCESS_SHARED);
	pthread_mutex_init(wr_lock, &attr);
	wr_completed = (pthread_cond_t *)malloc(sizeof(pthread_cond_t));
#endif

	rc_init(on_pre_conn,
		on_connection,
		on_completion,
		on_disconnect);

	ec = rdma_create_event_channel();

	if(!listen_port)
		pthread_create(&comm_thread, NULL, client_loop, NULL);
	else
		pthread_create(&comm_thread, NULL, server_loop, port);

	rdma_initialized = 1;
}

void shutdown_rdma_agent()
{
#if 0
	void *ret;
	int sockfd = -1;

	int n = rc_connection_count();

	for(int i=0; i<n; i++) {
		sockfd = rc_next_connection(sockfd);
		if(rc_terminated(sockfd) != RC_CONNECTION_TERMINATED)
			rc_disconnect(get_connection(sockfd));
	}

   	//if(pthread_join(comm_thread, &ret) != 0)
	//	rc_die("pthread_join() error");
#endif
}

static void* client_loop()
{
	rdma_event_loop(ec, 0, 1); /* exit upon disconnect */
	rdma_destroy_event_channel(ec);
	debug_print("exiting rc_client_loop\n");
	return NULL;
}

static void* server_loop(void *port)
{
	struct sockaddr_in6 addr;
	struct rdma_cm_id *cm_id = NULL;

	memset(&addr, 0, sizeof(addr));
	addr.sin6_family = AF_INET6;
	addr.sin6_port = htons(atoi(port));

	rdma_create_id(ec, &cm_id, NULL, RDMA_PS_TCP);
	rdma_bind_addr(cm_id, (struct sockaddr *)&addr);
	rdma_listen(cm_id, 100); /* backlog=10 is arbitrary */

	printf("[RDMA-Server] Listening on port %d for connections. interrupt (^C) to exit.\n", atoi(port));

	rdma_event_loop(ec, 0, 0); /* do not exit upon disconnect */

	rdma_destroy_id(cm_id);
	rdma_destroy_event_channel(ec);

	debug_print("exiting rc_server_loop\n");

	return 0;
}

//request connection to another RDMA agent (non-blocking)
//returns socket descriptor if successful, otherwise -1
int add_connection(char* ip, char *port, int app_type, int polling_loop, int flags) 
{
	debug_print("attempting to add connection to %s:%s\n", ip, port);

	if(!rdma_initialized)
		rc_die("can't add connection; client must be initialized first\n");

	struct addrinfo *addr;
	struct rdma_cm_id *cm_id = NULL;

	getaddrinfo(ip, port, NULL, &addr);

	rdma_create_id(ec, &cm_id, NULL, RDMA_PS_TCP);
	rdma_resolve_addr(cm_id, NULL, addr->ai_addr, TIMEOUT_IN_MS);

	freeaddrinfo(addr);

	int sockfd = init_connection(cm_id, app_type, polling_loop, flags);

	printf("[RDMA-Client] Creating connection (status:pending) to %s:%s on sockfd %d\n", ip, port, sockfd);

	return sockfd;
}

#if 0
struct sockaddr create_connection(char* ip, uint16_t port)
{
	struct addrinfo *addr;
	getaddrinfo(host, port, NULL, &addr);

	rdma_create_id(ec, &cm_id, NULL, RDMA_PS_TCP);
	rdma_resolve_addr(cm_id, NULL, addr->ai_addr, TIMEOUT_IN_MS);
	return addr;
}
#endif

static void on_pre_conn(struct rdma_cm_id *id)
{
	struct conn_context *ctx = (struct conn_context *)id->context;

	// if no MRs provided, trigger registeration callback
	//if(mrs == NULL)
	mr_register(ctx, mrs, num_mrs, msg_size);

	//for(int i=0; i<MAX_BUFFER; i++)
	//	receive_message(id, 0);
}

static void on_connection(struct rdma_cm_id *id)
{
	struct conn_context *ctx = (struct conn_context *)id->context;

	printf("Connection established [sockfd:%d] [qpnum: %d]\n", ctx->sockfd, id->qp->qp_num);

	app_conn_event(ctx->sockfd);

	//sleep(1);
#if 0
	if(!ibw_cmpxchg(&ctx->mr_init_sent, 0, 1)) {
		int i = create_message(id, MSG_INIT, num_mrs);
		printf("SEND --> MSG_INIT [advertising %d memory regions]\n", num_mrs);
		send_message(id, i);
	}
#endif
}

static void on_disconnect(struct rdma_cm_id *id)
{
	struct conn_context *ctx = (struct conn_context *)id->context;
	app_disc_event(ctx->sockfd);
	printf("Connection terminated [sockfd:%d]\n", ctx->sockfd);
	//free(ctx);
}

#if 1
static void on_completion(struct ibv_wc *wc)
{
	struct rdma_cm_id *id = find_connection_by_wc(wc);
	struct conn_context *ctx = (struct conn_context *)id->context;

	if (wc->opcode & IBV_WC_RECV) {
		uint32_t rcv_i = wc->wr_id;

		//if(wc->opcode == IBV_WC_RECV_RDMA_WITH_IMM || wc->wr_id == 99) {
	       if(rc_ready(ctx->sockfd)) {	
			uint32_t app_id = ntohl(wc->imm_data);
			debug_print("application callback: seqn = %u\n", app_id);
			//if(app_id) {
				update_pending(id, app_id);
				struct app_context imm_msg;
				imm_msg.id = app_id;
				imm_msg.sockfd = ctx->sockfd;
				imm_msg.data = 0;
				app_recv_event(&imm_msg);
				return;
			//}
		}
	       else
		       rc_die("invalid message\n");
	}
	else { 
		debug_print("skipping message with opcode:%i, wr_id:%lu \n", wc->opcode, wc->wr_id);
		return;
	}
}
#else
static void on_completion(struct ibv_wc *wc)
{
	struct rdma_cm_id *id = find_connection_by_wc(wc);
	struct conn_context *ctx = (struct conn_context *)id->context;

	if (wc->opcode & IBV_WC_RECV) {
		uint32_t rcv_i = wc->wr_id;

		//if(wc->opcode == IBV_WC_RECV_RDMA_WITH_IMM || wc->wr_id == 99) {
	       if(rc_ready(ctx->sockfd)) {	
			uint32_t app_id = ntohl(wc->imm_data);
			debug_print("application callback: seqn = %u\n", app_id);
			//if(app_id) {
				update_pending(id, app_id);
				struct app_context imm_msg;
				imm_msg.id = app_id;
				imm_msg.sockfd = ctx->sockfd;
				imm_msg.data = 0;
				app_recv_event(&imm_msg);
				return;
			//}
		}
#if 0
		else if (ctx->msg_rcv[rcv_i]->id == MSG_INVALID) {
			//FIXME: ignore for now
			printf("Ignoring undefined receive completion event\n");
			return;
		}
#endif
		
		if(ctx->msg_rcv[rcv_i] == NULL)
			rc_die("invalid message buffer\n");

		if (ctx->msg_rcv[rcv_i]->id == MSG_INIT) {
			pthread_spin_lock(&ctx->init_lock);
			if(ctx->app_type < 0)
				ctx->app_type = ctx->msg_rcv[rcv_i]->meta.mr.addr;
			ctx->remote_mr_total = ctx->msg_rcv[rcv_i]->meta.mr.length;
			ctx->mr_init_recv = 1;
			printf("RECV <-- MSG_INIT [remote node advertises %d memory regions]\n",
						ctx->remote_mr_total);

			if(!ibw_cmpxchg(&ctx->mr_init_sent, 0, 1)) {
				int send_i = create_message(id, MSG_INIT, num_mrs);
				printf("SEND --> MSG_INIT [advertising %d memory regions]\n", num_mrs);
				send_message(id, send_i);
	
			}

			if(!mr_all_recv(ctx))
				receive_message(id, rcv_i);
			

#if 0
			//if all local memory region keys haven't yet been synced, send the next
			if(!mr_all_sent(ctx)) {	
				int send_i = create_message(id, MSG_MR, 0);
				send_message(id, send_i);
				printf("%s", "SEND --> MSG_MR\n");
			}
			else {
				debug_print("finished syncing local MRs\n");
			}
#endif

			// FIXME remove. this is only for testing
			// pre-post 100 imm receives
			//for(int i=0 ; i<100; i++)
			//	receive_imm(id, 0);

			pthread_spin_unlock(&ctx->init_lock);

		}
		else if (ctx->msg_rcv[rcv_i]->id == MSG_MR) {
			pthread_spin_lock(&ctx->init_lock);

			int idx = ctx->msg_rcv[rcv_i]->meta.mr.type;
			printf("%s", "RECV <-- MSG_MR\n");
			if(idx > MAX_MR-1)
				rc_die("memory region number outside of MAX_MR");
			ctx->remote_mr[idx] = (struct mr_context *)calloc(1, sizeof(struct mr_context));
			ctx->remote_mr[idx]->type = idx;
			ctx->remote_mr[idx]->addr = ctx->msg_rcv[rcv_i]->meta.mr.addr;
			ctx->remote_mr[idx]->length = ctx->msg_rcv[rcv_i]->meta.mr.length;
			ctx->remote_mr[idx]->rkey = ctx->msg_rcv[rcv_i]->meta.mr.rkey;
			ctx->remote_mr_ready[idx] = 1;

			if(mr_all_synced(ctx)) {
				debug_print("[DEBUG] RECV COMPL - ALL SYNCED: buffer %d\n", rcv_i);
				rc_set_state(id, RC_CONNECTION_READY);
				app_conn_event(ctx->sockfd);
			}

			if(!mr_all_recv(ctx))
				receive_message(id, rcv_i);

			//send an ACK
			//create_message(id, MSG_MR_ACK);
			//send_message(id);
			pthread_spin_unlock(&ctx->init_lock);
		}
		else if (ctx->msg_rcv[rcv_i]->id == MSG_READY) {
			//do nothing
		}
		else if (ctx->msg_rcv[rcv_i]->id == MSG_DONE) {
			rc_disconnect(id);
			return;
		}
		else if (ctx->msg_rcv[rcv_i]->id == MSG_CUSTOM) {
			ctx->msg_rcv[rcv_i]->meta.app.sockfd = ctx->sockfd;

			//adding convenience pointers to data blocks
			ctx->msg_rcv[rcv_i]->meta.app.data = ctx->msg_rcv[rcv_i]->data;

			uint32_t app_id = ctx->msg_rcv[rcv_i]->meta.app.id; 

			debug_print("application callback: seqn = %u\n", app_id);
			
			//debug_print("trigger application callback\n");
			app_recv_event(&ctx->msg_rcv[rcv_i]->meta.app);

			//only trigger delivery notifications for app_ids greater than 0
			if(app_id) {
 				update_pending(id, app_id);
			}	
		}
		else {
			printf("invalid completion event with undefined id (%i) \n", ctx->msg_rcv[rcv_i]->id);
			rc_die("");
		}

		//FIXME: uncomment to enable RPC functionality
		//receive_message(id, rcv_i);
#if 0
		if(ctx->poll_enable)
			receive_message(id, rcv_i);
		else
			receive_imm(id, rcv_i);
#endif
	}
	else if(wc->opcode == IBV_WC_RDMA_WRITE) {
	}
	// FIXME temporary workaround; we currently avoid using library's RPC buffers after initialization
	//else if(wc->opcode == IBV_WC_SEND && wc->wr_id != 99) {
	else if(wc->opcode == IBV_WC_SEND && !rc_ready(ctx->sockfd)) {
		pthread_spin_lock(&ctx->init_lock);
		int i = rc_release_buffer(ctx->sockfd, wc->wr_id);
		if (ctx->msg_send[i]->id == MSG_INIT || ctx->msg_send[i]->id == MSG_MR) {
			//printf("received MSG_MR_ACK\n");
			if(ctx->msg_send[i]->id == MSG_MR) {
				int idx = ctx->msg_send[i]->meta.mr.type;
				ctx->local_mr_sent[idx] = 1;
			}

			//if all local memory region keys haven't yet been synced, send the next
			if(!mr_all_sent(ctx)) {	
				int j = create_message(id, MSG_MR, 0);
				send_message(id, j);
				printf("%s", "SEND --> MSG_MR\n");
			}
			else if(mr_all_synced(ctx)) {
					debug_print("[DEBUG] SEND COMPL - ALL SYNCED: wr_id %lu buffer %d\n", wc->wr_id, i);
					rc_set_state(id, RC_CONNECTION_READY);
					app_conn_event(ctx->sockfd);
			}
		}
		pthread_spin_unlock(&ctx->init_lock);
	}
	else { 
		debug_print("skipping message with opcode:%i, wr_id:%lu \n", wc->opcode, wc->wr_id);
		return;
	}
}
#endif
