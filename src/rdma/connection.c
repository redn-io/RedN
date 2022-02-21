#include "mlx5_intf.h"
#include "connection.h"

#define MELLANOX_EXPERIMENTAL_VERBS

//event channel. used to setup rdma RCs and communicate mr keys
struct rdma_event_channel *ec = NULL;

//local memory regions (for rdma reads/writes)
int num_mrs;
struct mr_context *mrs = NULL;

int msg_size; //msg buffer size

//XXX: cc
int global_sq_sz = MAX_SEND_QUEUE_SIZE;

pthread_mutexattr_t attr;
pthread_mutex_t cq_lock;

const int TIMEOUT_IN_MS = 500;
const char* DEFAULT_PORT = "12345";

//static uint32_t cqe_archive[ARCHIVE_SIZE] = {0};
static struct context *s_ctx = NULL;
static int *s_conn_bitmap = NULL;
static struct rdma_cm_id **s_conn_ids = NULL;
static pre_conn_cb_fn s_on_pre_conn_cb = NULL;
static connect_cb_fn s_on_connect_cb = NULL;
static completion_cb_fn s_on_completion_cb = NULL;
static disconnect_cb_fn s_on_disconnect_cb = NULL;

int exit_rc_loop = 0;

extern inline void set_seed(int seed);
extern inline int fastrand(int seed);
extern inline int cmp_counters(uint32_t a, uint32_t b);
extern inline int diff_counters(uint32_t a, uint32_t b);
extern inline int find_first_empty_bit_and_set(int bitmap[], int n);
extern inline int find_first_empty_bit(int bitmap[], int n);
extern inline int find_next_empty_bit(int idx, int bitmap[], int n);
extern inline int find_first_set_bit_and_empty(int bitmap[], int n);
extern inline int find_first_set_bit(int bitmap[], int n);
extern inline int find_next_set_bit(int idx, int bitmap[], int n);
extern inline int find_bitmap_weight(int bitmap[], int n);
extern inline struct sockaddr_in * copy_ipv4_sockaddr(struct sockaddr_storage *in);

__attribute__((visibility ("hidden"))) 
int init_connection(struct rdma_cm_id *id, int type, int always_poll, int flags)
{
	int sockfd = find_first_empty_bit_and_set(s_conn_bitmap, MAX_CONNECTIONS);

	if(sockfd < 0)
		rc_die("can't open new connection; number of open sockets == MAX_CONNECTIONS");

	debug_print("adding connection on socket #%d\n", sockfd);

	struct conn_context *ctx = (struct conn_context *)calloc(1, sizeof(struct conn_context));
	ctx->local_mr = (struct ibv_mr **)calloc(MAX_MR, sizeof(struct ibv_mr*));
	ctx->remote_mr = (struct mr_context **)calloc(MAX_MR, sizeof(struct mr_context*));
	ctx->remote_mr_ready = (int *)calloc(MAX_MR, sizeof(int));
	ctx->local_mr_ready = (int *)calloc(MAX_MR, sizeof(int));
	ctx->local_mr_sent = (int *)calloc(MAX_MR, sizeof(int));
	ctx->msg_send_mr = (struct ibv_mr **)calloc(MAX_BUFFER, sizeof(struct ibv_mr*));
	ctx->msg_rcv_mr = (struct ibv_mr **)calloc(MAX_BUFFER, sizeof(struct ibv_mr*));
	ctx->msg_send = (struct message **)calloc(MAX_BUFFER, sizeof(struct message*));
	ctx->msg_rcv = (struct message **)calloc(MAX_BUFFER, sizeof(struct message*));
	ctx->send_slots = (int *)calloc(MAX_BUFFER, sizeof(int));
	ctx->pendings = NULL;
	ctx->buffer_bindings = NULL;
	ctx->poll_permission = 1;
	ctx->poll_always = always_poll;
	ctx->poll_enable = 1;
	ctx->flags = flags;

	id->context = ctx;
	ctx->id = id;

	pthread_mutex_init(&ctx->wr_lock, NULL);
	pthread_cond_init(&ctx->wr_completed, NULL);
	pthread_spin_init(&ctx->post_lock, PTHREAD_PROCESS_PRIVATE);
	pthread_spin_init(&ctx->buffer_lock, PTHREAD_PROCESS_PRIVATE);
	pthread_spin_init(&ctx->init_lock, PTHREAD_PROCESS_PRIVATE);
	pthread_spin_init(&ctx->bf_lock, PTHREAD_PROCESS_PRIVATE);

	s_conn_bitmap[sockfd] = 1;
	ctx->sockfd = sockfd;
	ctx->app_type = type;
	s_conn_ids[sockfd] = id;

	return sockfd;
}

__attribute__((visibility ("hidden"))) 
int setup_connection(struct rdma_cm_id * id, struct rdma_conn_param * cm_params)
{
#ifdef EXP_VERBS
	//FIXME: cc
	struct ibv_exp_qp_init_attr qp_attr;
#else
	struct ibv_qp_init_attr qp_attr;
#endif

	struct conn_context *ctx = (struct conn_context *)id->context;

	struct rc_metadata *rc_meta = NULL;

	try_build_device(id);

#if 1	
	if(cm_params)
		rc_meta = (struct rc_metadata *) cm_params->private_data;

	// modify connection parameters using metadata exchanged between client and server
	if(rc_meta) {
		debug_print("private data %p (len: given %d expected %lu)\n",
				rc_meta, cm_params->private_data_len,
				sizeof(struct rc_metadata) + MAX_MR * sizeof(struct ibv_mr));

		if(sizeof(struct rc_metadata) > cm_params->private_data_len)	
			rc_die("invalid connection param length");
		
		ctx->flags = rc_meta->flags;
	        ctx->app_type = rc_meta->type;

		mr_remote_update(ctx, rc_meta->addr, rc_meta->length, rc_meta->rkey, rc_meta->mr_count);
	}

#endif

	// build send, receive, and completion queues
	build_cq_channel(id);

	build_qp_attr(id, &qp_attr);

#ifdef EXP_VERBS
	if(rdma_create_qp_exp(id, &qp_attr))
		rc_die("queue pair creation failed");
#else
	if(rdma_create_qp(id, rc_get_pd(id), &qp_attr))
		rc_die("queue pair creation failed");
#endif

	struct mlx5dv_obj dv_obj = {};
	memset((void *)&dv_obj, 0, sizeof(struct mlx5dv_obj));
	ctx->iqp = (struct mlx5dv_qp *)malloc(sizeof(struct mlx5dv_qp));

	dv_obj.qp.in = id->qp;
	dv_obj.qp.out = ctx->iqp;

 	int ret = mlx5dv_init_obj(&dv_obj, MLX5DV_OBJ_QP);

	ctx->sq_wrid = calloc(ctx->iqp->sq.wqe_cnt, sizeof(uint64_t));

	if(qp_attr.sq_sig_all)
		ctx->sq_signal_bits = MLX5_WQE_CTRL_CQ_UPDATE;
	else
		ctx->sq_signal_bits = 0;

#if 0
	struct ibv_exp_qp_attr attr;
	memset(&attr, 0, sizeof(attr));
	attr.max_dest_rd_atomic	= 2;
	attr.max_rd_atomic = 2;
	attr.qp_state = IBV_QPS_SQD; 
#if 0
	if (ibv_exp_modify_qp(id->qp, &attr,
		IBV_QP_STATE)) {
		//IBV_QP_MAX_DEST_RD_ATOMIC)) {
		rc_die("ibv_modify_qp1() failed");
		return 1;
	}
#endif

	if (ibv_exp_modify_qp(id->qp, &attr,
		IBV_QP_MAX_QP_RD_ATOMIC)) {
		//IBV_QP_MAX_DEST_RD_ATOMIC)) {
		rc_die("ibv_modify_qp2() failed");
		return 1;
	}
#endif

	// update connection hash tables
	struct id_record *entry = (struct id_record *)calloc(1, sizeof(struct id_record));
	struct sockaddr_in *addr_p = copy_ipv4_sockaddr(&id->route.addr.src_storage);
	
	if(addr_p == NULL)
		rc_die("compatibility issue: can't use a non-IPv4 address");

	entry->addr = *addr_p;	
	entry->qp_num = id->qp->qp_num;
	entry->id = id;

	//add the structure to both hash tables
	HASH_ADD(qp_hh, s_ctx->id_by_qp, qp_num, sizeof(uint32_t), entry);
	HASH_ADD(addr_hh, s_ctx->id_by_addr, addr, sizeof(struct sockaddr_in), entry);
}

__attribute__((visibility ("hidden"))) 
int finalize_connection(struct rdma_cm_id * id, struct rdma_conn_param * cm_params)
{
	struct conn_context *ctx = (struct conn_context *)id->context;

	struct rc_metadata *rc_meta = NULL;

#if 1
	if(cm_params)
		rc_meta = (struct rc_metadata *) cm_params->private_data;

	// modify connection parameters using metadata exchanged between client and server
	if(rc_meta) {
		debug_print("private data %p (len: given %d expected %lu)\n",
				rc_meta, cm_params->private_data_len,
				sizeof(struct rc_metadata));
		if(sizeof(struct rc_metadata) > cm_params->private_data_len)
			rc_die("invalid connection param length");

		mr_remote_update(ctx, rc_meta->addr, rc_meta->length, rc_meta->rkey, rc_meta->mr_count);
	}
#endif

	rc_set_state(id, RC_CONNECTION_READY);
}

#ifdef EXP_VERBS
__attribute__((visibility ("hidden")))
void query_device_cap(struct ibv_context *verbs)
{

	struct ibv_exp_device_attr attr;

	//memset(&attr, 0, sizeof(attr));

	if(ibv_exp_query_device(verbs, &attr))
		rc_die("ibv_exp_query_device() failed");

	//printf("exp_device_cap_flags: %lx\n", attr.exp_device_cap_flags);
	printf("Maximum # of QPs: %d\n", attr.max_qp);
	printf("Maximum # of outstanding WRs: %d\n", attr.max_qp_wr);
	printf("Maximum # of outstanding Atoms/Rds: %d\n", attr.max_qp_rd_atom);
	printf("Maximum depth for Atoms/Rds: %d\n", attr.max_qp_init_rd_atom);
	printf("-- Supported features --\n");
	printf("Atomic BEndian replies: %s\n", attr.exp_atomic_cap & IBV_EXP_ATOMIC_HCA_REPLY_BE ? "YES":"NO");
	printf("Core-direct: %s\n", attr.exp_device_cap_flags & IBV_EXP_DEVICE_CROSS_CHANNEL ? "YES":"NO");
	printf("Collectives:\n");
	printf("  [int operations]\n");
	printf("	* ADD    -> %s\n", attr.calc_cap.int_ops & IBV_EXP_CALC_OP_ADD ? "YES":"NO");
	printf("	* BAND   -> %s\n", attr.calc_cap.int_ops & IBV_EXP_CALC_OP_BAND ? "YES":"NO");
	printf("	* BXOR   -> %s\n", attr.calc_cap.int_ops & IBV_EXP_CALC_OP_BXOR ? "YES":"NO");
	printf("	* BOR    -> %s\n", attr.calc_cap.int_ops & IBV_EXP_CALC_OP_BOR ? "YES":"NO");
	printf("	* MAXLOC -> %s\n", attr.calc_cap.int_ops & IBV_EXP_CALC_OP_MAXLOC ? "YES":"NO");
	printf("  [uint operations]\n");
	printf("	* ADD    -> %s\n", attr.calc_cap.uint_ops & IBV_EXP_CALC_OP_ADD ? "YES":"NO");
	printf("	* BAND   -> %s\n", attr.calc_cap.uint_ops & IBV_EXP_CALC_OP_BAND ? "YES":"NO");
	printf("	* BXOR   -> %s\n", attr.calc_cap.uint_ops & IBV_EXP_CALC_OP_BXOR ? "YES":"NO");
	printf("	* BOR    -> %s\n", attr.calc_cap.uint_ops & IBV_EXP_CALC_OP_BOR ? "YES":"NO");
	printf("	* MAXLOC -> %s\n", attr.calc_cap.uint_ops & IBV_EXP_CALC_OP_MAXLOC ? "YES":"NO");
	printf("  [fp operations]\n");
	printf("	* ADD    -> %s\n", attr.calc_cap.fp_ops & IBV_EXP_CALC_OP_ADD ? "YES":"NO");
	printf("	* BAND   -> %s\n", attr.calc_cap.fp_ops & IBV_EXP_CALC_OP_BAND ? "YES":"NO");
	printf("	* BXOR   -> %s\n", attr.calc_cap.fp_ops & IBV_EXP_CALC_OP_BXOR ? "YES":"NO");
	printf("	* BOR    -> %s\n", attr.calc_cap.fp_ops & IBV_EXP_CALC_OP_BOR ? "YES":"NO");
	printf("	* MAXLOC -> %s\n", attr.calc_cap.fp_ops & IBV_EXP_CALC_OP_MAXLOC ? "YES":"NO");
}
#endif

#if 0
__attribute__((visibility ("hidden"))) 
void try_build_device(struct ibv_context *verbs)
{
	if (s_ctx) {
		if (s_ctx->ctx != verbs)
			rc_die("cannot handle events in more than one device context.");
		return;
	}

	printf("RDMA Device detected. Querying capabilities..\n");

#ifdef EXP_VERBS
	query_device_cap(verbs);
#endif

	debug_print("Building device context..\n");

	s_ctx = (struct context *)malloc(sizeof(struct context));

	s_ctx->ctx = verbs;
	s_ctx->pd = ibv_alloc_pd(s_ctx->ctx);
	s_ctx->id_by_addr = NULL;
	s_ctx->id_by_qp = NULL;

	debug_print("s_ctx %p\n", s_ctx);
}
#else
__attribute__((visibility ("hidden"))) 
void try_build_device(struct rdma_cm_id *id)
{
	struct conn_context *ctx = (struct conn_context *)id->context;

	if (!s_ctx) {
		s_ctx = (struct context *)malloc(sizeof(struct context));
		s_ctx->id_by_addr = NULL;
		s_ctx->id_by_qp = NULL;
		s_ctx->n_dev = 0;

		for(int i=0; i<MAX_DEVICES; i++) {
			s_ctx->ctx[i] = NULL;
			s_ctx->pd[i] = NULL;
		}
	}


	for(int i=0; i<s_ctx->n_dev; i++) {
		if(s_ctx->ctx[i] == id->verbs) {
			ctx->devid = i;
			return;
		}
	}

	if(s_ctx->n_dev == MAX_DEVICES)
		rc_die("failed to allocate new rdma device. try increasing MAX_DEVICES.");

	// allocate new device
	debug_print("initializing rdma device-%d\n", s_ctx->n_dev);
	s_ctx->ctx[s_ctx->n_dev] = id->verbs;
	s_ctx->pd[s_ctx->n_dev] = ibv_alloc_pd(id->verbs);
	ctx->devid = s_ctx->n_dev;
	s_ctx->n_dev++;

#endif
}


__attribute__((visibility ("hidden"))) 
void build_cq_channel(struct rdma_cm_id *id)
{
	struct conn_context *ctx = (struct conn_context *)id->context;

	int idx = ctx->devid;

	// create completion queue channel
	ctx->comp_channel = ibv_create_comp_channel(s_ctx->ctx[idx]);

	if (!ctx->comp_channel)
		rc_die("ibv_create_comp_channel() failed");

	ctx->cq = ibv_create_cq(s_ctx->ctx[idx], 50, NULL, ctx->comp_channel, 0); /* cqe=10 is arbitrary */

	if (!ctx->cq)
		rc_die("Failed to create CQ");

#ifdef EXP_VERBS
	if((ctx->flags & IBV_EXP_QP_CREATE_MANAGED_SEND) | (ctx->flags & IBV_EXP_QP_CREATE_MANAGED_RECV)) {
		struct ibv_exp_cq_attr attr = {
			.comp_mask       = IBV_EXP_CQ_ATTR_CQ_CAP_FLAGS,
			.cq_cap_flags    = IBV_EXP_CQ_IGNORE_OVERRUN
		};

		if (ibv_exp_modify_cq(ctx->cq, &attr, IBV_EXP_CQ_CAP_FLAGS)) {
			rc_die("Failed to modify CQ");
		}
	}
#endif

	ibv_req_notify_cq(ctx->cq, 0);

#if 1
	struct mlx5dv_obj dv_obj = {};
	memset((void *)&dv_obj, 0, sizeof(struct mlx5dv_obj));
	ctx->icq = (struct mlx5dv_cq *)malloc(sizeof(struct mlx5dv_cq));

	dv_obj.cq.in = ctx->cq;
	dv_obj.cq.out = ctx->icq;

 	int ret = mlx5dv_init_obj(&dv_obj, MLX5DV_OBJ_CQ);
#endif

	// poll completion queues in the background (always or only during bootstrap)
#if 0 
	printf("creating background thread to poll completions (spinning)\n");
	pthread_create(&ctx->cq_poller_thread, NULL, poll_cq_spinning_loop, ctx);
#else
	printf("creating background thread to poll completions (blocking)\n");
	pthread_create(&ctx->cq_poller_thread, NULL, poll_cq_blocking_loop, ctx);
#endif

#if 0
	//bind polling thread to a specific core to avoid contention
	cpu_set_t cpuset;
	int max_cpu_available = 0;
	int core_id = 1;

	CPU_ZERO(&cpuset);
	sched_getaffinity(0, sizeof(cpuset), &cpuset);

	// Search for the last / highest cpu being set
	for (int i = 0; i < 8 * sizeof(cpu_set_t); i++) {
		if (!CPU_ISSET(i, &cpuset)) {
			max_cpu_available = i;
			break;
		}
	}
	if(max_cpu_available <= 0)
		rc_die("unexpected config; # of available cpu cores must be > 0");

	core_id = (ctx->sockfd) % (max_cpu_available-1) + 2;

	printf("assigning poll_cq loop for sockfd %d to core %d\n", ctx->sockfd, core_id); 

	CPU_ZERO(&cpuset);
	CPU_SET(core_id, &cpuset); //try to bind pollers to different cores

	int ret = pthread_setaffinity_np(ctx->cq_poller_thread, sizeof(cpu_set_t), &cpuset);
	if(ret != 0)
		rc_die("failed to bind polling thread to a CPU core");
#endif
}

#ifdef EXP_VERBS
__attribute__((visibility ("hidden"))) 
void build_qp_attr(struct rdma_cm_id *id, struct ibv_exp_qp_init_attr *qp_attr)
{
	struct conn_context *ctx = (struct conn_context *)id->context;

	memset(qp_attr, 0, sizeof(*qp_attr));

	qp_attr->send_cq = ctx->cq;
	qp_attr->recv_cq = ctx->cq;
	qp_attr->qp_type = IBV_QPT_RC;

	//FIXME: change back!
	//qp_attr->cap.max_send_wr = MAX_SEND_QUEUE_SIZE;
	//qp_attr->cap.max_recv_wr = MAX_RECV_QUEUE_SIZE;
	qp_attr->cap.max_send_wr = global_sq_sz;
	qp_attr->cap.max_recv_wr = global_sq_sz;
	//qp_attr->cap.max_recv_wr = 120;


	//qp_attr->cap.max_send_sge = 32;
	//qp_attr->cap.max_recv_sge = 32;
	qp_attr->cap.max_send_sge = 16;
	qp_attr->cap.max_recv_sge = 16;

	qp_attr->comp_mask |= IBV_EXP_QP_INIT_ATTR_CREATE_FLAGS | IBV_EXP_QP_INIT_ATTR_PD;
	qp_attr->pd = rc_get_pd(id);

	//FIXME: cc
	qp_attr->exp_create_flags = IBV_EXP_QP_CREATE_CROSS_CHANNEL | ctx->flags;

	//if(qp_attr->exp_create_flags & IBV_EXP_QP_CREATE_MANAGED_SEND)
	qp_attr->exp_create_flags |= IBV_EXP_QP_CREATE_IGNORE_SQ_OVERFLOW;

#ifdef ATOMIC_BE_REPLY
	//FIXME: only used this if IBV_EXP_ATOMIC_HCA_REPLY_BE capability is supported
	qp_attr->exp_create_flags |= IBV_EXP_QP_CREATE_ATOMIC_BE_REPLY;
#endif	

	debug_print("Creating QP for sock #%d [SendQ - size: %d] [RecvQ - size: %d] flags %d\n",
			ctx->sockfd, qp_attr->cap.max_send_wr, qp_attr->cap.max_recv_wr, ctx->flags);
}

#else
__attribute__((visibility ("hidden"))) 
void build_qp_attr(struct rdma_cm_id *id, struct ibv_qp_init_attr *qp_attr)
{
	struct conn_context *ctx = (struct conn_context *)id->context;

	memset(qp_attr, 0, sizeof(*qp_attr));

	qp_attr->send_cq = ctx->cq;
	qp_attr->recv_cq = ctx->cq;
	qp_attr->qp_type = IBV_QPT_RC;

	qp_attr->cap.max_send_wr = MAX_SEND_QUEUE_SIZE;
	qp_attr->cap.max_recv_wr = MAX_RECV_QUEUE_SIZE;
	qp_attr->cap.max_send_sge = 16;
	qp_attr->cap.max_recv_sge = 16;
}
#endif

__attribute__((visibility ("hidden"))) 
void build_rc_params(struct rdma_cm_id *id, struct rdma_conn_param *params)
{
	struct conn_context *ctx = (struct conn_context *)id->context;

	memset(params, 0, sizeof(*params));

	params->initiator_depth = params->responder_resources = 1;
	params->rnr_retry_count = 7; /* infinite retry */

	struct rc_metadata *meta = (struct rc_metadata *)calloc(1, sizeof(struct rc_metadata));

	meta->flags = ctx->flags;
	meta->type = ctx->app_type;
	meta->mr_count = num_mrs;
	for(int i=0; i<meta->mr_count; i++) {
		meta->addr[i] = (uintptr_t) ctx->local_mr[i]->addr;
		meta->length[i] = ctx->local_mr[i]->length;
		meta->rkey[i] = ctx->local_mr[i]->rkey;
	}

	params->private_data = meta;
	//params->private_data_len = sizeof(struct rc_metadata) + MAX_MR * sizeof(struct ibv_mr);
	params->private_data_len = sizeof(*meta);

	if(sizeof(*meta) > 56)
		rc_die("metadata length greater than max allowed size\n");
	//else {
}

__attribute__((visibility ("hidden"))) 
void rdma_event_loop(struct rdma_event_channel *ec, int exit_on_connect, int exit_on_disconnect)
{
	struct rdma_cm_event *event = NULL;
	struct rdma_conn_param cm_params;

	while (rdma_get_cm_event(ec, &event) == 0) {
		struct rdma_cm_event event_copy;

		memcpy(&event_copy, event, sizeof(*event));
		debug_print("received event[%d]: %s\n", event_copy.event, rdma_event_str(event_copy.event));

		rdma_ack_cm_event(event);

		//try_build_device(event_copy.id->verbs);

		if (event_copy.event == RDMA_CM_EVENT_ADDR_RESOLVED) {
			rdma_resolve_route(event_copy.id, TIMEOUT_IN_MS);

		}
		else if (event_copy.event == RDMA_CM_EVENT_ROUTE_RESOLVED) {	
			setup_connection(event_copy.id, NULL);

			if (s_on_pre_conn_cb) {
				debug_print("trigger pre-connection callback\n");
				s_on_pre_conn_cb(event_copy.id);
			}

			build_rc_params(event_copy.id, &cm_params);
			int err = rdma_connect(event_copy.id, &cm_params);
			if(err)
				rc_die("failed to connect\n");
		}
		else if (event_copy.event == RDMA_CM_EVENT_CONNECT_REQUEST) {
			int always_poll = 1;

			struct rc_metadata *rc_meta_temp;
			if(&event_copy.param.conn)
				rc_meta_temp = (struct rc_metadata *) (&event_copy.param.conn)->private_data;

#ifdef EXP_VERBS
			if((rc_meta_temp->flags & IBV_EXP_QP_CREATE_MANAGED_SEND) || (rc_meta_temp->flags & IBV_EXP_QP_CREATE_MANAGED_RECV))
				always_poll = 1;
#endif

			init_connection(event_copy.id, -1, always_poll, 0);
			setup_connection(event_copy.id, &event_copy.param.conn);

			// ((struct conn_context *) event_copy.id->context)->flags =
			//	 ((rc_meta_t *)cm_params.private_data)->flags;

			//build_connection(event_copy.id);
			if (s_on_pre_conn_cb) {
				debug_print("trigger pre-connection callback\n");
				s_on_pre_conn_cb(event_copy.id);
			}

			build_rc_params(event_copy.id, &cm_params);
			rdma_accept(event_copy.id, &cm_params);

		}
		else if (event_copy.event == RDMA_CM_EVENT_ESTABLISHED) {
			finalize_connection(event_copy.id, &event->param.conn);
			
			if (s_on_connect_cb) {
				debug_print("trigger post-connection callback\n");
				s_on_connect_cb(event_copy.id);
			}


			if (exit_on_connect)
				break;
		}
		else if (event_copy.event == RDMA_CM_EVENT_DISCONNECTED) {
			if (s_on_disconnect_cb) {
				debug_print("trigger disconnection callback\n");
				s_on_disconnect_cb(event_copy.id);
			}

			rc_disconnect(event_copy.id);

			if (exit_on_disconnect) {
				rc_clear(event_copy.id);
				break;
			}

		}
		else if (event_copy.event == RDMA_CM_EVENT_REJECTED) {
			debug_print("rejection reason: %d\n", event_copy.status);
			rc_die("Connection failure. Exiting..");
		}
		else if (event_copy.event == RDMA_CM_EVENT_TIMEWAIT_EXIT) {
			//this event indicates that the recently destroyed queue pair is ready to be reused
			//at this point, clean up any allocated memory for connection
			rc_clear(event_copy.id);
		}
		else {
			rc_die("Unhandled event. Exiting..");
		}
	}
}

__attribute__((visibility ("hidden"))) 
void update_completions(struct ibv_wc *wc)
{
	//FIXME: one hash search is performed for every send completion. Optimize!
	struct rdma_cm_id *id = find_connection_by_wc(wc);
	struct conn_context *ctx = (struct conn_context *)id->context;

	//signal any threads blocking on wr.id
	if (wc->opcode & IBV_WC_RECV) {
		debug_print("COMPLETION --> (RECV WR #%lu) [qp_num %u]\n", wc->wr_id, id->qp->qp_num);
		//ctx->last_rcv_compl = 1;
	}
	else {
		debug_print("COMPLETION --> (SEND WR #%lu) [qp_num %u]\n", wc->wr_id, id->qp->qp_num);
		//pthread_mutex_lock(&ctx->wr_lock);
		ctx->last_send_compl = wc->wr_id;
		//pthread_cond_broadcast(&ctx->wr_completed);	
		//pthread_mutex_unlock(&ctx->wr_lock);
	}
}

__attribute__((visibility ("hidden")))
void spin_till_response(struct rdma_cm_id *id, uint32_t seqn)
{
	struct ibv_wc wc;

	struct conn_context *ctx = (struct conn_context *)id->context;

	debug_print("spinning till response with seqn %u (last received seqn -> %u)\n",
			seqn, last_compl_wr_id(ctx, 0));

	//while(!ibw_cmpxchg(&ctx->poll_permission, 1, 0))
	//	ibw_cpu_relax();

	while(ctx->last_rcv_compl < seqn || ((ctx->last_rcv_compl - seqn) > MAX_PENDING)) {
		poll_cq(ctx->cq, &wc);
		ibw_cpu_relax();
	}

	return;
	//if(ibw_cmpxchg(&ctx->poll_permission, 0, 1))
	//	rc_die("failed to give up permission; possible race condition");
}

__attribute__((visibility ("hidden")))
void block_till_response(struct rdma_cm_id *id, uint32_t seqn)
{
	struct ibv_cq *cq;
	struct ibv_wc wc;
	void *ev_ctx;

	struct conn_context *ctx = (struct conn_context *)id->context;

	debug_print("spinning till response with seqn %u (last received seqn -> %u)\n",
			seqn, last_compl_wr_id(ctx, 0));

	//while(!ibw_cmpxchg(&ctx->poll_permission, 1, 0))
	//	ibw_cpu_relax();

	while(ctx->last_rcv_compl < seqn || ((ctx->last_rcv_compl - seqn) > MAX_PENDING)) {
		ibv_get_cq_event(ctx->comp_channel, &cq, &ev_ctx);
		ibv_ack_cq_events(cq, 1);
		ibv_req_notify_cq(cq, 0);
		poll_cq(ctx->cq, &wc);
		ibw_cpu_relax();
	}

	return;
	//if(ibw_cmpxchg(&ctx->poll_permission, 0, 1))
	//	rc_die("failed to give up permission; possible race condition");
}

//spin till we receive a completion with wr_id (overrides poll_cq loop)
void spin_till_completion(struct rdma_cm_id *id, uint32_t wr_id)
{
	struct ibv_wc wc;

	struct conn_context *ctx = (struct conn_context *)id->context;

	debug_print("spinning till WR %u completes (last completed WR -> %u)\n",
			wr_id, last_compl_wr_id(ctx, 1));
#if 0
	while(!ibw_cmpxchg(&ctx->poll_permission, 1, 0))
		ibw_cpu_relax();

	while(cmp_counters(wr_id, last_compl_wr_id(ctx, 1)) > 0) {
		poll_cq(ctx->cq, &wc);
		//printf("cmp_counters: input: (wr_id: %u, last_wr_id: %u) | output: %u\n",
		//	       wr_id, last_compl_wr_id(ctx, 1), cmp_counters(wr_id, last_compl_wr_id(ctx, 1)));
		ibw_cpu_relax();
	}

	if(ibw_cmpxchg(&ctx->poll_permission, 0, 1))
		rc_die("failed to reset permission; possible race condition");
#else
	while(cmp_counters(wr_id, last_compl_wr_id(ctx, 1)) > 0) {
		ibw_cpu_relax();
	}
#endif
}


//spin till we receive a completion with wr_id (overrides poll_cq loop)
void block_till_completion(struct rdma_cm_id *id, uint32_t wr_id)
{
	struct ibv_cq *cq;
	struct ibv_wc wc;
	void *ev_ctx;

	struct conn_context *ctx = (struct conn_context *)id->context;

	debug_print("blocking till WR %u completes (last completed WR -> %u)\n",
			wr_id, last_compl_wr_id(ctx, 1));
#if 1
	while(!ibw_cmpxchg(&ctx->poll_permission, 1, 0))
		ibw_cpu_relax();

	while(cmp_counters(wr_id, last_compl_wr_id(ctx, 1)) > 0) {
		ibv_get_cq_event(ctx->comp_channel, &cq, &ev_ctx);
		ibv_ack_cq_events(cq, 1);
		ibv_req_notify_cq(cq, 0);
		poll_cq(ctx->cq, &wc);
		ibw_cpu_relax();
	}

	if(ibw_cmpxchg(&ctx->poll_permission, 0, 1))
		rc_die("failed to reset permission; possible race condition");
#else
	while(cmp_counters(wr_id, last_compl_wr_id(ctx, 1)) > 0) {
		ibw_cpu_relax();
	}
#endif
}

//poll completions in a looping (blocking)
__attribute__((visibility ("hidden"))) 
void * poll_cq_blocking_loop(void *ctx)
{
	//if(stick_this_thread_to_core(POLL_THREAD_CORE_ID) == EINVAL)
	//	rc_die("failed to pin poll thread to core");

	struct ibv_cq *cq;
	struct ibv_wc wc;
	void *ev_ctx;

	//int ran_once = 0;
	while(((struct conn_context*)ctx)->poll_enable) {
		ibv_get_cq_event(((struct conn_context*)ctx)->comp_channel, &cq, &ev_ctx);
		ibv_ack_cq_events(cq, 1);
		ibv_req_notify_cq(cq, 0);
		poll_cq(((struct conn_context*)ctx)->cq, &wc);
	}

	printf("end poll_cq loop for sockfd %d\n", ((struct conn_context*)ctx)->sockfd);
	return NULL;
}

__attribute__((visibility ("hidden"))) 
void * poll_cq_spinning_loop(void *ctx)
{
	//if(stick_this_thread_to_core(POLL_THREAD_CORE_ID) == EINVAL)
	//	rc_die("failed to pin poll thread to core");

	struct ibv_wc wc;
	//int ran_once = 0;

	while(((struct conn_context*)ctx)->poll_enable) {
		if(((struct conn_context*)ctx)->poll_permission)
			poll_cq(((struct conn_context*)ctx)->cq, &wc);	
		ibw_cpu_relax();
	}

	printf("end poll_cq loop for sockfd %d\n", ((struct conn_context*)ctx)->sockfd);
	return NULL;
}

__attribute__((visibility ("hidden"))) 
inline void poll_cq(struct ibv_cq *cq, struct ibv_wc *wc)
{
	while(ibv_poll_cq(cq, 1, wc)) {
		if (wc->status == IBV_WC_SUCCESS) {
			update_completions(wc);


			//debug_print("trigger completion callback\n");
			s_on_completion_cb(wc);
		}
		else {
#if 1
			const char *descr;
			descr = ibv_wc_status_str(wc->status);
			struct rdma_cm_id *id = find_connection_by_wc(wc);
			struct conn_context *ctx = (struct conn_context *)id->context;
			printf("COMPLETION FAILURE on sockfd %d (%s WR #%lu) status[%d] = %s\n",
					ctx->sockfd, (wc->opcode & IBV_WC_RECV)?"RECV":"SEND",
					wc->wr_id, wc->status, descr);
#endif
			//rc_die("poll_cq: status is not IBV_WC_SUCCESS");
		}
	}
}

__attribute__((visibility ("hidden"))) 
inline void poll_cq_debug(struct ibv_cq *cq, struct ibv_wc *wc)
{
	while(ibv_poll_cq(cq, 1, wc)) {
		if (wc->status == IBV_WC_SUCCESS) {
			update_completions(wc);

			//debug_print("trigger completion callback\n");
			s_on_completion_cb(wc);

			if(wc->opcode & IBV_WC_RECV)
				return;
		}
		else {
#ifdef DEBUG
			const char *descr;
			descr = ibv_wc_status_str(wc->status);
			debug_print("COMPLETION FAILURE (%s WR #%lu) status[%d] = %s\n",
					(wc->opcode & IBV_WC_RECV)?"RECV":"SEND",
					wc->wr_id, wc->status, descr);
#endif
			rc_die("poll_cq: status is not IBV_WC_SUCCESS");
		}
	}
}

#if 0
__attribute__((visibility ("hidden"))) 
int poll_cq_for_nuance(uint64_t wr_id)
{
	//int total_cq_poll_count = 0;
        struct ibv_cq *cq = s_ctx->cq;
	struct ibv_wc wc;
	int success = 0;
	int cq_poll_count = 0;

	debug_print("----check WR completion (id:%lu)", wr_id);
#if 1
	int i;
	pthread_mutex_lock(shared_cq_lock);
	
	//first, we do a quick search through the archive
	//we start from the last written archive entry
	for(i = max(archive_idx-1, 0); i >= 0; i--) {
		debug_print("checking archive entry[%d] for WR id. given: %u, exp: %lu\n",
		        i, cqe_archive[i], wr_id);
		if(wr_id == cqe_archive[i]) {
			success = 1;
			cqe_archive[i] = 0;
			break;
		}
	}

	//we lookup the rest of the entries if we didn't find wr_id
	for(i = archive_idx;  i < ARCHIVE_SIZE && !success; i++) {
		debug_print("checking archive entry[%d] for WR id. given: %u, exp: %lu\n",
			i, cqe_archive[i], wr_id);
		if(wr_id == cqe_archive[i]) {
			success = 1;
			cqe_archive[i] = 0;
			break;
		}
	}

	if(success) {
		debug_print("found nuance %lu\n", wr_id);
		pthread_mutex_unlock(shared_cq_lock);
		return cq_poll_count;
	}

#endif

	//printf("checking CQ for nuance %lu\n", wr_id);
	if (ibv_req_notify_cq(cq, 0))
		rc_die("Could not request CQ notification");

	if (ibv_get_cq_event(s_ctx->comp_channel, &cq, (void *)&s_ctx->ctx))
		rc_die("Failed to get cq_event");

 	while(ibv_poll_cq(cq, 1, &wc)) {
		cq_poll_count++;
		if (wc.status == IBV_WC_SUCCESS) {
			debug_print("polling CQ for WR id. given: %lu, exp: %lu\n",
					wr_id, wc.wr_id);
			if((!success && wc.wr_id == wr_id) || !wr_id) {	
				success = 1;
			}
			else {
				cqe_archive[archive_idx] = wc.wr_id;
				archive_idx = (archive_idx + 1) % sizeof(cqe_archive);
			}
		}
		else
			rc_die("poll_cq: status is not IBV_WC_SUCCESS");
	}

	ibv_ack_cq_events(cq, 1);

	pthread_mutex_unlock(shared_cq_lock);

	if(success)
		return cq_poll_count;
	else {
		fprintf(stderr, "[error] wrong completion event. expected: %lu, found: %lu\n",
				wr_id, wc.wr_id);
		exit(EXIT_FAILURE);
	}
}
#endif

__attribute__((visibility ("hidden"))) 
struct rdma_cm_id* find_next_connection(struct rdma_cm_id* id)
{
	int i = 0;

	if(id == NULL)
		i = find_first_set_bit(s_conn_bitmap, MAX_CONNECTIONS);
	else {
		struct conn_context *ctx = (struct conn_context *) id->context;
		i = find_next_set_bit(ctx->sockfd, s_conn_bitmap, MAX_CONNECTIONS);
	}

	if(i >= 0)
		return get_connection(i);
	else
		return NULL;	 
}

__attribute__((visibility ("hidden"))) 
struct rdma_cm_id* find_connection_by_addr(struct sockaddr_in *addr)
{
	struct id_record *entry = NULL;
	//debug_print("[hash] looking up id with sockaddr: %s:%hu\n",
	//		inet_ntoa(addr->sin_addr), addr->sin_port);
	HASH_FIND(addr_hh, s_ctx->id_by_addr, addr, sizeof(*addr), entry);
	if(!entry)
		rc_die("hash lookup failed; id doesn't exist");
	return entry->id;
}

__attribute__((visibility ("hidden"))) 
struct rdma_cm_id* find_connection_by_wc(struct ibv_wc *wc)
{
	struct id_record *entry = NULL;
	//debug_print("[hash] looking up id with qp_num: %u\n", wc->qp_num);
	HASH_FIND(qp_hh, s_ctx->id_by_qp, &wc->qp_num, sizeof(wc->qp_num), entry);
	if(!entry)
		rc_die("hash lookup failed; id doesn't exist");
	return entry->id;
}

__attribute__((visibility ("hidden"))) 
struct rdma_cm_id* get_connection(int sockfd)
{
	if(sockfd > MAX_CONNECTIONS)
		rc_die("invalid sockfd; must be less than MAX_CONNECTIONS");

	if(s_conn_bitmap[sockfd])
		return s_conn_ids[sockfd];
	else
		return NULL;
}

//iterate over hashtable and execute anonymous function for each id
//note: function has to accept two arguments of type: rdma_cm_id* & void* (in that order)
__attribute__((visibility ("hidden"))) 
void execute_on_connections(void* custom_arg, void custom_func(struct rdma_cm_id*, void*))
{
	struct id_record *i = NULL;

	for(i=s_ctx->id_by_qp; i!=NULL; i=i->qp_hh.next) {
		custom_func(i->id, custom_arg);
	}
}

__attribute__((visibility ("hidden"))) 
void rc_init(pre_conn_cb_fn pc, connect_cb_fn conn, completion_cb_fn comp, disconnect_cb_fn disc)
{
	debug_print("initializing RC module\n");
	
	s_conn_bitmap = calloc(MAX_CONNECTIONS, sizeof(int));
	s_conn_ids = (struct rdma_cm_id **)calloc(MAX_CONNECTIONS, sizeof(struct rdma_cm_id*));

	s_on_pre_conn_cb = pc;
	s_on_connect_cb = conn;
	s_on_completion_cb = comp;
	s_on_disconnect_cb = disc;
}

__attribute__((visibility ("hidden"))) 
void rc_set_state(struct rdma_cm_id *id, int new_state)
{
	struct conn_context *ctx = (struct conn_context *) id->context;

	if(ctx->state == new_state)
		return;

	printf("modify state for socket #%d from %d to %d\n", ctx->sockfd, ctx->state, new_state);

	ctx->state = new_state;
	
	if((ctx->state == RC_CONNECTION_READY && !ctx->poll_always) || ctx->state == RC_CONNECTION_TERMINATED)
		ctx->poll_enable = 0;	
  		//void *ret;
    		//if(pthread_join(ctx->cq_poller_thread, &ret) != 0)
		//	rc_die("pthread_join() error");
	if(ctx->state == RC_CONNECTION_TERMINATED)
		pthread_cancel(ctx->cq_poller_thread);
}

void rc_set_sq_sz(int size)
{
	global_sq_sz = size;
}


int rc_connection_count()
{
	//return HASH_CNT(qp_hh, s_ctx->id_by_qp);
	return find_bitmap_weight(s_conn_bitmap, MAX_CONNECTIONS); 
}


int rc_next_connection(int cur)
{
	int i = 0;

	if(cur < 0)
		i = find_first_set_bit(s_conn_bitmap, MAX_CONNECTIONS);
	else {
		i = find_next_set_bit(cur, s_conn_bitmap, MAX_CONNECTIONS);
	}

	if(i >= 0)
		return i;
	else
		return -1;	 
}

int rc_connection_meta(int sockfd)
{
	if(get_connection(sockfd)) {
		struct conn_context *ctx = (struct conn_context *) s_conn_ids[sockfd]->context;
		return ctx->app_type;
	}
	else
		return -1;
}

char* rc_connection_ip(int sockfd) {
	if(get_connection(sockfd)) {
		struct sockaddr_in *addr_in = copy_ipv4_sockaddr(&s_conn_ids[sockfd]->route.addr.dst_storage);
		char *s = malloc(sizeof(char)*INET_ADDRSTRLEN);
		s = inet_ntoa(addr_in->sin_addr);
		return s;
	}
	else
		return NULL;
}

int rc_connection_qpnum(int sockfd)
{
	if(get_connection(sockfd)) {
		struct conn_context *ctx = (struct conn_context *) s_conn_ids[sockfd]->context;
		return ctx->id->qp->qp_num;
	}
	else
		return -1;
}

int rc_connection_cqnum(int sockfd)
{
	if(get_connection(sockfd)) {
		struct conn_context *ctx = (struct conn_context *) s_conn_ids[sockfd]->context;
		return ctx->icq->cqn;
	}
	else
		return -1;
}

int rc_ready(int sockfd)
{
	if(get_connection(sockfd)) {
		struct conn_context *ctx = (struct conn_context *) s_conn_ids[sockfd]->context;
		if(ctx->state == RC_CONNECTION_READY)
			return 1;
		else
			return 0;
	}
	else
		return 0;
}

int rc_active(int sockfd)
{
	if(get_connection(sockfd)) {
		struct conn_context *ctx = (struct conn_context *) s_conn_ids[sockfd]->context;
		if(ctx->state >= RC_CONNECTION_ACTIVE)
			return 1;
		else
			return 0;
	}
	else
		return 0;
}

int rc_terminated(int sockfd)
{
	if(get_connection(sockfd)) {
		struct conn_context *ctx = (struct conn_context *) s_conn_ids[sockfd]->context;
		if(ctx->state == RC_CONNECTION_TERMINATED)
			return 1;
		else
			return 0;
	}
	else
		return 0;
}

#if 0
struct ibv_wq_buffer * rc_get_wq_buffer(int sockfd)
{
	struct rdma_cm_id *id  = get_connection(sockfd);
	
	struct ibv_wq_buffer *b = ibv_ex_get_wq_buffer(id->qp, 0);

	return b;	
}

#endif

//FIXME: need a synchronization mechanism in case of simultaneous acquisitions
__attribute__((visibility ("hidden"))) 
int _rc_acquire_buffer(int sockfd, void ** ptr, int user)
{
#if 0
	int i;
	int timeout = 5;

	while(!rc_active(sockfd)) {
		if(timeout == 0)
			rc_die("failed to get acquire msg buffer; connection is inactive");
		debug_print("connection is inactive; sleeping for 1 sec...\n");
		timeout--;
		sleep(1);
	}

	struct conn_context *ctx = (struct conn_context *) get_connection(sockfd)->context;

	/*
	//busy wait, since this is likely on the fast path
	do {
		i = find_first_empty_bit(ctx->send_slots, MAX_BUFFER);
		ibw_cpu_relax();
	} while(i < 0);
	*/

	i = find_first_empty_bit(ctx->send_slots, MAX_BUFFER);

	//FIXME: consider a more robust approach for buffer acquisition instead
	//of simply failing.
	//As it stands, busy waiting for buffers can be problematic if buffer
	//acquisitions are happening during 'on_application_event' callbacks.
	//Doing so will stop the CQ polling loop, resulting in a deadlock.
	if(i < 0) {
		rc_die("failed to acquire buffer. consider increasing MAX_BUFFER");
	}

	ctx->send_slots[i] = 1;

	if(user) {
		//if this is not called by our internal bootstrap protocol, we provide data buffer for caller
		ctx->msg_send[i]->meta.app.id = 0; //set app.id to zero
		ctx->msg_send[i]->meta.app.data = ctx->msg_send[i]->data; //set a convenience pointer for data
		*ptr = (void *) &ctx->msg_send[i]->meta.app; //doesn't matter if we return app or mr
	}

	return i;
#endif
#if 1
	struct conn_context *ctx = (struct conn_context *) get_connection(sockfd)->context;
	int i = ctx->send_idx++ % MAX_BUFFER;
	debug_print("acquire buffer ID = %d on sockfd %d\n", i, sockfd);

	if(user) {
		ctx->msg_send[i]->meta.app.id = 0; //set app.id to zero
		ctx->msg_send[i]->meta.app.data = ctx->msg_send[i]->data; //set a convenience pointer for data
		*ptr = (void *) &ctx->msg_send[i]->meta.app; //doesn't matter if we return app or mr
	}

	return i;
#endif
	//return 0;
}

int rc_acquire_buffer(int sockfd, struct app_context ** ptr)
{
	return _rc_acquire_buffer(sockfd, (void **) ptr, 1);
}

//bind acquired buffer to specific wr id
__attribute__((visibility ("hidden"))) 
int rc_bind_buffer(struct rdma_cm_id *id, int buffer, uint32_t wr_id)
{
#if 1
	debug_print("binding buffer[%d] --> (SEND WR #%u)\n", buffer, wr_id);
	struct conn_context *ctx = (struct conn_context *) id->context;

	struct buffer_record *rec = calloc(1, sizeof(struct buffer_record));
	rec->wr_id = wr_id;
	rec->buff_id = buffer;

	/*
	struct buffer_record *current_b, *tmp_b;
	HASH_ITER(hh, ctx->buffer_bindings, current_b, tmp_b) {
		debug_print("buffer_binding record: wr_id:%u buff_id:%u\n", current_b->wr_id, current_b->buff_id);
	}
	*/

	pthread_spin_lock(&ctx->buffer_lock);
	HASH_ADD(hh, ctx->buffer_bindings, wr_id, sizeof(rec->wr_id), rec);
	pthread_spin_unlock(&ctx->buffer_lock);
	return 1;
#endif
	//return 1;
}

__attribute__((visibility ("hidden"))) 
int rc_release_buffer(int sockfd, uint32_t wr_id)
{
#if 1
	struct conn_context *ctx = (struct conn_context *) s_conn_ids[sockfd]->context;
	struct buffer_record *b;

	pthread_spin_lock(&ctx->buffer_lock);
	HASH_FIND(hh, ctx->buffer_bindings, &wr_id, sizeof(wr_id), b);
	if(b) {
		debug_print("released buffer[%d] --> (SEND WR #%u)\n", b->buff_id, wr_id);
		HASH_DEL(ctx->buffer_bindings, b);
		if(ctx->send_slots[b->buff_id])
			ctx->send_slots[b->buff_id] = 0;
		int ret = b->buff_id;
		free(b);

		pthread_spin_unlock(&ctx->buffer_lock);
		return ret;
	}
	else {
		pthread_spin_unlock(&ctx->buffer_lock);
		rc_die("failed to release buffer. possible race condition.\n");
	}

	return -1;
#endif
	//return 0;
}

__attribute__((visibility ("hidden"))) 
void rc_disconnect(struct rdma_cm_id *id)
{
	struct conn_context *ctx = (struct conn_context *)id->context;

	debug_print("terminating connection on socket #%d\n", ctx->sockfd);

	rc_set_state(id, RC_CONNECTION_TERMINATED);

#if 1
	//delete from hashtables
	struct id_record *entry = NULL;
	HASH_FIND(qp_hh, s_ctx->id_by_qp, &id->qp->qp_num, sizeof(id->qp->qp_num), entry);
	if(!entry)
		rc_die("hash delete failed; id doesn't exist");
	HASH_DELETE(qp_hh, s_ctx->id_by_qp, entry);
	HASH_DELETE(addr_hh, s_ctx->id_by_addr, entry);
	free(entry);

	struct app_response *current_p, *tmp_p;
	struct buffer_record *current_b, *tmp_b;

	HASH_ITER(hh, ctx->pendings, current_p, tmp_p) {
		HASH_DEL(ctx->pendings,current_p);
		free(current_p);          
	}

	HASH_ITER(hh, ctx->buffer_bindings, current_b, tmp_b) {
		HASH_DEL(ctx->buffer_bindings,current_b);
		free(current_b);          
	}

	//destroy queue pair and disconnect
	rdma_destroy_qp(id);
	rdma_disconnect(id);
#endif
}

__attribute__((visibility ("hidden"))) 
void rc_clear(struct rdma_cm_id *id)
{
	struct conn_context *ctx = (struct conn_context *)id->context;

	if(!rc_terminated(ctx->sockfd))
		rc_die("can't clear metadata for non-terminated connection");

	debug_print("clearing connection metadata for socket #%d\n", ctx->sockfd);
	s_conn_bitmap[ctx->sockfd] = 0;
	s_conn_ids[ctx->sockfd] = NULL;

	for(int i=0; i<MAX_MR; i++) {
		if(ctx->local_mr_ready[i]) {
#if 0
			//XXX removing this for now; only need to deregister MR once
			debug_print("deregistering mr[addr:%lx, len:%lu]\n",
					(uintptr_t)ctx->local_mr[i]->addr, ctx->local_mr[i]->length);
			ibv_dereg_mr(ctx->local_mr[i]);
#endif
		}
		if(ctx->remote_mr_ready[i])
			free(ctx->remote_mr[i]);
	}

	free(ctx->local_mr);
	free(ctx->local_mr_ready);
	free(ctx->local_mr_sent);
	free(ctx->remote_mr);
	free(ctx->remote_mr_ready);

	for(int i=0; i<MAX_BUFFER; i++) {
		debug_print("deregistering msg_send_mr[addr:%lx, len:%lu]\n",
				(uintptr_t)ctx->msg_send_mr[i]->addr, ctx->msg_send_mr[i]->length);
		debug_print("deregistering msg_rcv_mr[addr:%lx, len:%lu]\n",
				(uintptr_t)ctx->msg_rcv_mr[i]->addr, ctx->msg_rcv_mr[i]->length);
		ibv_dereg_mr(ctx->msg_send_mr[i]);
		ibv_dereg_mr(ctx->msg_rcv_mr[i]);
		free(ctx->msg_send[i]);
		free(ctx->msg_rcv[i]);
	}

	free(ctx->msg_send_mr);
	free(ctx->msg_rcv_mr);
	free(ctx->msg_send);
	free(ctx->msg_rcv);
 
	free(ctx);
	free(id);
}

void rc_die(const char *reason)
{
	fprintf(stderr, "%s [error code: %d]\n", reason, errno);
	exit(EXIT_FAILURE);
}

__attribute__((visibility ("hidden"))) 
struct ibv_pd * rc_get_pd(struct rdma_cm_id *id)
{
#if 1
	struct conn_context *ctx = (struct conn_context *) id->context;

	if(ctx->devid > MAX_DEVICES-1)
		rc_die("invalid rdma device index for connection.");

	return s_ctx->pd[ctx->devid];
#else
	return s_ctx->pd;
#endif
}

struct ibv_context * rc_get_context(int id)
{
	return s_ctx->ctx[id];
}

