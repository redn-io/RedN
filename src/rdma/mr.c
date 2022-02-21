
#include "connection.h"
#include "mlx5_intf.h"
#include "mr.h"

struct ibv_mr * mr_regions[MAX_MR] = {NULL};

__attribute__((visibility ("hidden"))) 
int mr_all_recv(struct conn_context *ctx)
{
	//debug_print("sockfd: %d recv count: %d num_mrs: %d\n", ctx->sockfd, find_bitmap_weight(ctx->remote_mr_ready,MAX_MR), num_mrs);
	if(ctx->remote_mr_total == find_bitmap_weight(ctx->remote_mr_ready,
				MAX_MR) && ctx->mr_init_recv)
		return 1;
	else
		return 0;
}

__attribute__((visibility ("hidden"))) 
int mr_all_sent(struct conn_context *ctx)
{
#if 0
	if(ctx->local_mr_to_sync == -1)
		return 1;
	else
		return 0;
#else
	//debug_print("sockfd: %d sent count: %d num_mrs: %d\n", ctx->sockfd, find_bitmap_weight(ctx->local_mr_sent,MAX_MR), num_mrs);
	if(num_mrs == find_bitmap_weight(ctx->local_mr_sent,
				MAX_MR))
		return 1;
	else
		return 0;
#endif
}

__attribute__((visibility ("hidden"))) 
int mr_all_synced(struct conn_context *ctx)
{
	if(mr_all_recv(ctx) && mr_all_sent(ctx))
		return 1;
	else
		return 0;
}

__attribute__((visibility ("hidden"))) 
int mr_local_ready(struct conn_context *ctx, int mr_id)
{
	if(mr_id > MAX_MR)
		rc_die("invalid memory region id; must be less than MAX_MR");

	if(ctx->local_mr_ready[mr_id])
		return 1;
	else
		return 0;
}

__attribute__((visibility ("hidden"))) 
int mr_remote_ready(struct conn_context *ctx, int mr_id)
{
	if(mr_id > MAX_MR)
		rc_die("invalid memory region id; must be less than MAX_MR");

	if(ctx->remote_mr_ready[mr_id])
		return 1;
	else
		return 0;
}

//FIXME: for now, we just hardcode permissions for memory registration
// (all provided mrs are given local/remote write permissions)
__attribute__((visibility ("hidden"))) 
void mr_register(struct conn_context *ctx, struct mr_context *mrs, int num_mrs, int msg_size)
{
	debug_print("[sockfd %d] registering %d memory regions & %d send/rcv buffers\n", ctx->sockfd, num_mrs, MAX_BUFFER*2);

	//if(num_mrs <= 0)
	//	return;

	int idx = 0;
	for(int i=0; i<num_mrs; i++) {
		debug_print("registering mr #%d with addr:%lu and size:%lu\n", i, mrs[i].addr, mrs[i].length);
		//int idx = mrs[i].type;
		idx = i;
		if(idx > MAX_MR-1)
			rc_die("memory region type outside of MAX_MR");

		uint64_t access_flags = IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_REMOTE_ATOMIC | IBV_ACCESS_REMOTE_READ;

		// only register memory once
		if(!mr_regions[i]) {
			if(mrs[i].physical) {
#ifdef EXP_VERBS
				debug_print("mr #%d set as physical memory\n", i);
				struct ibv_exp_reg_mr_in in = {0};
				in.pd = rc_get_pd(ctx->id);
#if 1
				in.addr = (void*)mrs[i].addr;
				in.length = mrs[i].length;
#else
				in.addr = NULL;
				in.length = 0;
#endif
				in.exp_access = (access_flags | IBV_EXP_ACCESS_PHYSICAL_ADDR);
				//ctx->local_mr[idx] = ibv_exp_reg_mr(&in);
				mr_regions[idx] = ibv_exp_reg_mr(&in);
#else
				rc_die("physical MRs not supported");
#endif
			}
			else {
				//ctx->local_mr[idx] = ibv_reg_mr(rc_get_pd(ctx->id), (void*)mrs[i].addr, mrs[i].length, access_flags);
				mr_regions[idx] = ibv_reg_mr(rc_get_pd(ctx->id), (void*)mrs[i].addr, mrs[i].length, access_flags);


			}
		}

		ctx->local_mr[idx] = mr_regions[idx];

		if(!ctx->local_mr[idx]) {
			//debug_print("registeration failed with errno: %d\n", errno);
			perror("memory registeration failed");
			rc_die("ibv_reg_mr failed");
		}
		ctx->local_mr_ready[idx] = 1;
		debug_print("registered local_mr[addr:%lx, len:%lu, rkey:%u, lkey:%u]\n",
				(uintptr_t)ctx->local_mr[idx]->addr, ctx->local_mr[idx]->length,
			       	ctx->local_mr[idx]->rkey, ctx->local_mr[idx]->lkey);
	}

#ifdef EXP_VERBS


#ifdef MODDED_DRIVER
	struct ibv_wq_buffer *b = ibv_ex_get_wq_buffer(ctx->id->qp, 0);
	ctx->sq_start = b->address;
	ctx->sq_wrid = b->wrid;
	ctx->sq_wqe_cnt = b->wqe_cnt;


#ifdef REGISTER_WQ
	if(idx+1 > MAX_MR-1)
			rc_die("memory region type outside of MAX_MR");

	ctx->local_mr[idx+1] = ibv_reg_mr(rc_get_pd(ctx->id), b->address, b->size, 
				IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_REMOTE_ATOMIC | IBV_ACCESS_REMOTE_READ );
	ctx->local_mr_ready[idx+1] = 1;
	ctx->sq_mr_idx = idx+1;
#endif

	free(b);

#else
	//XXX need to create sq_wrid (should be updated by verbs.c functions)
	ctx->sq_start = ctx->iqp->sq.buf;
	ctx->sq_wqe_cnt = ctx->iqp->sq.wqe_cnt;
#endif
	ctx->sq_end = ctx->iqp->sq.buf + (ctx->sq_wqe_cnt * ctx->iqp->sq.stride);
	mlx5_build_ctrl_metadata(ctx);
	
	//ctx->sq_wqe_cnt = b->size / MLX5_SEND_WQE_BB;
	num_mrs++;

#endif	

	//update local_mr_to_sync idx
	ctx->local_mr_to_sync = find_first_set_bit(ctx->local_mr_ready, MAX_MR);

	printf("Registering msg buffers with size: %lu\n", sizeof(struct message) + sizeof(char)*msg_size);
	for(int i=0; i<MAX_BUFFER; i++) {
		//ctx->msg_send[i] = (struct message*) calloc(1, sizeof(struct message));
		//ctx->msg_rcv[i] = (struct message*) calloc(1, sizeof(struct message));

		if(posix_memalign((void **)&ctx->msg_send[i], sysconf(_SC_PAGESIZE), sizeof(*ctx->msg_send[i])+sizeof(char)*msg_size))
			rc_die("posix_memalign failed");

		ctx->msg_send_mr[i] = ibv_reg_mr(rc_get_pd(ctx->id), ctx->msg_send[i], (sizeof(*ctx->msg_send[i])+sizeof(char)*msg_size),
				IBV_ACCESS_LOCAL_WRITE);

		if(!ctx->msg_send_mr[i])
			rc_die("ibv_reg_mr failed");

		debug_print("registered msg_send_mr[addr:%lx, len:%lu]\n",
				(uintptr_t)ctx->msg_send_mr[i]->addr, ctx->msg_send_mr[i]->length);

		if(posix_memalign((void **)&ctx->msg_rcv[i], sysconf(_SC_PAGESIZE), sizeof(*ctx->msg_rcv[i])+sizeof(char)*msg_size))
			rc_die("posix_memalign failed");

		ctx->msg_rcv_mr[i] = ibv_reg_mr(rc_get_pd(ctx->id), ctx->msg_rcv[i], (sizeof(*ctx->msg_rcv[i])+sizeof(char)*msg_size),
				IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE);

		if(!ctx->msg_rcv_mr[i])
			rc_die("ibv_reg_mr failed");

		//debug_print("CHECK X - [%d] DATA1 %p - DATA2 %p\n", i,
		//		ctx->msg_send[i]->meta.app.data, ctx->msg_send[i]->data);

		debug_print("registered msg_rcv_mr[addr:%lx, len:%lu]\n",
				(uintptr_t)ctx->msg_rcv_mr[i]->addr, ctx->msg_rcv_mr[i]->length);
	}
}

__attribute__((visibility ("hidden"))) 
void mr_remote_update(struct conn_context *ctx, addr_t *addr, addr_t *length, uint32_t *rkey, int mr_count)
{
	for(int i=0; i<mr_count; i++) {
		debug_print("[sockfd %d] updating remote mr #%d with addr:%lu and size:%lu\n", ctx->sockfd, i, addr[i], length[i]);
		ctx->remote_mr[i] = (struct mr_context *)calloc(1, sizeof(struct mr_context));
		ctx->remote_mr[i]->type = i;
		ctx->remote_mr[i]->addr = addr[i];
		ctx->remote_mr[i]->length = length[i];
		ctx->remote_mr[i]->rkey = rkey[i];
		ctx->remote_mr_ready[i] = 1;
	}
}

__attribute__((visibility ("hidden"))) 
void mr_prepare_msg(struct conn_context *ctx, int buffer, int msg_type)
{
	int i = buffer;
	if(msg_type == MSG_MR) {
		int id = mr_next_to_sync(ctx);
		if(!mr_local_ready(ctx, id))
			rc_die("failed to prepare MSG_MR; memory region metadata unavailable");

		ctx->msg_send[i]->id = msg_type;
		ctx->msg_send[i]->meta.mr.type = id;
		ctx->msg_send[i]->meta.mr.addr = (uintptr_t)ctx->local_mr[id]->addr;
		ctx->msg_send[i]->meta.mr.length = ctx->local_mr[id]->length;
		ctx->msg_send[i]->meta.mr.rkey = ctx->local_mr[id]->rkey;
	}
	else
		rc_die("failed to prepare msg; undefined type");
}

__attribute__((visibility ("hidden"))) 
int mr_next_to_sync(struct conn_context *ctx)
{
	int idx = ctx->local_mr_to_sync;

	//debug_print("IDX: %d\n", idx);
	//for(int i = 0 ; i < MAX_MR; i++)
	//	printf("local_mr_ready[%d] = %d\n", i, ctx->local_mr_ready[i]);

	if(!ctx->local_mr_ready[idx])
		rc_die("failed to find mr to sync; invalid local_mr index");

	//find next local_mr_to_sync
	for(int i=idx+1; i<MAX_MR; i++) {
		if(ctx->local_mr_ready[i])
			ctx->local_mr_to_sync = i; 
	}

	ctx->local_mr_to_sync = find_next_set_bit(idx, ctx->local_mr_ready, MAX_MR);

	return idx;
}

uint64_t mr_local_key(int sockfd, int mr_id)
{
	int timeout = 10;
	//printf("fetching local mr metadata for sockfd %d\n", sockfd);
	while(!rc_active(sockfd)) {
		if(timeout == 0)
			rc_die("failed to get local memory address; connection is not active\n");
		debug_print("connection for sockfd %d isn't currently active; sleeping for 1 sec...\n", sockfd);
		timeout--;
		sleep(1);
	}
	
	struct rdma_cm_id *id = get_connection(sockfd);
	struct conn_context *ctx = (struct conn_context *)id->context;
	while(!mr_local_ready(ctx, mr_id)) {
		if(timeout == 0)
			rc_die("failed to get local memory address; no metadata available for region\n");
		debug_print("mr metadata for sockfd %d haven't yet been received; sleeping for 1 sec...\n", sockfd);
		timeout--;
		sleep(1);
	}

	return ctx->local_mr[mr_id]->lkey;
}

uint64_t mr_local_rkey(int sockfd, int mr_id)
{
	int timeout = 10;
	//printf("fetching local mr metadata for sockfd %d\n", sockfd);
	while(!rc_active(sockfd)) {
		if(timeout == 0)
			rc_die("failed to get local memory address; connection is not active\n");
		debug_print("connection for sockfd %d isn't currently active; sleeping for 1 sec...\n", sockfd);
		timeout--;
		sleep(1);
	}
	
	struct rdma_cm_id *id = get_connection(sockfd);
	struct conn_context *ctx = (struct conn_context *)id->context;
	while(!mr_local_ready(ctx, mr_id)) {
		if(timeout == 0)
			rc_die("failed to get local memory address; no metadata available for region\n");
		debug_print("mr metadata for sockfd %d haven't yet been received; sleeping for 1 sec...\n", sockfd);
		timeout--;
		sleep(1);
	}

	return ctx->local_mr[mr_id]->rkey;
}



uint64_t mr_local_addr(int sockfd, int mr_id)
{
	int timeout = 10;
	//printf("fetching local mr metadata for sockfd %d\n", sockfd);
	while(!rc_active(sockfd)) {
		if(timeout == 0)
			rc_die("failed to get local memory address; connection is not active\n");
		debug_print("connection for sockfd %d isn't currently active; sleeping for 1 sec...\n", sockfd);
		timeout--;
		sleep(1);
	}
	
	struct rdma_cm_id *id = get_connection(sockfd);
	struct conn_context *ctx = (struct conn_context *)id->context;
	while(!mr_local_ready(ctx, mr_id)) {
		if(timeout == 0)
			rc_die("failed to get local memory address; no metadata available for region\n");
		debug_print("mr metadata for sockfd %d haven't yet been received; sleeping for 1 sec...\n", sockfd);
		timeout--;
		sleep(1);
	}

	return (uintptr_t) ctx->local_mr[mr_id]->addr;
}

uint64_t mr_remote_key(int sockfd, int mr_id)
{
	int timeout = 10;
	//debug_print("fetching remote mr metadata\n");
	while(!rc_active(sockfd)) {
		if(timeout == 0)
			rc_die("failed to get remote memory address; connection is not active\n");
		debug_print("connection for sockfd %d isn't currently active; sleeping for 1 sec...\n", sockfd);
		timeout--;
		sleep(1);
	}
	
	struct rdma_cm_id *id = get_connection(sockfd);
	struct conn_context *ctx = (struct conn_context *)id->context;
	while(!mr_remote_ready(ctx, mr_id)) {
		if(timeout == 0)
			rc_die("failed to get remote memory address; no metadata available for region\n");
		debug_print("mr metadata for sockfd %d haven't yet been received; sleeping for 1 sec...\n", sockfd);
		timeout--;
		sleep(1);
	}

	return ctx->remote_mr[mr_id]->rkey;
}

uint64_t mr_remote_addr(int sockfd, int mr_id)
{
	int timeout = 10;
	//debug_print("fetching remote mr metadata\n");
	while(!rc_active(sockfd)) {
		if(timeout == 0)
			rc_die("failed to get remote memory address; connection is not active\n");
		debug_print("connection for sockfd %d isn't currently active; sleeping for 1 sec...\n", sockfd);
		timeout--;
		sleep(1);
	}
	
	struct rdma_cm_id *id = get_connection(sockfd);
	struct conn_context *ctx = (struct conn_context *)id->context;
	while(!mr_remote_ready(ctx, mr_id)) {
		if(timeout == 0)
			rc_die("failed to get remote memory address; no metadata available for region\n");
		debug_print("mr metadata for sockfd %d haven't yet been received; sleeping for 1 sec...\n", sockfd);
		timeout--;
		sleep(1);
	}

	return ctx->remote_mr[mr_id]->addr;
}

int mr_get_sq_idx(int sockfd)
{
	int timeout = 10;
	//debug_print("fetching remote mr metadata\n");
	while(!rc_active(sockfd)) {
		if(timeout == 0)
			rc_die("failed to get remote memory address; connection is not active\n");
		debug_print("connection for sockfd %d isn't currently active; sleeping for 1 sec...\n", sockfd);
		timeout--;
		sleep(1);
	}
	
	struct rdma_cm_id *id = get_connection(sockfd);
	struct conn_context *ctx = (struct conn_context *)id->context;

	return ctx->sq_mr_idx;
}

struct ibv_mr * register_wq(int wq_sock, int pd_sock)
{
	struct ibv_mr *local_mr;
 	struct rdma_cm_id *wq_id = get_connection(wq_sock);
	struct rdma_cm_id *pd_id = get_connection(pd_sock);

#ifdef MODDED_DRIVER
	struct ibv_wq_buffer *b = ibv_ex_get_wq_buffer(wq_id->qp, 0);
	local_mr = ibv_reg_mr(rc_get_pd(pd_id), b->address, b->size, 
				IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_REMOTE_ATOMIC | IBV_ACCESS_REMOTE_READ );
#else
	struct conn_context *ctx = (struct conn_context *)wq_id->context;
	local_mr = ibv_reg_mr(rc_get_pd(pd_id), ctx->sq_start, (ctx->sq_wqe_cnt * ctx->iqp->sq.stride), 
				IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_REMOTE_ATOMIC | IBV_ACCESS_REMOTE_READ );
#endif

	return local_mr;
}

