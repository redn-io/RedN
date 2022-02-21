#ifndef RDMA_VERBS_H
#define RDMA_VERBS_H

#include <limits.h>
#include <rdma/rdma_cma.h>
#include <infiniband/verbs.h>


//#include "connection.h"
//#include "mr.h"
#include "utils.h"
#include "globals.h"
#include "common.h"

//#define IBV_INLINE_THRESHOLD 128

#define IBV_TEXT(STR) #STR
#define IBV_ENUM(PREFIX) IBV_WR_ ## PREFIX
#define IBV_STR(PREFIX, SUFFIX) IBV_TEXT(PREFIX) IBV_TEXT(SUFFIX)
#define IBV_WITHIN_MR_RANGE(inner, outer) range_valid((addr_t)inner->addr, inner->length, (addr_t)outer->addr, outer->length)

#define IBV_WRAPPER_HEADER(x)	 uint32_t IBV_WRAPPER_ ## x ## _ASYNC(int sockfd, rdma_meta_t *meta,\
					int local_id, int remote_id);\
				 void IBV_WRAPPER_ ## x ## _SYNC(int sockfd, rdma_meta_t *meta,\
					int local_id, int remote_id);\
				 void IBV_WRAPPER_ ## x ## _ASYNC_ALL(rdma_meta_t *meta,\
					int local_id, int remote_id);\
				 void IBV_WRAPPER_ ## x ## _SYNC_ALL(rdma_meta_t *meta,\
					int local_id, int remote_id);

#define IBV_WRAPPER_FUNC(x)      uint32_t IBV_WRAPPER_ ## x ## _ASYNC(int sockfd, rdma_meta_t *meta,\
					int local_id, int remote_id){\
					IBV_WRAPPER_OP_ASYNC(sockfd, meta, local_id, remote_id, IBV_ENUM(x));}\
				 void IBV_WRAPPER_ ## x ## _SYNC(int sockfd, rdma_meta_t *meta,\
					int local_id, int remote_id) {\
				 	IBV_WRAPPER_OP_SYNC(sockfd, meta, local_id, remote_id, IBV_ENUM(x));}\
				 void IBV_WRAPPER_ ## x ## _ASYNC_ALL(rdma_meta_t *meta,\
					int local_id, int remote_id) {\
					IBV_WRAPPER_OP_ASYNC_ALL(meta, local_id, remote_id, IBV_ENUM(x));}\
				 void IBV_WRAPPER_ ## x ## _SYNC_ALL(rdma_meta_t *meta,\
						int local_id, int remote_id) {\
					IBV_WRAPPER_OP_SYNC_ALL(meta, local_id, remote_id, IBV_ENUM(x));}

// TODO: merge with IBV_WRAPPER_* functions
#define IBV_CALC_ENUM(PREFIX) IBV_EXP_CALC_OP_ ## PREFIX

#define IBV_CALC_HEADER(x)	uint32_t IBV_CALC_ ## x ## _ASYNC(int sockfd, rdma_meta_t *meta,\
						int local_id, int remote_id);\
				void IBV_CALC_ ## x ## _SYNC(int sockfd, rdma_meta_t *meta,\
						int local_id, int remote_id);


#define IBV_CALC_FUNC(x)	uint32_t IBV_CALC_ ## x ## _ASYNC(int sockfd, rdma_meta_t *meta,\
						int local_id, int remote_id){\
					IBV_CALC_OP_ASYNC(sockfd, meta, local_id, remote_id, IBV_CALC_ENUM(x));}\
				void IBV_CALC_ ## x ## _SYNC(int sockfd, rdma_meta_t *meta,\
						int local_id, int remote_id){\
					IBV_CALC_OP_SYNC(sockfd, meta, local_id, remote_id, IBV_CALC_ENUM(x));}

//#ifdef EXP_VERBS
IBV_CALC_HEADER(MAXLOC)
IBV_CALC_HEADER(BXOR)
IBV_CALC_HEADER(BOR)
IBV_CALC_HEADER(BAND)
IBV_CALC_HEADER(ADD)
//#endif

IBV_WRAPPER_HEADER(SEND)
IBV_WRAPPER_HEADER(SEND_WITH_IMM)
IBV_WRAPPER_HEADER(RDMA_READ)
IBV_WRAPPER_HEADER(RDMA_WRITE)
IBV_WRAPPER_HEADER(RDMA_WRITE_WITH_IMM)

static inline int range_valid(addr_t inner_addr, addr_t inner_len,
		addr_t outer_addr, addr_t outer_len)
{
	if((inner_addr + inner_len > outer_addr + outer_len)
			|| inner_addr < outer_addr) {
#ifdef DEBUG
		char ineq_str[2];
		if(inner_addr < outer_addr)
			ineq_str[0] = '<';
		else if(inner_addr == outer_addr)
			ineq_str[0] = '=';
		else
			ineq_str[0] = '>';

		if(inner_addr + inner_len > outer_addr + outer_len)
			ineq_str[1] = '>';
		else if(inner_addr + inner_len == outer_addr + outer_len)
			ineq_str[1] = '=';
		else
			ineq_str[1] = '<';

		debug_print("inner_start[%lx] %c outer_start[%lx] | inner_end[%lx] %c outer_end[%lx]\n",
				inner_addr, ineq_str[0], outer_addr, inner_addr + inner_len, ineq_str[1],
				outer_addr + outer_len);
#endif
		return 0;
	}
	else
		return 1;
}

static inline char* stringify_verb(int opcode)
{
	switch(opcode) {
		case IBV_WR_RDMA_WRITE:
		       return "RDMA_WRITE";
		       break;
		case IBV_WR_RDMA_WRITE_WITH_IMM:
		       return "RDMA_WRITE_IMM";
		       break;
		case IBV_WR_SEND:
		       return "RDMA_SEND";
		       break;
		case IBV_WR_SEND_WITH_IMM:
		       return "RDMA_SEND_IMM";
		       break;
		case IBV_WR_RDMA_READ:
		       return "RDMA_READ";
		       break;
		case IBV_WR_ATOMIC_FETCH_AND_ADD:
		       return "ATOMIC_FETCH_AND_ADD";
		       break;
		case IBV_WR_ATOMIC_CMP_AND_SWP:
		       return "ATOMIC_CMP_AND_SWP";
		       break;
		default:
		       //printf("enum WAIT %d provided enum %d\n", IBV_EXP_WR_CQE_WAIT, opcode);
		       return "UNDEFINED";
	}
}

#ifdef EXP_VERBS
static inline char* stringify_exp_verb(int opcode)
{
	switch(opcode) {
		case IBV_EXP_WR_RDMA_WRITE:
		       return "[EXP] RDMA_WRITE";
		       break;
		case IBV_EXP_WR_RDMA_WRITE_WITH_IMM:
		       return "[EXP] RDMA_WRITE_IMM";
		       break;
		case IBV_EXP_WR_SEND:
		       return "[EXP] RDMA_SEND";
		       break;
		case IBV_EXP_WR_SEND_WITH_IMM:
		       return "[EXP] RDMA_SEND_IMM";
		       break;
		case IBV_EXP_WR_RDMA_READ:
		       return "[EXP] RDMA_READ";
		       break;
		case IBV_EXP_WR_ATOMIC_FETCH_AND_ADD:
		       return "[EXP] ATOMIC_FETCH_AND_ADD";
		       break;
		case IBV_EXP_WR_ATOMIC_CMP_AND_SWP:
		       return "[EXP] ATOMIC_CMP_AND_SWP";
		       break;
		case IBV_EXP_WR_CQE_WAIT:
		       return "[EXP] CQE_WAIT";
		       break;
		default:
		       return "UNDEFINED";
	}
}
#endif

static inline int op_one_sided(int opcode)
{
	if((opcode == IBV_WR_RDMA_READ) || (opcode == IBV_WR_RDMA_WRITE)
		       || (opcode == IBV_WR_RDMA_WRITE_WITH_IMM)) {
		return 1;
	}
	else
		return 0;
}

//increments last work request id for a specified connection
//send == 0 --> wr type is receive
//send == 1 --> wr type is send
static inline uint32_t next_wr_id(struct conn_context *ctx, int send)
{
	//we maintain seperate wr_ids for send/rcv queues since
	//there is no ordering between their work requests
	if(send) {
		if(send >= 1) {
			return __sync_add_and_fetch(&ctx->last_send, 0x00000001); 
			//return ++ctx->last_send;
		}
#if 0
		else if(send == 2) {
			ctx->last_send++;
			ctx->last_msg = ctx->last_send;
			return ctx->last_send;
		}
#endif
		//else
		//	rc_die("undefined 'send' flag");
			
	}
	else
		return ++ctx->last_rcv;

	return 0;
}

#if 0
// get the size of the input wqe
static inline uint32_t get_wqe_size(struct ibv_exp_send_wr *wr)
{
	uint32_t size = 0;
	int opcode = wr->exp_opcode;

	// add control segment
	size += sizeof(struct mlx5_wqe_ctrl_seg);

	switch(opcode) {
		case IBV_EXP_WR_RDMA_READ:
		case IBV_EXP_WR_RDMA_WRITE:
		case IBV_EXP_WR_RDMA_WRITE_WITH_IMM:
			seg  += sizeof(struct mlx5_wqe_raddr_seg);
			break;
		case IBV_EXP_WR_SEND:
		case IBV_EXP_WR_SEND_WITH_IMM:
		       break;
		case IBV_EXP_WR_ATOMIC_FETCH_AND_ADD:
		case IBV_EXP_WR_ATOMIC_CMP_AND_SWP:
			size += sizeof(struct mlx5_wqe_raddr_seg);
			size += sizeof(struct mlx5_wqe_atomic_seg); 
			break;
		case IBV_EXP_WR_CQE_WAIT:
			size += sizeof(struct mlx5_wqe_wait_en_seg);
			break;
		default:
			break;
	}
}
#endif

uint32_t IBV_NEXT_WR_ID(int sockfd);
uint32_t IBV_NEXT_WR_IDX(int sockfd, int inc);
struct ibv_cq * IBV_GET_CQ(int sockfd);
struct wqe_ctrl_seg * IBV_FIND_WQE(int sockfd, uint32_t wr_id);
struct wqe_ctrl_seg * IBV_GET_WQE(int sockfd, uint32_t idx);


//verb wrappers
void IBV_WRAPPER_OP_SYNC_ALL(rdma_meta_t *meta, int local_id, int remote_id, int opcode);
void IBV_WRAPPER_OP_ASYNC_ALL(rdma_meta_t *meta, int local_id, int remote_id, int opcode);
void IBV_WRAPPER_OP_SYNC(int sockfd, rdma_meta_t *meta, int local_id, int remote_id, int opcode);
uint32_t IBV_WRAPPER_OP_ASYNC(int sockfd, rdma_meta_t *meta, int local_id, int remote_id, int opcode);
uint32_t IBV_SEND_ASYNC(int sockfd, addr_t src, addr_t size, uint32_t imm, int local_id);
void IBV_SEND_SYNC(int sockfd, addr_t src, addr_t size, uint32_t imm, int local_id);
uint32_t IBV_POST_ASYNC(int sockfd, struct ibv_send_wr *wr);
void IBV_POST_SYNC(int sockfd, struct ibv_send_wr *wr);
//#ifdef EXP_VERBS
uint32_t IBV_EXP_POST_ASYNC(int sockfd, struct ibv_exp_send_wr *wr);
void IBV_EXP_POST_SYNC(int sockfd, struct ibv_exp_send_wr *wr);
//#endif
void IBV_RECEIVE_IMM(int sockfd);
void IBV_RECEIVE(int sockfd, addr_t addr, addr_t size, int local_id);
void IBV_RECEIVE_ANY(int sockfd, addr_t addr, addr_t size, uint64_t lkey);
void IBV_RECEIVE_SG(int sockfd, rdma_meta_t *meta, uint64_t lkey);

//messaging wrappers
void IBV_WRAPPER_SEND_MSG_SYNC(int sockfd, int buffer_id, int solicit);
uint32_t IBV_WRAPPER_SEND_MSG_ASYNC(int sockfd, int buffer_id, int solicit);
void IBV_RECEIVE_MSG(int sockfd, int buffer);

//Advanced
//#ifdef EXP_VERBS
uint32_t IBV_TRIGGER(int msockfd, int sockfd, int count);
uint32_t IBV_TRIGGER_EXPLICIT(int msockfd, int sockfd, int count);
void IBV_SET_TRIGGER(int msockfd, int sockfd, int ntriggers, int nwait);
uint32_t IBV_WAIT_EXPLICIT(int msockfd, int sockfd, uint32_t count);
uint32_t IBV_WAIT(int msockfd, int sockfd);
uint32_t IBV_WAIT_TILL(int msockfd, int sockfd, uint32_t count);
//#endif
uint32_t IBV_CAS_ASYNC(int sockfd, addr_t src, addr_t dst, addr_t compare, addr_t swap, uint64_t lkey, uint64_t rkey, int fence);
void IBV_CAS_SYNC(int sockfd, addr_t src, addr_t dst, addr_t compare, addr_t swap, uint64_t lkey, uint64_t rkey, int fence);
uint32_t IBV_FETCH_ADD_ASYNC(int sockfd, addr_t src, addr_t dst, addr_t size, addr_t cmp, uint64_t lkey, uint64_t rkey);
void IBV_FETCH_ADD_SYNC(int sockfd, addr_t src, addr_t dst, addr_t size, addr_t cmp, uint64_t lkey, uint64_t rkey);
uint32_t IBV_CONVERT_ENDIAN_ASYNC(int sockfd, addr_t src, addr_t dst, addr_t size, uint64_t lkey, uint64_t rkey);
void IBV_CONVERT_ENDIAN_SYNC(int sockfd, addr_t src, addr_t dst, addr_t size, uint64_t lkey, uint64_t rkey);
uint32_t IBV_NOOP_ASYNC(int sockfd, int signaled);
void IBV_NOOP_SYNC(int sockfd, int signaled);
//#ifdef EXP_VERBS
uint32_t IBV_CALC_OP_ASYNC(int sockfd, rdma_meta_t *meta, int local_id, int remote_id, int opcode);
void IBV_CALC_OP_SYNC(int sockfd, rdma_meta_t *meta, int local_id, int remote_id, int opcode);
//#endif

//WR creations
//#ifdef EXP_VERBS
struct ibv_exp_send_wr * ibv_create_exp_wait_wr(int sendQ, int listenQ, int n_wait, int last);
struct ibv_exp_send_wr * ibv_create_exp_send_wr(int sendQ, int opcode, int local, int remote, addr_t size);
//#endif

//verb waiting
void IBV_AWAIT_RESPONSE(int sockfd, uint32_t app_id);
void IBV_AWAIT_RESPONSE_NOTIFY(int sockfd, uint32_t app_id);
void IBV_AWAIT_WORK_COMPLETION(int sockfd, uint32_t wr_id);
void IBV_AWAIT_WORK_COMPLETION_NOTIFY(int sockfd, uint32_t wr_id);
void IBV_AWAIT_PENDING_WORK_COMPLETIONS(int sockfd);

//pending messages
void register_pending(struct rdma_cm_id *id, uint32_t app_id);
void remove_pending(struct rdma_cm_id *id, struct app_response *p);
void update_pending(struct rdma_cm_id *id, uint32_t app_id);

//basic post operations
uint32_t send_rdma_operation(struct rdma_cm_id *id, rdma_meta_t *meta, int local_id, int remote_id, int opcode);
uint32_t send_message(struct rdma_cm_id *id, int buffer_id);
void receive_message(struct rdma_cm_id *id, int buffer_id);
void receive_imm(struct rdma_cm_id *id, int buffer_id);

#endif
