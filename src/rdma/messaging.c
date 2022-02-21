#include "messaging.h"

//internal messaging protocol
__attribute__((visibility ("hidden"))) 
int create_message(struct rdma_cm_id *id, int msg_type, int value)
{
	struct conn_context *ctx = (struct conn_context *)id->context;

	//for the messaging protocol, we first check that previous message was transmitted
	//before overwriting local send buffer (to avoid corrupting data)

	void *ptr = NULL;
	int i = _rc_acquire_buffer(ctx->sockfd, &ptr, 0);

	if(msg_type == MSG_MR) {
		debug_print("[BOOTSTRAP] creating MSG_MR on buffer[%d]\n", i);
		mr_prepare_msg(ctx, i, msg_type);
	}
	else if(msg_type == MSG_INIT) {
		debug_print("[BOOTSTRAP] creating MSG_INIT on buffer[%d]\n", i);
		ctx->msg_send[i]->id = MSG_INIT;
		if(ctx->app_type >= 0)
			ctx->msg_send[i]->meta.mr.addr = ctx->app_type;
		ctx->msg_send[i]->meta.mr.length = value;
		if(value > MAX_MR)
			rc_die("error; local mr count shouldn't exceed MAX_MR");
	}
	else
		rc_die("create message failed; invalid type");
	return i;
}
