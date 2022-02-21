#ifndef RDMA_MESSAGING_H
#define RDMA_MESSAGING_H

#include <stddef.h>
#include <stdint.h>

#include "common.h"
#include "utils.h"
#include "mr.h"

enum message_id
{
	MSG_INVALID = 0,
	MSG_INIT,
	MSG_MR,
	MSG_READY,
	MSG_DONE,
	MSG_CUSTOM
};

struct message
{
	int id;

	union
	{
		struct mr_context mr;
		struct app_context app;
	} meta;

	char data[];
};

int create_message(struct rdma_cm_id *id, int msg_type, int value);

#endif
