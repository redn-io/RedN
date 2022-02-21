#ifndef RDMA_GLOBALS_H
#define RDMA_GLOBALS_H

#include <stdio.h>
#include <assert.h>
#include <inttypes.h>

#define IBV_INLINE_THRESHOLD 128
#define MAX_DEVICES 2 // max # of RDMA devices/ports
#define MAX_CONNECTIONS 1000 // max # of RDMA connections per peer
#define MAX_MR 2 // max # of memory regions per connection (XXX keep this value at 2 or lower for now)
#define MAX_PENDING 500 // max # of pending app responses per connection
#define MAX_BUFFER 1 // max # of msg buffers per connection
#define MAX_SEND_QUEUE_SIZE 1024 // depth of rdma send queue
#define MAX_RECV_QUEUE_SIZE 1024 // depth of rdma recv queue

// max # of rdma operations that can be batched together
// must be < MAX_SEND_QUEUE_SIZE
#define MAX_BATCH_SIZE 50

typedef uint64_t addr_t;

#endif
