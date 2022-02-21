# Makefile template for shared library

CC = gcc -g -D_GNU_SOURCE -o0 # C compiler
LIBMLX5_DIR := $(abspath $(CURDIR)/../../lib/libmlx5)
RDMA_DIR := $(abspath $(CURDIR)/../../build/)
INCLUDES  := -I$(RDMA_DIR)/../src/rdma/ -I$(LIBMLX5_DIR)/src/ #-I$(RDMA_DIR)/lib/libibverbs/include -I.

LDLIBS = -lrdma -lm -pthread -lrt
LDFLAGS += -Wl,-rpath=$(abspath $(RDMA_DIR)) -L$(RDMA_DIR) 

RM = rm -f  # rm command
DEPS = time_stat.c
#SRCS = rpc_client.c rpc_server.c cc_lat.c cc_bw.c if_lat.c if_bw.c calc_lat.c noop_lat.c atm_lat.c atm_bw.c write_lat.c write_bw.c read_lat.c loop_lat.c loop_bw.c hash_get.c fork_test.c lock_test.c chain_lat.c chain_bw.c if_cas_bw.c if_copy_bw.c hash_cas_bw.c hash_copy_bw.c test.c test2.c test3.c cqe_err.c example_test.c example_cas_test.c
SRCS = hash_bench.c linkedlist_bench.c
OBJS = $(SRCS:.c=.o)
OBJS += $(DEPS:.c=.o)
BIN = $(SRCS:.c=)

.PHONY: clean

all: $(BIN)

$(BIN):$(OBJS)
		$(CC) $@.o time_stat.o ${LDFLAGS} ${LDLIBS} -o $@

%.o: %.c
		$(CC) $(INCLUDES) -c $(CFLAGS) -o $@ $<

clean:
		rm -f $(OBJS) $(BIN)
