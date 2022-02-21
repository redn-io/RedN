#ifndef LOCK_BENCH_H
#define LOCK_BENCH_H

#ifdef __cplusplus
extern "C" {
#endif

struct __attribute__((__packed__)) ll_bucket {
	uint8_t key[3];
	uint64_t addr;
	uint64_t next;
	uint64_t value[131072]; //XXX inline values for now
};


#ifdef __cplusplus
}
#endif

#endif
