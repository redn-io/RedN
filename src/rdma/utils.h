#ifndef RDMA_UTILS_H
#define RDMA_UTILS_H

#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <fcntl.h>
#include <pthread.h>
#include <unistd.h>
#include <fcntl.h>
#include <libgen.h>
#include <inttypes.h>
#include <semaphore.h>
#include <sys/syscall.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <stdarg.h>

#define get_tid() syscall(__NR_gettid)
//DEBUG macros
#ifdef DEBUG
 #define debug_print(fmt, args...) fprintf(stderr, "DEBUG[tid:%lu][%s:%d]: " fmt, \
		     	 	get_tid(), __FILE__, __LINE__, ##args)
#else
 #define debug_print(fmt, args...) /*  Don't do anything in release builds */
#endif

/*
 * min()/max()/clamp() macros that also do
 * strict type-checking.. See the
 * "unnecessary" pointer comparison.
 */
#define min(a, b) ({\
		__typeof__(a) _a = a;\
		__typeof__(b) _b = b;\
		_a < _b ? _a : _b; })

#define max(a, b) ({\
		__typeof__(a) _a = a;\
		__typeof__(b) _b = b;\
		_a > _b ? _a : _b; })

#define ibw_cpu_relax() __asm__ volatile("pause\n": : :"memory")

#define ibw_cmpxchg(P, O, N) __sync_val_compare_and_swap((P), (O), (N))

#define ibw_unused(expr) do { (void)(expr); } while (0)

extern unsigned int g_seed;

inline void set_seed(int seed) {
	g_seed = seed;
}

inline int fastrand(int seed) { 
	seed = (214013*seed+2531011); 
	return (seed>>16)&0x7FFF; 
}

static inline unsigned DIV_ROUND_UP(unsigned n, unsigned d)
{
	return (n + d - 1u) / d;
}

__attribute__((visibility ("hidden"))) 
inline int cmp_counters(uint32_t a, uint32_t b) {
	if (a == b)
		return 0;
	else if((a - b) < UINT32_MAX/2)
		return 1;
	else
		return -1;
}

__attribute__((visibility ("hidden"))) 
inline int diff_counters(uint32_t a, uint32_t b) {
	if (a >= b)
		return a - b;
	else
		return b - a;
}

inline int find_first_empty_bit_and_set(int bitmap[], int n)
{
	for(int i=0; i<n; i++) {
		if(!ibw_cmpxchg(&bitmap[i], 0, 1))
			return i;
	}
	return -1;							
}

inline int find_first_empty_bit(int bitmap[], int n)
{
	for(int i=0; i<n; i++) {
		if(!bitmap[i])
			return i;
	}
	return -1;
}

inline int find_next_empty_bit(int idx, int bitmap[], int n)
{
	for(int i=idx+1; i<n; i++) {
		if(!bitmap[i])
			return i;
	}
	return -1;
}

inline int find_first_set_bit_and_empty(int bitmap[], int n)
{
	for(int i=0; i<n; i++) {
		if(ibw_cmpxchg(&bitmap[i], 1, 0))
			return i;
	}
	return -1;							
}

inline int find_first_set_bit(int bitmap[], int n)
{
	for(int i=0; i<n; i++) {
		if(bitmap[i])
			return i;
	}
	return -1;
}

inline int find_next_set_bit(int idx, int bitmap[], int n)
{
	for(int i=idx+1; i<n; i++) {
		if(bitmap[i])
			return i;
	}
	return -1;
}

inline int find_bitmap_weight(int bitmap[], int n)
{
	int weight = 0;
	for(int i=0; i<n; i++) {
		weight += bitmap[i];
	}
	return weight;
}

#if 0
// core_id = 0, 1, ... n-1, where n is the system's number of cores
__attribute__((visibility ("hidden"))) 
inline int stick_this_thread_to_core(int core_id) {
	int num_cores = sysconf(_SC_NPROCESSORS_ONLN);
	if (core_id < 0 || core_id >= num_cores)
		return EINVAL;

	cpu_set_t cpuset;
	CPU_ZERO(&cpuset);
	CPU_SET(core_id, &cpuset);

	pthread_t current_thread = pthread_self();    
	return pthread_setaffinity_np(current_thread, sizeof(cpu_set_t), &cpuset);
}
#endif

__attribute__((visibility ("hidden"))) 
inline struct sockaddr_in * copy_ipv4_sockaddr(struct sockaddr_storage *in)
{
	if(in->ss_family == AF_INET) {
		struct sockaddr_in *out = (struct sockaddr_in *) calloc(1, sizeof(struct sockaddr_in));
		memcpy(out, in, sizeof(struct sockaddr_in));
		return out;
	}
	else {
		return NULL;
	}
}

#endif
