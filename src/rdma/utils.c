#include "utils.h"

unsigned int g_seed;

#if 0

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

#endif
