#include <stdio.h>
#include <time.h>
#include <sys/time.h>

#ifdef __cplusplus
extern "C" {
#endif

struct time_stats {
#if 0
   struct timeval time_start;
#else
   struct timespec time_start;
#endif
   int n, count;
   double* time_v;
   volatile int barrier;
};

void time_stats_init(struct time_stats*, int);
void time_stats_start(struct time_stats*);
void time_stats_stop(struct time_stats*);
void time_stats_print(struct time_stats*, char*);
double time_stats_get_avg(struct time_stats*);

#ifdef __cplusplus
}
#endif
