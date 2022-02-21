#include "time_stat.h"
#include <time.h>
#include <sys/time.h>
#include <math.h>
#include <stdlib.h>

void time_stats_init(struct time_stats* ts, int n)
{
   ts->time_v = (double *)malloc(sizeof(double)*n);
   ts->n = n;
   ts->count = 0;
   ts->barrier = 0;
}

inline void time_stats_start(struct time_stats* ts)
{
#if 0
   gettimeofday(&ts->time_start, NULL);
#else
   clock_gettime(CLOCK_MONOTONIC, &ts->time_start);
   if(ts->barrier != 0)
	   exit(-1);
#endif
}

inline void time_stats_stop(struct time_stats* ts)
{
#if 0
   struct timeval end;
   gettimeofday(&end, NULL);

   struct timeval t_elap;
   timersub(&end,&ts->time_start,&t_elap);

   double sec = (double)(t_elap.tv_sec * 1000000.0 + (double)t_elap.tv_usec) / 1000000.0;
#else
   struct timespec end;
   clock_gettime(CLOCK_MONOTONIC, &end);
   double start_sec = (double)(ts->time_start.tv_sec * 1000000000.0 + (double)ts->time_start.tv_nsec) / 1000000000.0;
   double end_sec = (double)(end.tv_sec * 1000000000.0 + (double)end.tv_nsec) / 1000000000.0;

   double sec = end_sec - start_sec;
#endif

   ts->time_v[ts->count++] = sec;

   if(ts->barrier != 0)
	   exit(-1);
}

double time_stats_get_avg(struct time_stats* ts)
{
  double sum = 0.0;
  for (int i = 0; i < ts->n; i++) {
    sum += ts->time_v[i];
  }
  return (double) sum / ts->n;
}

static int compare_latency(const void *a, const void *b) {
	if ((*(double *)a) > (*(double *)b)) {
		return 1;
	} else if (*(double *)a == *(double *)b) {
		return 0;
	} else 
		return -1;
}

void time_stats_print(struct time_stats* ts, char* msg)
{
   double sum = 0.0;
   double min = 0.0, max = 0.0, lat_50 =0.0, lat_99 = 0.0, lat_99_9 = 0.0, lat_99_99, lat_99_999 = 0.0;
   double curlat = 0.0;
   float perc;
   int _50, _99, _99_9, _99_99, _99_999;

   _50 = (int)((float)ts->count * 0.5);
   _99 = (int)((float)ts->count * 0.99);
   _99_9 = (int)((float)ts->count * 0.999);
   _99_99 = (int)((float)ts->count * 0.9999);
   _99_999 = (int)((float)ts->count * 0.99999);

   qsort(ts->time_v, ts->count, sizeof(double), compare_latency);

   for (int i = 0; i < ts->count; i++) {
	   //printf("time %d: %f usec \n", i+1, ts->time_v[i] * 1000000.0);
	   /*
	   if (ts->time_v[i] != curlat || i == (ts->count - 1)) {
		   curlat = ts->time_v[i];
		   perc = ((float)(i+1)*100)/ts->count;
		   printf("%.2f%% <= %d milliseconds\n", perc, curlat);
	   }
	   */

	   if (i == _50)
		   lat_50 = ts->time_v[i];
	   else if (i == _99)
		   lat_99 = ts->time_v[i];
	   else if (i == _99_9)  
		   lat_99_9 = ts->time_v[i];
	   else if (i == _99_99)  
		   lat_99_99 = ts->time_v[i];
	   else if (i == _99_999)
		   lat_99_999 = ts->time_v[i];

	   sum += ts->time_v[i];

	   if(ts->time_v[i] > max){
		   max = ts->time_v[i];
	   }

	   if(ts->time_v[i] < min || min == 0.0) {
		   min = ts->time_v[i];
	   }

	   //printf("latency[%d]: %.2f usec\n", i, ts->time_v[i] * 1000000.0);

   }
   double avg = sum / ts->count;

   double sum1 = 0.0;
   for (int i = 0; i < ts->count; i++){
	   sum1 += pow((ts->time_v[i] - avg), 2);
   }

   double variance = sum1 / ts->count;
   double std = sqrt(variance);

   printf("%s\n", msg);
   printf("\tcount %d\n", ts->count);
   printf("\tavg: %.3f msec (%.2f usec)\n", avg * 1000.0, avg * 1000000.0);
   printf("\tmin: %.3f msec (%.2f usec)\n", min * 1000.0, min * 1000000.0);
   printf("\tmax: %.3f msec (%.2f usec)\n", max * 1000.0, max * 1000000.0);
   printf("\tstd: %.3f msec (%.2f usec)\n", std * 1000.0, std * 1000000.0);
   printf("\t50 percentile    : %.3f msec (%.2f usec)\n", lat_50 * 1000.0, lat_50 * 1000000.0);
   printf("\t99 percentile    : %.3f msec (%.2f usec)\n", lat_99 * 1000.0, lat_99 * 1000000.0);
   printf("\t99.9 percentile  : %.3f msec (%.2f usec)\n", lat_99_9 * 1000.0, lat_99_9 * 1000000.0);
   //printf("\t99.99 percentile : %.3f msec (%.2f usec)\n", lat_99_99 * 1000.0, lat_99_99 * 1000000.0);
   //printf("\t99.999 percentile: %.3f msec (%.2f usec)\n", lat_99_999 * 1000.0, lat_99_999 * 1000000.0);
}
