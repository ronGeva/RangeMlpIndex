#include "benchmark_mlp.h"

double bm_duration_passed_ms(struct timespec* start, struct timespec* end)
{
	double start_time_ms = start->tv_sec * 1000 + ((double)start->tv_nsec / 1000000);
	double end_time_ms = end->tv_sec * 1000 + ((double)end->tv_nsec / 1000000);
	double duration_ms = end_time_ms - start_time_ms;

	return duration_ms;
}
