#pragma once

#include <time.h>

#ifdef __cplusplus
extern "C" {
#endif

double bm_duration_passed_ms(struct timespec* start, struct timespec* end);

#ifdef __cplusplus
}
#endif
