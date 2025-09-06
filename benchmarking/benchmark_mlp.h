#pragma once

#include <time.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef struct _BenchmarkTree {
    // Inserts an entry into the range [key, key].
    int (*Insert) (void* tree, unsigned long long key, void* entry);

    // Inserts an entry into the range [first, last].
    int (*InsertRange) (void* tree, unsigned long first,
		                unsigned long last, void *entry);

    // Finds the first entry between index and max.
    // Assigns the first value beyond the found range into index.
    void* (*Find) (void* tree, unsigned long long* index, unsigned long long max);

    // loads the entry at index
    void* (*Load) (void* tree, unsigned long long index);

    // Erases the range that contains index
    void* (*Erase) (void* tree, unsigned long long index);

    void* tree;
} BenchmarkTree;

typedef enum _BenchmarkOperationType {
    BenchmarkOpInsert,
    BenchmarkOpInsertRange,
    BenchmarkOpFind,
    BenchmarkOpLoad,
    BenchmarkOpErase,
} BenchmarkOperationType;

typedef struct _BenchmarkOperation {
    BenchmarkOperationType type;
    union {
        struct {
            unsigned long long insert_key;
            void* insert_entry;
        };
        struct {
            unsigned long long insert_range_first;
            unsigned long long insert_range_last;
            void* insert_range_entry;
        };
        struct {
            unsigned long long find_index;
            unsigned long long find_max;
        };
        struct {
            unsigned long long load_index;
        };
        struct {
            unsigned long long erase_index;
        };
    };
} BenchmarkOperation;

double bm_duration_passed_ms(struct timespec* start, struct timespec* end);

typedef int(*InsertFunc)(void* object, unsigned long long key);

void bm_add_on_item_mlpindex(void* object, InsertFunc func, unsigned long long key);

void bm_run_benchmarks(BenchmarkTree* tree, BenchmarkOperation* operations,
                       int operation_count, char* benchmark_name);

void bm_run_workloadA(BenchmarkTree* tree);

void bm_run_workloadB(BenchmarkTree* tree);

void bm_run_workloadC(BenchmarkTree* tree);

#ifdef __cplusplus
}
#endif
