#define _GNU_SOURCE
#include "benchmark_mlp.h"
#include <stdio.h>
#include <sched.h>
#include <pthread.h>

double bm_duration_passed_ms(struct timespec* start, struct timespec* end)
{
	double start_time_ms = start->tv_sec * 1000 + ((double)start->tv_nsec / 1000000);
	double end_time_ms = end->tv_sec * 1000 + ((double)end->tv_nsec / 1000000);
	double duration_ms = end_time_ms - start_time_ms;

	return duration_ms;
}

void bm_add_on_item_mlpindex(void* object, InsertFunc func, unsigned long long key)
{
	func(object, key);
}

static void bm_perform_operation(BenchmarkTree* tree, BenchmarkOperation* operation)
{
	switch (operation->type)
	{
		case BenchmarkOpInsert:
			tree->Insert(tree->tree, operation->insert_key, operation->insert_entry);
			break;
		case BenchmarkOpInsertRange:
			tree->InsertRange(tree->tree, operation->insert_range_first,
							  operation->insert_range_last, operation->insert_range_entry);
			break;
		case BenchmarkOpFind:
			tree->Find(tree->tree, &operation->find_index, operation->find_max);
			break;
		case BenchmarkOpLoad:
			tree->Load(tree->tree, operation->load_index);
			break;
		case BenchmarkOpErase:
			tree->Erase(tree->tree, operation->erase_index);
			break;
		default:
			return; // error
	}
}

// pin the current thread to the current CPU
static void bm_pin_thread_to_current_cpu(void) {
    cpu_set_t cpuset;
	int cpu = sched_getcpu();
    CPU_ZERO(&cpuset);
    CPU_SET(cpu, &cpuset);

    pid_t tid = gettid();  // Linux-specific: get thread ID
    if (sched_setaffinity(tid, sizeof(cpu_set_t), &cpuset) != 0) {
        perror("sched_setaffinity");
        exit(1);
    }
}

void bm_run_benchmarks(BenchmarkTree* tree, BenchmarkOperation* operations,
                       int operation_count, char* benchmark_name)
{
	struct timespec start, end;
	bm_pin_thread_to_current_cpu();

	clock_gettime(CLOCK_MONOTONIC, &start);
	for (int i = 0; i < operation_count; i++)
	{
		BenchmarkOperation* operation = &operations[i];
		bm_perform_operation(tree, operation);
	}
	clock_gettime(CLOCK_MONOTONIC, &end);

	double duration_ms = bm_duration_passed_ms(&start, &end);
	printf("Benchmark %s took %.3f ms\n", benchmark_name, duration_ms);
}

void bm_run_workloadA(BenchmarkTree* tree)
{
	BenchmarkOperation operations[1000];
	for (int i = 0; i < 1000; i++)
	{
		operations[i].type = BenchmarkOpInsert;
		operations[i].insert_key = i;
		operations[i].insert_entry = NULL;
	}

	bm_run_benchmarks(tree, operations, 1000, "A");
}

void bm_run_workloadB(BenchmarkTree* tree)
{
	BenchmarkOperation operations[20000];
	for (int i = 0; i < 10000; i++)
	{
		operations[i].type = BenchmarkOpInsert;
		operations[i].insert_key = i;
		operations[i].insert_entry = NULL;
	}
	for (int i = 10000; i < 20000; i++)
	{
		operations[i].type = BenchmarkOpErase;
		operations[i].erase_index = i - 10000;
	}

	bm_run_benchmarks(tree, operations, 20000, "B");
}
