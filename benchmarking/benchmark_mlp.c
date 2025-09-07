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

static void bm_pin_thread_to_cpu(int cpu) {
    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);
    CPU_SET(cpu, &cpuset);

    pid_t tid = gettid();  // Linux-specific: get thread ID
    if (sched_setaffinity(tid, sizeof(cpu_set_t), &cpuset) != 0) {
        perror("sched_setaffinity");
        exit(1);
    }
}

// pin the current thread to the current CPU
static void bm_pin_thread_to_current_cpu(void)
{
	bm_pin_thread_to_cpu(sched_getcpu());
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

typedef struct _WorkLoadRoutineOperations {
	BenchmarkOperation* operations;
	int operation_count;
	BenchmarkTree* tree;
	unsigned long iterations;
} WorkLoadRoutineOperations;

typedef struct _WorkLoadRoutineContext {
	int cpu;
	WorkLoadRoutineOperations* operations;
} WorkLoadRoutineContext;

static void* bm_thread_perform_operations(void* context)
{
	WorkLoadRoutineContext* routine_context = context;
	WorkLoadRoutineOperations* operations = routine_context->operations;
	BenchmarkTree* tree = operations->tree;

	bm_pin_thread_to_cpu(routine_context->cpu);

	for	(int iteration = 0; iteration < operations->iterations; iteration++)
	{
		for (int i = 0; i < operations->operation_count; i++)
		{
			BenchmarkOperation* operation = &operations->operations[i];
			bm_perform_operation(tree, operation);
		}
	}

	return NULL;
}

void bm_run_workloadC(BenchmarkTree* tree)
{
	// create 4 threads, 1 writer and 3 readers
	// The writer inserts 100K elements and then
	// removes them all.
	// The readers query 200K elements.
	BenchmarkOperation* writer_operations = malloc(sizeof(BenchmarkOperation) * 200000);
	for (int i = 0; i < 100000; i++)
	{
		writer_operations[i].type = BenchmarkOpInsert;
		writer_operations[i].insert_key = i;
		writer_operations[i].insert_entry = NULL;
	}
	for (int i = 100000; i < 200000; i++)
	{
		writer_operations[i].type = BenchmarkOpErase;
		writer_operations[i].erase_index = i - 100000;
	}

	BenchmarkOperation* reader_operations = malloc(sizeof(BenchmarkOperation) * 200000);
	for (int i = 0; i < 200000; i++)
	{
		reader_operations[i].type = BenchmarkOpLoad;
		reader_operations[i].load_index = i % 100000;
	}

	pthread_t threads[4];
	WorkLoadRoutineOperations writer_ops;
	writer_ops.operations = writer_operations;
	writer_ops.operation_count = 200000;
	writer_ops.tree = tree;
	writer_ops.iterations = 3;
	WorkLoadRoutineContext writer_context;
	writer_context.operations = &writer_ops;
	writer_context.cpu = 0;

	WorkLoadRoutineOperations reader_ops;
	reader_ops.operations = reader_operations;
	reader_ops.operation_count = 200000;
	reader_ops.tree = tree;
	reader_ops.iterations = 3;
	WorkLoadRoutineContext reader_context1 = {
		.cpu = 1,
		.operations = &reader_ops
	};
	WorkLoadRoutineContext reader_context2 = {
		.cpu = 2,
		.operations = &reader_ops
	};
	WorkLoadRoutineContext reader_context3 = {
		.cpu = 3,
		.operations = &reader_ops
	};

	struct timespec start, end;
	clock_gettime(CLOCK_MONOTONIC, &start);
	pthread_create(&threads[0], NULL, &bm_thread_perform_operations, &writer_context);
	pthread_create(&threads[1], NULL, &bm_thread_perform_operations, &reader_context1);
	pthread_create(&threads[2], NULL, &bm_thread_perform_operations, &reader_context2);
	pthread_create(&threads[3], NULL, &bm_thread_perform_operations, &reader_context3);

	pthread_join(threads[3], NULL);
	pthread_join(threads[2], NULL);
	pthread_join(threads[1], NULL);
	pthread_join(threads[0], NULL);

	clock_gettime(CLOCK_MONOTONIC, &end);

	double duration_ms = bm_duration_passed_ms(&start, &end);
	printf("Benchmark C took %.3f ms\n", duration_ms);

	free(reader_operations);
	free(writer_operations);
}
