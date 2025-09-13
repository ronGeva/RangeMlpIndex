#define _GNU_SOURCE
#include "benchmark_mlp.h"
#include <stdio.h>
#include <sched.h>
#include <pthread.h>
#include <unistd.h> // Required for sleep()
#include <stdlib.h> // For rand() and srand()

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
	int* stop_event;
	unsigned long long operations_done;
} WorkLoadRoutineOperations;

typedef struct _WorkLoadRoutineContext {
	int cpu;
	WorkLoadRoutineOperations* operations;
} WorkLoadRoutineContext;

static void bm_perform_operations_once(BenchmarkTree* tree, BenchmarkOperation* operations,
									   int operation_count)
{
	for (int i = 0; i < operation_count; i++)
	{
		BenchmarkOperation* operation = &operations[i];
		bm_perform_operation(tree, operation);
	}
}

static void* bm_thread_perform_operations(void* context)
{
	WorkLoadRoutineContext* routine_context = context;
	WorkLoadRoutineOperations* operations = routine_context->operations;
	BenchmarkTree* tree = operations->tree;

	bm_pin_thread_to_cpu(routine_context->cpu);

	if (operations->stop_event)
	{
		while (!(*operations->stop_event))
		{
			bm_perform_operations_once(tree, operations->operations, operations->operation_count);
			operations->operations_done++;
		}
	}
	else
	{
		for	(int iteration = 0; iteration < operations->iterations; iteration++)
		{
			bm_perform_operations_once(tree, operations->operations,
									   operations->operation_count);
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
	WorkLoadRoutineOperations writer_ops = { 0 };
	writer_ops.operations = writer_operations;
	writer_ops.operation_count = 200000;
	writer_ops.tree = tree;
	writer_ops.iterations = 1;
	WorkLoadRoutineContext writer_context;
	writer_context.operations = &writer_ops;
	writer_context.cpu = 0;

	WorkLoadRoutineOperations reader_ops = { 0 };
	reader_ops.operations = reader_operations;
	reader_ops.operation_count = 200000;
	reader_ops.tree = tree;
	reader_ops.iterations = 1;
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

typedef enum _BenchmarkDAccessPattern {
	AccessPatternAllRange,
	AccessPatternExclusiveRanges,
	AccessPatternRandom
} BenchmarkDAccessPattern;

typedef struct _BenchmarkDSettings {
	BenchmarkDAccessPattern access_pattern;
	// a number between 0 and 100 representing the precentage of time we
	// want the writer thread to spend in the data structure
	int writer_cpu_usage;
	int number_of_readers;
	int duration_seconds;
} BenchmarkDSettings;

// generate a range contained in [4096, INT_MAX]
void generate_random_range(int* start, int* end)
{
	unsigned int values_range = 0x7fffffff;
	*start = (((unsigned int)rand() % values_range) + 4096) % values_range;

	int remaining_range = values_range - *start;
	if (remaining_range)
	{
		*end = *start;
	}
	else
	{
		*end = *start + (rand() % remaining_range);
	}
}

void bm_run_workloadD_with_settings(BenchmarkTree* tree, BenchmarkDSettings* settings)
{
	WorkLoadRoutineContext* reader_contexts = NULL;
	BenchmarkOperation* writer_operations = NULL;
	BenchmarkOperation** reader_operations_array =
	 malloc(sizeof(BenchmarkOperation*) * settings->number_of_readers);
	pthread_t* threads = NULL;

	int writer_operation_count;
	int reader_operation_count;

	switch(settings->access_pattern)
	{
		case AccessPatternAllRange:
		{
			writer_operations = malloc(sizeof(BenchmarkOperation) * 2);
			writer_operations[0].insert_range_entry = NULL;
			writer_operations[0].insert_range_first = 0;
			writer_operations[0].insert_range_last = 0xffffffff;
			writer_operations[0].type = BenchmarkOpInsertRange;

			writer_operations[1].erase_index = 0;
			writer_operations[1].type = BenchmarkOpErase;
			writer_operation_count = 2;

			for (int i = 0; i < settings->number_of_readers; i++)
			{
				reader_operations_array[i] = malloc(sizeof(BenchmarkOperation));
				reader_operations_array[i][0].load_index = 0;
				reader_operations_array[i][0].type = BenchmarkOpLoad;
			}
			reader_operation_count = 1;

			break;
		}
		case AccessPatternExclusiveRanges:
		{
			int thread_count = settings->number_of_readers + 1;
			unsigned long long exclusive_range_size = 0xffffffff / thread_count;

			writer_operations = malloc(sizeof(BenchmarkOperation) * 2);
			writer_operations[0].insert_range_entry = NULL;
			writer_operations[0].insert_range_first = 0;
			writer_operations[0].insert_range_last = exclusive_range_size - 1;
			writer_operations[0].type = BenchmarkOpInsertRange;

			writer_operations[1].erase_index = 0;
			writer_operations[1].type = BenchmarkOpErase;
			writer_operation_count = 2;

			for (int i = 0; i < settings->number_of_readers; i++)
			{
				reader_operations_array[i] = malloc(sizeof(BenchmarkOperation));
				reader_operations_array[i][0].load_index = i * exclusive_range_size;
				reader_operations_array[i][0].type = BenchmarkOpLoad;
			}
			reader_operation_count = 1;

			break;
		}
		case AccessPatternRandom:
		{
			srand(time(NULL));

			writer_operation_count = 10000;
			writer_operations = malloc(sizeof(BenchmarkOperation) * writer_operation_count);
			for (int i = 0; i < writer_operation_count; i++)
			{
				if (rand() % 3 == 0)
				{
					writer_operations[i].erase_index = rand();
					writer_operations[i].type = BenchmarkOpErase;
				}
				else
				{
					int start, end;
					generate_random_range(&start, &end);
					writer_operations[i].insert_range_entry = NULL;
					writer_operations[i].insert_range_first = start;
					writer_operations[i].insert_range_last = end;
					writer_operations[i].type = BenchmarkOpInsertRange;
				}
			}
			
			reader_operation_count = 10000;
			for (int i = 0; i < settings->number_of_readers; i++)
			{
				reader_operations_array[i] = malloc(sizeof(BenchmarkOperation) * reader_operation_count);
				for (int j = 0; j < reader_operation_count; j++)
				{
					int index = rand();
					if (rand() % 2 == 0)
					{
						reader_operations_array[i][j].load_index = index;
						reader_operations_array[i][j].type = BenchmarkOpLoad;
					}
					else
					{
						int possible_range = 0x7fffffff - index;
						int max_index = index + (rand() % possible_range);

						reader_operations_array[i][j].find_index = index;
						reader_operations_array[i][j].find_max = max_index;
						reader_operations_array[i][j].type = BenchmarkOpFind;
					}
				}
			}

			break;
		}
		default:
			return;
	}

	int stop_event;
	stop_event = 0;
	threads = malloc(sizeof(pthread_t) * (settings->number_of_readers + 1));
	WorkLoadRoutineOperations writer_ops = { 0 };
	writer_ops.operations = writer_operations;
	writer_ops.operation_count = writer_operation_count;
	writer_ops.tree = tree;
	writer_ops.stop_event = &stop_event;
	writer_ops.operations_done = 0;
	WorkLoadRoutineContext writer_context;
	writer_context.operations = &writer_ops;
	writer_context.cpu = 0;

	reader_contexts = malloc(sizeof(WorkLoadRoutineContext) * settings->number_of_readers);
	
	for (int i = 0; i < settings->number_of_readers; i++)
	{
		reader_contexts[i].cpu = i + 1;
		reader_contexts[i].operations = malloc(sizeof(WorkLoadRoutineOperations));
		reader_contexts[i].operations->operation_count = reader_operation_count;
		reader_contexts[i].operations->stop_event = &stop_event;
		reader_contexts[i].operations->operations = reader_operations_array[i];
		reader_contexts[i].operations->tree = tree;
		reader_contexts[i].operations->operations_done = 0;
	}
	
	pthread_create(&threads[0], NULL, &bm_thread_perform_operations, &writer_context);
	for (int i = 1; i <= settings->number_of_readers; i++)
	{
		pthread_create(&threads[i], NULL, &bm_thread_perform_operations, &reader_contexts[i-1]);
	}

	sleep(settings->duration_seconds);
	stop_event = 1;

	for (int i = settings->number_of_readers; i > 0; i--)
	{
		pthread_join(threads[i], NULL);
	}
	pthread_join(threads[0], NULL);

	unsigned long long readers_operations = 0;
	for (int i = 0; i < settings->number_of_readers; i++)
	{
		readers_operations += reader_contexts[i].operations->operations_done;
	}
	readers_operations /= settings->number_of_readers;

	printf("Benchmark D: average reader operations done in %d seconds: %llu. Writer operations: %llu, access pattern=%d"
		   " number of readers=%d\n",
		   settings->duration_seconds, readers_operations, writer_context.operations->operations_done,
		   settings->access_pattern, settings->number_of_readers);

	// free resources
	for (int i = 0; i < settings->number_of_readers; i++)
	{
		free(reader_contexts[i].operations);
		free(reader_operations_array[i]);
	}
	free(reader_operations_array);
	free(reader_contexts);

	free(writer_operations);

	free(threads);
}

void bm_run_workloadD(BenchmarkTree* tree)
{
	BenchmarkDSettings settings;
	settings.access_pattern = AccessPatternAllRange;
	settings.number_of_readers = 3;
	settings.writer_cpu_usage = 100;
	settings.duration_seconds = 5;

	bm_run_workloadD_with_settings(tree, &settings);

	settings.access_pattern = AccessPatternExclusiveRanges;
	bm_run_workloadD_with_settings(tree, &settings);

	settings.access_pattern = AccessPatternRandom;
	bm_run_workloadD_with_settings(tree, &settings);
}

typedef struct _BenchmarkSettingsRandom {
	int number_of_readers;
	int writer_on;
	int number_of_reader_operations;
	int perecentage_find_operations;
	int duration_seconds;
	int initial_inserts;
	int writer_operation_count;
} BenchmarkSettingsRandom;

void bm_insert_random_ranges(BenchmarkTree* tree, int amount_of_inserts)
{
	BenchmarkOperation* writer_operations = NULL;
	srand(time(NULL));
	writer_operations = malloc(sizeof(BenchmarkOperation) * amount_of_inserts);
	for (int i = 0; i < amount_of_inserts; i++)
	{
		int start, end;
		generate_random_range(&start, &end);
		writer_operations[i].insert_range_entry = NULL;
		writer_operations[i].insert_range_first = start;
		writer_operations[i].insert_range_last = end;
		writer_operations[i].type = BenchmarkOpInsertRange;
	}

	bm_perform_operations_once(tree, writer_operations, amount_of_inserts);

	free(writer_operations);
}

BenchmarkOperation* bm_create_random_writer_operations(BenchmarkSettingsRandom* settings)
{
	if (!settings->writer_on)
		return NULL;

	BenchmarkOperation* writer_operations = NULL;
	int writer_operation_count = settings->writer_operation_count;
	writer_operations = malloc(sizeof(BenchmarkOperation) * writer_operation_count);
	for (int i = 0; i < writer_operation_count; i++)
	{
		// randomly choose whether to insert an entry or remove an entry
		if (rand() % 2 == 0)
		{
			writer_operations[i].erase_index = rand();
			writer_operations[i].type = BenchmarkOpErase;
		}
		else
		{
			int start, end;
			generate_random_range(&start, &end);
			writer_operations[i].insert_range_entry = NULL;
			writer_operations[i].insert_range_first = start;
			writer_operations[i].insert_range_last = end;
			writer_operations[i].type = BenchmarkOpInsertRange;
		}
	}

	return writer_operations;
}

BenchmarkOperation** bm_create_random_reader_operations(BenchmarkSettingsRandom* settings)
{
	BenchmarkOperation** reader_operations_array =
	 malloc(sizeof(BenchmarkOperation*) * settings->number_of_readers);

	int reader_operation_count = settings->number_of_reader_operations;
	for (int i = 0; i < settings->number_of_readers; i++)
	{
		reader_operations_array[i] = malloc(sizeof(BenchmarkOperation) * reader_operation_count);
		for (int j = 0; j < reader_operation_count; j++)
		{
			int index = rand();
			// randomly call load or find
			if (rand() % 100 >= settings->perecentage_find_operations)
			{
				reader_operations_array[i][j].load_index = index;
				reader_operations_array[i][j].type = BenchmarkOpLoad;
			}
			else
			{
				int possible_range = 0x7fffffff - index;
				int max_index = index + (rand() % possible_range);

				reader_operations_array[i][j].find_index = index;
				reader_operations_array[i][j].find_max = max_index;
				reader_operations_array[i][j].type = BenchmarkOpFind;
			}
		}
	}

	return reader_operations_array;
}

void bm_run_workload_random_accesses(BenchmarkTree* tree, BenchmarkSettingsRandom* settings)
{
	WorkLoadRoutineContext* reader_contexts = NULL;
	BenchmarkOperation* writer_operations = NULL;
	BenchmarkOperation** reader_operations_array = NULL;
	pthread_t* threads = NULL;

	bm_insert_random_ranges(tree, settings->initial_inserts);

	writer_operations = bm_create_random_writer_operations(settings);
	reader_operations_array = bm_create_random_reader_operations(settings);

	int stop_event;
	stop_event = 0;
	threads = malloc(sizeof(pthread_t) * (settings->number_of_readers + 1));
	WorkLoadRoutineOperations writer_ops = {
		.stop_event = &stop_event,
		.operations = writer_operations,
		.operation_count = settings->writer_operation_count,
		.tree = tree,
		.operations_done = 0
	};

	WorkLoadRoutineContext writer_context = {
		.cpu = 0,
		.operations = &writer_ops
	};
	
	// initialize reader contexts
	reader_contexts = malloc(sizeof(WorkLoadRoutineContext) * settings->number_of_readers);
	for (int i = 0; i < settings->number_of_readers; i++)
	{
		reader_contexts[i].cpu = i + 1;
		reader_contexts[i].operations = malloc(sizeof(WorkLoadRoutineOperations));
		reader_contexts[i].operations->operation_count = settings->number_of_reader_operations;
		reader_contexts[i].operations->stop_event = &stop_event;
		reader_contexts[i].operations->operations = reader_operations_array[i];
		reader_contexts[i].operations->tree = tree;
		reader_contexts[i].operations->operations_done = 0;
	}
	
	// create threads
	if (settings->writer_on)
	{
		pthread_create(&threads[0], NULL, &bm_thread_perform_operations, &writer_context);
	}
	for (int i = 1; i <= settings->number_of_readers; i++)
	{
		pthread_create(&threads[i], NULL, &bm_thread_perform_operations, &reader_contexts[i-1]);
	}

	sleep(settings->duration_seconds);
	stop_event = 1;

	for (int i = settings->number_of_readers; i > 0; i--)
	{
		pthread_join(threads[i], NULL);
	}
	if (settings->writer_on)
	{
		pthread_join(threads[0], NULL);
	}

	unsigned long long readers_operations = 0;
	for (int i = 0; i < settings->number_of_readers; i++)
	{
		readers_operations += reader_contexts[i].operations->operations_done;
	}
	readers_operations /= settings->number_of_readers;

	printf("Benchmark E: average reader operations done in %d seconds=%llu, readers=%d, init_inserts=%d"
		   ", precentage_find=%d, writer_on=%d, writer_ops_count=%d\n",
		   settings->duration_seconds, readers_operations, settings->number_of_readers, settings->initial_inserts,
		   settings->perecentage_find_operations, settings->writer_on, settings->writer_operation_count);

	// free resources
	for (int i = 0; i < settings->number_of_readers; i++)
	{
		free(reader_contexts[i].operations);
		free(reader_operations_array[i]);
	}
	free(reader_operations_array);
	free(reader_contexts);

	if (settings->writer_on)
	{
		free(writer_operations);
	}

	free(threads);
}

void bm_run_workloadE(BenchmarkTree* tree)
{
	// modify the for loops to control the settings of the benchmark
	for (int writer_on = 1; writer_on < 2; writer_on++)
	{
		for (int readers_count = 1; readers_count <= 8; readers_count *= 2)
		{
			for (int initial_inserts = 100; initial_inserts <= 100000; initial_inserts*=10)
			{
				for (int percentage_find = 20; percentage_find < 100; percentage_find+=20)
				{
					BenchmarkSettingsRandom settings = {
						.duration_seconds = 2,
						.writer_operation_count = 10000000,
						.number_of_reader_operations = 10000,
						.writer_on = writer_on,
						.number_of_readers = readers_count,
						.initial_inserts = initial_inserts,
						.perecentage_find_operations = percentage_find
					};

					bm_run_workload_random_accesses(tree, &settings);
				}
			}
		}
	}
}
