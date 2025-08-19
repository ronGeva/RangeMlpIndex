#include "common.h"
#include "MlpSetUInt64.h"

#include "gtest/gtest.h"

#include <thread>
#include <atomic>
#include <vector>
#include <random>
#include <cstdio>
#include <chrono>
#include <pthread.h>
#include <sched.h>

namespace {

// Helper function to set CPU affinity for a thread
void SetThreadAffinity(std::thread& thread, int coreId) {
    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);
    CPU_SET(coreId, &cpuset);
    int rc = pthread_setaffinity_np(thread.native_handle(), sizeof(cpu_set_t), &cpuset);
    if (rc != 0) {
        printf("Warning: Failed to set thread affinity to core %d (error: %d)\n", coreId, rc);
    }
}

// Concurrency test: one writer inserts sequential keys while several readers
// concurrently query Exist and LowerBound for keys known to be already inserted.
// Contract: exactly one writer; multiple concurrent readers allowed.
// NOTE: Threads are explicitly bound to different CPU cores (writer on core 0, readers on cores 1, 2, etc.)
TEST(MlpSetUInt64, ConcurrentInsertAndQueriesFixedThreads)
{
    const int kTotalThreads = 4;
    
    
    // 1 writer + 7 readers
    const uint64_t kNumInserts = 200000; // keep runtime reasonable in CI

    MlpSetUInt64::MlpSet ms;
    ms.Init(kNumInserts + 1024);

    std::atomic<uint64_t> insertedCount{0};
    std::atomic<bool> stopReaders{false};

    uint64_t writerTimeNs = 0;
    std::thread writer([&]() {
        auto t0 = std::chrono::steady_clock::now();
        for (uint64_t v = 0; v < kNumInserts; v++)
        {
            bool inserted = ms.Insert(v);
            ReleaseAssert(inserted);
            // Additional fence to ensure Insert completion is visible before count update
            // std::atomic_thread_fence(std::memory_order_seq_cst);
            insertedCount.store(v + 1, std::memory_order_seq_cst);
            // DEBUG("inserted " << v);
// #ifndef NDEBUG
            // if (((v + 1) % 200000ULL) == 0ULL)
            // {
                // std::cout << "T" << std::this_thread::get_id() << " inserted " << (v + 1) << std::endl;
            // }
// #endif
        }
        stopReaders.store(true, std::memory_order_seq_cst);
        auto t1 = std::chrono::steady_clock::now();
        writerTimeNs = (uint64_t)std::chrono::duration_cast<std::chrono::nanoseconds>(t1 - t0).count();
    });
    
    // Bind writer thread to core 0
    SetThreadAffinity(writer, 0);

    std::vector<std::thread> readers;
    std::vector<uint64_t> readerCounts;
    std::vector<uint64_t> readerTimesNs;
    readers.reserve(kTotalThreads - 1);
    readerCounts.resize(kTotalThreads - 1, 0);
    readerTimesNs.resize(kTotalThreads - 1, 0);
    for (int t = 0; t < kTotalThreads - 1; t++)
    {
        readers.emplace_back([&, t]() {
            std::mt19937_64 rng(static_cast<uint64_t>(t) + 123456789ULL);
            uint64_t localCount = 0;
            auto t0 = std::chrono::steady_clock::now();
            while (!stopReaders.load(std::memory_order_acquire))
            {
                uint64_t c = insertedCount.load(std::memory_order_seq_cst);
                if (c == 0) { continue; }

                // Additional fence to ensure we see all writes that happened before the count update
                // std::atomic_thread_fence(std::memory_order_seq_cst);

                uint64_t key = rng() % c; // choose a key guaranteed inserted

                bool existed = ms.Exist(key);
                if (!existed) {
                    printf("Reader %d found key %llu not existed %llu\n", t, (unsigned long long)key, (unsigned long long)c);
                    
                }
                ReleaseAssert(existed);

                bool found;
                uint64_t lb = ms.LowerBound(key, found);
                if (!found || lb != key) {
                    printf("Reader %d !found key %llu lb=%llu not found %llu\n", t, (unsigned long long)key, (unsigned long long)lb, (unsigned long long)c);
                }
                ReleaseAssert(found && lb == key);

                localCount++;
            }
            readerCounts[t] = localCount;
            auto t1 = std::chrono::steady_clock::now();
            readerTimesNs[t] = (uint64_t)std::chrono::duration_cast<std::chrono::nanoseconds>(t1 - t0).count();
        });
        
        // Bind each reader thread to a different core (starting from core 1)
        SetThreadAffinity(readers.back(), t + 1);
    }

    writer.join();
    for (auto &th : readers) { th.join(); }

    // Final sanity: spot-check a prefix deterministically
    for (uint64_t v = 0; v < 1000 && v < kNumInserts; v++)
    {
        bool existed = ms.Exist(v);
        ReleaseAssert(existed);
        bool found;
        uint64_t lb = ms.LowerBound(v, found);
        ReleaseAssert(found && lb == v);
    }

    // Print per-reader counts and total
    uint64_t total = 0;
    uint64_t totalReaderTimeNs = 0;
    for (int i = 0; i < (int)readerCounts.size(); i++)
    {
        double ms = (double)readerTimesNs[i] * 1e-6;
        double avg = readerCounts[i] ? (double)readerTimesNs[i] / (double)readerCounts[i] : 0.0;
        double throughput = readerTimesNs[i] ? (double)readerCounts[i] / ((double)readerTimesNs[i] * 1e-9) : 0.0;
        printf("Reader %d found: %llu, time: %.3f ms, avg: %.1f ns/op, throughput: %.1f ops/sec\n",
               i, (unsigned long long)readerCounts[i], ms, avg, throughput);
        total += readerCounts[i];
        totalReaderTimeNs += readerTimesNs[i];
    }
    
    // Calculate combined reader throughput based on average reader time
    double avgReaderTimeNs = readerCounts.size() ? (double)totalReaderTimeNs / (double)readerCounts.size() : 0.0;
    double combinedReaderThroughput = avgReaderTimeNs ? (double)total / (avgReaderTimeNs * 1e-9) : 0.0;
    printf("Combined reader throughput: %.1f ops/sec\n", combinedReaderThroughput);
    
    double wms = (double)writerTimeNs * 1e-6;
    double wavg = kNumInserts ? (double)writerTimeNs / (double)kNumInserts : 0.0;
    double wThroughput = writerTimeNs ? (double)kNumInserts / ((double)writerTimeNs * 1e-9) : 0.0;
    printf("Writer inserted: %llu, time: %.3f ms, avg: %.1f ns/op, throughput: %.1f ops/sec\n",
           (unsigned long long)kNumInserts, wms, wavg, wThroughput);
    printf("Total reader queries found: %llu\n", (unsigned long long)total);
}

// Concurrency test: one writer inserts sequential keys IN REVERSE ORDER while several readers
// concurrently query Exist and LowerBound for keys known to be already inserted.
// Contract: exactly one writer; multiple concurrent readers allowed.
// NOTE: Threads are explicitly bound to different CPU cores (writer on core 0, readers on cores 1, 2, etc.)
TEST(MlpSetUInt64, ConcurrentInsertAndQueriesReverseOrder)
{
    const int kTotalThreads = 20;
    
    
    // 1 writer + 7 readers
    const uint64_t kNumInserts = 2000; // keep runtime reasonable in CI

    MlpSetUInt64::MlpSet ms;
    ms.Init(kNumInserts + 1024);

    std::atomic<uint64_t> insertedCount{0};
    std::atomic<bool> stopReaders{false};

    uint64_t writerTimeNs = 0;
    std::thread writer([&]() {
        auto t0 = std::chrono::steady_clock::now();
        // Insert keys in REVERSE order: kNumInserts-1, kNumInserts-2, ..., 1, 0
        for (uint64_t i = 0; i < kNumInserts; i++)
        {
            uint64_t v = kNumInserts - 1 - i;  // Reverse order insertion
            bool inserted = ms.Insert(v);
            ReleaseAssert(inserted);
            
            // Update total count of inserted keys
            insertedCount.store(i + 1, std::memory_order_seq_cst);
            
            // DEBUG("inserted " << v);
// #ifndef NDEBUG
            // if (((i + 1) % 200000ULL) == 0ULL)
            // {
                // std::cout << "T" << std::this_thread::get_id() << " inserted " << (i + 1) << " keys, last key: " << v << std::endl;
            // }
// #endif
        }
        stopReaders.store(true, std::memory_order_seq_cst);
        auto t1 = std::chrono::steady_clock::now();
        writerTimeNs = (uint64_t)std::chrono::duration_cast<std::chrono::nanoseconds>(t1 - t0).count();
    });
    
    // Bind writer thread to core 0
    SetThreadAffinity(writer, 0);

    std::vector<std::thread> readers;
    std::vector<uint64_t> readerCounts;
    std::vector<uint64_t> readerTimesNs;
    readers.reserve(kTotalThreads - 1);
    readerCounts.resize(kTotalThreads - 1, 0);
    readerTimesNs.resize(kTotalThreads - 1, 0);
    for (int t = 0; t < kTotalThreads - 1; t++)
    {
        readers.emplace_back([&, t]() {
            std::mt19937_64 rng(static_cast<uint64_t>(t) + 123456789ULL);
            uint64_t localCount = 0;
            auto t0 = std::chrono::steady_clock::now();
            while (!stopReaders.load(std::memory_order_acquire))
            {
                uint64_t c = insertedCount.load(std::memory_order_seq_cst);
                if (c == 0) { continue; }

                // Pick a random offset from the highest keys that have been inserted
                // Since we insert in reverse order, the first c keys inserted are: kNumInserts-1, kNumInserts-2, ..., kNumInserts-c
                uint64_t offset = rng() % c;
                uint64_t key = kNumInserts - 1 - offset;

                bool existed = ms.Exist(key);
                if (!existed) {
                    printf("Reader %d found key %llu not existed (reverse order test)\n", t, (unsigned long long)key);
                }
                ReleaseAssert(existed);

                bool found;
                uint64_t lb = ms.LowerBound(key, found);
                if (!found || lb != key) {
                    printf("Reader %d !found key %llu lb=%llu not found (reverse order test)\n", t, (unsigned long long)key, (unsigned long long)lb);
                }
                ReleaseAssert(found && lb == key);

                localCount++;
            }
            readerCounts[t] = localCount;
            auto t1 = std::chrono::steady_clock::now();
            readerTimesNs[t] = (uint64_t)std::chrono::duration_cast<std::chrono::nanoseconds>(t1 - t0).count();
        });
        
        // Bind each reader thread to a different core (starting from core 1)
        SetThreadAffinity(readers.back(), t + 1);
    }

    writer.join();
    for (auto &th : readers) { th.join(); }

    // Final sanity: spot-check a prefix deterministically
    for (uint64_t v = 0; v < 1000 && v < kNumInserts; v++)
    {
        bool existed = ms.Exist(v);
        ReleaseAssert(existed);
        bool found;
        uint64_t lb = ms.LowerBound(v, found);
        ReleaseAssert(found && lb == v);
    }

    // Print per-reader counts and total
    uint64_t total = 0;
    uint64_t totalReaderTimeNs = 0;
    for (int i = 0; i < (int)readerCounts.size(); i++)
    {
        double ms = (double)readerTimesNs[i] * 1e-6;
        double avg = readerCounts[i] ? (double)readerTimesNs[i] / (double)readerCounts[i] : 0.0;
        double throughput = readerTimesNs[i] ? (double)readerCounts[i] / ((double)readerTimesNs[i] * 1e-9) : 0.0;
        printf("Reader %d found: %llu, time: %.3f ms, avg: %.1f ns/op, throughput: %.1f ops/sec (reverse order)\n",
               i, (unsigned long long)readerCounts[i], ms, avg, throughput);
        total += readerCounts[i];
        totalReaderTimeNs += readerTimesNs[i];
    }
    
    // Calculate combined reader throughput based on average reader time
    double avgReaderTimeNs = readerCounts.size() ? (double)totalReaderTimeNs / (double)readerCounts.size() : 0.0;
    double combinedReaderThroughput = avgReaderTimeNs ? (double)total / (avgReaderTimeNs * 1e-9) : 0.0;
    printf("Combined reader throughput: %.1f ops/sec (reverse order)\n", combinedReaderThroughput);
    
    double wms = (double)writerTimeNs * 1e-6;
    double wavg = kNumInserts ? (double)writerTimeNs / (double)kNumInserts : 0.0;
    double wThroughput = writerTimeNs ? (double)kNumInserts / ((double)writerTimeNs * 1e-9) : 0.0;
    printf("Writer inserted: %llu, time: %.3f ms, avg: %.1f ns/op, throughput: %.1f ops/sec (reverse order)\n",
           (unsigned long long)kNumInserts, wms, wavg, wThroughput);
    printf("Total reader queries found: %llu (reverse order test)\n", (unsigned long long)total);
}

} // anonymous namespace


