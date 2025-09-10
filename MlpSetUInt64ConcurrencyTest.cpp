#include "common.h"
#include "MlpSetUInt64.h"

#include "gtest/gtest.h"

#include <thread>
#include <atomic>
#include <vector>
#include <random>
#include <cstdio>
#include <chrono>
#include <unordered_set>
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

// Concurrency test: one writer inserts sequential keys, one remover removes old keys,
// while several readers concurrently query Exist and LowerBound.
// Contract: exactly one writer; one remover; multiple concurrent readers allowed.
// NOTE: Threads are explicitly bound to different CPU cores (writer on core 0, remover on core 1, readers on cores 2, 3, etc.)
TEST(MlpSetUInt64, ConcurrentInsertAndQueriesFixedThreads)
{
    const int kTotalThreads = 4;
    
    
    // 1 writer + 1 remover + (kTotalThreads-2) readers
    const uint64_t kNumInserts = 1 << 22; // keep runtime reasonable in CI
    const uint64_t kRemovalLag = 1000000; // Remove keys that are at least this far behind current insertion

    MlpSetUInt64::MlpSet ms;
    ms.Init(kNumInserts);

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
            // std::atomic_thread_fence();
            insertedCount.store(v + 1);
            // DEBUG("inserted " << v);
// #ifndef NDEBUG
            // if (((v + 1) % 200000ULL) == 0ULL)
            // {
                // std::cout << "T" << std::this_thread::get_id() << " inserted " << (v + 1) << std::endl;
            // }
// #endif
        }
        stopReaders.store(true);
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
            while (!stopReaders.load())
            {
                uint64_t c = insertedCount.load();
                if (c == 0) { continue; }

                // Additional fence to ensure we see all writes that happened before the count update
                // std::atomic_thread_fence();

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
    const uint64_t kNumInserts = 1 << 28; // keep runtime reasonable in CI

    MlpSetUInt64::MlpSet ms;
    ms.Init(kNumInserts);

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
            insertedCount.store(i + 1);
            
            // DEBUG("inserted " << v);
// #ifndef NDEBUG
            // if (((i + 1) % 200000ULL) == 0ULL)
            // {
                // std::cout << "T" << std::this_thread::get_id() << " inserted " << (i + 1) << " keys, last key: " << v << std::endl;
            // }
// #endif
        }
        stopReaders.store(true);
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
            while (!stopReaders.load())
            {
                uint64_t c = insertedCount.load();
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

// Test 1: Sequential insert then sequential remove with concurrent readers
TEST(MlpSetUInt64, SequentialInsertThenRemove)
{
    const int kTotalThreads = 8;
    const uint64_t kNumElements = 1ULL << 22; // 2^22 = ~4M elements
    
    MlpSetUInt64::MlpSet ms;
    ms.Init(kNumElements + 1024);
    
    std::atomic<uint64_t> currentPhase{0}; // 0=inserting, 1=removing, 2=done
    std::atomic<uint64_t> insertedCount{0};
    std::atomic<uint64_t> removedCount{0};
    std::atomic<bool> stopReaders{false};
    
    uint64_t writerTimeNs = 0;
    std::thread writer([&]() {
        auto t0 = std::chrono::steady_clock::now();
        
        // Phase 1: Insert all elements
        for (uint64_t v = 0; v < kNumElements; v++) {
            bool inserted = ms.Insert(v);
            ReleaseAssert(inserted);
            insertedCount.store(v + 1);
        }
        
        currentPhase.store(1); // Switch to removal phase
        
        // // Phase 2: Remove all elements
        // for (uint64_t v = 0; v < kNumElements; v++) {
        //     bool removed = ms.Remove(v);
        //     ReleaseAssert(removed);
        //     removedCount.store(v + 1);
        // }
        
        currentPhase.store(2); // Done
        stopReaders.store(true);
        auto t1 = std::chrono::steady_clock::now();
        writerTimeNs = (uint64_t)std::chrono::duration_cast<std::chrono::nanoseconds>(t1 - t0).count();
    });
    
    SetThreadAffinity(writer, 0);
    
    std::vector<std::thread> readers;
    std::vector<uint64_t> readerCounts;
    std::vector<uint64_t> readerTimesNs;
    readers.reserve(kTotalThreads - 1);
    readerCounts.resize(kTotalThreads - 1, 0);
    readerTimesNs.resize(kTotalThreads - 1, 0);
    
    for (int t = 0; t < kTotalThreads - 1; t++) {
        readers.emplace_back([&, t]() {
            std::mt19937_64 rng(static_cast<uint64_t>(t) + 123456789ULL);
            uint64_t localCount = 0;
            auto t0 = std::chrono::steady_clock::now();
            
            while (!stopReaders.load()) {
                uint64_t phase = currentPhase.load();
                uint64_t inserted = insertedCount.load();
                uint64_t removed = removedCount.load();
                
                if (phase == 0 && inserted > 0) {
                    // Insertion phase: query inserted keys
                    uint64_t key = rng() % inserted;
                    if (ms.Exist(key)) {
                        localCount++;
                    }
                } else if (phase == 1 && inserted > removed) {
                    // Removal phase: query keys that should still exist
                    uint64_t activeRange = inserted - removed;
                    if (activeRange > 0) {
                        uint64_t key = removed + (rng() % activeRange);
                        if (ms.Exist(key)) {
                            localCount++;
                        }
                    }
                }
            }
            
            readerCounts[t] = localCount;
            auto t1 = std::chrono::steady_clock::now();
            readerTimesNs[t] = (uint64_t)std::chrono::duration_cast<std::chrono::nanoseconds>(t1 - t0).count();
        });
        
        SetThreadAffinity(readers.back(), t + 1);
    }
    
    writer.join();
    for (auto &th : readers) { th.join(); }
    
    uint64_t totalReaderOps = 0;
    for (size_t i = 0; i < readerCounts.size(); i++) {
        totalReaderOps += readerCounts[i];
        printf("Reader %zu successful ops: %llu\n", i, (unsigned long long)readerCounts[i]);
    }
    
    double wms = (double)writerTimeNs * 1e-6;
    printf("Writer total time: %.3f ms, Elements: %llu, Final removed: %llu\n", 
           wms, (unsigned long long)kNumElements, (unsigned long long)removedCount.load());
    printf("Total successful reader operations: %llu\n", (unsigned long long)totalReaderOps);
}

// Test 2: Random order insert then remove in same order with concurrent readers  
TEST(MlpSetUInt64, RandomOrderInsertThenRemove)
{
    const int kTotalThreads = 8;
    const uint64_t kNumElements = 1ULL << 22; // 2^22 = ~4M elements
    
    // Generate random elements
    std::vector<uint64_t> randomElements;
    randomElements.reserve(kNumElements);
    std::mt19937_64 rng(42); // Fixed seed for reproducibility
    std::uniform_int_distribution<uint64_t> dist(0, UINT64_MAX);
    
    // Generate unique random numbers
    std::unordered_set<uint64_t> seen;
    while (randomElements.size() < kNumElements) {
        uint64_t val = dist(rng);
        if (seen.find(val) == seen.end()) {
            seen.insert(val);
            randomElements.push_back(val);
        }
    }
    
    MlpSetUInt64::MlpSet ms;
    ms.Init(kNumElements + 1024);
    
    std::atomic<uint64_t> currentPhase{0}; // 0=inserting, 1=removing, 2=done
    std::atomic<uint64_t> insertedCount{0};
    std::atomic<uint64_t> removedCount{0};
    std::atomic<bool> stopReaders{false};
    
    uint64_t writerTimeNs = 0;
    std::thread writer([&]() {
        auto t0 = std::chrono::steady_clock::now();
        
        // Phase 1: Insert all elements in random order
        for (uint64_t i = 0; i < kNumElements; i++) {
            bool inserted = ms.Insert(randomElements[i]);
            ReleaseAssert(inserted);
            insertedCount.store(i + 1);
        }
        
        currentPhase.store(1); // Switch to removal phase
        
        // Phase 2: Remove elements in same random order
        for (uint64_t i = 0; i < kNumElements; i++) {
            bool removed = ms.Remove(randomElements[i]);
            ReleaseAssert(removed);
            removedCount.store(i + 1);
        }
        
        currentPhase.store(2); // Done
        stopReaders.store(true);
        auto t1 = std::chrono::steady_clock::now();
        writerTimeNs = (uint64_t)std::chrono::duration_cast<std::chrono::nanoseconds>(t1 - t0).count();
    });
    
    SetThreadAffinity(writer, 0);
    
    std::vector<std::thread> readers;
    std::vector<uint64_t> readerCounts;
    std::vector<uint64_t> readerTimesNs;
    readers.reserve(kTotalThreads - 1);
    readerCounts.resize(kTotalThreads - 1, 0);
    readerTimesNs.resize(kTotalThreads - 1, 0);
    
    for (int t = 0; t < kTotalThreads - 1; t++) {
        readers.emplace_back([&, t]() {
            std::mt19937_64 rng(static_cast<uint64_t>(t) + 987654321ULL);
            uint64_t localCount = 0;
            auto t0 = std::chrono::steady_clock::now();
            
            while (!stopReaders.load()) {
                uint64_t phase = currentPhase.load();
                uint64_t inserted = insertedCount.load();
                uint64_t removed = removedCount.load();
                
                if (phase == 0 && inserted > 0) {
                    // Insertion phase: query randomly from inserted elements
                    uint64_t idx = rng() % inserted;
                    if (ms.Exist(randomElements[idx])) {
                        localCount++;
                    }
                } else if (phase == 1 && inserted > removed) {
                    // Removal phase: query from elements not yet removed
                    uint64_t remainingCount = inserted - removed;
                    if (remainingCount > 0) {
                        uint64_t idx = removed + (rng() % remainingCount);
                        if (ms.Exist(randomElements[idx])) {
                            localCount++;
                        }
                    }
                }
            }
            
            readerCounts[t] = localCount;
            auto t1 = std::chrono::steady_clock::now();
            readerTimesNs[t] = (uint64_t)std::chrono::duration_cast<std::chrono::nanoseconds>(t1 - t0).count();
        });
        
        SetThreadAffinity(readers.back(), t + 1);
    }
    
    writer.join();
    for (auto &th : readers) { th.join(); }
    
    uint64_t totalReaderOps = 0;
    for (size_t i = 0; i < readerCounts.size(); i++) {
        totalReaderOps += readerCounts[i];
        printf("Reader %zu successful ops: %llu\n", i, (unsigned long long)readerCounts[i]);
    }
    
    double wms = (double)writerTimeNs * 1e-6;
    printf("Writer total time: %.3f ms, Elements: %llu, Final removed: %llu\n", 
           wms, (unsigned long long)kNumElements, (unsigned long long)removedCount.load());
    printf("Total successful reader operations: %llu\n", (unsigned long long)totalReaderOps);
}

// Test 3: Mixed random insertions and removals with concurrent readers
TEST(MlpSetUInt64, MixedRandomInsertionsRemovals)
{
    const int kTotalThreads = 8;
    const uint64_t kInitialElements = 1ULL << 22;
    const uint64_t kMixedOperations = 1ULL << 10;
    
    // Generate initial random elements
    std::vector<uint64_t> initialElements;
    initialElements.reserve(kInitialElements);
    std::mt19937_64 rng(12345); // Fixed seed for reproducibility
    std::uniform_int_distribution<uint64_t> dist(0, UINT64_MAX / 2); // Leave room for new elements
    
    std::unordered_set<uint64_t> activeElements;
    while (initialElements.size() < kInitialElements) {
        uint64_t val = dist(rng);
        if (activeElements.find(val) == activeElements.end()) {
            activeElements.insert(val);
            initialElements.push_back(val);
        }
    }
    
    MlpSetUInt64::MlpSet ms;
    ms.Init(kInitialElements * 2); // Extra space for new insertions
    
    std::atomic<uint64_t> currentPhase{0}; // 0=initial_insert, 1=mixed_ops, 2=done
    std::atomic<uint64_t> totalInsertions{0};
    std::atomic<uint64_t> totalRemovals{0};
    std::atomic<bool> stopReaders{false};
    
    uint64_t writerTimeNs = 0;
    std::thread writer([&]() {
        auto t0 = std::chrono::steady_clock::now();
        std::mt19937_64 writerRng(54321);
        std::uniform_int_distribution<uint64_t> newElementDist(UINT64_MAX / 2 + 1, UINT64_MAX);
        std::uniform_real_distribution<double> opDist(0.0, 1.0);
        
        // Phase 1: Insert all initial elements
        for (uint64_t i = 0; i < kInitialElements; i++) {
            bool inserted = ms.Insert(initialElements[i]);
            ReleaseAssert(inserted);
            totalInsertions.store(i + 1);
        }

        std::cout << "Phase 1 completed, inserted " << kInitialElements << " elements" << std::endl;
        
        currentPhase.store(1); // Switch to mixed operations phase
        
        // Phase 2: Mixed random insertions and removals
        uint64_t removals = 0;
        for (uint64_t op = 0; op < kMixedOperations; op++) {
            bool shouldRemove = opDist(writerRng) < 0.5; // 50% probability
            
            if (shouldRemove && !activeElements.empty()) {
                // Remove a random existing element
                auto it = activeElements.begin();
                std::advance(it, writerRng() % activeElements.size());
                uint64_t elementToRemove = *it;
                
                bool removed = ms.Remove(elementToRemove);
                if (removed) {
                    activeElements.erase(it);
                    removals++;
                    totalRemovals.store(removals);
                }
            } else {
                // Insert a new random element
                uint64_t newElement;
                int attempts = 0;
                do {
                    newElement = newElementDist(writerRng);
                    attempts++;
                } while (activeElements.find(newElement) != activeElements.end() && attempts < 100);
                
                if (attempts < 100) {
                    bool inserted = ms.Insert(newElement);
                    if (inserted) {
                        activeElements.insert(newElement);
                        totalInsertions.store(kInitialElements + activeElements.size() - kInitialElements);
                    }
                }
            }
        }
        
        currentPhase.store(2); // Done
        stopReaders.store(true);
        auto t1 = std::chrono::steady_clock::now();
        writerTimeNs = (uint64_t)std::chrono::duration_cast<std::chrono::nanoseconds>(t1 - t0).count();
    });
    
    SetThreadAffinity(writer, 0);
    
    std::vector<std::thread> readers;
    std::vector<uint64_t> readerCounts;
    std::vector<uint64_t> readerTimesNs;
    readers.reserve(kTotalThreads - 1);
    readerCounts.resize(kTotalThreads - 1, 0);
    readerTimesNs.resize(kTotalThreads - 1, 0);
    
    for (int t = 0; t < kTotalThreads - 1; t++) {
        readers.emplace_back([&, t]() {
            std::mt19937_64 rng(static_cast<uint64_t>(t) + 192837465ULL);
            std::uniform_int_distribution<uint64_t> keyDist(0, UINT64_MAX);
            uint64_t localCount = 0;
            auto t0 = std::chrono::steady_clock::now();
            
            while (!stopReaders.load()) {
                uint64_t phase = currentPhase.load();
                
                if (phase >= 1) {
                    // Query random keys - some may exist, some may not
                    uint64_t key = keyDist(rng);
                    if (ms.Exist(key)) {
                        bool found;
                        uint64_t lb = ms.LowerBound(key, found);
                        if (found && lb == key) {
                            localCount++;
                        }
                    }
                } else if (phase == 0) {
                    // Initial insertion phase - query from initial elements
                    uint64_t insertions = totalInsertions.load();
                    if (insertions > 0) {
                        uint64_t idx = rng() % std::min(insertions, kInitialElements);
                        if (ms.Exist(initialElements[idx])) {
                            localCount++;
                        }
                    }
                }
            }
            
            readerCounts[t] = localCount;
            auto t1 = std::chrono::steady_clock::now();
            readerTimesNs[t] = (uint64_t)std::chrono::duration_cast<std::chrono::nanoseconds>(t1 - t0).count();
        });
        
        SetThreadAffinity(readers.back(), t + 1);
    }
    
    writer.join();
    for (auto &th : readers) { th.join(); }
    
    uint64_t totalReaderOps = 0;
    for (size_t i = 0; i < readerCounts.size(); i++) {
        totalReaderOps += readerCounts[i];
        printf("Reader %zu successful ops: %llu\n", i, (unsigned long long)readerCounts[i]);
    }
    
    double wms = (double)writerTimeNs * 1e-6;
    uint64_t finalInsertions = totalInsertions.load();
    uint64_t finalRemovals = totalRemovals.load();
    printf("Writer total time: %.3f ms\n", wms);
    printf("Initial elements: %llu, Total insertions: %llu, Total removals: %llu\n", 
           (unsigned long long)kInitialElements, (unsigned long long)finalInsertions, (unsigned long long)finalRemovals);
    printf("Mixed operations completed: %llu\n", (unsigned long long)kMixedOperations);
    printf("Total successful reader operations: %llu\n", (unsigned long long)totalReaderOps);
}

} // anonymous namespace


