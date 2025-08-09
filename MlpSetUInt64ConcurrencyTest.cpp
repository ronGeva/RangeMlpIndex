#include "common.h"
#include "MlpSetUInt64.h"
#include "gtest/gtest.h"

#include <thread>
#include <atomic>
#include <vector>
#include <random>
#include <cstdio>
#include <chrono>

namespace {

// Concurrency test: one writer inserts sequential keys while several readers
// concurrently query Exist and LowerBound for keys known to be already inserted.
// Contract: exactly one writer; multiple concurrent readers allowed.
TEST(MlpSetUInt64, ConcurrentInsertAndQueriesFixedThreads)
{
    const int kTotalThreads = 10; // 1 writer + 7 readers
    const uint64_t kNumInserts = 2000; // keep runtime reasonable in CI

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
            insertedCount.store(v + 1, std::memory_order_release);
#ifndef NDEBUG
            if (((v + 1) % 200ULL) == 0ULL)
            {
                std::cout << "T" << std::this_thread::get_id() << " inserted " << (v + 1) << std::endl;
            }
#endif
        }
        stopReaders.store(true, std::memory_order_release);
        auto t1 = std::chrono::steady_clock::now();
        writerTimeNs = (uint64_t)std::chrono::duration_cast<std::chrono::nanoseconds>(t1 - t0).count();
    });

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
                uint64_t c = insertedCount.load(std::memory_order_acquire);
                if (c == 0) { continue; }

                uint64_t key = rng() % c; // choose a key guaranteed inserted

                bool existed = ms.Exist(key);
                ReleaseAssert(existed);

                bool found;
                uint64_t lb = ms.LowerBound(key, found);
                ReleaseAssert(found && lb == key);

                // Promise variant should also resolve to the exact key
                MlpSetUInt64::MlpSet::Promise p = ms.LowerBound(key);
                ReleaseAssert(p.IsValid());
                uint64_t lb2 = p.Resolve();
                ReleaseAssert(lb2 == key);

                localCount++;
            }
            readerCounts[t] = localCount;
            auto t1 = std::chrono::steady_clock::now();
            readerTimesNs[t] = (uint64_t)std::chrono::duration_cast<std::chrono::nanoseconds>(t1 - t0).count();
        });
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
    for (int i = 0; i < (int)readerCounts.size(); i++)
    {
        double ms = (double)readerTimesNs[i] * 1e-6;
        double avg = readerCounts[i] ? (double)readerTimesNs[i] / (double)readerCounts[i] : 0.0;
        printf("Reader %d found: %llu, time: %.3f ms, avg: %.1f ns/op\n",
               i, (unsigned long long)readerCounts[i], ms, avg);
        total += readerCounts[i];
    }
    double wms = (double)writerTimeNs * 1e-6;
    double wavg = kNumInserts ? (double)writerTimeNs / (double)kNumInserts : 0.0;
    printf("Writer inserted: %llu, time: %.3f ms, avg: %.1f ns/op\n",
           (unsigned long long)kNumInserts, wms, wavg);
    printf("Total reader queries found: %llu\n", (unsigned long long)total);
}

} // anonymous namespace


