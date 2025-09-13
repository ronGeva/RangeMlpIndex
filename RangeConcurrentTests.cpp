#include "MlpSetUInt64Range.h"
#include <iostream>
#include <thread>
#include <vector>
#include <atomic>
#include <chrono>
#include <random>
#include <cassert>
#include <set>
#include <mutex>
#include <iomanip> // For fixed and setprecision

using namespace MlpSetUInt64;
using namespace std;
using namespace chrono;

// Test 1: Multiple readers, single writer pattern
void test_single_writer_multiple_readers() {
    cout << "\n=== Test 1: Single Writer, Multiple Readers ===" << endl;
    
    MlpRangeTree tree;
    tree.Init(1000000);
    
    atomic<bool> writerDone(false);
    const int numReaders = 4;
    const int numWrites = 1000;
    
    // Writer thread - inserts values
    thread writer([&]() {
        for (int i = 0; i < numWrites; i++) {
            int* data = new int(i);
            if (i % 3 == 0) {
                // Insert range
                uint64_t start = i * 100;
                uint64_t end = start + 50;
                tree.InsertRange(start, end, data);
            } else {
                // Insert single point - using Store since you don't have Insert(key, value)
                tree.InsertSinglePoint(i * 100, data);
            }
            
            // Small delay to let readers work
        }
        writerDone = true;
        cout << "Writer completed " << numWrites << " insertions" << endl;
    });
    
    // Reader threads - continuously read
    vector<thread> readers;
    for (int r = 0; r < numReaders; r++) {
        readers.emplace_back([&, r]() {
            int found = 0;
            int notFound = 0;
            mt19937 gen(r);
            uniform_int_distribution<> dis(0, numWrites * 100);
            
            while (!writerDone || found < 100) {
                uint64_t key = dis(gen);
                void* result = tree.Load(key);
                if (result) found++;
                else notFound++;
                
            }
            
            cout << "Reader " << r << " found " << found 
                 << " values, not found " << notFound << endl;
        });
    }
    
    writer.join();
    for (auto& r : readers) {
        r.join();
    }
}

// Test 2: Concurrent insertions (multiple writers)
void test_concurrent_insertions() {
    cout << "\n=== Test 2: Concurrent InsertRange ===" << endl;
    
    MlpRangeTree tree;
    tree.Init(1000000);
    
    const int numThreads = 4;
    const int rangesPerThread = 1000;
    
    vector<thread> threads;
    atomic<int> totalSuccess(0);
    atomic<int> totalFailed(0);
    
    for (int t = 0; t < numThreads; t++) {
        threads.emplace_back([&, t]() {
            int successful = 0;
            int failed = 0;
            
            // Each thread inserts in its own range to minimize conflicts
            uint64_t base = t * 10000000;
            
            for (int i = 0; i < rangesPerThread; i++) {
                int* data = new int(t * 10000 + i);
                uint64_t start = base + i * 100;
                uint64_t end = start + 50;
                
                // Try to insert range
                bool result = tree.InsertRange(start, end, data);
                if (result) successful++;
                else failed++;
            }
            
            totalSuccess += successful;
            totalFailed += failed;
            cout << "Thread " << t << " completed: " << successful 
                 << " successful, " << failed << " failed" << endl;
        });
    }
    
    for (auto& t : threads) {
        t.join();
    }
    
    cout << "Total successful insertions: " << totalSuccess << endl;
    cout << "Total failed insertions: " << totalFailed << endl;
}

// Test 3: Mixed operations stress test (only 1 writer allowed)
void test_mixed_operations() {
    cout << "\n=== Test 3: Mixed Operations Stress Test (1 writer only) ===" << endl;
    
    MlpRangeTree tree;
    tree.Init(1000000);
    
    // Pre-populate tree
    for (int i = 0; i < 1000; i++) {
        int* data = new int(i);
        if (i % 2 == 0) {
            tree.InsertSinglePoint(i * 100, data);
        } else {
            tree.InsertRange(i * 100, i * 100 + 50, data);
        }
    }
    
    const int numReaderThreads = 7;
    const int opsPerThread = 5000;
    const int writerOps = opsPerThread;

    vector<thread> threads;

    // Writer thread (only one allowed)
    threads.emplace_back([&]() {
        mt19937 gen(0);
        uniform_int_distribution<> opDist(0, 99);
        uniform_int_distribution<> keyDist(0, 200000);

        int inserts = 0, erases = 0, loads = 0, ranges = 0;

        for (int i = 0; i < writerOps; i++) {
            int op = opDist(gen);
            uint64_t key = keyDist(gen);

            if (op < 15) {
                // 15% Store (single point)
                int* data = new int(i);
                tree.InsertSinglePoint(key, data);
                inserts++;
            } else if (op < 25) {
                // 10% InsertRange
                int* data = new int(i);
                tree.InsertRange(key, key + 20, data);
                ranges++;
            } else if (op < 35) {
                // 10% Erase
                tree.Erase(key);
                erases++;
            } else {
                // 65% Load
                void* result = tree.Load(key);
                loads++;
                if (result) {
                    int value = *(int*)result;
                    // Basic sanity check
                    if (value < 0) {
                        cout << "ERROR: Value is negative: " << value << endl;
                        exit(1);
                    }
                }
            }
        }

        cout << "Writer thread completed: " 
             << inserts << " stores, "
             << ranges << " ranges, " 
             << erases << " erases, " 
             << loads << " loads" << endl;
    });

    // Reader threads (read-only)
    for (int t = 1; t <= numReaderThreads; t++) {
        threads.emplace_back([&, t]() {
            mt19937 gen(t);
            uniform_int_distribution<> keyDist(0, 200000);

            int loads = 0;
            for (int i = 0; i < opsPerThread; i++) {
                uint64_t key = keyDist(gen);
                void* result = tree.Load(key);
                loads++;
                if (result) {
                    int value = *(int*)result;
                    // Basic sanity check
                    if (value < 0) {
                        cout << "ERROR: Value is negative: " << value << endl;
                        exit(1);
                    }
                }
            }
            cout << "Reader thread " << t << " completed: " << loads << " loads" << endl;
        });
    }

    for (auto& t : threads) {
        t.join();
    }
    
    cout << "All threads completed. Final tree size: " << tree.Count() << endl;
}

// Test 4: Verify correctness under concurrency
void test_correctness() {
    cout << "\n=== Test 4: Correctness Verification ===" << endl;
    
    MlpRangeTree tree;
    tree.Init(100000);
    
    const int numRanges = 500;
    mutex verifyMutex;
    set<pair<uint64_t, uint64_t>> insertedRanges;
    
    // Phase 1: Concurrent range insertions with tracking
    vector<thread> inserters;
    for (int t = 0; t < 4; t++) {
        inserters.emplace_back([&, t]() {
            for (int i = 0; i < numRanges / 4; i++) {
                uint64_t start = t * 100000 + i * 100;
                uint64_t end = start + 50;
                int* data = new int(start);
                
                if (tree.InsertRange(start, end, data)) {
                    lock_guard<mutex> lock(verifyMutex);
                    insertedRanges.insert({start, end});
                }
            }
        });
    }
    
    for (auto& t : inserters) {
        t.join();
    }
    
    cout << "Inserted " << insertedRanges.size() << " unique ranges" << endl;
    
    // Phase 2: Verify all inserted ranges can be loaded
    atomic<int> verifyErrors(0);
    vector<thread> verifiers;
    
    for (int t = 0; t < 4; t++) {
        verifiers.emplace_back([&]() {
            for (const auto& range : insertedRanges) {
                // Check start of range
                void* result = tree.Load(range.first);
                if (!result) {
                    verifyErrors++;
                    cout << "ERROR: Could not load start of range " << range.first << endl;
                } else {
                    int value = *(int*)result;
                    if (value != (int)range.first) {
                        verifyErrors++;
                        cout << "ERROR: Wrong value for range start " << range.first 
                             << " (got " << value << ")" << endl;
                    }
                }
                
                // Check middle of range
                uint64_t middle = (range.first + range.second) / 2;
                result = tree.Load(middle);
                if (!result) {
                    verifyErrors++;
                    cout << "ERROR: Could not load middle of range at " << middle << endl;
                }
                
                // Check end of range
                result = tree.Load(range.second);
                if (!result) {
                    verifyErrors++;
                    cout << "ERROR: Could not load end of range " << range.second << endl;
                }
            }
        });
    }
    
    for (auto& t : verifiers) {
        t.join();
    }
    
    if (verifyErrors == 0) {
        cout << "SUCCESS: All ranges verified correctly" << endl;
    } else {
        cout << "FAILED: " << verifyErrors << " verification errors" << endl;
    }
}

// Test 5: Reader-Writer interaction
void test_reader_writer_interaction() {
    cout << "\n=== Test 5: Reader-Writer Interaction ===" << endl;
    
    MlpRangeTree tree;
    tree.Init(100000);
    
    atomic<bool> stopFlag(false);
    atomic<int> readerChecks(0);
    atomic<int> writerOps(0);
    
    // Writer thread continuously modifies a specific range
    thread writer([&]() {
        int value = 0;
        while (!stopFlag) {
            int* data = new int(value);
            
            // Alternate between inserting and erasing
            if (value % 2 == 0) {
                tree.InsertRange(1000, 2000, data);
            } else {
                tree.Erase(1500);  // Erase middle of range
            }
            
            value++;
            writerOps++;
            this_thread::sleep_for(microseconds(100));
        }
    });
    
    // Multiple readers continuously check the range
    vector<thread> readers;
    for (int r = 0; r < 3; r++) {
        readers.emplace_back([&]() {
            while (!stopFlag) {
                // Try to load from middle of range
                void* result = tree.Load(1500);
                readerChecks++;
                
                // If found, verify it's a valid pointer
                if (result) {
                    int value = *(int*)result;
                    assert(value >= 0);
                }
                
                this_thread::sleep_for(microseconds(50));
            }
        });
    }
    
    // Let it run for a while
    this_thread::sleep_for(seconds(2));
    stopFlag = true;
    
    writer.join();
    for (auto& r : readers) {
        r.join();
    }
    
    cout << "Writer performed " << writerOps << " operations" << endl;
    cout << "Readers performed " << readerChecks << " checks" << endl;
}

// Test 6: Same test as test 1 but using regular MlpSet instead of MlpRangeTree
void test_single_writer_multiple_readers_regular_mlpset() {
    cout << "\n=== Test 6: Single Writer, Multiple Readers (Regular MlpSet) ===" << endl;
    
    MlpSet tree;
    tree.Init(1000000);
    
    atomic<bool> writerDone(false);
    const int numReaders = 4;
    const int numWrites = 1000;
    
    // Writer thread - inserts values
    thread writer([&]() {
        for (int i = 0; i < numWrites; i++) {
            // Insert single point using regular MlpSet
            bool inserted = tree.Insert(i * 100);
            assert(inserted);
        }
        writerDone = true;
        cout << "Writer completed " << numWrites << " insertions" << endl;
    });
    
    // Reader threads - continuously read
    vector<thread> readers;
    for (int r = 0; r < numReaders; r++) {
        readers.emplace_back([&, r]() {
            int found = 0;
            int notFound = 0;
            mt19937 gen(r);
            uniform_int_distribution<> dis(0, numWrites * 100);
            
            while (!writerDone || found < 100) {
                uint64_t key = dis(gen);
                bool result = tree.Exist(key);
                if (result) found++;
                else notFound++;
            }
            
            cout << "Reader " << r << " found " << found 
                 << " values, not found " << notFound << endl;
        });
    }
    
    writer.join();
    for (auto& r : readers) {
        r.join();
    }
    
    cout << "Regular MlpSet test completed successfully!" << endl;
}

// Test 7: Same test as test 6 but using LowerBound instead of Exist
void test_single_writer_multiple_readers_regular_mlpset_lowerbound() {
    cout << "\n=== Test 7: Single Writer, Multiple Readers (Regular MlpSet with LowerBound) ===" << endl;
    
    MlpSet tree;
    tree.Init(1000000);
    
    atomic<bool> writerDone(false);
    const int numReaders = 4;
    const int numWrites = 1000;
    
    // Writer thread - inserts values
    thread writer([&]() {
        for (int i = 0; i < numWrites; i++) {
            // Insert single point using regular MlpSet
            bool inserted = tree.Insert(i * 100);
            assert(inserted);
        }
        writerDone = true;
        cout << "Writer completed " << numWrites << " insertions" << endl;
    });
    
    // Reader threads - continuously read using LowerBound
    vector<thread> readers;
    for (int r = 0; r < numReaders; r++) {
        readers.emplace_back([&, r]() {
            int found = 0;
            int notFound = 0;
            mt19937 gen(r);
            uniform_int_distribution<> dis(0, numWrites * 100);
            
            while (!writerDone || found < 100) {
                uint64_t key = dis(gen);
                bool lbFound;
                uint64_t lbResult = tree.LowerBound(key, lbFound);
                if (lbFound) {
                    found++;
                    // Optional: verify the result makes sense
                    if (lbResult < key) {
                        cout << "ERROR: LowerBound(" << key << ") returned " << lbResult << " which is < key" << endl;
                    }
                } else {
                    notFound++;
                }
            }
            
            cout << "Reader " << r << " found " << found 
                 << " values, not found " << notFound << endl;
        });
    }
    
    writer.join();
    for (auto& r : readers) {
        r.join();
    }
    
    cout << "Regular MlpSet LowerBound test completed successfully!" << endl;
}

// Test 8: Large-scale stress test with 2^24 samples followed by mixed operations
void test_large_scale_stress() {
    cout << "\n=== Test 8: Large-scale Stress Test (2^24 samples + mixed ops) ===" << endl;
    
    MlpRangeTree tree;
    const uint64_t totalSamples = 1ULL << 24; // 16M samples
    tree.Init(totalSamples * 2); // Extra space for safety
    
    atomic<uint64_t> insertedCount(0);
    atomic<bool> insertionsDone(false);
    atomic<bool> mixedOpsDone(false);
    atomic<bool> stopReaders(false);
    
    // Track inserted keys as (start, end) tuples - single points have end = start
    vector<pair<uint64_t, uint64_t>> insertedRanges;
    insertedRanges.reserve(totalSamples);
    
    // Use bucket-based locking to reduce contention
    const int NUM_BUCKETS = 128;  // Power of 2 for efficient modulo
    vector<mutex> rangesMutexes(NUM_BUCKETS);
    atomic<size_t> rangesCount(0);  // Track size atomically
    
    // Helper function to get bucket index based on hash
    auto getBucketIndexForAccess = [](size_t index) -> int {
        return index % NUM_BUCKETS;
    };
    
    // Helper function to get bucket index for new insertions (round-robin)
    auto getBucketIndexForInsert = [](size_t index) -> int {
        return index % NUM_BUCKETS;
    };
    
    // Track removed ranges to avoid false positives
    set<pair<uint64_t, uint64_t>> removedRanges;
    mutex removedMutex;
    
    const int numReaderThreads = 22;
    
    cout << "Starting large-scale test with " << totalSamples << " samples..." << endl;
    auto startTime = high_resolution_clock::now();
    
    // Writer thread - Phase 1: inserts 16M samples, Phase 2: mixed operations
    thread writer([&]() {
        mt19937_64 rng(42); // Fixed seed for reproducibility
        uniform_int_distribution<uint64_t> keyDist(0, UINT64_MAX / 2);
        uniform_int_distribution<int> typeDist(0, 99);
        
        auto writerStart = high_resolution_clock::now();
        uint64_t batchSize = 1000000;
        
        // ===== PHASE 1: BULK INSERTIONS =====
        cout << "\n--- Phase 1: Bulk insertions ---" << endl;
        
        for (uint64_t i = 0; i < totalSamples; i++) {
            uint64_t key = keyDist(rng);
            int* data = new int(static_cast<int>(i % 1000000)); // Reuse values to save memory
            
            bool inserted = false;
            uint64_t rangeStart = key;
            uint64_t rangeEnd = key;
            
            // 70% single points, 30% ranges
            if (typeDist(rng) < 70) {
                // Insert single point
                inserted = tree.InsertSinglePoint(key, data);
                rangeEnd = key; // Single point: start == end
            } else {
                // Insert range (small ranges to avoid too much overlap)
                uint64_t rangeSize = 10 + (rng() % 100); // Range size 10-109
                if (key + rangeSize > key) { // Check for overflow
                    rangeEnd = key + rangeSize;
                    inserted = tree.InsertRange(key, rangeEnd, data);
                } else {
                    inserted = tree.InsertSinglePoint(key, data);
                    rangeEnd = key; // Fallback to single point
                }
            }
            
            // Track successfully inserted ranges
            if (inserted) {
                size_t newIndex = insertedRanges.size();
                int bucketIndex = getBucketIndexForInsert(newIndex);
                lock_guard<mutex> lock(rangesMutexes[bucketIndex]);
                insertedRanges.push_back({rangeStart, rangeEnd});
                rangesCount.fetch_add(1); // Increment size atomically
            }
            
            insertedCount.store(i + 1);
            
            // Progress reporting
            if ((i + 1) % batchSize == 0) {
                auto now = high_resolution_clock::now();
                auto elapsed = duration_cast<milliseconds>(now - writerStart).count();
                double rate = (double)(i + 1) / elapsed * 1000.0;
                cout << "Inserted " << (i + 1) << " / " << totalSamples 
                     << " (" << fixed << setprecision(1) 
                     << (100.0 * (i + 1)) / totalSamples << "%) "
                     << "Rate: " << fixed << setprecision(0) << rate << " ops/sec" << endl;
            }
        }
        
        insertionsDone = true;
        auto phase1End = high_resolution_clock::now();
        auto phase1Time = duration_cast<milliseconds>(phase1End - writerStart).count();
        double phase1Rate = (double)totalSamples / phase1Time * 1000.0;
        
        cout << "Phase 1 completed: " << totalSamples << " insertions in " 
             << phase1Time << "ms (avg rate: " << fixed << setprecision(0) 
             << phase1Rate << " ops/sec)" << endl;
        cout << "Successfully inserted ranges: " << insertedRanges.size() << endl;
        cout << "Final tree count after phase 1: " << tree.Count() << endl;
        
        // ===== PHASE 2: MIXED OPERATIONS =====
        cout << "\n--- Phase 2: Mixed operations on large tree ---" << endl;
        
        const uint64_t mixedOpsCount = totalSamples / 8; // Do 2M mixed operations
        uniform_int_distribution<int> opDist(0, 99);
        uniform_int_distribution<size_t> rangeIndexDist(0, insertedRanges.size() - 1);
        
        auto phase2Start = high_resolution_clock::now();
        uint64_t insertOps = 0, removeOps = 0, loadOps = 0;
        uint64_t successfulInserts = 0, successfulRemoves = 0, successfulLoads = 0;
        
        for (uint64_t i = 0; i < mixedOpsCount; i++) {
            int op = opDist(rng);
            
            if (op < 30 && !insertedRanges.empty()) {
                // 30% Remove existing range
                size_t rangeIndex;
                pair<uint64_t, uint64_t> rangeToRemove;
                bool validRemoval = false;
                
                // First, get a random index without locking
                size_t currentSize = insertedRanges.size();
                if (currentSize > 0) {
                    uniform_int_distribution<size_t> tempIndexDist(0, currentSize - 1);
                    rangeIndex = tempIndexDist(rng);
                    int bucketIndex = getBucketIndexForAccess(rangeIndex);
                    
                    // Lock only the specific bucket
                    lock_guard<mutex> lock(rangesMutexes[bucketIndex]);
                    if (rangeIndex < insertedRanges.size()) {
                        rangeToRemove = insertedRanges[rangeIndex];
                        // Remove from tracking list using swap-and-pop for efficiency
                        if (rangeIndex < insertedRanges.size() - 1) {
                            insertedRanges[rangeIndex] = insertedRanges.back();
                        }
                        insertedRanges.pop_back();
                        validRemoval = true;
                    }
                }
                
                if (validRemoval) {
                    // For ranges, erase the start point; for single points, erase the point
                    bool removed = tree.Erase(rangeToRemove.first);
                    removeOps++;
                    if (removed) {
                        successfulRemoves++;
                        // Track successful removal
                        lock_guard<mutex> lock(removedMutex);
                        removedRanges.insert(rangeToRemove);
                    }
                }
                
            } else if (op < 60) {
                // 30% Insert new key/range
                uint64_t newKey = keyDist(rng);
                int* data = new int(static_cast<int>((totalSamples + i) % 1000000));
                
                bool inserted = false;
                uint64_t rangeStart = newKey;
                uint64_t rangeEnd = newKey;
                
                if (typeDist(rng) < 70) {
                    inserted = tree.InsertSinglePoint(newKey, data);
                    rangeEnd = newKey; // Single point
                } else {
                    uint64_t rangeSize = 10 + (rng() % 100);
                    if (newKey + rangeSize > newKey) {
                        rangeEnd = newKey + rangeSize;
                        inserted = tree.InsertRange(newKey, rangeEnd, data);
                    } else {
                        inserted = tree.InsertSinglePoint(newKey, data);
                        rangeEnd = newKey; // Fallback to single point
                    }
                }
                
                insertOps++;
                if (inserted) {
                    successfulInserts++;
                    size_t newIndex = insertedRanges.size();
                    int bucketIndex = getBucketIndexForInsert(newIndex);
                    lock_guard<mutex> lock(rangesMutexes[bucketIndex]);
                    insertedRanges.push_back({rangeStart, rangeEnd});
                    rangesCount.fetch_add(1); // Increment size atomically
                }
                
            } else {
                // 40% Load random key
                uint64_t queryKey = keyDist(rng);
                void* result = tree.Load(queryKey);
                loadOps++;
                if (result) {
                    successfulLoads++;
                    // Sanity check
                    int value = *(int*)result;
                    if (value < 0 || value >= 1000000) {
                        cout << "ERROR: Writer got invalid value " << value 
                             << " for key " << queryKey << endl;
                    }
                }
            }
            
            // Progress reporting for mixed ops
            if ((i + 1) % (mixedOpsCount / 20) == 0) {
                auto now = high_resolution_clock::now();
                auto elapsed = duration_cast<milliseconds>(now - phase2Start).count();
                double rate = elapsed > 0 ? (double)(i + 1) / elapsed * 1000.0 : 0;
                cout << "Mixed ops: " << (i + 1) << " / " << mixedOpsCount 
                     << " (" << fixed << setprecision(1) 
                     << (100.0 * (i + 1)) / mixedOpsCount << "%) "
                     << "Rate: " << fixed << setprecision(0) << rate << " ops/sec" << endl;
            }
        }
        
        mixedOpsDone = true;
        cout << "Writer: Setting mixedOpsDone = true" << endl;
        auto phase2End = high_resolution_clock::now();
        auto phase2Time = duration_cast<milliseconds>(phase2End - phase2Start).count();
        double phase2Rate = phase2Time > 0 ? (double)mixedOpsCount / phase2Time * 1000.0 : 0;
        
        cout << "Phase 2 completed: " << mixedOpsCount << " mixed operations in " 
             << phase2Time << "ms (avg rate: " << fixed << setprecision(0) 
             << phase2Rate << " ops/sec)" << endl;
        cout << "Insert ops: " << insertOps << " (successful: " << successfulInserts << ")" << endl;
        cout << "Remove ops: " << removeOps << " (successful: " << successfulRemoves << ")" << endl;
        cout << "Load ops: " << loadOps << " (successful: " << successfulLoads << ")" << endl;
        cout << "Remaining ranges: " << insertedRanges.size() << endl;
        cout << "Final tree count: " << tree.Count() << endl;
    });
    
    // Reader threads - continuously read during both phases
    vector<thread> readers;
    vector<atomic<uint64_t>> readerStats(numReaderThreads);
    vector<atomic<uint64_t>> readerFound(numReaderThreads);
    
    for (int i = 0; i < numReaderThreads; i++) {
        readerStats[i] = 0;
        readerFound[i] = 0;
    }
    
    for (int r = 0; r < numReaderThreads; r++) {
        readers.emplace_back([&, r]() {
            mt19937_64 rng(r + 1000); // Different seed per reader
            uniform_int_distribution<uint64_t> randomKeyDist(0, UINT64_MAX / 2);
            uniform_int_distribution<int> strategyDist(0, 99);
            uint64_t localOps = 0;
            uint64_t localFound = 0;
            
            auto readerStart = high_resolution_clock::now();
            
            while (!stopReaders) {
                uint64_t key;
                bool shouldExist = false;
                pair<uint64_t, uint64_t> expectedRange;
                // Strategy for choosing keys to query:
                // 60% from inserted ranges (when available), 40% random
                
                if (strategyDist(rng) < 60) {
                    // Try to get a range from the inserted ranges
                    size_t currentSize = insertedRanges.size();
                    if (currentSize > 0) {
                        uniform_int_distribution<size_t> rangeIndexDist(0, currentSize - 1);
                        size_t rangeIndex = rangeIndexDist(rng);
                        int bucketIndex = getBucketIndexForAccess(rangeIndex);
                        
                        // Lock only the specific bucket
                        lock_guard<mutex> lock(rangesMutexes[bucketIndex]);
                        if (rangeIndex < insertedRanges.size()) {
                            expectedRange = insertedRanges[rangeIndex];
                            
                            // Pick a random key within this range
                            if (expectedRange.first == expectedRange.second) {
                                // Single point
                                key = expectedRange.first;
                            } else {
                                // Range: pick random key within [start, end]
                                uniform_int_distribution<uint64_t> withinRangeDist(expectedRange.first, expectedRange.second);
                                key = withinRangeDist(rng);
                            }
                            shouldExist = true;
                        }
                    }
                }
                
                if (!shouldExist) {
                    // Query random key - these may or may not exist
                    key = randomKeyDist(rng);
                }
                
                void* result = tree.Load(key);
                
                localOps++;
                if (result) {
                    localFound++;
                    
                    // Sanity check the value
                    int value = *(int*)result;
                    if (value < 0 || value >= 1000000) {
                        cout << "ERROR: Reader " << r << " got invalid value " << value 
                             << " for key " << key << endl;
                        exit(1);
                    }
                } else if (shouldExist) {
                    // Key should exist but wasn't found - check if it was removed
                    bool wasRemoved = false;
                    {
                        lock_guard<mutex> lock(removedMutex);
                        wasRemoved = removedRanges.count(expectedRange) > 0;
                    }
                    
                    if (!wasRemoved) {
                        cout << "FATAL ERROR: Reader " << r << " could not find key " << key 
                             << " which should exist in range [" << expectedRange.first 
                             << ", " << expectedRange.second << "]" << endl;
                        cout << "This indicates a concurrency bug or data corruption!" << endl;
                        exit(1);
                    }
                    // If it was removed, this is expected behavior
                }
                
                // Update stats periodically to avoid too much atomic overhead
                if (localOps % 10000 == 0) {
                    readerStats[r].store(localOps);
                    readerFound[r].store(localFound);
                }
                
                // Small yield to let other threads work
                if (localOps % 1000 == 0) {
                    this_thread::yield();
                }
            }
            
            // Final update
            readerStats[r].store(localOps);
            readerFound[r].store(localFound);
            
            auto readerEnd = high_resolution_clock::now();
            auto readerTime = duration_cast<milliseconds>(readerEnd - readerStart).count();
            double readerRate = readerTime > 0 ? (double)localOps / readerTime * 1000.0 : 0;
            
            cout << "Reader " << r << " completed: " << localOps << " operations, "
                 << localFound << " found (" << fixed << setprecision(1) 
                 << (100.0 * localFound) / localOps << "%), "
                 << "rate: " << fixed << setprecision(0) << readerRate << " ops/sec" << endl;
        });
    }
    
    // Monitor progress
    thread monitor([&]() {
        while (!mixedOpsDone.load()) {  // Use explicit load() for clarity
            this_thread::sleep_for(seconds(10));
            
            uint64_t inserted = insertedCount.load();
            uint64_t totalReaderOps = 0;
            uint64_t totalReaderFound = 0;
            
            for (int i = 0; i < numReaderThreads; i++) {
                totalReaderOps += readerStats[i].load();
                totalReaderFound += readerFound[i].load();
            }
            
            string phase = insertionsDone.load() ? "Phase 2 (mixed ops)" : "Phase 1 (insertions)";
            cout << "[PROGRESS " << phase << "] Inserted: " << inserted << " / " << totalSamples
                 << ", Reader ops: " << totalReaderOps 
                 << " (found: " << totalReaderFound << ")" << endl;
        }
        cout << "Monitor thread detected mixedOpsDone = true, exiting..." << endl;
    });
    
    // Wait for writer to complete both phases
    writer.join();
    
    // Let readers continue for a bit after all operations are done
    cout << "All writer operations complete, letting readers run for 10 more seconds..." << endl;
    this_thread::sleep_for(seconds(10));
    
    // Stop everything
    stopReaders = true;
    monitor.join();
    
    for (auto& r : readers) {
        r.join();
    }
    
    auto endTime = high_resolution_clock::now();
    auto totalTime = duration_cast<milliseconds>(endTime - startTime).count();
    
    // Final statistics
    uint64_t totalReaderOps = 0;
    uint64_t totalReaderFound = 0;
    
    for (int i = 0; i < numReaderThreads; i++) {
        totalReaderOps += readerStats[i].load();
        totalReaderFound += readerFound[i].load();
    }
    
    cout << "\n=== FINAL STATISTICS ===" << endl;
    cout << "Total test time: " << totalTime << "ms" << endl;
    cout << "Phase 1 insertions: " << insertedCount.load() << endl;
    cout << "Phase 2 mixed operations: " << (totalSamples / 8) << endl;
    cout << "Total reader operations: " << totalReaderOps << endl;
    cout << "Total reader hits: " << totalReaderFound 
         << " (" << fixed << setprecision(2) 
         << (100.0 * totalReaderFound) / totalReaderOps << "%)" << endl;
    cout << "Tree final count: " << tree.Count() << endl;
    
    double overallRate = (double)(insertedCount.load() + (totalSamples / 8) + totalReaderOps) / totalTime * 1000.0;
    cout << "Overall throughput: " << fixed << setprecision(0) 
         << overallRate << " ops/sec" << endl;
}
int main_() {
    cout << "=== MlpRangeTree Concurrency Test Suite ===" << endl;
    
    // First run the regular MlpSet test to prove it works
    // test_single_writer_multiple_readers_regular_mlpset();

    // Then run the range version that fails
    // test_single_writer_multiple_readers();
    // test_concurrent_insertions();
    // test_mixed_operations();
    // test_correctness();
    // test_reader_writer_interaction();
    test_large_scale_stress(); // Add the new stress test
    
    cout << "\n=== All tests completed ===" << endl;
    
    return 0;
}