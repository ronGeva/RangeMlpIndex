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
int main() {
    cout << "=== MlpRangeTree Concurrency Test Suite ===" << endl;
    
    // First run the regular MlpSet test to prove it works
    // test_single_writer_multiple_readers_regular_mlpset();

    // Then run the range version that fails
    test_single_writer_multiple_readers();
    test_concurrent_insertions();
    test_mixed_operations();
    test_correctness();
    test_reader_writer_interaction();
    
    cout << "\n=== All tests completed ===" << endl;
    
    return 0;
}