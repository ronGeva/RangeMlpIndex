#include "MlpSetUInt64Range.h"
#include <iostream>
#include <chrono>
#include <vector>
#include <random>
#include <algorithm>
#include <iomanip>
#include <map>
#include <set>

using namespace MlpSetUInt64;
using namespace std;
using namespace std::chrono;

class Timer {
    high_resolution_clock::time_point start;
    const char* name;
public:
    Timer(const char* n) : name(n), start(high_resolution_clock::now()) {}
    ~Timer() {
        auto end = high_resolution_clock::now();
        auto duration = duration_cast<microseconds>(end - start).count();
        cout << "  " << name << ": " << fixed << setprecision(3) 
             << (duration / 1000.0) << " ms" << endl;
    }
};

// Benchmark results structure
struct BenchmarkResult {
    double insertSingleTime;
    double insertRangeTime;
    double lookupHitTime;
    double lookupMissTime;
    double eraseTime;
    double iterateTime;
    size_t numOperations;
};

void benchmark_single_points(size_t num_points) {
    cout << "\n=== Benchmarking Single Points (" << num_points << " points) ===" << endl;
    
    MlpRangeTree tree;
    tree.Init(num_points * 2);
    
    // Generate random keys
    mt19937_64 gen(42);
    uniform_int_distribution<uint64_t> dist(0, UINT64_MAX);
    
    vector<uint64_t> keys;
    set<uint64_t> unique_keys;
    while (unique_keys.size() < num_points) {
        unique_keys.insert(dist(gen));
    }
    keys.assign(unique_keys.begin(), unique_keys.end());
    
    vector<int> data(num_points);
    for (size_t i = 0; i < num_points; i++) {
        data[i] = i;
    }
    
    // Benchmark insertions
    {
        Timer t("Insert single points");
        for (size_t i = 0; i < num_points; i++) {
            tree.Store(keys[i], &data[i]);
        }
    }
    
    // Benchmark lookups (hits)
    {
        Timer t("Lookup existing keys");
        for (size_t i = 0; i < num_points; i++) {
            void* result = tree.Load(keys[i]);
            if (!result) {
                cout << "ERROR: Failed to find key " << keys[i] << endl;
            }
        }
    }
    
    // Benchmark lookups (misses)
    vector<uint64_t> missing_keys;
    for (size_t i = 0; i < num_points; i++) {
        missing_keys.push_back(dist(gen));
    }
    
    {
        Timer t("Lookup missing keys");
        for (size_t i = 0; i < num_points; i++) {
            void* result = tree.Load(missing_keys[i]);
            if (result && unique_keys.find(missing_keys[i]) == unique_keys.end()) {
                cout << "ERROR: Found non-existent key!" << endl;
            }
        }
    }
    cout << "here" << endl;

    {
        Timer t("Erase keys");
        for (size_t i = 0; i < num_points / 2; i++) {
            tree.Erase(keys[i]);
        }
    }
}

void benchmark_ranges(size_t num_ranges, size_t avg_range_size) {
    cout << "\n=== Benchmarking Ranges (" << num_ranges 
         << " ranges, avg size " << avg_range_size << ") ===" << endl;
    
    MlpRangeTree tree;
    tree.Init(num_ranges * 10);
    
    mt19937_64 gen(42);
    uniform_int_distribution<uint64_t> start_dist(0, UINT64_MAX / 2);
    uniform_int_distribution<uint64_t> size_dist(1, avg_range_size * 2);
    
    vector<pair<uint64_t, uint64_t>> ranges;
    vector<int> data(num_ranges);
    
    // Generate non-overlapping ranges
    set<uint64_t> used_starts;
    for (size_t i = 0; i < num_ranges; i++) {
        uint64_t start;
        do {
            start = start_dist(gen);
        } while (used_starts.count(start));
        
        uint64_t size = size_dist(gen);
        uint64_t end = start + size;
        
        ranges.push_back({start, end});
        used_starts.insert(start);
        data[i] = i;
    }
    
    // Sort ranges by start position
    sort(ranges.begin(), ranges.end());
    
    // Benchmark range insertions
    {
        Timer t("Insert ranges");
        for (size_t i = 0; i < num_ranges; i++) {
            tree.StoreRange(ranges[i].first, ranges[i].second, &data[i]);
        }
    }
    
    // Benchmark lookups within ranges
    vector<uint64_t> lookup_keys;
    for (size_t i = 0; i < num_ranges; i++) {
        // Sample a point in the middle of each range
        uint64_t mid = ranges[i].first + (ranges[i].second - ranges[i].first) / 2;
        lookup_keys.push_back(mid);
    }
    
    {
        Timer t("Lookup keys in ranges");
        for (uint64_t key : lookup_keys) {
            void* result = tree.Load(key);
            if (!result) {
                cout << "ERROR: Failed to find key " << key << " in range" << endl;
            }
        }
    }
    
    // Benchmark range deletions
    {
        Timer t("Erase ranges");
        for (size_t i = 0; i < num_ranges / 2; i++) {
            // tree.EraseRange(ranges[i].first, ranges[i].second);
        }
    }
}

void benchmark_mixed_workload(size_t num_operations) {
    cout << "\n=== Mixed Workload Benchmark (" << num_operations << " ops) ===" << endl;
    
    MlpRangeTree tree;
    tree.Init(num_operations * 2);
    
    mt19937_64 gen(42);
    uniform_int_distribution<int> op_dist(0, 99);
    uniform_int_distribution<uint64_t> key_dist(0, 1000000);
    uniform_int_distribution<uint64_t> range_size_dist(1, 1000);
    
    vector<int> data(num_operations);
    for (size_t i = 0; i < num_operations; i++) {
        data[i] = i;
    }
    
    size_t inserts = 0, lookups = 0, erases = 0, range_inserts = 0;
    
    {
        Timer t("Mixed operations");
        
        for (size_t i = 0; i < num_operations; i++) {
            int op = op_dist(gen);
            
            if (op < 30) {
                // 30% - Insert single point
                uint64_t key = key_dist(gen);
                tree.Store(key, &data[i]);
                inserts++;
            } else if (op < 50) {
                // 20% - Insert range
                uint64_t start = key_dist(gen);
                uint64_t size = range_size_dist(gen);
                tree.StoreRange(start, start + size, &data[i]);
                range_inserts++;
            } else if (op < 85) {
                // 35% - Lookup
                uint64_t key = key_dist(gen);
                tree.Load(key);
                lookups++;
            } else {
                // 15% - Erase
                uint64_t key = key_dist(gen);
                tree.Erase(key);
                erases++;
            }
        }
    }
    
    cout << "  Operations breakdown:" << endl;
    cout << "    Single inserts: " << inserts << endl;
    cout << "    Range inserts: " << range_inserts << endl;
    cout << "    Lookups: " << lookups << endl;
    cout << "    Erases: " << erases << endl;
}

void benchmark_large_ranges(size_t num_ranges) {
    cout << "\n=== Large Ranges Benchmark (" << num_ranges << " large ranges) ===" << endl;
    
    MlpRangeTree tree;
    tree.Init(num_ranges * 10);
    
    vector<int> data(num_ranges);
    
    // Create very large non-overlapping ranges
    uint64_t current_start = 0;
    uint64_t range_size = 1000000; // 1 million per range
    uint64_t gap_size = 1000;
    
    vector<pair<uint64_t, uint64_t>> ranges;
    for (size_t i = 0; i < num_ranges; i++) {
        ranges.push_back({current_start, current_start + range_size});
        current_start += range_size + gap_size;
        data[i] = i;
    }
    
    // Insert large ranges
    {
        Timer t("Insert large ranges");
        for (size_t i = 0; i < num_ranges; i++) {
            tree.StoreRange(ranges[i].first, ranges[i].second, &data[i]);
        }
    }
    
    // Lookup random points across all ranges
    mt19937_64 gen(42);
    vector<uint64_t> lookup_points;
    for (size_t i = 0; i < num_ranges * 100; i++) {
        size_t range_idx = i % num_ranges;
        uniform_int_distribution<uint64_t> dist(ranges[range_idx].first, ranges[range_idx].second);
        lookup_points.push_back(dist(gen));
    }
    
    {
        Timer t("Random lookups in large ranges");
        for (uint64_t key : lookup_points) {
            void* result = tree.Load(key);
            if (!result) {
                cout << "ERROR: Failed lookup at " << key << endl;
            }
        }
    }
    
    // Test FindNext performance
    {
        Timer t("FindNext operations");
        uint64_t current = 0;
        size_t found_count = 0;
        
        for (size_t i = 0; i < num_ranges; i++) {
            uint64_t start, end;
            void* value;
            if (tree.FindNext(current, start, end, value)) {
                found_count++;
                current = end + gap_size; // Jump to next range
            } else {
                break;
            }
        }
        
        if (found_count != num_ranges) {
            cout << "ERROR: FindNext found " << found_count << " ranges, expected " << num_ranges << endl;
        }
    }
}

void benchmark_stress_test() {
    cout << "\n=== Stress Test ===" << endl;
    
    MlpRangeTree tree;
    tree.Init(10000000); // 10 million capacity
    
    mt19937_64 gen(42);
    uniform_int_distribution<uint64_t> dist(0, UINT32_MAX);
    
    const size_t batch_size = 100000;
    const size_t num_batches = 10;
    
    vector<int> data(batch_size * num_batches);
    
    for (size_t batch = 0; batch < num_batches; batch++) {
        cout << "\n  Batch " << (batch + 1) << "/" << num_batches << endl;
        
        // Insert batch
        {
            Timer t("  Insert batch");
            for (size_t i = 0; i < batch_size; i++) {
                size_t idx = batch * batch_size + i;
                uint64_t key = dist(gen);
                
                if (i % 10 == 0) {
                    // 10% ranges
                    uint64_t end = key + (dist(gen) % 1000);
                    tree.StoreRange(key, end, &data[idx]);
                } else {
                    // 90% single points
                    tree.Store(key, &data[idx]);
                }
            }
        }
        
        // Query batch
        {
            Timer t("  Query batch");
            for (size_t i = 0; i < batch_size; i++) {
                uint64_t key = dist(gen);
                tree.Load(key);
            }
        }
    }
    
    cout << "\n  Final statistics:" << endl;
    cout << "    Total operations: " << (batch_size * num_batches * 2) << endl;
}

void compare_with_std_map() {
    cout << "\n=== Comparison with std::map ===" << endl;
    
    const size_t num_keys = 100000;
    
    mt19937_64 gen(42);
    uniform_int_distribution<uint64_t> dist(0, UINT64_MAX);
    
    vector<uint64_t> keys;
    set<uint64_t> unique_keys;
    while (unique_keys.size() < num_keys) {
        unique_keys.insert(dist(gen));
    }
    keys.assign(unique_keys.begin(), unique_keys.end());
    
    vector<int> data(num_keys);
    
    // Benchmark MlpRangeTree
    cout << "\n  MlpRangeTree:" << endl;
    {
        MlpRangeTree tree;
        tree.Init(num_keys * 2);
        
        auto start = high_resolution_clock::now();
        for (size_t i = 0; i < num_keys; i++) {
            tree.Store(keys[i], &data[i]);
        }
        auto end = high_resolution_clock::now();
        cout << "    Insert: " << duration_cast<microseconds>(end - start).count() / 1000.0 << " ms" << endl;
        
        start = high_resolution_clock::now();
        for (size_t i = 0; i < num_keys; i++) {
            tree.Load(keys[i]);
        }
        end = high_resolution_clock::now();
        cout << "    Lookup: " << duration_cast<microseconds>(end - start).count() / 1000.0 << " ms" << endl;
    }
    
    // Benchmark std::map
    cout << "\n  std::map:" << endl;
    {
        map<uint64_t, void*> stdmap;
        
        auto start = high_resolution_clock::now();
        for (size_t i = 0; i < num_keys; i++) {
            stdmap[keys[i]] = &data[i];
        }
        auto end = high_resolution_clock::now();
        cout << "    Insert: " << duration_cast<microseconds>(end - start).count() / 1000.0 << " ms" << endl;
        
        start = high_resolution_clock::now();
        for (size_t i = 0; i < num_keys; i++) {
            stdmap.find(keys[i]);
        }
        end = high_resolution_clock::now();
        cout << "    Lookup: " << duration_cast<microseconds>(end - start).count() / 1000.0 << " ms" << endl;
    }
}

int main(int argc, char** argv) {
    cout << "========================================" << endl;
    cout << "   MlpRangeTree Performance Benchmark" << endl;
    cout << "========================================" << endl;

    // Run different benchmark scenarios
    benchmark_single_points(10000);
    benchmark_single_points(100000);
    benchmark_single_points(1000000);
    
    benchmark_ranges(1000, 100);      // 1K ranges, avg size 100
    benchmark_ranges(10000, 1000);    // 10K ranges, avg size 1K
    benchmark_ranges(100000, 10);     // 100K small ranges
    
    benchmark_mixed_workload(100000);
    benchmark_mixed_workload(1000000);
    
    benchmark_large_ranges(100);
    benchmark_large_ranges(1000);
    
    compare_with_std_map();
    
    // benchmark_stress_test();
    
    cout << "\n========================================" << endl;
    cout << "   Benchmark Complete" << endl;
    cout << "========================================" << endl;
    
    return 0;
}