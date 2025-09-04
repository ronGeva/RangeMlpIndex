#include "MlpSetUInt64Range.h"
#include <iostream>
#include <cassert>
#include <vector>
#include <cstdlib>

using namespace MlpSetUInt64;
using namespace std;

#define TEST(name) cout << "\n=== Testing: " << name << " ===" << endl;
#define ASSERT(cond) do { \
    if (!(cond)) { \
        cout << "FAILED: " << #cond << " at line " << __LINE__ << endl; \
        exit(1); \
    } \
} while(0)
#define PASS() cout << "  ✓ Passed" << endl;

void test_single_points() {
    TEST("Single Point Operations");
    
    MlpRangeTree tree;
    tree.Init(100000);
    
    // Store some single values
    int data1 = 42;
    int data2 = 84;
    int data3 = 168;
    
    cout << "  Inserting single points: 100, 200, 300" << endl;
    ASSERT(tree.Store(100, &data1));
    ASSERT(tree.Store(200, &data2));
    ASSERT(tree.Store(300, &data3));
    
    // Verify they exist
    cout << "  Checking loads..." << endl;
    ASSERT(tree.Load(100) == &data1);
    ASSERT(tree.Load(200) == &data2);
    ASSERT(tree.Load(300) == &data3);
    
    // Verify gaps return nullptr
    ASSERT(tree.Load(99) == nullptr);
    ASSERT(tree.Load(101) == nullptr);
    ASSERT(tree.Load(150) == nullptr);
    ASSERT(tree.Load(250) == nullptr);
    
    PASS();
}

void test_basic_range() {
    TEST("Basic Range Operations");
    
    MlpRangeTree tree;
    tree.Init(100000);
    
    int data = 999;
    
    cout << "  Inserting range [1000, 2000]" << endl;
    ASSERT(tree.StoreRange(1000, 2000, &data));
    
    cout << "  Checking points in range..." << endl;
    ASSERT(tree.Load(1000) == &data);  // Start
    ASSERT(tree.Load(1500) == &data);  // Middle
    ASSERT(tree.Load(2000) == &data);  // End
    
    cout << "  Checking points outside range..." << endl;
    ASSERT(tree.Load(999) == nullptr);   // Before
    ASSERT(tree.Load(2001) == nullptr);  // After
    
    PASS();
}

void test_range_overwrite() {
    TEST("Range Overwrite");
    
    MlpRangeTree tree;
    tree.Init(100000);
    
    int data1 = 111;
    int data2 = 222;
    
    cout << "  Inserting range [100, 200]" << endl;
    ASSERT(tree.StoreRange(100, 200, &data1));
    ASSERT(tree.Load(150) == &data1);
    
    cout << "  Overwriting with range [150, 250] (partial overlap)" << endl;
    ASSERT(tree.StoreRange(150, 250, &data2));
    
    cout << "  Checking that entire old range is gone..." << endl;
    ASSERT(tree.Load(100) == nullptr);  // Old range completely removed
    ASSERT(tree.Load(149) == nullptr);
    
    cout << "  Checking new range exists..." << endl;
    ASSERT(tree.Load(150) == &data2);
    ASSERT(tree.Load(200) == &data2);
    ASSERT(tree.Load(250) == &data2);
    
    PASS();
}

void test_erase() {
    TEST("Erase Operations");
    
    MlpRangeTree tree;
    tree.Init(100000);
    
    int data = 777;
    
    cout << "  Inserting range [500, 600]" << endl;
    ASSERT(tree.StoreRange(500, 600, &data));
    
    cout << "  Erasing by key 550 (middle of range)" << endl;
    ASSERT(tree.Erase(550));
    
    cout << "  Checking entire range is gone..." << endl;
    ASSERT(tree.Load(500) == nullptr);
    ASSERT(tree.Load(550) == nullptr);
    ASSERT(tree.Load(600) == nullptr);
    
    PASS();
}

void test_mixed_operations() {
    TEST("Mixed Single Points and Ranges");
    
    MlpRangeTree tree;
    tree.Init(100000);
    
    int data1 = 1;
    int data2 = 2;
    int data3 = 3;
    int data4 = 4;
    
    cout << "  Creating mixed structure:" << endl;
    cout << "    Point at 50" << endl;
    ASSERT(tree.Store(50, &data1));
    
    cout << "    Range [100, 200]" << endl;
    ASSERT(tree.StoreRange(100, 200, &data2));
    
    cout << "    Point at 250" << endl;
    ASSERT(tree.Store(250, &data3));
    
    cout << "    Range [300, 400]" << endl;
    ASSERT(tree.StoreRange(300, 400, &data4));
    
    cout << "  Verifying structure..." << endl;
    ASSERT(tree.Load(50) == &data1);
    ASSERT(tree.Load(150) == &data2);
    ASSERT(tree.Load(250) == &data3);
    ASSERT(tree.Load(350) == &data4);
    
    cout << "  Checking gaps..." << endl;
    ASSERT(tree.Load(75) == nullptr);
    ASSERT(tree.Load(225) == nullptr);
    ASSERT(tree.Load(275) == nullptr);
    
    PASS();
}

void test_find_next() {
    TEST("FindNext Operation");
    
    MlpRangeTree tree;
    tree.Init(100000);
    
    int data1 = 100;
    int data2 = 200;
    
    cout << "  Setting up: point at 100, range [200, 300]" << endl;
    ASSERT(tree.Store(100, &data1));
    ASSERT(tree.StoreRange(200, 300, &data2));
    
    uint64_t start, end;
    void* foundData;
    
    cout << "  FindNext from 100 (single point)..." << endl;
    ASSERT(tree.FindNext(100, start, end, foundData));
    ASSERT(start == 100);
    ASSERT(end == 100);  // Single point
    ASSERT(foundData == &data1);
    
    cout << "  FindNext from 200 (range start)..." << endl;
    ASSERT(tree.FindNext(200, start, end, foundData));
    ASSERT(start == 200);
    ASSERT(end == 300);
    ASSERT(foundData == &data2);
    
    cout << "  FindNext from 250 (middle of range)..." << endl;
    ASSERT(tree.FindNext(250, start, end, foundData));
    ASSERT(start == 200);
    ASSERT(end == 300);
    ASSERT(foundData == &data2);
    
    PASS();
}

void test_large_scale() {
    TEST("Large Scale Operations");
    
    MlpRangeTree tree;
    tree.Init(1000000);
    
    cout << "  Inserting 1000 ranges..." << endl;
    vector<int> data(1000);
    for (int i = 0; i < 1000; i++) {
        data[i] = i;
        uint64_t start = i * 1000;
        uint64_t end = start + 500;  // Ranges: [0,500], [1000,1500], [2000,2500], ...
        ASSERT(tree.StoreRange(start, end, &data[i]));
    }
    
    cout << "  Verifying some ranges..." << endl;
    ASSERT(tree.Load(250) == &data[0]);    // In first range
    ASSERT(tree.Load(1250) == &data[1]);   // In second range
    ASSERT(tree.Load(999250) == &data[999]); // In last range
    
    cout << "  Checking gaps between ranges..." << endl;
    ASSERT(tree.Load(750) == nullptr);     // Gap between first and second
    ASSERT(tree.Load(1750) == nullptr);    // Gap between second and third
    
    cout << "  Erasing middle range [500000, 500500]..." << endl;
    ASSERT(tree.Erase(500250));  // Middle of range 500
    ASSERT(tree.Load(500250) == nullptr);
    
    PASS();
}

void test_edge_cases() {
    TEST("Edge Cases");
    
    MlpRangeTree tree;
    tree.Init(100000);
    
    int data1 = 1;
    int data2 = 2;
    
    cout << "  Testing single-element range [100, 100]" << endl;
    ASSERT(tree.StoreRange(100, 100, &data1));
    ASSERT(tree.Load(100) == &data1);
    ASSERT(tree.Load(99) == nullptr);
    ASSERT(tree.Load(101) == nullptr);
    
    cout << "  Testing adjacent ranges [200, 300] and [301, 400]" << endl;
    ASSERT(tree.StoreRange(200, 300, &data1));
    ASSERT(tree.StoreRange(301, 400, &data2));
    ASSERT(tree.Load(300) == &data1);
    ASSERT(tree.Load(301) == &data2);
    
    cout << "  Testing EraseRange with partial overlap" << endl;
    tree.EraseRange(250, 350);  // Should remove both ranges completely
    ASSERT(tree.Load(200) == nullptr);
    ASSERT(tree.Load(300) == nullptr);
    ASSERT(tree.Load(301) == nullptr);
    ASSERT(tree.Load(400) == nullptr);
    
    PASS();
}

void run_all_tests() {
    cout << "\n===================================" << endl;
    cout << "   MlpRangeTree Test Suite" << endl;
    cout << "===================================" << endl;
    
    test_single_points();
    test_basic_range();
    test_range_overwrite();
    test_erase();
    test_mixed_operations();
    test_find_next();
    test_large_scale();
    test_edge_cases();
    
    cout << "\n===================================" << endl;
    cout << "   ALL TESTS PASSED! 🎉" << endl;
    cout << "===================================" << endl;
}

int main(int argc, char** argv) {
    try {
        run_all_tests();
    } catch (const exception& e) {
        cout << "\nException caught: " << e.what() << endl;
        return 1;
    } catch (...) {
        cout << "\nUnknown exception caught!" << endl;
        return 1;
    }
    
    return 0;
}