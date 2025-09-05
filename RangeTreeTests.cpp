#include "MlpSetUInt64Range.h"
#include <cassert>
#include <iostream>

using namespace MlpSetUInt64;

void TestBasicOperations() {
    MlpRangeTree tree;
    tree.Init(100000);
    
    for (int i = 0; i <= 0xFFFF; i++) {
        tree.Store(i, (void*)(uintptr_t)i);
    }

    for (int i = 0; i <= 0xFFFF; i++) {
        assert(tree.Load(i) == (void*)(uintptr_t)i);
    }

    for (int i = 0; i <= 0xFFFF; i++) {
        assert(tree.Erase(i));
    }



    // // Test single point operations
    // int data1 = 42;
    // assert(tree.Store(100, &data1));
    // assert(tree.Load(100) == &data1);
    // assert(tree.Load(99) == nullptr);
    // assert(tree.Load(101) == nullptr);
    
    // // Test range operations
    // int data2 = 84;
    // assert(tree.StoreRange(200, 300, &data2));
    
    // // Check various points in range
    // assert(tree.Load(200) == &data2);  // Start of range
    // assert(tree.Load(250) == &data2);  // Middle of range
    // assert(tree.Load(300) == &data2);  // End of range
    // assert(tree.Load(199) == nullptr); // Before range
    // assert(tree.Load(301) == nullptr); // After range
    
    // // Test overwrite - should remove entire [200,300] range
    // int data3 = 168;
    // assert(tree.StoreRange(250, 350, &data3));
    
    // // Original range [200, 300] should be completely gone
    // assert(tree.Load(200) == nullptr);
    // assert(tree.Load(249) == nullptr);
    
    // // New range [250, 350] should exist
    // assert(tree.Load(250) == &data3);
    // assert(tree.Load(300) == &data3);
    // assert(tree.Load(350) == &data3);
    
    // // Test erase - removes entire range containing key
    // assert(tree.Erase(275));
    // assert(tree.Load(250) == nullptr);
    // assert(tree.Load(275) == nullptr);
    // assert(tree.Load(350) == nullptr);
    
    // // Test InsertRange (only if empty)
    // int data4 = 336;
    // assert(tree.InsertRange(400, 500, &data4));
    // assert(tree.Load(450) == &data4);
    
    // // Should fail - range overlaps
    // int data5 = 672;
    // assert(!tree.InsertRange(450, 550, &data5));
    // assert(tree.Load(450) == &data4);  // Still old data
    
    // // Test FindNext
    // uint64_t start, end;
    // void* foundData;
    // assert(tree.FindNext(400, start, end, foundData));
    // assert(start == 400);
    // assert(end == 500);
    // assert(foundData == &data4);
    
    std::cout << "All tests passed!" << std::endl;
}

int main() {
    TestBasicOperations();
    return 0;
}