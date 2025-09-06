#include "MlpSetUInt64.h"
#include <iostream>

using namespace MlpSetUInt64;
using namespace std;

int main() {
    MlpSet set;
    set.Init(100000);
    
    // The exact keys from your range test
    uint64_t start1 = 0x8eca19fd2a0fb, start2 = 0x2e5532006d59ac, start3 = 0x2f1afaa25a1949;
    uint64_t end1 = 0x8eca19fd2a130, end2 = 0x2e5532006d59f8, end3 = 0x2f1afaa25a19c9;
    
    // Insert all 6 keys
    //set.Insert(start1);
    //set.Insert(end1);
    //set.Insert(start2);
    //set.Insert(end2);
    set.Insert(start3);
    set.Insert(end3);
    
    // Remove range 1 keys
    //set.Remove(start1);
    //set.Remove(end1);
    
    // Remove range 3 keys
    set.Remove(start3);
    set.Remove(end3);
    
    cout << "Done - no assertion means it worked" << endl;
    
    return 0;
}