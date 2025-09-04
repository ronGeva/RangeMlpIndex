#pragma once

#include "MlpSetUInt64.h"

namespace MlpSetUInt64 {

class MlpRangeTree : public MlpSet {
public:
    // Initialize the range tree
    using MlpSet::Init;  // Use parent's Init
    
    // Store a value at a specific key (overwrites any existing range/value)
    bool Store(uint64_t key, void* value);
    
    // Store a value for an entire range [start, end] inclusive
    bool StoreRange(uint64_t start, uint64_t end, void* value);
    
    // Insert range only if empty (returns false if any part is occupied)
    bool InsertRange(uint64_t start, uint64_t end, void* value);
    
    // Load the value associated with a key (returns nullptr if not found)
    void* Load(uint64_t key);
    
    // Erase the entire range containing the key
    bool Erase(uint64_t key);
    
    // Erase a specific range [start, end]
    bool EraseRange(uint64_t start, uint64_t end);
    
    // Find next range from index upwards
    bool FindNext(uint64_t from, uint64_t& rangeStart, uint64_t& rangeEnd, void*& value);

private:
    // Result from QueryLCPWithNode - returns both value and node location
    struct NodeResult {
        uint64_t key;
        CuckooHashTableNode* node;  // Direct pointer to the node
        bool found;
        
        // Helper method
        bool isLeaf() const { return found && node && node->IsLeaf(); }
    };
    
    // Enhanced QueryLCP that returns the node pointer
    NodeResult QueryLCPWithNode(uint64_t key);
    
    // Range operations helpers
    void ClearRange(uint64_t start, uint64_t end);
    bool InsertRangeNodes(uint64_t start, uint64_t end, void* value);
};

} // namespace MlpSetUInt64