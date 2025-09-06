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
    // bool EraseRange(uint64_t start, uint64_t end);
    
    // Find next range from index upwards
    bool FindNext(uint64_t from, uint64_t& rangeStart, uint64_t& rangeEnd, void*& value);


    class Iterator {
    public:
        Iterator(MlpRangeTree* tree, uint64_t start = 0);
        
        // Check if iterator is valid
        bool Valid() const { return valid; }
        
        // Move to next entry
        void Next();
        
        // Get current entry details - all inline for performance
        uint64_t StartKey() const { return currentStart; }
        uint64_t EndKey() const { return currentEnd; }
        void* Value() const { return currentValue; }
        bool IsRange() const { return currentStart != currentEnd; }
        
    private:
        MlpRangeTree* tree;
        bool valid;
        uint64_t currentStart;
        uint64_t currentEnd;
        void* currentValue;
        uint64_t nextSearchKey;  // Track where to search next
        uint32_t starting_generation; // Track the generation when the iterator was created
    };

    // Add these methods to MlpRangeTree class:
    Iterator Begin() { return Iterator(this, 0); }
    Iterator BeginFrom(uint64_t start) { return Iterator(this, start); }

    // Inline ForEach for performance
    template<typename Callback>
    void ForEach(uint64_t start, uint64_t end, Callback cb) {
        Iterator it(this, start);
        while (it.Valid() && it.StartKey() <= end) {
            uint64_t effectiveStart = (start > it.StartKey()) ? start : it.StartKey();
            uint64_t effectiveEnd = (end < it.EndKey()) ? end : it.EndKey();
            cb(effectiveStart, effectiveEnd, it.Value());
            it.Next();
        }
    }

    // Simple counting methods
    size_t Count() {
        size_t count = 0;
        Iterator it(this, 0);
        while (it.Valid()) {
            count++;
            it.Next();
        }
        return count;
    }

    bool IsEmpty() {
        Iterator it(this, 0);
        return !it.Valid();
    }

private:
    // Result from QueryLCPWithNode - returns both value and node location
    struct NodeResult {
        uint64_t key;
        CuckooHashTableNode* node;  // Direct pointer to the node
        bool found;
        bool generationValid;
        
        // Helper method
        bool isLeaf() const { return found && node && node->IsLeaf(); }
    };
    
    // Enhanced QueryLCP that returns the node pointer
    NodeResult QueryLCPWithNode(uint64_t key, uint32_t generation);
    
    // Range operations helpers
    void ClearRange(uint64_t start, uint64_t end);
    bool InsertRangeNodes(uint64_t start, uint64_t end, void* value, uint32_t generation);
};

} // namespace MlpSetUInt64