#include "MlpSetUInt64Range.h"
#include <vector>
#include <iostream>

namespace MlpSetUInt64 {

    #define QueryAndReturnIfNotValid(resultName, key, generation) \
    NodeResult resultName = tree->QueryLCPWithNode(key, generation); \
    if (!resultName.generationValid) { \
        valid = false; \
        return; \
    } \
    if (!resultName.found) { \
        valid = false; \
        return; \
    }

// Enhanced QueryLCP that returns node pointer to avoid double traversal
MlpRangeTree::NodeResult MlpRangeTree::QueryLCPWithNode(uint64_t key, uint32_t generation) {
    NodeResult result;
    result.found = false;
    result.node = nullptr;
    result.generationValid = true;
    
    // First find using LowerBound
    bool found;
    Promise lowerBoundPromise = MlpSet::LowerBoundInternal(key, found, generation);
    if (!found || !lowerBoundPromise.IsValid()) {
        return result;
    }
    if (!lowerBoundPromise.IsGenerationValid(generation)) {
        result.found = false;
        result.generationValid = false;
        return result;
    }
    uint64_t lowerBoundKey = lowerBoundPromise.Resolve();
    
    // Now locate the node using QueryLCP
    uint32_t ilen;
    uint64_t _allPositions1[4], _allPositions2[4], _expectedHash[4];
    uint32_t* allPositions1 = reinterpret_cast<uint32_t*>(_allPositions1);
    uint32_t* allPositions2 = reinterpret_cast<uint32_t*>(_allPositions2);
    uint32_t* expectedHash = reinterpret_cast<uint32_t*>(_expectedHash);
    
    int lcpLen = m_hashTable.QueryLCPInternal(lowerBoundKey, ilen, 
                                      allPositions1, allPositions2, 
                                      expectedHash, generation);
    if (lcpLen < 0) {
        result.generationValid = false;
        return result;
    }

    
    if (lcpLen == 8) {
        // It's a leaf - find which position has it
        uint32_t pos = allPositions1[ilen-1];
        CuckooHashTableNode* node = &m_hashTable.ht[pos];
        
        if (!node->IsEqualNoHash(lowerBoundKey, ilen)) {
            pos = allPositions2[ilen-1];
            node = &m_hashTable.ht[pos];
        }
        
        result.found = true;
        result.key = lowerBoundKey;
        result.node = node;
    }
    
    return result;
}

void* MlpRangeTree::Load(uint64_t key) {
    while (true) {
        uint32_t generation = cur_generation.load();
        NodeResult result = QueryLCPWithNode(key, generation);

        if (!result.generationValid) {
            continue;
        }
        
        if (!result.found || !result.node->IsLeaf()) {
            return nullptr;
        }

        CuckooHashTableNode::LeafType type = result.node->GetLeafType();

        void *ret_val = nullptr;
        switch (type) {
            case CuckooHashTableNode::LEAF_SINGLE:
            case CuckooHashTableNode::LEAF_RANGE_START:
                // Return data only if exact key match
                if (result.key == key) {
                    ret_val = result.node->GetLeafData();
                }
                break;
            case CuckooHashTableNode::LEAF_RANGE_END:
                // We're definitely inside this range
                // Get data from the corresponding start node
                {
                    uint64_t startKey = result.node->GetRangeStart();
                    NodeResult startResult = QueryLCPWithNode(startKey, generation);
                    if (startResult.found && startResult.node->IsLeaf()) {
                        ret_val = startResult.node->GetLeafData();
                    } else if (!startResult.generationValid) {
                        continue;
                    }
                }
            }
        if (result.node->LoadGeneration() > generation) {
            continue;
        }
        return ret_val;
    }
}

bool MlpRangeTree::InsertSinglePoint(uint64_t key, void* value) { //
    uint32_t generation = cur_generation.load() + 1;
    ResetGenerationsIfNeeded(generation);

    // Check if key already exists
    NodeResult existing = QueryLCPWithNode(key, UINT32_MAX);
    if (existing.found && existing.node->IsLeaf()) {
        // Key already exists - Insert should fail
        return false;
    }
    
    // Insert as single point
    bool inserted = MlpSet::Insert(key, generation);
    
    if (inserted) {
        // Find the node we just inserted
        uint32_t ilen;
        uint64_t _allPositions1[4], _allPositions2[4], _expectedHash[4];
        uint32_t* allPositions1 = reinterpret_cast<uint32_t*>(_allPositions1);
        uint32_t* allPositions2 = reinterpret_cast<uint32_t*>(_allPositions2);
        uint32_t* expectedHash = reinterpret_cast<uint32_t*>(_expectedHash);
        
        int lcpLen = m_hashTable.QueryLCPInternal(key, ilen, 
                                          allPositions1, allPositions2, 
                                          expectedHash, UINT32_MAX);
        
        if (lcpLen == 8) {
            uint32_t pos = allPositions1[ilen-1];
            CuckooHashTableNode* node = &m_hashTable.ht[pos];
            node->SetGeneration(generation);
            if (!node->IsEqualNoHash(key, ilen)) {
                pos = allPositions2[ilen-1];
                node = &m_hashTable.ht[pos];
            }

            // Use the efficient in-place methods
            node->SetLeafType(CuckooHashTableNode::LEAF_SINGLE);
            node->SetLeafData(value);
        }
        cur_generation.store(generation);
    }

    return inserted;
}

bool MlpRangeTree::StoreRange(uint64_t start, uint64_t end, void* value) { //
    if (start > end) return false;

    uint32_t generation = cur_generation.load() + 1;
    ResetGenerationsIfNeeded(generation);

    // Clear any overlapping ranges/values
    ClearRange(start, end);
    
    // Insert the new range
    bool inserted = InsertRangeNodes(start, end, value, generation);

    cur_generation.store(generation);
    return inserted;
}

bool MlpRangeTree::InsertRange(uint64_t start, uint64_t end, void* value) { //
    if (start > end) return false;

    uint32_t generation = cur_generation.load() + 1;
    ResetGenerationsIfNeeded(generation);

    // Check if range is empty
    NodeResult current = QueryLCPWithNode(start, UINT32_MAX);
    // Check if we start inside an existing range
    if (current.found) {
        if (current.node->IsLeaf() && 
            current.node->GetLeafType() == CuckooHashTableNode::LEAF_RANGE_END) {
            return false; // Starting inside an existing range
        }

        // Check all nodes in [start, end]
        while (current.found && current.key <= end) {
            // Any node in our range means it's not empty
            return false;
        }
    }
    // Range is empty, insert it
    bool inserted = InsertRangeNodes(start, end, value, generation);
    cur_generation.store(generation);
    return inserted;
}

bool MlpRangeTree::InsertRangeNodes(uint64_t start, uint64_t end, void* value, uint32_t generation) {
    // Insert end node first (for consistency during concurrent reads)
    if (!MlpSet::Insert(end, generation)) {
        return false;
    }
    
    // Configure end node
    {
        uint32_t ilen;
        uint64_t _allPositions1[4], _allPositions2[4], _expectedHash[4];
        uint32_t* allPositions1 = reinterpret_cast<uint32_t*>(_allPositions1);
        uint32_t* allPositions2 = reinterpret_cast<uint32_t*>(_allPositions2);
        uint32_t* expectedHash = reinterpret_cast<uint32_t*>(_expectedHash);
        
        int lcpLen = m_hashTable.QueryLCPInternal(end, ilen, 
                                                  allPositions1, allPositions2, 
                                                  expectedHash, UINT32_MAX);
        
        if (lcpLen == 8) {
            uint32_t pos = allPositions1[ilen-1];
            CuckooHashTableNode* node = &m_hashTable.ht[pos];
            node->SetGeneration(generation);
            if (!node->IsEqualNoHash(end, ilen)) {
                pos = allPositions2[ilen-1];
                node = &m_hashTable.ht[pos];
            }
            
            node->SetLeafType(CuckooHashTableNode::LEAF_RANGE_END);
            node->SetRangeStart(start);
        }
    }
    
    // Insert start node
    if (!MlpSet::Insert(start, generation)) {
        MlpSet::Remove(end, generation); // Rollback
        return false;
    }
    
    // Configure start node
    {
        uint32_t ilen;
        uint64_t _allPositions1[4], _allPositions2[4], _expectedHash[4];
        uint32_t* allPositions1 = reinterpret_cast<uint32_t*>(_allPositions1);
        uint32_t* allPositions2 = reinterpret_cast<uint32_t*>(_allPositions2);
        uint32_t* expectedHash = reinterpret_cast<uint32_t*>(_expectedHash);
        
        int lcpLen = m_hashTable.QueryLCPInternal(start, ilen, 
                                                  allPositions1, allPositions2, 
                                                  expectedHash, UINT32_MAX);
        
        if (lcpLen == 8) {
            uint32_t pos = allPositions1[ilen-1];
            CuckooHashTableNode* node = &m_hashTable.ht[pos];
            node->SetGeneration(generation);
            if (!node->IsEqualNoHash(start, ilen)) {
                pos = allPositions2[ilen-1];
                node = &m_hashTable.ht[pos];
            }
            
            node->SetLeafType(CuckooHashTableNode::LEAF_RANGE_START);
            node->SetLeafData(value);
        }
    }
    
    return true;
}

void MlpRangeTree::ClearRange(uint64_t start, uint64_t end) {
    std::vector<std::pair<uint64_t, uint64_t>> rangesToRemove;
    std::vector<uint64_t> pointsToRemove;
    
    NodeResult current = QueryLCPWithNode(start, UINT32_MAX);
    
    // If we start inside a range, mark it for removal
    if (current.found && current.node->IsLeaf() && 
        current.node->GetLeafType() == CuckooHashTableNode::LEAF_RANGE_END) {
        uint64_t rangeStart = current.node->GetRangeStart();
        rangesToRemove.push_back({rangeStart, current.key});
        current = QueryLCPWithNode(current.key + 1, UINT32_MAX);
    }
    
    // Process all nodes in [start, end]
    while (current.found && current.key <= end) {
        if (!current.node->IsLeaf()) {
            current = QueryLCPWithNode(current.key + 1, UINT32_MAX);
            continue;
        }
        
        uint64_t nextKey = current.key + 1;
        CuckooHashTableNode::LeafType type = current.node->GetLeafType();
        
        switch (type) {
            case CuckooHashTableNode::LEAF_SINGLE:
                pointsToRemove.push_back(current.key);
                break;
                
            case CuckooHashTableNode::LEAF_RANGE_START:
                // Find the corresponding end node
                {
                    NodeResult endResult = QueryLCPWithNode(current.key + 1, UINT32_MAX);
                    if (endResult.found && endResult.node->IsLeaf() && 
                        endResult.node->GetLeafType() == CuckooHashTableNode::LEAF_RANGE_END) {
                        rangesToRemove.push_back({current.key, endResult.key});
                        nextKey = endResult.key + 1;
                    }
                }
                break;
                
            case CuckooHashTableNode::LEAF_RANGE_END:
                // Should not happen if we handle RANGE_START correctly
                break;
        }
        
        current = QueryLCPWithNode(nextKey, UINT32_MAX);
    }
    
    // Remove all collected ranges
    for (const auto& range : rangesToRemove) {
        MlpSet::Remove(range.first, UINT32_MAX);   // Remove start // TODO: Fix after Shai finishes!!!!
        MlpSet::Remove(range.second, UINT32_MAX);  // Remove end // TODO: Fix after Shai finishes!!!!
    }
    
    // Remove all collected points
    for (uint64_t point : pointsToRemove) {
        MlpSet::Remove(point, UINT32_MAX);   // TODO: Fix after Shai finishes!!!!
    }
}

bool MlpRangeTree::Erase(uint64_t key) { //
    NodeResult result = QueryLCPWithNode(key, UINT32_MAX);
    if (!result.found || !result.node->IsLeaf()) {
        return false;
    }

    uint32_t generation = cur_generation.load() + 1;
    ResetGenerationsIfNeeded(generation);
    CuckooHashTableNode::LeafType type = result.node->GetLeafType();
    bool ret_val = false;
    switch (type) {
        case CuckooHashTableNode::LEAF_SINGLE:
            if (result.key == key) {
                ret_val = MlpSet::Remove(key, generation);
            }
            break;
            
        case CuckooHashTableNode::LEAF_RANGE_START:
            if (result.key == key) {
                // Remove entire range - find the end
                NodeResult endResult = QueryLCPWithNode(key + 1, UINT32_MAX);
                if (endResult.found && endResult.node->IsLeaf() && 
                    endResult.node->GetLeafType() == CuckooHashTableNode::LEAF_RANGE_END) {
                    MlpSet::Remove(key, generation);
                    MlpSet::Remove(endResult.key, generation);
                    ret_val = true;
                }
            }
            break;
            
        case CuckooHashTableNode::LEAF_RANGE_END:
            // Remove entire range containing this key
            {
                uint64_t rangeStart = result.node->GetRangeStart();
                MlpSet::Remove(rangeStart, generation);
                MlpSet::Remove(result.key, generation);
                ret_val = true;
            }
    }
    cur_generation.store(generation);
    return ret_val;
}

// bool MlpRangeTree::EraseRange(uint64_t start, uint64_t end) { //
//     if (start > end) return false;
//     ClearRange(start, end);
//     return true;
// }

bool MlpRangeTree::FindNext(uint64_t from, uint64_t& rangeStart, uint64_t& rangeEnd, void*& value) {
    while (true) {
        uint32_t generation = cur_generation.load();
        NodeResult result = QueryLCPWithNode(from, generation);
        if (!result.generationValid) {
            continue;
        }
        if (!result.found || !result.node->IsLeaf()) {
            return false;
        }

        bool ret_val = false;
        
        CuckooHashTableNode::LeafType type = result.node->GetLeafType();
        switch (type) {
            case CuckooHashTableNode::LEAF_SINGLE:
                rangeStart = rangeEnd = result.key;
                value = result.node->GetLeafData();
                ret_val = true;
                break;

            case CuckooHashTableNode::LEAF_RANGE_START:
                rangeStart = result.key;
                // Find the end
                {
                    NodeResult endResult = QueryLCPWithNode(result.key + 1, generation);
                    if (endResult.found && endResult.node->IsLeaf() && 
                        endResult.node->GetLeafType() == CuckooHashTableNode::LEAF_RANGE_END) {
                        rangeEnd = endResult.key;
                        value = result.node->GetLeafData();
                        ret_val = true;
                    } else if (!endResult.generationValid) {
                        continue;
                    }
                    if (endResult.node->LoadGeneration() > generation) {
                        continue;
                    }
                }
                break;
                
            case CuckooHashTableNode::LEAF_RANGE_END:
                // We're in a range
                rangeStart = result.node->GetRangeStart();
                rangeEnd = result.key;
                // Get data from start node
                {
                    NodeResult startResult = QueryLCPWithNode(rangeStart, generation);
                    if (startResult.found && startResult.node->IsLeaf()) {
                        value = startResult.node->GetLeafData();
                        ret_val = true;
                    } else if (!startResult.generationValid) {
                        continue;
                    }
                    if (startResult.node->LoadGeneration() > generation) {
                        continue;
                    }
                }
                break;
        }
        
        if (result.node->LoadGeneration() > generation) {
            continue;
        }
        return ret_val;
    }
}

MlpRangeTree::Iterator::Iterator(MlpRangeTree* t, uint64_t start) 
    : tree(t), valid(false), nextSearchKey(start), starting_generation(tree->cur_generation.load()) {
    QueryAndReturnIfNotValid(result, start, starting_generation);
    
    
    if (!result.found || !result.node->IsLeaf()) {
        return;
    }
    
    CuckooHashTableNode::LeafType type = result.node->GetLeafType();
    
    switch (type) {
        case CuckooHashTableNode::LEAF_SINGLE:
            currentStart = currentEnd = result.key;
            currentValue = result.node->GetLeafData();
            nextSearchKey = result.key + 1;
            valid = true;
            break;
            
        case CuckooHashTableNode::LEAF_RANGE_START:
            currentStart = result.key;
            currentValue = result.node->GetLeafData();
            // Find end node
            {
                QueryAndReturnIfNotValid(endResult, result.key + 1, starting_generation);
                if (endResult.found && endResult.node->IsLeaf() && 
                    endResult.node->GetLeafType() == CuckooHashTableNode::LEAF_RANGE_END) {
                    currentEnd = endResult.key;
                    nextSearchKey = endResult.key + 1;
                    valid = true;
                }
            }
            break;
            
        case CuckooHashTableNode::LEAF_RANGE_END:
            // We're inside a range
            currentEnd = result.key;
            currentStart = result.node->GetRangeStart();
            nextSearchKey = result.key + 1;
            // Get value from start node
            {
                QueryAndReturnIfNotValid(startResult, currentStart, starting_generation);
                if (startResult.found && startResult.node->IsLeaf()) {
                    currentValue = startResult.node->GetLeafData();
                    valid = true;
                }
            }
            break;
    }
}

void MlpRangeTree::Iterator::Next() {
    if (!valid) return;
    
    QueryAndReturnIfNotValid(result, nextSearchKey, starting_generation);
    
    CuckooHashTableNode::LeafType type = result.node->GetLeafType();
    
    // If we hit a RANGE_END, skip it (we already processed this range)
    if (type == CuckooHashTableNode::LEAF_RANGE_END) {
        nextSearchKey = result.key + 1;
        QueryAndReturnIfNotValid(result, nextSearchKey, starting_generation);
        
        type = result.node->GetLeafType();
    }
    
    switch (type) {
        case CuckooHashTableNode::LEAF_SINGLE:
            currentStart = currentEnd = result.key;
            currentValue = result.node->GetLeafData();
            nextSearchKey = result.key + 1;
            break;
            
        case CuckooHashTableNode::LEAF_RANGE_START:
            currentStart = result.key;
            currentValue = result.node->GetLeafData();
            // Find end
            {
                QueryAndReturnIfNotValid(endResult, result.key + 1, starting_generation);
                if (endResult.found && endResult.node->IsLeaf() && 
                    endResult.node->GetLeafType() == CuckooHashTableNode::LEAF_RANGE_END) {
                    currentEnd = endResult.key;
                    nextSearchKey = endResult.key + 1;
                } else {
                    valid = false;
                    return;
                }
            }
            break;
            
        default:
            valid = false;
            break;
    }
}

} // namespace MlpSetUInt64
