#include "MlpSetUInt64Range.h"
#include <vector>

namespace MlpSetUInt64 {

// Enhanced QueryLCP that returns node pointer to avoid double traversal
MlpRangeTree::NodeResult MlpRangeTree::QueryLCPWithNode(uint64_t key) {
    NodeResult result;
    result.found = false;
    result.node = nullptr;
    
    // First find using LowerBound
    bool found;
    uint64_t lowerBoundKey = MlpSet::LowerBound(key, found);
    
    if (!found) {
        return result;
    }
    
    // Now locate the node using QueryLCP
    uint32_t ilen;
    uint64_t _allPositions1[4], _allPositions2[4], _expectedHash[4];
    uint32_t* allPositions1 = reinterpret_cast<uint32_t*>(_allPositions1);
    uint32_t* allPositions2 = reinterpret_cast<uint32_t*>(_allPositions2);
    uint32_t* expectedHash = reinterpret_cast<uint32_t*>(_expectedHash);
    
    int lcpLen = m_hashTable.QueryLCP(lowerBoundKey, ilen, 
                                      allPositions1, allPositions2, 
                                      expectedHash, cur_generation);
    
    if (lcpLen == 8) {
        // It's a leaf - find which position has it
        uint32_t pos = allPositions1[7];
        CuckooHashTableNode* node = &m_hashTable.ht[pos];
        
        if (!node->IsEqualNoHash(lowerBoundKey, 8)) {
            pos = allPositions2[7];
            node = &m_hashTable.ht[pos];
        }
        
        result.found = true;
        result.key = lowerBoundKey;
        result.node = node;
    }
    
    return result;
}

void* MlpRangeTree::Load(uint64_t key) {
    NodeResult result = QueryLCPWithNode(key);
    
    if (!result.found || !result.node->IsLeaf()) {
        return nullptr;
    }
    
    CuckooHashTableNode::LeafType type = result.node->GetLeafType();
    
    switch (type) {
        case CuckooHashTableNode::LEAF_SINGLE:
            // Return data only if exact key match
            return (result.key == key) ? result.node->GetLeafData() : nullptr;
            
        case CuckooHashTableNode::LEAF_RANGE_START:
            // We're in range only if this start node is exactly our key
            return (result.key == key) ? result.node->GetLeafData() : nullptr;
            
        case CuckooHashTableNode::LEAF_RANGE_END:
            // We're definitely inside this range
            // Get data from the corresponding start node
            {
                uint64_t startKey = result.node->GetRangeStart();
                NodeResult startResult = QueryLCPWithNode(startKey);
                return (startResult.found && startResult.node->IsLeaf()) ? 
                       startResult.node->GetLeafData() : nullptr;
            }
    }
    
    return nullptr;
}

bool MlpRangeTree::Store(uint64_t key, void* value) {
    // First remove any existing value/range at this key
    Erase(key);
    
    // Insert as single point
    bool inserted = MlpSet::Insert(key);
    
    if (inserted) {
        // Find the node we just inserted
        uint32_t ilen;
        uint64_t _allPositions1[4], _allPositions2[4], _expectedHash[4];
        uint32_t* allPositions1 = reinterpret_cast<uint32_t*>(_allPositions1);
        uint32_t* allPositions2 = reinterpret_cast<uint32_t*>(_allPositions2);
        uint32_t* expectedHash = reinterpret_cast<uint32_t*>(_expectedHash);
        
        int lcpLen = m_hashTable.QueryLCP(key, ilen, 
                                          allPositions1, allPositions2, 
                                          expectedHash, cur_generation);
        
        if (lcpLen == 8) {
            uint32_t pos = allPositions1[7];
            CuckooHashTableNode* node = &m_hashTable.ht[pos];
            
            if (!node->IsEqualNoHash(key, 8)) {
                pos = allPositions2[7];
                node = &m_hashTable.ht[pos];
            }
            
            // Use the efficient in-place methods
            node->SetLeafType(CuckooHashTableNode::LEAF_SINGLE);
            node->SetLeafData(value);
        }
    }
    
    return inserted;
}

bool MlpRangeTree::StoreRange(uint64_t start, uint64_t end, void* value) {
    if (start > end) return false;
    
    // Clear any overlapping ranges/values
    ClearRange(start, end);
    
    // Insert the new range
    return InsertRangeNodes(start, end, value);
}

bool MlpRangeTree::InsertRange(uint64_t start, uint64_t end, void* value) {
    if (start > end) return false;
    
    // Check if range is empty
    NodeResult current = QueryLCPWithNode(start);
    
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
    return InsertRangeNodes(start, end, value);
}

bool MlpRangeTree::InsertRangeNodes(uint64_t start, uint64_t end, void* value) {
    // Insert end node first (for consistency during concurrent reads)
    if (!MlpSet::Insert(end)) {
        return false;
    }
    
    // Configure end node
    {
        uint32_t ilen;
        uint64_t _allPositions1[4], _allPositions2[4], _expectedHash[4];
        uint32_t* allPositions1 = reinterpret_cast<uint32_t*>(_allPositions1);
        uint32_t* allPositions2 = reinterpret_cast<uint32_t*>(_allPositions2);
        uint32_t* expectedHash = reinterpret_cast<uint32_t*>(_expectedHash);
        
        int lcpLen = m_hashTable.QueryLCP(end, ilen, 
                                          allPositions1, allPositions2, 
                                          expectedHash, cur_generation);
        
        if (lcpLen == 8) {
            uint32_t pos = allPositions1[7];
            CuckooHashTableNode* node = &m_hashTable.ht[pos];
            
            if (!node->IsEqualNoHash(end, 8)) {
                pos = allPositions2[7];
                node = &m_hashTable.ht[pos];
            }
            
            node->SetLeafType(CuckooHashTableNode::LEAF_RANGE_END);
            node->SetRangeStart(start);
        }
    }
    
    // Insert start node
    if (!MlpSet::Insert(start)) {
        MlpSet::Remove(end); // Rollback
        return false;
    }
    
    // Configure start node
    {
        uint32_t ilen;
        uint64_t _allPositions1[4], _allPositions2[4], _expectedHash[4];
        uint32_t* allPositions1 = reinterpret_cast<uint32_t*>(_allPositions1);
        uint32_t* allPositions2 = reinterpret_cast<uint32_t*>(_allPositions2);
        uint32_t* expectedHash = reinterpret_cast<uint32_t*>(_expectedHash);
        
        int lcpLen = m_hashTable.QueryLCP(start, ilen, 
                                          allPositions1, allPositions2, 
                                          expectedHash, cur_generation);
        
        if (lcpLen == 8) {
            uint32_t pos = allPositions1[7];
            CuckooHashTableNode* node = &m_hashTable.ht[pos];
            
            if (!node->IsEqualNoHash(start, 8)) {
                pos = allPositions2[7];
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
    
    NodeResult current = QueryLCPWithNode(start);
    
    // If we start inside a range, mark it for removal
    if (current.found && current.node->IsLeaf() && 
        current.node->GetLeafType() == CuckooHashTableNode::LEAF_RANGE_END) {
        uint64_t rangeStart = current.node->GetRangeStart();
        rangesToRemove.push_back({rangeStart, current.key});
        current = QueryLCPWithNode(current.key + 1);
    }
    
    // Process all nodes in [start, end]
    while (current.found && current.key <= end) {
        if (!current.node->IsLeaf()) {
            current = QueryLCPWithNode(current.key + 1);
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
                    NodeResult endResult = QueryLCPWithNode(current.key + 1);
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
        
        current = QueryLCPWithNode(nextKey);
    }
    
    // Remove all collected ranges
    for (const auto& range : rangesToRemove) {
        MlpSet::Remove(range.first);   // Remove start
        MlpSet::Remove(range.second);  // Remove end
    }
    
    // Remove all collected points
    for (uint64_t point : pointsToRemove) {
        MlpSet::Remove(point);
    }
}

bool MlpRangeTree::Erase(uint64_t key) {
    NodeResult result = QueryLCPWithNode(key);
    
    if (!result.found || !result.node->IsLeaf()) {
        return false;
    }
    
    CuckooHashTableNode::LeafType type = result.node->GetLeafType();
    
    switch (type) {
        case CuckooHashTableNode::LEAF_SINGLE:
            if (result.key == key) {
                return MlpSet::Remove(key);
            }
            break;
            
        case CuckooHashTableNode::LEAF_RANGE_START:
            if (result.key == key) {
                // Remove entire range - find the end
                NodeResult endResult = QueryLCPWithNode(key + 1);
                if (endResult.found && endResult.node->IsLeaf() && 
                    endResult.node->GetLeafType() == CuckooHashTableNode::LEAF_RANGE_END) {
                    MlpSet::Remove(key);
                    MlpSet::Remove(endResult.key);
                    return true;
                }
            }
            break;
            
        case CuckooHashTableNode::LEAF_RANGE_END:
            // Remove entire range containing this key
            {
                uint64_t rangeStart = result.node->GetRangeStart();
                MlpSet::Remove(rangeStart);
                MlpSet::Remove(result.key);
                return true;
            }
    }
    
    return false;
}

bool MlpRangeTree::EraseRange(uint64_t start, uint64_t end) {
    if (start > end) return false;
    ClearRange(start, end);
    return true;
}

bool MlpRangeTree::FindNext(uint64_t from, uint64_t& rangeStart, uint64_t& rangeEnd, void*& value) {
    NodeResult result = QueryLCPWithNode(from);
    
    if (!result.found || !result.node->IsLeaf()) {
        return false;
    }
    
    CuckooHashTableNode::LeafType type = result.node->GetLeafType();
    
    switch (type) {
        case CuckooHashTableNode::LEAF_SINGLE:
            rangeStart = rangeEnd = result.key;
            value = result.node->GetLeafData();
            return true;
            
        case CuckooHashTableNode::LEAF_RANGE_START:
            rangeStart = result.key;
            // Find the end
            {
                NodeResult endResult = QueryLCPWithNode(result.key + 1);
                if (endResult.found && endResult.node->IsLeaf() && 
                    endResult.node->GetLeafType() == CuckooHashTableNode::LEAF_RANGE_END) {
                    rangeEnd = endResult.key;
                    value = result.node->GetLeafData();
                    return true;
                }
            }
            break;
            
        case CuckooHashTableNode::LEAF_RANGE_END:
            // We're in a range
            rangeStart = result.node->GetRangeStart();
            rangeEnd = result.key;
            // Get data from start node
            {
                NodeResult startResult = QueryLCPWithNode(rangeStart);
                if (startResult.found && startResult.node->IsLeaf()) {
                    value = startResult.node->GetLeafData();
                    return true;
                }
            }
            break;
    }
    
    return false;
}

} // namespace MlpSetUInt64