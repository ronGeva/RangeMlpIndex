#pragma once

#include "common.h"
#include <shared_mutex>
#include <atomic>

#include <mutex>
static std::mutex debug_print_mutex;


// #define TRACE

#define NUM_CHILDREN(generation) ((generation >> 24) & 0xff) + 1
#define SET_NUM_CHILDREN(generation, k) (generation.store((generation.load(std::memory_order_seq_cst) & 0x00ffffff) | (k << 24), std::memory_order_seq_cst))

#ifndef TRACE
#define DEBUG(msg)
#else
#define DEBUG(msg) do { \
    	               auto n = std::chrono::system_clock::now(); \
    	               auto ns = std::chrono::duration_cast<std::chrono::nanoseconds>(n.time_since_epoch()) % std::chrono::seconds(1); \
    	               std::time_t t = std::chrono::system_clock::to_time_t(n); \
    	               std::tm tm = *std::localtime(&t); \
					   const char* file = std::strrchr(__FILE__, '/'); \
					   file = file ? file + 1 : __FILE__; \				   
					   std::lock_guard<std::mutex> lock(debug_print_mutex); \
    	               std::cout << std::put_time(&tm, "%H:%M:%S.") << std::setw(9) << std::setfill('0') << ns.count() << " " \
    	                         << "T" << std::this_thread::get_id() << ' ' << file << ':' << __LINE__ << ':' << __func__ \
    	                         << " - " << msg << std::endl; \
					} while(0)
#endif
#include <optional>

namespace MlpSetUInt64
{


// Displacement can't be protected by generation.
static std::shared_mutex displacement_mutex;

class LockGuard {
public:
	LockGuard(std::shared_mutex *m, bool is_shared);
	~LockGuard();

private:
	std::shared_mutex *m;
	bool _is_shared;
};

// Cuckoo hash table node
//
struct CuckooHashTableNode
{
	// root ======[parent]--child--*---------path-compression-string-------[this]--child-- ....... -- [minimum value in subtree]
	//                          indexLen                                 fullKeyLen                   8-byte minKey
	//
	// 2 bit: occupy flag, 00 = not used, 10 = used as node, 11 = used as bitmap
	// 3 bit: length of the indexing part of the key (1-8), indexLen in above diagram
	// 3 bit: length of the full key containing path-compressed bytes (1-8), fullKeyLen in above diagram
	// 3 bit: 000 = using internal map, 100 = pointer external map, otherwise offset of the external bitmap 
	// 3 bit: # of childs if using internal map, 0 + bitmap's highest 2 bits if using external bitmap
	// 18 bit: hash 
	//
	uint32_t hash;	
	// points to min node in this subtree - UNUSED in the original implementation
	// generation first byte is used for num of children, second byte for generation.
	std::atomic<uint32_t> generation;
	// the min node's full key
	// the first indexLen bytes prefix is this node's index into the hash table
	// the first fullKeyLen bytes prefix is this node's index plus path compression part
	// the whole minKey is the min node's key
	//
	uint64_t minKey;
	// the child map
	// when using internal map, each byte stores a child
	// when using external bitmap, each bit represent whether the corresponding child exists
	// when using pointer external bitmap, this is the pointer to the 32-byte bitmap
	// when it is a leaf, this is the opaque data pointer
	// 
	std::atomic<uint64_t> childMap;

	// Copy fields from another node in a way that works with std::atomic
	void CopyWithoutGeneration(const CuckooHashTableNode& other) {
		hash = other.hash;
		minKey = other.minKey;
		childMap.store(other.childMap.load(std::memory_order_seq_cst), std::memory_order_seq_cst);
	}

	
	void Clear()
	{
		hash = 0;
		generation.store(0, std::memory_order_seq_cst);
		minKey = 0;
		childMap.store(0, std::memory_order_seq_cst);
	}

	bool IsEqual(uint32_t expectedHash, int shiftLen, uint64_t shiftedKey)
	{
		return ((hash & 0xf803ffffU) == expectedHash) && (minKey >> shiftLen == shiftedKey);
	}
	
	bool IsEqualNoHash(uint64_t key, int len)
	{
		return ((hash & 0xf8000000U) == (0x80000000U | uint32_t(((len) - 1) << 27)) && (key >> (64-8*len)) == (minKey >> (64-8*len)));
	}
	
	int GetOccupyFlag()
	{
		uint32_t hashVal = hash;
		assert((hashVal >> 30) != 1);
		return hashVal >> 30;
	}
	
	bool IsOccupied()
	{
		return GetOccupyFlag() != 0;
	}
	
	bool IsNode()
	{
		assert(IsOccupied());
		return GetOccupyFlag() == 2;
	}
	
	// the assert-less version
	//
	bool IsOccupiedAndNode()
	{
		return GetOccupyFlag() == 2;
	}
	
	uint32_t GetHash18bit()
	{
		assert(IsNode());
		return hash & ((1 << 18) - 1);
	}
	
	int GetIndexKeyLen()
	{
		assert(IsNode());
		return 1 + ((hash >> 27) & 7);
	}
	
	uint64_t GetIndexKey()
	{
		assert(IsNode());
		int shiftLen = 64 - GetIndexKeyLen() * 8;
		return minKey >> shiftLen << shiftLen;
	}
	
	// DANGER: make sure you know what you are doing...
	//
	void AlterIndexKeyLen(int newIndexKeyLen)
	{
		assert(IsNode());
		uint32_t hashVal = hash;
		hashVal &= 0xc7ffffffU;
		hashVal |= (newIndexKeyLen - 1) << 27;
		hash = hashVal;
	}
	
	// DANGER: make sure you know what you are doing...
	//
	void AlterHash18bit(uint32_t hash18bit)
	{
		assert(IsNode());
		assert(0 <= hash18bit && hash18bit < (1<<18));
		uint32_t hashVal = hash;
		hashVal &= 0xfffc0000;
		hashVal |= hash18bit;
		hash = hashVal;
	}
	
	int GetFullKeyLen()
	{
		assert(IsNode());
		return 1 + ((hash >> 24) & 7);
	}
	
	uint64_t GetFullKey()
	{
		assert(IsNode());
		return minKey;
	}
	
	bool IsLeaf()
	{
		assert(IsNode());
		return GetFullKeyLen() == 8;
	}
	
	bool IsUsingInternalChildMap()
	{
		assert(IsNode());
		return ((hash >> 21) & 7) == 0;
	}
	
	bool IsExternalPointerBitMap()
	{
		assert(IsNode() && !IsUsingInternalChildMap());
		return ((hash >> 21) & 7) == 4;
	}
	
	int GetChildNum()
	{
		if (NUM_CHILDREN(generation.load(std::memory_order_seq_cst)) <= 7)
		{
			return 1 + ((hash >> 18) & 7);
		}

		return NUM_CHILDREN(generation.load(std::memory_order_seq_cst));
	}
	
	void SetChildNum(int k)
	{
		if (k <= 8 && k != 0)
		{
			hash &= 0xffe3ffffU;
			hash |= ((k-1) << 18);
		}

		if (k != 0) {
			SET_NUM_CHILDREN(generation, k - 1);
		} else {
			SET_NUM_CHILDREN(generation, 0);
		}
	}
	
	void Init(int ilen, int dlen, uint64_t dkey, uint32_t hash18bit, int firstChild, uint32_t start_gen);
	
	int FindNeighboringEmptySlot();
	
	void BitMapSet(int child, bool on=true);
	
	// TODO: free external bitmap memory when hash table is destroyed
	//
	uint64_t* AllocateExternalBitMap();
	
	// Switch from internal child list to internal/external bitmap
	//
	void ExtendToBitMap(uint32_t generation);
	
	// Find minimum child >= given child
	// returns -1 if larger child does not exist
	//
	int LowerBoundChild(uint32_t child);
	
	// Check if given child exists
	//
	bool ExistChild(int child);
	
	// Add a new child, must not exist
	//
	void AddChild(int child, uint32_t generation);

	void RevertToInternalBitmap();

	// Remove a child, must exist
	// Returns whether we now have 0 children
	bool RemoveChild(int child);

	// for debug only, get list of all children in sorted order
	//
	vector<int> GetAllChildren();
	
	// Copy its internal bitmap to external
	//
	uint64_t* CopyToExternalBitMap();
	
	// Move this node as well as its bitmap to target
	//
	void MoveNode(CuckooHashTableNode* target, uint32_t generation);
	
	// Relocate its internal bitmap to another position
	//
	void RelocateBitMap();
};

static_assert(sizeof(CuckooHashTableNode) == 24, "size of node should be 24");

// This class does not own the main hash table's memory
// TODO: it should manage the external bitmap memory, but not implemented yet
//
class CuckooHashTable
{
public:

#ifdef ENABLE_STATS
	struct Stats
	{
		uint32_t m_slowpathCount;
		uint32_t m_movedNodesCount;
		uint32_t m_relocatedBitmapsCount;
		uint32_t m_lcpResultHistogram[9];
		Stats();
		void ClearStats();
		void ReportStats();
	};
#endif

	class LookupMustExistPromise
	{
	public:
		LookupMustExistPromise() : valid(0) {}
		LookupMustExistPromise(CuckooHashTableNode* h)
			: valid(1)
			, h1(h)
			, h2(nullptr)
		{ }
		
		LookupMustExistPromise(uint16_t valid, uint16_t shiftLen, 
		                       CuckooHashTableNode* h1, CuckooHashTableNode* h2, 
		                       uint32_t expectedHash, uint64_t shiftedKey)
			: valid(valid)
			, shiftLen(shiftLen)
			, h1(h1)
			, h2(h2)
			, expectedHash(expectedHash)
			, shiftedKey(shiftedKey)
		{ }
		
		bool IsValid() { return valid; }

		bool IsGenerationValid(uint32_t generation) { 
			return h1->generation.load(std::memory_order_seq_cst) <= generation && (h2 == nullptr || h2->generation.load(std::memory_order_seq_cst) <= generation);
		}
		
		uint64_t Resolve()
		{
			assert(IsValid());
			if (h2 == nullptr || h1->IsEqual(expectedHash, shiftLen, shiftedKey))
			{
				return h1->minKey;
			}
			else
			{
				return h2->minKey;
			}
		}

		
		
		void Prefetch()
		{
			assert(IsValid());
			if (h2 != nullptr)
			{
				MEM_PREFETCH(*h1);
				MEM_PREFETCH(*h2);
			}
		}
		
	private:
		uint16_t valid;
		uint16_t shiftLen;
		CuckooHashTableNode* h1;
		CuckooHashTableNode* h2;
		uint32_t expectedHash;
		uint64_t shiftedKey;
	};
	
	CuckooHashTable();
	
	void Init(CuckooHashTableNode* _ht, uint64_t _mask);
	
	// Execute Cuckoo displacements to make up a slot for the specified key
	//
	uint32_t ReservePositionForInsert(int ilen, uint64_t dkey, uint32_t hash18bit, bool& exist, bool& failed, uint32_t generation);
	
	// Insert a node into the hash table
	// Since we use path-compression, if the node is not a leaf, it must has at least one child already known
	// In case it is a leaf, firstChild should be -1
	//
	uint32_t Insert(int ilen, int dlen, uint64_t dkey, int firstChild, bool& exist, bool& failed, uint32_t generation);

	// Single point lookup, returns index in hash table if found
	//
	uint32_t Lookup(int ilen, uint64_t ikey, bool& found);

	// Removes a node from the hash table
	// Returns true if the node is removed, false if it does not exist
	bool Remove(int ilen, uint64_t key);

	// Single point lookup on a key that is supposed to exist
	//
	CuckooHashTable::LookupMustExistPromise GetLookupMustExistPromise(int ilen, uint64_t ikey);
	
	// Fast LCP query using vectorized hash computation and memory level parallelism
	// Since we only store nodes of depth >= 3 in hash table, 
	// this function will return 2 if the LCP is < 3 (even if the real LCP is < 2).
	// In case this function returns > 2,
	//   idxLen will be the index len of the lcp node in hash table (so allPositions1[idxLen - 1] will be the lcp node)
	//   for i >= idxLen - 1, allPositions1[i] will be the node for prefix i+1 (0 if not exist)
	//   for 2 <= i < idxLen - 1, allPositions1[i] and allPositions2[i] will be the possible 2 places where the node show up,
	//   and expectedHash[i] will be its expected hash value.
	// allPositions1, allPositions2, expectedHash must be buffers at least 32 bytes long. 
	//
	int QueryLCPInternal(uint64_t key, 
                 		 uint32_t& idxLen, 
                 		 uint32_t* allPositions1, 
                 		 uint32_t* allPositions2, 
                 		 uint32_t* expectedHash,
				 		 uint32_t generation);

	int QueryLCP(uint64_t key, 
                 uint32_t& idxLen, 
                 uint32_t* allPositions1, 
                 uint32_t* allPositions2, 
                 uint32_t* expectedHash,
				 std::atomic<uint32_t>& cur_generation);
	
	// hash table array pointer
	//
	CuckooHashTableNode* ht;
	// hash table mask (always a power of 2 minus 1)
	//
	uint32_t htMask;
#ifdef ENABLE_STATS
	// statistic info
	//
	Stats stats;
#endif

private:
	void HashTableCuckooDisplacement(uint32_t victimPosition, int rounds, bool& failed, uint32_t generation);
	
#ifndef NDEBUG
	bool m_hasCalledInit;
#endif
};

// Removed global empty_promise to fix race condition - now use local construction

class MlpSet
{

public:
// Single writer implies no need for atomic operation. On the other hand using atomic we,
// can reduce amount of times we see an old generation which may cause more restarts, might be worth to try both atomic and non-atomic
std::atomic<uint32_t> cur_generation;

#ifdef ENABLE_STATS
	struct Stats
	{
		uint32_t m_lowerBoundParentPathStepsHistogram[8];
		Stats();
		void ClearStats();
		void ReportStats();
	};
	Stats stats;
#endif

	typedef CuckooHashTable::LookupMustExistPromise Promise;
	
	MlpSet();
	~MlpSet();
	
	// Initialize the set to hold at most maxSetSize elements
	//
	void Init(uint32_t maxSetSize);
	
	// Insert an element, returns true if the insertion took place, false if the element already exists
	//
	bool Insert(uint64_t value);

	// Removes an element, returns true if the removal took place, false if the element doesn't exists
	// bool Remove(uint64_t value);
	
	// Returns whether the specified value exists in the set
	//
	bool Exist(uint64_t value);
	
	// Returns the minimum value greater or equal to the specified value
	// set `found` to false and return -1 if specified value is larger than all values in set 
	//
	uint64_t LowerBound(uint64_t value, bool& found);
	
	// Returns a promise for lower_bound
	// Promise.IsValid() denotes if lower_bound doesn't exist
	// The promise can be resolved via Promise.Resolve() to get the lower_bound
	//
	MlpSet::Promise LowerBound(uint64_t value);
	
	// For debug purposes only
	//
	std::atomic<uint64_t>* GetRootPtr() { return m_root; }
	std::atomic<uint64_t>* GetLv1Ptr() { return m_treeDepth1; }
	std::atomic<uint64_t>* GetLv2Ptr() { return m_treeDepth2; }
	CuckooHashTable* GetHtPtr() { return &m_hashTable; }
	
#ifdef ENABLE_STATS
	void ClearStats();
	void ReportStats();
#endif

private:
	MlpSet::Promise LowerBoundInternal(uint64_t value, bool& found, uint32_t generation);

	void ClearL1Cache(uint64_t value, std::optional<uint64_t> successor);

	void ClearL2Cache(uint64_t value, std::optional<uint64_t> successor);

	std::optional<uint64_t> ClearL1AndL2Caches(uint64_t value);
	
	// we mmap memory all at once, hold the pointer to the memory chunk
	// TODO: this needs to changed after we support hash table resizing 
	//
	void* m_memoryPtr;
	uint64_t m_allocatedSize;
	
	// flat bitmap mapping parts of the tree
	// root and depth 1 should be in L1 or L2 cache
	// root of the tree, length 256 bits (32B)
	//
	std::atomic<uint64_t>* m_root;
	// lv1 of the tree, 256^2 bits (8KB)
	//
	std::atomic<uint64_t>* m_treeDepth1;
	// lv2 of the tree, 256^3 bits (2MB), not supposed to be in cache
	//
	std::atomic<uint64_t>* m_treeDepth2;
	// hash mapping parts of the tree, starting at lv3
	//
	CuckooHashTable m_hashTable;
	
#ifndef NDEBUG
	bool m_hasCalledInit;
#endif
};

}	// namespace MlpSetUInt64
 
