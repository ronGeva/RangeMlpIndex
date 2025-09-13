#include "common.h"
#include "MlpSetUInt64.h"
#include "MlpSetUInt64Range.h"

#include "gtest/gtest.h"

#include "benchmark_mlp.h"

namespace {

int MlpSetBmInsert(void* tree, unsigned long long key, void* entry)
{
    entry;

    MlpSetUInt64::MlpSet* s = reinterpret_cast<MlpSetUInt64::MlpSet*>(tree);
    return s->Insert(key) ? 0 : 1;
}

void* MlpSetBmLoad(void* tree, unsigned long long index)
{
	MlpSetUInt64::MlpSet* s = reinterpret_cast<MlpSetUInt64::MlpSet*>(tree);
    s->Exist(index);
    return NULL;
}

void* MlpSetBmErase(void* tree, unsigned long long index)
{
    MlpSetUInt64::MlpSet* s = reinterpret_cast<MlpSetUInt64::MlpSet*>(tree);
    s->Remove(index);
    return NULL;
}

void MlpSetInitBmTree(MlpSetUInt64::MlpSet& s, BenchmarkTree& bm_tree)
{
    s.Init(4194304);
    memset(&bm_tree, 0, sizeof(bm_tree));
    bm_tree.tree = &s;

    bm_tree.Insert = &MlpSetBmInsert;
    bm_tree.Load = &MlpSetBmLoad;
    bm_tree.Erase = &MlpSetBmErase;
}

TEST(MlpSetBenchmarking, MlpSetBenchmarkA)
{
	MlpSetUInt64::MlpSet s;
    BenchmarkTree bm_tree;
    MlpSetInitBmTree(s, bm_tree);
    bm_run_workloadA(&bm_tree);
}

TEST(MlpSetBenchmarking, MlpSetBenchmarkB)
{
	MlpSetUInt64::MlpSet s;
    BenchmarkTree bm_tree;
    MlpSetInitBmTree(s, bm_tree);
    bm_run_workloadB(&bm_tree);
}

TEST(MlpSetBenchmarking, MlpSetBenchmarkC)
{
	MlpSetUInt64::MlpSet s;
    BenchmarkTree bm_tree;
    MlpSetInitBmTree(s, bm_tree);
    bm_run_workloadC(&bm_tree);
}

int MlpRangeBmInsertRange(void* tree, unsigned long first,
		                unsigned long last, void *entry)
{
    MlpSetUInt64::MlpRangeTree* t = reinterpret_cast<MlpSetUInt64::MlpRangeTree*>(tree);
    return t->InsertRange(first, last, entry);
}

void* MlpRangeBmFind(void* tree, unsigned long long* index, unsigned long long max)
{
    MlpSetUInt64::MlpRangeTree* t = reinterpret_cast<MlpSetUInt64::MlpRangeTree*>(tree);
    void* entry;
    uint64_t index_u64 = *index;
    uint64_t max_u64 = max;

    bool success = t->FindNext(index_u64, index_u64, max_u64, entry);
    if (success)
    {
        *index = index_u64;
        return entry;
    }

    return NULL;
}

void* MlpRangeBmLoad(void* tree, unsigned long long index)
{
	MlpSetUInt64::MlpRangeTree* t = reinterpret_cast<MlpSetUInt64::MlpRangeTree*>(tree);
    return t->Load(index);
}

void* MlpRangeBmErase(void* tree, unsigned long long index)
{
    MlpSetUInt64::MlpRangeTree* t = reinterpret_cast<MlpSetUInt64::MlpRangeTree*>(tree);
    t->Erase(index);

    return NULL;
}

void MlpRangeInitBmTree(MlpSetUInt64::MlpRangeTree& t, BenchmarkTree& bm_tree)
{
    t.Init(4194304);
    memset(&bm_tree, 0, sizeof(bm_tree));
    bm_tree.tree = &t;

    bm_tree.InsertRange = &MlpRangeBmInsertRange;
    bm_tree.Find = &MlpRangeBmFind;
    bm_tree.Load = &MlpRangeBmLoad;
    bm_tree.Erase = &MlpRangeBmErase;
}

TEST(MlpRangeBenchmarking, MlpRangeBenchmarkE)
{
    MlpSetUInt64::MlpRangeTree tree;
    BenchmarkTree bm_tree;

    MlpRangeInitBmTree(tree, bm_tree);

    bm_run_workloadE(&bm_tree);
}

} // anonymous namespace


