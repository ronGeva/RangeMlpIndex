#include "common.h"
#include "MlpSetUInt64.h"

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

} // anonymous namespace


