#include "MlpSetUInt64.h"

int main()
{
    MlpSetUInt64::MlpSet s;
    s.Init(4194304);

    for (uint64_t i = 0; i < 199; i++)
    {
        s.Insert(i);
    }

    // generate a "random" order of values to remove
    std::vector<uint64_t> values;
    for (size_t x = 0; x < 199; x++)
    {
        uint64_t value = (3 * x + 1) % 199;
        values.push_back(value);
    }

    for (uint64_t i: values)
    {
        s.Remove(i);
        if (s.Exist(i))
        {
            std::cout << "Error: " << i << " should not exist" << std::endl;
        }
    }
    
    return 0;
}