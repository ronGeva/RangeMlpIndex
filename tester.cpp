#include "MlpSetUInt64.h"

int main()
{
    MlpSetUInt64::MlpSet s;
    s.Init(4194304);
    s.Insert(1);
    s.Insert(2);
    if (s.Exist(1))
    {
        printf("1 exists\n");
    }
    else
    {
        printf("1 does not exist\n");
    }

    s.Exist(2);
    if (s.Exist(2))
    {
        printf("2 exists\n");
    }
    else
    {
        printf("2 does not exist\n");
    }

    printf("removing 1\n");
    s.Remove(1);
    s.Exist(1);

    //printf("ptr=%x, errno=%d\n", ptr, errno);
    return 0;
}