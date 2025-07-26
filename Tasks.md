1. Make the allocation/de-allocation platform-specific (or make it run in any environment using C++ code).
   Currently the mmap usage doesn't work on WSL.
2. Implement remove.
3. Implement multi-threaded remove.
4. Make Insert multi-threaded.
5. Implement tests for 2,3,4.
6. Make the compilation faster (more options for tests?)
