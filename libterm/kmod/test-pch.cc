#include <iostream>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <cassert>

#define SZ_1G (1l << 30)
// #define SZ_2G (2l << 30)
#define PAGE_SIZE (4096)


static inline uint64_t wall_time_ns()
{
    // thread_local auto first_time = std::chrono::steady_clock::now();
    // auto current = std::chrono::steady_clock::now();
    // return std::chrono::duration_cast<std::chrono::nanoseconds>(current - first_time).count();
    struct timespec ts{};
    clock_gettime(CLOCK_REALTIME, &ts);
    return ts.tv_nsec + ts.tv_sec * 1'000'000'000;
}

int main()
{
    int fd = open("/proc/pch-nvme1n1p2", O_RDONLY);
    assert(fd >= 0);
    uint8_t v;
    
    auto ts1 = wall_time_ns();
    
    // char *buf = new char[SZ_1G / PAGE_SIZE];
    // pread(fd, buf, SZ_1G, 0);
    //     for (int i = 0; i < (SZ_1G / PAGE_SIZE); i++) {
    //     printf("%u", buf[i]);
    // }
    for (size_t i = 0; i < SZ_1G; i += PAGE_SIZE) {
        auto ret = pread(fd, &v, 1, i);
        assert(ret == 1);
        printf("%u", v);
    }

    printf("\n");
    auto ts2 = wall_time_ns();
    printf("\n");
    auto elapsed = ts2 - ts1;
    printf("total %luns, average %luns\n", elapsed, elapsed / (SZ_1G / PAGE_SIZE));

    return 0;
}
