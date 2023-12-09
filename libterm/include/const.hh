#include <cstdint>

#define SZ_K (1ul << 10)
#define SZ_M (1ul << 20)
#define SZ_1K (1ul << 10)
#define SZ_2K (2ul << 10)
#define SZ_4K (4ul << 10)
#define SZ_64K (64ul << 10)
#define SZ_256K (256ul << 10)
#define SZ_512K (512ul << 10)
#define SZ_1M (1ul << 20)
#define SZ_2M (2ul << 20)
#define SZ_4M (4ul << 20)
#define SZ_256M (256ul << 20)
#define SZ_1G (1ul << 30)
#define SZ_2G (2ul << 30)

#define PAGE_SHIFT 12
#define PAGE_SIZE (1ul << PAGE_SHIFT)
#define SECTOR_SIZE 512

using u8 = uint8_t;
using u16 = uint16_t;
using u32 = uint32_t;
using u64 = uint64_t;
