#pragma once
#include <util.hh>

namespace util {
    using ulong_t = uint64_t;
    static constexpr inline uint64_t kBitsPerByte = 8;
    static constexpr inline uint64_t kBitsPerLong = sizeof(ulong_t) * kBitsPerByte;
    static constexpr inline uint64_t kBitsPerU64 = sizeof(uint64_t) * kBitsPerByte;
    static constexpr inline uint64_t kPageSize = 4096;

    static bool test_bit(uint32_t nr, const uint64_t *addr)
    {
        return 1ul & (addr[nr / kBitsPerLong] >> (nr % kBitsPerLong));
    }

    static inline void set_bit(uint32_t nr, uint64_t *addr)
    {
        addr[nr / kBitsPerLong] |= 1UL << (nr % kBitsPerLong);
    }

    static inline void clear_bit(uint32_t nr, uint64_t *addr)
    {
        addr[nr / kBitsPerLong] &= ~(1UL << (nr % kBitsPerLong));
    }

    static constexpr uint64_t bits_to_longs(uint64_t bits)
    {
        return util::div_up(bits, kBitsPerLong);
    }

    static constexpr uint64_t bits_to_u64s(uint64_t bits)
    {
        return util::div_up(bits, kBitsPerU64);
    }

    static constexpr uint64_t bits_to_long_aligned_bytes(uint64_t bits)
    {
        return util::bits_to_longs(bits) * sizeof(ulong_t);
    }

    static uint64_t mr_length_to_pgs(uint64_t mr_length)
    {
        return util::div_up(mr_length, kPageSize);
    }

    class Bitmap
    {
        ulong_t *data_{};
        uint32_t bits_{};
    public:
        using Uptr = std::unique_ptr<Bitmap>;
        Bitmap(uint32_t bits, bool set) : bits_(bits)
        {
            data_ = new ulong_t[bits_to_longs(bits_)];
            if (set) {
                set_all();
            } else {
                clear_all();
            }
        }
        ~Bitmap()
        {
            delete[] data_;
        }
        bool test_bit(uint32_t nr)
        {
            return util::test_bit(nr, data_);
        }
        void set_bit(uint32_t nr)
        {
            util::set_bit(nr, data_);
        }
        void assign_bit(uint32_t nr, bool set)
        {
            if (set) {
                set_bit(nr);
            } else {
                clear_bit(nr);
            }
        }
        void clear_bit(uint32_t nr)
        {
            util::clear_bit(nr, data_);
        }
        void clear_all()
        {
            memset(data_, 0, sizeof(ulong_t) * bits_to_longs(bits_));
        }
        void set_all()
        {
            memset(data_, 0xff, sizeof(ulong_t) * bits_to_longs(bits_));
        }
    };
}