#pragma once

#include <concepts>

#define ENABLE_CACHELINE_ALIGN 1
#if ENABLE_CACHELINE_ALIGN
#define __cacheline_aligned alignas(64)
#else
#define __cacheline_aligned
#endif

#define __packed __attribute__((__packed__))

inline void compiler_barrier() { asm volatile("" ::: "memory"); }

template <typename Ref, typename T>
concept explicit_ref = std::same_as<Ref, std::reference_wrapper<T>>;

#define DELETE_MOVE_CONSTRUCTOR_AND_ASSIGNMENT(Class) \
    Class(Class&&) = delete;                          \
    Class& operator=(Class&&) = delete;
#define DELETE_COPY_CONSTRUCTOR_AND_ASSIGNMENT(Class) \
    Class(const Class&) = delete;                     \
    Class& operator=(const Class&) = delete;
