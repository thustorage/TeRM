#pragma once

// mine
#include "util.hh"
#include <print-stack.hh>

#include <atomic>
#include <string>
#include <cassert>
#include <cstdio>
#include <cstdlib>

#include <chrono>
#include <sys/time.h>

static inline uint64_t NowMicros()
{
    static constexpr uint64_t kUsecondsPerSecond = 1000000;
    struct timeval tv;
    gettimeofday(&tv, nullptr);
    return static_cast<uint64_t>(tv.tv_sec) * kUsecondsPerSecond + tv.tv_usec;
}

#define TIMER_START(x) \
    const auto timer_##x = std::chrono::steady_clock::now()

#define TIMER_STOP(x)                                          \
    x += std::chrono::duration_cast<std::chrono::nanoseconds>( \
             std::chrono::steady_clock::now() - timer_##x)     \
             .count()

template <typename T>
struct Timer
{
    Timer(T &res) : start_time_(std::chrono::steady_clock::now()), res_(res) {}

    ~Timer()
    {
        res_ += std::chrono::duration_cast<std::chrono::nanoseconds>(
                    std::chrono::steady_clock::now() - start_time_)
                    .count();
    }

    std::chrono::steady_clock::time_point start_time_;
    T &res_;
};

#ifdef LOGGING
#define TIMER_START_LOGGING(x) TIMER_START(x)
#define TIMER_STOP_LOGGING(x) TIMER_STOP(x)
#define COUNTER_ADD_LOGGING(x, y) x += (y)
#else
#define TIMER_START_LOGGING(x)
#define TIMER_STOP_LOGGING(x)
#define COUNTER_ADD_LOGGING(x, y)
#endif

// #define LOGGING

#ifdef LOGGING
#define LOG(fmt, ...)                                                        \
    fprintf(stderr, "\033[1;31mLOG(<%s>:%d %s): \033[0m" fmt "\n", __FILE__, \
            __LINE__, __func__, ##__VA_ARGS__)
#else
#define LOG(fmt, ...)
#endif

class SpinLock
{
public:
    SpinLock() : mutex(false) {}
    SpinLock(std::string name) : mutex(false), name(name) {}

    bool try_lock()
    {
        bool expect = false;
        return mutex.compare_exchange_strong(
            expect, true, std::memory_order_release, std::memory_order_relaxed);
    }

    void lock()
    {
        uint64_t startOfContention = 0;
        bool expect = false;
        while (!mutex.compare_exchange_weak(expect, true, std::memory_order_release,
                                            std::memory_order_relaxed))
        {
            expect = false;
            debugLongWaitAndDeadlock(&startOfContention);
        }
        if (startOfContention != 0)
        {
            contendedTime += NowMicros() - startOfContention;
            ++contendedAcquisitions;
        }
    }

    void unlock() { mutex.store(0, std::memory_order_release); }

    void report()
    {
        LOG("spinlock %s: contendedAcquisitions %lu contendedTime %lu us",
            name.c_str(), contendedAcquisitions, contendedTime);
    }

private:
    std::atomic_bool mutex;
    std::string name;
    uint64_t contendedAcquisitions = 0;
    uint64_t contendedTime = 0;

    void debugLongWaitAndDeadlock(uint64_t *startOfContention)
    {
        if (*startOfContention == 0)
        {
            *startOfContention = NowMicros();
        }
        else
        {
            uint64_t now = NowMicros();
            if (now >= *startOfContention + 1000000)
            {
                LOG("%s SpinLock locked for one second; deadlock?", name.c_str());
            }
        }
    }
};

// read write_single lock
class ReadWriteLock
{
    // the lowest bit is used for writer
public:
    bool TryReadLock()
    {
        uint64_t old_val = lock_value.load(std::memory_order_acquire);
        while (true)
        {
            if (old_val & 1 || old_val > 1024)
            {
                break;
            }
            uint64_t new_val = old_val + 2;
            bool cas = lock_value.compare_exchange_weak(old_val, new_val,
                                                        std::memory_order_acq_rel,
                                                        std::memory_order_acquire);
            if (cas)
            {
                return true;
            }
        }
        return false;
    }

    void lock_shared()
    {
        while (!TryReadLock())
            ;
    }

    void unlock_shared()
    {
        uint64_t old_val = lock_value.load(std::memory_order_acquire);
        while (true)
        {
            if (old_val <= 1)
            {
                assert(old_val >= 2);
                return;
            }
            uint64_t new_val = old_val - 2;
            if (lock_value.compare_exchange_weak(old_val, new_val))
            {
                break;
            }
        }
    }

    bool TryWriteLock()
    {
        uint64_t old_val = lock_value.load(std::memory_order_acquire);
        while (true)
        {
            if (old_val & 1)
            {
                return false;
            }
            uint64_t new_val = old_val | 1;
            bool cas = lock_value.compare_exchange_weak(old_val, new_val);
            if (cas)
            {
                break;
            }
        }
        // got write_single lock, waiting for readers
        while (lock_value.load(std::memory_order_acquire) != 1)
        {
            asm("nop");
        }
        return true;
    }

    void lock()
    {
        while (!TryWriteLock())
            ;
    }

    void unlock()
    {
        assert(lock_value == 1);
        lock_value.store(0);
    }

private:
    std::atomic_uint_fast64_t lock_value{0};
};

#define CAS(ptr, oldval, newval) \
    (__sync_bool_compare_and_swap(ptr, oldval, newval))
class ReaderFriendlyLock
{
    std::vector<uint64_t[8]> lock_vec_;
public:
    DELETE_COPY_CONSTRUCTOR_AND_ASSIGNMENT(ReaderFriendlyLock);
    
    ReaderFriendlyLock(ReaderFriendlyLock &&rhs) noexcept
    {
        *this = std::move(rhs);
    }
    ReaderFriendlyLock& operator=(ReaderFriendlyLock &&rhs) {
        std::swap(this->lock_vec_, rhs.lock_vec_);
        return *this;
    }

    ReaderFriendlyLock() : lock_vec_(util::Schedule::max_nr_threads())
    {
        for (int i = 0; i < util::Schedule::max_nr_threads(); ++i)
        {
            lock_vec_[i][0] = 0;
            lock_vec_[i][1] = 0;
        }
    }

    bool lock()
    {
        for (int i = 0; i < util::Schedule::max_nr_threads(); ++i)
        {
            while (!CAS(&lock_vec_[i][0], 0, 1))
            {
            }
        }
        return true;
    }

    bool try_lock()
    {
        for (int i = 0; i < util::Schedule::max_nr_threads(); ++i)
        {
            if (!CAS(&lock_vec_[i][0], 0, 1))
            {
                for (i--; i >= 0; i--) {
                    compiler_barrier();
                    lock_vec_[i][0] = 0;
                }
                return false;
            }
        }
        return true;
    }

    bool try_lock_shared()
    {
        if (lock_vec_[util::Schedule::thread_id()][1]) {
            pr_once(info, "recursive lock!");
            return true;
        }
        return CAS(&lock_vec_[util::Schedule::thread_id()][0], 0, 1);
    }

    bool lock_shared()
    {
        if (lock_vec_[util::Schedule::thread_id()][1]) {
            pr_once(info, "recursive lock!");
            return true;
        }
        while (!CAS(&lock_vec_[util::Schedule::thread_id()][0], 0, 1))
        {
        }
        lock_vec_[util::Schedule::thread_id()][1] = 1;
        return true;
    }

    void unlock()
    {
        compiler_barrier();
        for (int i = 0; i < util::Schedule::max_nr_threads(); ++i)
        {
            lock_vec_[i][0] = 0;
        }
    }

    void unlock_shared()
    {
        compiler_barrier();
        lock_vec_[util::Schedule::thread_id()][0] = 0;
        lock_vec_[util::Schedule::thread_id()][1] = 0;
    }
};
