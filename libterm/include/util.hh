#pragma once

#include "compile.hh"
#include <logging.hh>
#include <dlfcn.h>
#include <utility>
#include <vector>
#include <ctime>
#include <string>
#include <shared_mutex>
#include <optional>
#include <cstring>
#include <source_location>
#include <atomic>
#include <numeric>
#include <thread>

#include <experimental/memory>

#ifndef RECORDER_VERBOSE
#define RECORDER_VERBOSE 0
#endif

namespace util 
{
    // a wrapper of the raw pointer, but explicitly indicates no ownership.
    // the object is managed by someone else. do not free it.
//    template <typename T>
//    class ObserverPtr {
//        T *p_ = nullptr;
//    public:
//        ObserverPtr() = default;
//        explicit ObserverPtr(T *p) : p_(p) {}
//        T* operator->() const { return p_; }
//        T& operator*() const { return *p_; }
//        auto get() const { return p_; }
//        operator bool() const {
//            return p_ != nullptr;
//        }
//    };


    // a wrapper of the raw pointer, indicating a non-owing pointer without need for detailed analysis by code readers.
    // the pointer is owned and managed by someone else, never free it.
    // google std::observer_ptr for more information.
    template <typename T>
    using ObserverPtr = std::experimental::observer_ptr<T>;

    template <typename T>
    ObserverPtr<T> make_observer(T *p)
    {
        return ObserverPtr<T>(p);
    }

    auto hash_as_bytes(const auto &v)
    {
        std::string_view sv{(const char *)&v, sizeof(v)};
        return std::hash<std::string_view>{}(sv); 
    }

    using u64 = uint64_t;
    static inline u64 hash_u64(u64 val) {
        const u64 kFNVOffsetBasis64 = 0xCBF29CE484222325;
        const u64 kFNVPrime64       = 1099511628211;
        u64 hash = kFNVOffsetBasis64;

        for (int i = 0; i < 8; i++) {
            u64 octet = val & 0x00ff;
            val = val >> 8;

            hash = hash ^ octet;
            hash = hash * kFNVPrime64;
        }
        return hash;
    }

    template <typename T, std::size_t kAligned>
    struct Aligned
    {
        inline static std::size_t aligned_bytes = kAligned;
        alignas(kAligned) T value{};

        Aligned() = default;
        Aligned(T t) : value(t) {}
        operator T&() { return value; }
        T* operator&() { return &value; }

        std::size_t hash() const {
            return std::hash<T>{}(value);
        }

        auto operator<=>(const util::Aligned<T, kAligned>& rhs) const = default;
    };
}

template <typename T, std::size_t kAligned>
struct std::hash<util::Aligned<T, kAligned>>
{
    std::size_t operator()(const util::Aligned<T, kAligned> &v) const noexcept
    {
        return v.hash();
    }
};

namespace util {
    #if ENABLE_CACHELINE_ALIGN
    template<typename T>
    using CachelineAligned = Aligned<T, 64>;
    #else
    template<typename T>
    using CachelineAligned = T;
    #endif
    static inline uint64_t wall_time_ns()
    {
        struct timespec ts{};
        clock_gettime(CLOCK_REALTIME, &ts);
        return ts.tv_nsec + ts.tv_sec * 1'000'000'000;
    }

    static inline constexpr auto div_up(auto v1, auto v2)
    {
        return (v1 + v2 - 1) / v2;
    }

    static inline constexpr auto div_down(auto v1, auto v2)
    {
        return v1 / v2;
    }
    
    static inline constexpr auto round_up(auto v1, auto v2)
    {
        return div_up(v1, v2) * v2;
    }
    
    static inline constexpr auto round_down(auto v1, auto v2)
    {
        return div_down(v1, v2) * v2;
    }

    static inline constexpr auto wrapped_inc(auto v1, auto v2)
    {
        return (v1 + 1) % v2;
    }

    static std::optional<std::string> getenv_string(const char *env)
    {
        const char *v = getenv(env);
        if (!v) {
            return std::nullopt;
        }
        return std::string(v);
    }

    static std::optional<int> getenv_int(const char *env)
    {
        const char *v = getenv(env);
        if (!v) {
            return std::nullopt;
        }
        return std::stoi(v);
    }

    static std::optional<bool> getenv_bool(const char *env)
    {
        auto v = getenv_int(env);
        if (v.has_value()) {
            return v.value() != 0;
        }
        return {};
    }

    std::vector<char> to_vec_char(const auto &v)
    {
        using T = decltype(v);
        std::vector<char> vec(sizeof(T));
        memcpy(vec.data(), &v, sizeof(T));
        return vec;
    }

    template <typename T>
    T from_vec_char(const std::vector<char> &vec)
    {
        T v;
        memcpy(&v, vec.data(), sizeof(T));
        return v;
    }

    static inline std::string vec_char_to_string(const std::vector<char> &vec)
    {
        std::string str;
        fmt::format_to(std::back_inserter(str), "{}bytes:", vec.size());
        for (int i = 0; i < vec.size(); i++) {
            fmt::format_to(std::back_inserter(str),
                "{:02x}", vec[i] & 0xff);
        }
        return str;
    }

    static inline void print_vec_char(const std::vector<char> &vec)
    {
        for (int i = 0; i < vec.size(); i++) {
            printf("%02x ", vec[i] & 0xff);
        }
        printf("\n");
    }

    template <typename T>
    size_t vec_data_bytes(const std::vector<T> &vec)
    {
        return vec.size() * sizeof(T);
    }

    static inline long long atoll_suffix(const char *str)
    {
        long long num = atoll(str);
        switch (str[strlen(str) - 1]) {
            case 'T':
            case 't':
                num *= 1024;
            case 'G':
            case 'g':
                num *= 1024;
            case 'M':
            case 'm':
                num *= 1024;
            case 'K':
            case 'k':
                num *= 1024;
        }
        return num;
    }

    static inline long long stoll_suffix(const std::string & str)
    {
        return atoll_suffix(str.c_str());
    }

    class Schedule {
        inline static std::atomic_int thread_counter_{};
        
        static int bind_cpu(int cpu_id)
        {
            cpu_set_t mask;
            CPU_ZERO(&mask);
            CPU_SET(cpu_id, &mask);
            int ret = sched_setaffinity(0, sizeof(mask), &mask);
            return ret;
        }

        // node166: 72/4=18
        // node168: 96/4=24
        // node184: 112/4=29
        static int nr_numa_cores()
        {
            static int nr = std::thread::hardware_concurrency() / 4;
            return nr;
        }

        static int bind_numa_core_ht(int numa, int core, bool hyper_threading)
        {
            static int nr_numa_cores = std::thread::hardware_concurrency() / 4;
            ASSERT(numa == 0 || numa == 1, "numa must be 0 or 1.");
            ASSERT(core >= 0 && core < nr_numa_cores, "core must within [0, %d)", nr_numa_cores);
            if (core < 0) {
                core += nr_numa_cores;
            }

            int cpu = numa * nr_numa_cores + core;
            if (hyper_threading) {
                cpu += 2 * nr_numa_cores;
            }

            int ret = bind_cpu(cpu);
            return ret;
        }

    public:
        static void bind_server_bg_cpu(int thread)
        {
            ASSERT(nr_numa_cores() == 28, "server must be on node184");
            ASSERT(thread >= 0 && thread < 40, "thread must be within [0, 34)");

            bool ht = thread % 2;
            int core = nr_numa_cores() - 1 - (thread / 2);
            int ret = bind_numa_core_ht(0, core, ht);
            ASSERT(ret == 0, "%s: failed to bind thread=%d to (numa=0, core=%d, ht=%u)", 
                strerror(errno),
                thread, core, ht);
        }

        static void bind_client_fg_cpu(int thread)
        {
            ASSERT(nr_numa_cores() == 18 || nr_numa_cores() == 24, "client must be on node166 or node168!");
            ASSERT(thread >= 0 && thread < 36, "thread must be [0, 36)");
            int numa = thread / 18;
            int core = thread % 18;
            int ret = bind_numa_core_ht(numa, core, false);
            ASSERT(ret == 0, "%s: failed to bind thread %d to (numa=%d, core=%d, ht=false)",
                strerror(errno), 
                thread, numa, core);
        }

        static int thread_id()
        {
            thread_local int id = thread_counter_.fetch_add(1);
            ASSERT(id < max_nr_threads(), "id out of max_nr_threads");
            return id;
        }

        static int max_nr_threads()
        {
            static int nr = std::thread::hardware_concurrency();
            return nr == 96 ? 72 : nr;
        }

        static int nr_threads()
        {
            return thread_counter_.load();
        }
    };

    template <typename T>
    class PerThread
    {
        std::vector<CachelineAligned<T>> data_;
    public:
        PerThread() : data_(Schedule::max_nr_threads())
        {
        }
        T &this_thread()
        {
            return data_[util::Schedule::thread_id()]; 
        }

        T &operator[](std::size_t idx) { return data_[idx]; }
        auto begin() { return data_.begin(); }
        auto end() { return data_.begin() + nr_threads(); }
        auto nr_threads() const { return Schedule::nr_threads(); }

        void fill(const CachelineAligned<T> &value)
        {
            std::fill(data_.begin(), data_.end(), value);
        }

        void reduce(T &value, std::function<void(T&, T&)> op)
        {
            for (auto &u : data_) {
                op(value, u);
            }
        }
    };

    // define our own source location class, to manipulate the format.
    class SourceLocation {
        uint32_t line_;
        std::string function_name_;
        std::string file_name_;
    public:
        SourceLocation(const char *file_full_path = __builtin_FILE(), const uint32_t line = __builtin_LINE(), const char *function_name = __builtin_FUNCTION())
                : line_(line), function_name_(function_name)
        {
            std::string str{"/"};
            str += file_full_path;
            auto pos = str.find_last_of('/');
            file_name_ = str.substr(pos + 1);
        }

        static SourceLocation current(const char *file_full_path = __builtin_FILE(), const uint32_t line = __builtin_LINE(), const char *function_name = __builtin_FUNCTION())
        {
            return SourceLocation{file_full_path, line, function_name};
        }

        auto file_name() { return file_name_.c_str(); }
        auto line() { return line_; }
        auto function_name() { return function_name_.c_str(); }
    };

#ifndef RECORDER_SINGLE_THREAD
#define RECORDER_SINGLE_THREAD 1
#endif
    template <bool Verbose = true>
    class Recorder {
    public:
        using result_type = std::tuple<uint64_t, uint64_t, uint64_t>; // cnt, sum, avg
    protected:
        std::string name_;
        PerThread<uint64_t> sum_;
        PerThread<uint64_t> cnt_;
        int report_thread_id_;
        uint64_t last_report_time_ = 0;

        bool enable_;

        SourceLocation src_loc_;

        void record(uint64_t v)
        {
            sum_.this_thread() += v;
            cnt_.this_thread() ++;
        }

        std::tuple<uint64_t, uint64_t, uint64_t> tick()
        {
            uint64_t sum = 0;
            for (auto &u : sum_) { sum += u; }
            uint64_t cnt = 0;
            for (auto &u : cnt_) { cnt += u; }
            u64 avg = cnt ? sum / cnt : 0;
            return std::make_tuple(cnt, sum, avg);
        }

        void print(const result_type &value)
        {
            pr_flf(src_loc_.file_name(), src_loc_.line(), src_loc_.function_name(), "%s",
                    fmt::format(std::locale(""), "{}, cnt={:L}, sum={:L}, avg={:L}", name_, std::get<0>(value),
                        std::get<1>(value), (std::get<1>(value)) / std::get<0>(value)).c_str());
        }

        void clear()
        {
            sum_.fill(0);
            cnt_.fill(0);
        }

        bool should_tick(uint64_t current_time_ns = util::wall_time_ns())
        {
            if (Schedule::thread_id() != report_thread_id_) {
                return false;
            }

            if (current_time_ns - last_report_time_ > 1'000'000'000) {
                last_report_time_ = current_time_ns;
                return true;
            }
            return false;
        }
    public:
        explicit Recorder(std::string name, bool enable = true, SourceLocation src_loc = SourceLocation::current())
            : name_(std::move(name)),
                enable_(enable),
                report_thread_id_{util::Schedule::thread_id()},
                src_loc_(std::move(src_loc))
        {}

        virtual ~Recorder() = default;

        void record_one(uint64_t v)
        {
            if (Verbose && !RECORDER_VERBOSE) return;
            if (!enable_) return;
            if (RECORDER_SINGLE_THREAD && util::Schedule::thread_id() != report_thread_id_) return;
            record(v);
            if (should_tick()) {
                auto r = tick();
                print(r);
                clear();
            }
        }
    };

    template <bool Verbose = true>
    class LatencyRecorder : public Recorder<Verbose> {
    private:
        PerThread<uint64_t> ts_begin_;

    public:
        LatencyRecorder(const std::string &name, bool enable = true, SourceLocation src_loc = SourceLocation::current())
            : Recorder<Verbose>(name, enable, std::move(src_loc)) {}

        void begin_one()
        {
            if (Verbose && !RECORDER_VERBOSE) return;
            if (!Recorder<Verbose>::enable_) return;
            if (RECORDER_SINGLE_THREAD && Schedule::thread_id() != Recorder<Verbose>::report_thread_id_) return;
            ts_begin_.this_thread() = wall_time_ns();
        }

        void end_one()
        {
            if (Verbose && !RECORDER_VERBOSE) return;
            if (!Recorder<Verbose>::enable_) return;
            if (RECORDER_SINGLE_THREAD && Schedule::thread_id() != Recorder<Verbose>::report_thread_id_) return;
            auto current_time_ns = wall_time_ns();
            auto latency_ns = current_time_ns - ts_begin_.this_thread();

            Recorder<Verbose>::record(latency_ns);
            if (Recorder<Verbose>::should_tick(current_time_ns)) {
                auto r = Recorder<Verbose>::tick();
                Recorder<Verbose>::print(r);
                Recorder<Verbose>::clear();
                return;
            }
            return;
        }
    };

    template <typename T>
    class LatencyRecorderHelper {
        T &lr_;
    public:
        LatencyRecorderHelper(T &lr) : lr_(lr)
        {
            lr_.begin_one();
        }
        ~LatencyRecorderHelper()
        {
            lr_.end_one();
        }
    };

    class LatencyDistribution {
    private:
        static constexpr size_t kMaxLatencyNs = 100'000'000; // 10ms
        static constexpr size_t kLatencyUnitNs = 100; // 0.1us
        static constexpr size_t kNrUnits = kMaxLatencyNs / kLatencyUnitNs;

        int nr_threads_;
        std::vector<std::vector<uint64_t>> bucket_;
        std::vector<CachelineAligned<uint64_t>> min_;
        std::vector<CachelineAligned<uint64_t>> max_;
        std::vector<CachelineAligned<uint64_t>> sum_;
        std::vector<CachelineAligned<uint64_t>> cnt_;
        std::vector<uint64_t> global_bucket_;

    public:
        LatencyDistribution(int nr_threads)
            : nr_threads_(nr_threads),
              bucket_(nr_threads_, std::vector<uint64_t>(kNrUnits)),
              min_(nr_threads_),
              max_(nr_threads_),
              sum_(nr_threads_),
              cnt_(nr_threads_),
              global_bucket_(kNrUnits)
        {
            for (int i = 0; i < nr_threads_; i++) {
                clear(i);
            }
        }

        void record(int thread, uint64_t latency_ns)
        {
            uint64_t id = latency_ns / kLatencyUnitNs;
            if (id >= kNrUnits) {
                bucket_[thread].back()++;
            } else {
                bucket_[thread][id]++;
            }

            min_[thread] = std::min<uint64_t>(min_[thread], latency_ns);
            max_[thread] = std::max<uint64_t>(max_[thread], latency_ns);
            sum_[thread] += latency_ns;
            cnt_[thread]++;
        }

        void clear(int thread)
        {
            auto &v = bucket_[thread];
            std::fill(v.begin(), v.end(), 0);
            min_[thread] = std::numeric_limits<uint64_t>::max();
            max_[thread] = std::numeric_limits<uint64_t>::min();
            sum_[thread] = 0;
            cnt_[thread] = 0;
        }

        auto get_and_clear()
        {
            uint64_t p50_lat = -1;
            uint64_t p90_lat = -1;
            uint64_t p95_lat = -1;
            uint64_t p99_lat = -1;
            uint64_t p999_lat = -1;
            uint64_t p9999_lat = -1;
            uint64_t min_lat = -1;
            uint64_t max_lat = 0;
            uint64_t sum = 0;
            uint64_t cnt = 0;

            std::fill(global_bucket_.begin(), global_bucket_.end(), 0);

            // std::string out;
            for (int thread = 0; thread < nr_threads_; thread++) {
                min_lat = std::min<uint64_t>(min_lat, min_[thread]);
                max_lat = std::max<uint64_t>(max_lat, max_[thread]);
                sum += sum_[thread];
                cnt += cnt_[thread];
                // fmt::format_to(std::back_inserter(out), "{} ", cnt_[thread].value);

                for (size_t i = 0; i < kNrUnits; i++) {
                    global_bucket_[i] += bucket_[thread][i];
                }
                clear(thread);
            }
            // pr_info("%s", out.c_str());

            uint64_t avg_lat = 0;
            if (cnt) {
                avg_lat = sum / cnt;
            }

            uint64_t total = std::accumulate(global_bucket_.begin(), global_bucket_.end(), 0ul); // total differs from cnt!
            if (total) {
                uint64_t cum = 0;
                for (size_t i = 0; i < kNrUnits; i++) {
                    cum += global_bucket_[i];
                    if (p50_lat == -1 && cum >= 50 * total / 100) {
                        p50_lat = i * kLatencyUnitNs;
                    }
                    if (p90_lat == -1 && cum >= 90 * total / 100) {
                        p90_lat = i * kLatencyUnitNs;

                    }
                    if (p95_lat == -1 && cum >= 95 * total / 100) {
                        p95_lat = i * kLatencyUnitNs;
                    }
                    if (p99_lat == -1 && cum >= 99 * total / 100) {
                        p99_lat = i * kLatencyUnitNs;
                    }
                    if (p999_lat == -1 && cum >= 999 * total / 1000) {
                        p999_lat = i * kLatencyUnitNs;
                    }
                    if (p9999_lat == -1 && cum >= 9999 * total / 10000) {
                        p9999_lat = i * kLatencyUnitNs;
                    }
                }
            }

            return std::make_tuple(cnt, avg_lat, min_lat, max_lat, p50_lat, p90_lat, p95_lat, p99_lat, p999_lat, p9999_lat);
        }

        auto report_and_clear()
        {
            auto [cnt, avg_lat, min_lat, max_lat, p50_lat, p90_lat, p95_lat, p99_lat, p999_lat, p9999_lat]
                 = get_and_clear();
            std::string str = fmt::format(std::locale(""),
                                          "cnt={:L}, avg={:L}, min={:L}, max={:L}, p50={:L}, p90={:L}, p95={:L}, p99={:L}, p999={:L}, p9999={:L}",
                                          cnt, avg_lat, min_lat, max_lat, p50_lat, p90_lat, p95_lat, p99_lat, p999_lat, p9999_lat);

            return str;
        }
    };

    template <class T>
    inline void hash_combine(std::size_t& seed, const T& v)
    {
        std::hash<T> hasher;
        seed ^= hasher(v) + 0x9e3779b9 + (seed<<6) + (seed>>2);
    }

//    class Span {
//        uint64_t begin_;
//        uint64_t end_;
//    public:
//            class Iter {
//                uint64_t value_;
//            public:
//                Iter(uint64_t value) : value_(value) {}
//                uint64_t operator*() { return value_; }
//                Iter& operator+=(const Iter &rhs)
//                {
//                    value_ += rhs.value_;
//                    return *this;
//                }
//
//            };
//        Span(uint64_t begin, uint64_t end) : begin_(begin), end_(end) {}
//        uint64_t size() const { return end_ - begin_; }
//        uint64_t begin() { return begin_; }
//        uint64_t end() { return end_; }
//    };
}
