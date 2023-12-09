#pragma once
#include "util.hh"

#include <libmemcached/memcached.hpp>
#include <vector>
#include <string>
#include <mutex>
#include <unistd.h>

namespace rdma {
    using std::vector;
    using std::string;

class SharedMetaService {
    std::string hostname_;
    in_port_t port_;
    memcache::Memcache memc_;
    std::mutex mutex_;

public:
    SharedMetaService(const std::string &hostname, in_port_t port)
        : hostname_{hostname}, port_{port}, memc_(hostname, port)
    {
        if (!memc_.setBehavior(MEMCACHED_BEHAVIOR_BINARY_PROTOCOL, 1)) {
            throw std::runtime_error("init memcached");
        }
    }

    std::string to_string() const
    {
        return hostname_ + ":" + std::to_string(port_);
    }

    void put(const std::string &key, const std::vector<char> &value)
    {
        std::unique_lock lk(mutex_);
//        while (true) {
//            memcached_return rc;
//            rc = memcached_set((memcached_st *)memc_.getImpl(), key.data(), key.size(),
//                               value.data(), value.size(), (time_t)0, 0);
//            if (rc == MEMCACHED_SUCCESS) break;
//            usleep(400);
//        }
        while (!memc_.set(key, value, 0, 0)) {
            usleep(400);
        }
//        pr_info("key={%s}, value={%s}", key.c_str(), util::vec_char_to_string(value).c_str());
    }

    std::vector<char> get(const std::string &key)
    {
        std::vector<char> ret_val;
        size_t value_size;
        uint32_t flags;
        memcached_return rc;

        std::unique_lock lk(mutex_);

//        while (true) {
//            char *value = memcached_get((memcached_st *)memc_.getImpl(), key.c_str(), key.length(),
//                                        &value_size, &flags, &rc);
//            if (value && rc == MEMCACHED_SUCCESS) {
//                ret_val.resize(value_size);
//                memcpy(ret_val.data(), value, value_size);
//                break;
//            }
//            usleep(400);
//        }

        do {
            bool success = memc_.get(key, ret_val);
            if (!success) {
                usleep(10000);
            } else {
                break;
            }
        } while (true);
//        pr_info("key={%s}, value={%s}", key.c_str(), util::vec_char_to_string(ret_val).c_str());

        return ret_val;
    }

    int get_int(const std::string &key)
    {
        std::vector<char> ret_val;
        ret_val = get(key);
        string str(ret_val.begin(), ret_val.end());
        return std::stoi(str);
    }

    uint64_t inc(const std::string &key, bool auto_retry = true)
    {
        uint64_t res;

        do {
            bool success = memc_.increment(key, 1, &res);
            if (!success && auto_retry) {
                usleep(10000);
            } else {
                break;
            }
        } while (true);

        return res;
    }

    uint64_t dec(const std::string &key)
    {
        uint64_t res;
        while (!memc_.decrement(key, 1, &res)) {
            usleep(10000);
        }
        return res;
    }
};
} // namespace pdp