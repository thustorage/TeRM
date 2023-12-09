#pragma once
#include <condition_variable>
#include <queue>
#include <vector>

namespace util {
//    template<typename T>
//    class Queue {
//        int sz_ = 0;
//
//        std::vector<T> last_vec_;
//
//        std::mutex mutex_;
//        std::condition_variable cv_;
//
//        int head_ = 0;
//        int tail_ = 0;
//
//        int max_sz() const { return sz_ + 1; }
//
//        int size() const { return (tail_ + max_sz() - head_) % max_sz(); }
//
//        bool empty() const { return size() == 0; }
//
//        bool full() const { return size() == max_sz() - 1; }
//
//        void inc_head() { head_ = (head_ + 1) % max_sz(); }
//
//        void inc_tail() { tail_ = (tail_ + 1) % max_sz(); }
//
//    public:
//        Queue() : Queue(128) {}
//        explicit Queue(int sz) : sz_(sz), last_vec_(sz + 1) {}
//
//        void push(auto &&value) {
//            std::unique_lock lk(mutex_);
//            cv_.wait(lk, [this] { return !full(); });
//
//            last_vec_[tail_] = std::forward<decltype(value)>(value);
//            inc_tail();
//
//            lk.unlock();
//            cv_.notify_all();
//        }
//
//        T pop() {
//            std::unique_lock lk(mutex_);
//            cv_.wait(lk, [this] { return !empty(); });
//
//            T value = std::move(last_vec_[head_]);
//            inc_head();
//
//            lk.unlock();
//            cv_.notify_all();
//            return value;
//        }
//    };
    template <typename T>
    class Queue {
        int max_size_{};
        std::queue<T> queue_;
        std::mutex mutex_;
        std::condition_variable cv_;
    public:
        int size() const { return queue_.size(); }
        bool empty() const { return queue_.size() == 0; }
        bool full() const { return size() == max_size_; }

    public:
        Queue() : Queue(128) {}
        explicit Queue(int max_size) : max_size_{max_size} {}
        Queue(Queue &&queue)
            : max_size_(queue.max_size_),
            queue_(std::move(queue.queue_)),
            mutex_(std::move(queue.mutex_)),
            cv_(std::move(queue.cv_))
        {
        }

        void push(auto &&value) {
            std::unique_lock lk(mutex_);
            cv_.wait(lk, [this] { return !full(); });
            queue_.push(std::forward<decltype(value)>(value));
            lk.unlock();

            cv_.notify_all();
        }

        T pop() {
            std::unique_lock lk(mutex_);
            cv_.wait(lk, [this] { return !empty(); });

            T value = std::move(queue_.front());
            queue_.pop();

            lk.unlock();
            cv_.notify_all();
            return value;
        }
    };
}