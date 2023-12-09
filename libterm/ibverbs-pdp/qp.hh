#pragma once
// mine
#include "global.hh"
#include "rpc.hh"
#include <queue.hh>
#include <lock.hh>
// sys
#include <sys/sysinfo.h>
// boost
// #include <boost/asio.hpp>
// c++
#include <mutex>
#include <functional>
#include <unordered_set>

namespace pdp {
    class WrId {
    private:
        union {
            uint64_t u64_value_;
            struct {
                uint32_t wr_seq_; // max_send_wr uint32_t
                uint8_t user_signaled_ : 1;
            };
        };
    public:
        WrId(uint64_t u64_value) : u64_value_(u64_value) {}
        WrId(uint32_t wr_seq, bool user_signaled) : wr_seq_(wr_seq),
                                                    user_signaled_(user_signaled) {}
        auto u64_value() const { return u64_value_; }
        auto wr_seq() const { return wr_seq_; }
        bool user_signaled() const { return user_signaled_; }
    };

    struct RcBytes {
        rdma::QueuePair::Identifier user_qp;
        rdma::QueuePair::Identifier shadow_qp;
        rdma::QueuePair::Identifier bitmap_qp;

        using Identifier = rdma::QueuePair::Identifier;
        auto identifier() const
        {
            return user_qp;
        }
        RcBytes() = default;
        explicit RcBytes(const std::vector<char> &vec_char)
        {
            memcpy(this, vec_char.data(), sizeof(RcBytes));
        }
        auto to_vec_char() const
        {
            return util::to_vec_char(*this);
        }
        auto to_string() const
        {
            char str[256];
            sprintf(str, "(%s){0x%08x,0x%08x,0x%08x}", user_qp.ctx_addr().to_string().c_str(), 
                user_qp.qp_num(), shadow_qp.qp_num(), bitmap_qp.qp_num());
            return std::string{str};
        }

        auto to_qp_string() const
        {
            char str[256];
            sprintf(str, "{0x%x,0x%x,0x%x}", 
                user_qp.qp_num(), shadow_qp.qp_num(), bitmap_qp.qp_num());
            return std::string{str};
        }
    };

    class CounterForRemoteMr;
    class RemoteMr;
    // pdp qp
    class RcQp {
    public:
        // pointers
        using Uptr = std::unique_ptr<RcQp>;

        // identifier
        using Identifier = util::CachelineAligned<uint32_t>; // qpn
        Identifier identifier() const { return uqp_->qp_num; }

        struct UserSendWr {
            uint64_t original_wr_id{};// for poll_cq. user's original wr_ids. we need to return it in g_ori_poll_cq
            ibv_send_wr wr{};// for resubmission. user's wr for resubmission. signal is always set. sg_list is copied. wr_id is original
            std::vector<ibv_sge> sg; // for resubmission.
            RcQp *prc{};
            uint32_t seq{};
            bool via_rpc{};
            bool user_signaled{};
#if PAGE_BITMAP_HINT_READ
            util::ObserverPtr<RemoteMr> remote_mr{};
            bool rdma_read; // whether we tried rdma_read in submission
#endif

            UserSendWr(const UserSendWr &rhs)
            {
                *this = rhs;
                wr.sg_list = sg.data();
            }

            UserSendWr(RcQp *prc, uint32_t max_send_sge) :
                    prc(prc),
                    sg(max_send_sge)
            {
                wr.sg_list = sg.data();
            }
        };

        // member variables
    private:
        util::ObserverPtr<rdma::Context> ctx_;

        // UQP
        struct ibv_qp *uqp_ = nullptr; // user QP
        bool uqp_sq_sig_all_ = false;
        struct ibv_qp_cap uqp_cap_;

        // SQP
        std::unique_ptr<rdma::RcQp> sqp_; // shadow RC by pdp
        std::unique_ptr<pdp::MessageConnection> msg_conn_;

        // pdp_rc_
        std::unique_ptr<rdma::RcQp> mr_qp_; // for mr prefetch, bitmap, ...

        std::vector<UserSendWr> user_send_wr_vec_;
        std::vector<bool> send_bitmap_; // 0 not used, 1 used.
        std::mutex mutex_; // protect
        std::unordered_map<uint32_t, std::unique_ptr<CounterForRemoteMr>> access_counters_;

        // methods
    private:
        static bool buffer_is_magic(const char *data, uint32_t len);
        static int sge_identify_magic(const ibv_sge &sge, uint64_t remote_addr, uint64_t *fault_bitmap);

        // only records *wr itself, without wr->next!!!
        void record_rdma_access(const ibv_send_wr *wr);

    public:
        RcQp(util::ObserverPtr<rdma::Context> ctx, struct ibv_qp *uqp, const ibv_qp_init_attr *init_attr);

        auto ctx() const { return ctx_; }
        auto msg_conn() const { return msg_conn_.get(); }
        auto mr_qp() const { return mr_qp_.get(); }
        RcBytes to_rc_bytes() const;

        auto get_user_qp_remote_identifier() { return rdma::QueuePair::Identifier{ctx_->remote_identifier(), uqp_->qp_num};}

        bool connect_to(const RcBytes &rc_bytes);

        bool post_process_wc(struct ibv_wc *wc);
        int post_send(struct ibv_send_wr *wr, struct ibv_send_wr **bad_wr);

        std::optional<uint32_t> find_send_seq();
        void clear_send_seq(uint32_t seq);
        void set_send_seq(uint32_t seq);
        bool test_send_seq(uint32_t seq);

        void rpc_wr_async(ibv_send_wr *wr);
        Message rpc_wr_wait(ibv_send_wr *wr);
    };

    class QpManager: public Manager<RcQp> {
    private:
        void do_in_reg(typename Map::iterator it) final;
        std::vector<std::vector<RcQp *>> rpc_qps_for_threads_;
        std::vector<std::unique_ptr<std::jthread>> rpc_server_threads_;
        int rpc_server_thread_id_current_{};
        void rpc_worker_func(const std::stop_token &stop_token, int rpc_server_thread_id, coroutine<void>::push_type& sink, int coro_id);
    };
} // namespace pdp
