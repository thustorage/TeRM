#pragma once
// mine
#include "global.hh"
#include "bitmap.hh"
// sys
#include <liburing.h>
#include <fcntl.h>
#include <sys/types.h>
#include <unistd.h>

// 3rd
#include <fmt/core.h>
#define BOOST_ALLOW_DEPRECATED_HEADERS
#include <boost/coroutine/all.hpp>
#include <libaio.h>
// c++
#include <utility>
#include <span>
#include <cstddef>

#define MAX_NR_RDMA_PAGES ((MAX_RDMA_LENGTH + SZ_4K) / SZ_4K)

namespace pdp {

    using boost::coroutines::coroutine;
    enum RpcOpcode : uint32_t {
        kReadSingle = 0,
        kReadSparce,
        kWriteSingle,
        kWriteInline,
        kRegisterCounter,
        kReturnBitmap,
        kReturnStatus,
    };

    struct ReadSingle {
        uint64_t s_addr; // remote_addr for the requester
        uint32_t s_lkey;
        uint64_t c_addr; // remote_identifier for the requester
        uint32_t c_rkey; // for the responder
        uint32_t len;
        u8 update_bitmap;

        constexpr size_t size() const { return sizeof(ReadSingle); }

        std::string to_string() const
        {
            return fmt::format("read_single,s_addr={:#x},s_lkey={:#x},c_addr={:#x},c_rkey={:#x},len={:#x}",
                s_addr, s_lkey, c_addr, c_rkey, len);
        }
    };

    struct ReadSparce {
        uint64_t s_addr;
        uint32_t s_lkey;
        uint64_t c_addr;
        uint32_t c_rkey;
        uint32_t len;
        uint64_t fault_bitmap[util::bits_to_u64s(MAX_NR_RDMA_PAGES)];
        u8 update_bitmap;

        constexpr size_t size() const { return sizeof(ReadSparce); }

        std::string to_string() const
        {
            return fmt::format("read_space,s_addr={:#x},s_lkey={:#x},c_addr={:#x},c_rkey={:#x},len={:#x}",
                               s_addr, s_lkey, c_addr, c_rkey, len);
        }
    };

    struct WriteSingle {
        uint64_t s_addr;
        uint32_t s_lkey;
        uint64_t c_addr;
        uint32_t c_rkey;
        uint32_t len;
        u8 update_bitmap;

        constexpr size_t size() const {
            return sizeof(WriteSingle);
        }

        std::string to_string() const
        {
            return fmt::format("write_single, s_addr={:#x}, s_lkey={:#x}, c_addr={:#x}, c_rkey={:#x}, len={:#x}",
                               s_addr, s_lkey, c_addr, c_rkey, len);
        }
    };

    struct WriteInline {
        uint64_t s_addr;
        uint32_t s_lkey;
        uint32_t len;
        uint8_t data[PAGE_SIZE];
        u8 update_bitmap;

        constexpr size_t size() const {
            return offsetof(WriteInline, data) + len;
        }

        constexpr size_t data_size() const { return len; }

        std::string to_string() const
        {
            return fmt::format("write_inline, s_addr={:#x}, s_lkey={:#x}, len={:#x}, data={:#x}",
                               s_addr, s_lkey, len, *(uint64_t *)data);
        }
    };

    struct RegisterCounter {
        uint32_t server_mr_key;
        rdma::RemoteMemoryRegion counter_mr;

        constexpr size_t size() const { return sizeof(RegisterCounter); }

        std::string to_string() const
        {
            return fmt::format("register_counter");
        }
    };

    struct ReturnBitmap {
        uint64_t present_bitmap[util::bits_to_u64s(MAX_NR_RDMA_PAGES)];
        constexpr size_t size() const { return sizeof(ReturnBitmap); }
        std::string to_string() const
        {
            return fmt::format("return_bitmap");
        }
    };

    struct Message {
        uint32_t opcode;
        union {
            ReadSingle read_single;
            ReadSparce read_sparce;
            WriteSingle write_single;
            WriteInline write_inline;
            RegisterCounter register_counter;
            ReturnBitmap return_bitmap;
        };

        [[nodiscard]] constexpr size_t size() const
        {
            switch (opcode) {
                case kReadSingle:
                    return offsetof(Message, read_single) + read_single.size();
                case kReadSparce:
                    return offsetof(Message, read_sparce) + read_sparce.size();
                case kWriteSingle:
                    return offsetof(Message, write_single) + write_single.size();
                case kWriteInline:
                    return offsetof(Message, write_inline) + write_inline.size();
                case kRegisterCounter:
                    return offsetof(Message, register_counter) + register_counter.size();
                case kReturnBitmap:
                    return offsetof(Message, return_bitmap) + return_bitmap.size();
                case kReturnStatus:
                    return sizeof(opcode);
                default:
                    return 0;
            }
        }

        [[nodiscard]] std::string to_string() const
        {
            std::string str = fmt::format("{{size={:#x},opcode=", size());
            switch (opcode) {
                case kReadSingle:
                    return str + read_single.to_string() + "}";
                case kReadSparce:
                    return str + read_sparce.to_string() + "}";
                case kWriteSingle:
                    return str + write_single.to_string() + "}";
                case kWriteInline:
                    return str + write_inline.to_string() + "}";
                case kRegisterCounter:
                    return str + register_counter.to_string() + "}";
                case kReturnBitmap:
                    return str + return_bitmap.to_string() + "}";
                case kReturnStatus:
                    return str + "return}";
                default:
                    return str + "unknown}";
            }
        }
    };

    class LocalMr;

    class RpcIo {
        static const size_t kBounceMrSize = MAX_NR_RDMA_PAGES * SZ_4K + SZ_4K;
        static const size_t kQueueDepth = kBounceMrSize / SZ_4K;
    private:
        rdma::LocalMemoryRegion::Uptr bounce_mr_;
        void *mr_base_addr_{};

        int nvme_fd_buffer_;
        int nvme_fd_direct_;
        int pch_fd_;
        static inline thread_local std::vector<io_context_t> io_context_vec_{std::vector<io_context_t>(MAX_NR_COROUTINES)};
        static inline thread_local std::vector<bool> inited_{std::vector<bool>(MAX_NR_COROUTINES)};

//        io_uring ring_{};

        std::vector<std::tuple<u64, u32>> access_vec_;
        struct io_req_t {
            u64 offset;
            u32 length;
            char *buffer;
            bool direct;

            bool try_merge(const io_req_t &rhs)
            {
                if (offset + length == rhs.offset
                        && buffer + length == rhs.buffer
                        && direct == rhs.direct) {
                    length += rhs.length;
                    return true;
                }
                return false;
            }

            std::string to_string() const
            {
                return fmt::format("offset={:#x},length={:#x},buffer={},direct={}",
                    offset, length, fmt::ptr(buffer), direct);
            }
        };
        std::vector<io_req_t> io_req_vec_;

    private:
        char *direct_sector_buffer()
        {
            return (char *)bounce_mr_->addr() + MAX_NR_RDMA_PAGES * PAGE_SIZE;
        }

    public:
        explicit RpcIo(util::ObserverPtr<rdma::Context> ctx);
        ~RpcIo()
        {
            close(nvme_fd_buffer_);
            close(nvme_fd_direct_);
            close(pch_fd_);
//            io_uring_queue_exit(&ring_);
        }

        auto bounce_mr() { return util::make_observer(bounce_mr_.get()); }

//        void do_io_by_uring(int nr_io_reqs, bool write);
        void do_io_by_psync(int nr_io_reqs, bool write, coroutine<void>::push_type& sink, int coro_id);
        void subsector_write(void *buffer, u32 length, u64 offset, coroutine<void>::push_type& sink, int coro_id);

        // rw_offset: in mr and ssd
        // mr_base_addr: virtual address, must be 4KB-aligned!
        // fault_bitmap: nullptr means all pages need to access
        std::span<std::tuple<u64, u32>>
        io(u64 rw_offset, u32 rw_count, const uint64_t *fault_bitmap, bool write, LocalMr *mr, coroutine<void>::push_type& sink, int coro_id);
    };

    class MessageConnection {
        static const uint64_t kSignalBatch = 16;
    private:
        util::ObserverPtr<rdma::RcQp> rc_qp_;
        rdma::LocalMemoryRegion::Uptr send_mr_;
        rdma::LocalMemoryRegion::Uptr recv_mr_;
        uint64_t send_counter_{};
        std::mutex mutex_;

        // PDP server only
        std::unique_ptr<RpcIo> rpc_io_;

    public:
        using Uptr = std::unique_ptr<MessageConnection>;

        void lock()
        {
            mutex_.lock();
        }

        void unlock()
        {
            mutex_.unlock();
        }

        explicit MessageConnection(util::ObserverPtr<rdma::RcQp> rc_qp)
                : rc_qp_(rc_qp)
        {
            static const auto &gv = GlobalVariables::instance();

            const reg_mr_t ori_reg_mr = gv.ori_ibv_symbols.reg_mr;

            rdma::Allocation send_alloc{sizeof(Message)};
            rdma::Allocation recv_alloc{sizeof(Message)};

            send_mr_ = std::make_unique<rdma::LocalMemoryRegion>(rc_qp_->ctx(), std::move(send_alloc), false, ori_reg_mr);
            recv_mr_ = std::make_unique<rdma::LocalMemoryRegion>(rc_qp_->ctx(), std::move(recv_alloc), false, ori_reg_mr);
            rc_qp_->receive(recv_mr_->pick_by_offset0(sizeof(Message)));

            if (!gv.configs.is_server) return;
            rpc_io_ = std::make_unique<RpcIo>(rc_qp_->ctx());
        }

        auto rc_qp() { return rc_qp_; }
        auto rpc_io() { return util::make_observer(rpc_io_.get()); }

        Message get(ibv_wc *wc = nullptr)
        {
            assert(rc_qp_);
            rc_qp_->wait_recv(wc);

            Message msg = *(Message *)recv_mr_->addr_;

            // post for the next
            rc_qp_->receive(recv_mr_->addr_, recv_mr_->lkey_, sizeof(Message));

            return msg;
        }

        bool try_get(Message *msg, void *write_bounce_buffer)
        {
            int ret = rc_qp_->poll_recv(1);
            if (ret <= 0) {
                return false;
            }

            auto *src_msg = (Message *)recv_mr_->addr();
            if (src_msg->opcode == RpcOpcode::kWriteInline) {
                memcpy(msg, src_msg, src_msg->size() - src_msg->write_inline.data_size());
                size_t offset_in_page = msg->write_single.s_addr % PAGE_SIZE;
                memcpy((char *)write_bounce_buffer + offset_in_page, src_msg->write_inline.data, src_msg->write_inline.len);
            } else {
                memcpy(msg, src_msg, src_msg->size());
            }

            rc_qp_->receive(recv_mr_->pick_by_offset0(sizeof(Message)));

            return true;
        }

        Message *send_mr_ptr()
        {
            return (Message *)send_mr_->addr();
        }

        bool should_signal() const
        {
            return send_counter_ % kSignalBatch == 0;
        }

        void inc_send_counter()
        {
            send_counter_++;
        }

        // auto batch signal and wait
        void post_send_wr_auto_wait(rdma::Wr *wr)
        {
            if (should_signal()) {
                wr->set_signaled(true);
            }
            rc_qp_->post_send_wr(wr);
            // the caller may have set the signal
            if (wr->signaled()) {
                ibv_wc wc{};
                rc_qp_->wait_send(&wc);
                ASSERT(wc.status == IBV_WC_SUCCESS, "fail to wait_send: %s", ibv_wc_status_str(wc.status));
            }
            inc_send_counter();
        }

        // auto batch signal and wait
        void send(const Message *msg)
        {
            if ((void *)msg != send_mr_->addr()) {
                memcpy(send_mr_->addr(), msg, msg->size());
            }
            rc_qp_->send(send_mr_->pick_by_offset0(msg->size()), should_signal());
            if (should_signal()) {
                ibv_wc wc{};
                rc_qp_->wait_send(&wc);
                ASSERT(wc.status == IBV_WC_SUCCESS, "fail to wait_send: %d(%s), qp=%s, send_counter=%lu", wc.status, ibv_wc_status_str(wc.status), 
                            rc_qp()->identifier().to_string().c_str(), send_counter_);
                ASSERT(wc.opcode == IBV_WC_SEND, "unexpected wc.opcode=0x%x, qp=%s, send_counter=%lu", wc.opcode, rc_qp()->identifier().to_string().c_str(), send_counter_);
            }
            inc_send_counter();
        }
    };
}
