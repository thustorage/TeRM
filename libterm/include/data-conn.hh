#pragma once
#include <utility>

#include "rdma.hh"

namespace rdma {
class DataConnection final {
public:
    using Uptr = std::unique_ptr<DataConnection>;
private:
    util::ObserverPtr<RcQp> rc_qp_;
    util::ObserverPtr<LocalMemoryRegion> local_mr_;
    std::unique_ptr<RemoteMemoryRegion> remote_mr_;
public:
    DataConnection(util::ObserverPtr<RcQp> rc_qp, util::ObserverPtr<LocalMemoryRegion> local_mr,
                   std::unique_ptr<RemoteMemoryRegion> remote_mr)
        : rc_qp_(rc_qp), 
        local_mr_(local_mr),
        remote_mr_(std::move(remote_mr)) {}
    
    // DataConnection(const Sptr<DataConnection>& data_conn)
    //     : rc_qp_(data_conn->rc_qp_), local_mr_(data_conn->local_mr_), remote_mr_(data_conn->remote_mr_)
    // {}

    // async
    bool read(const MemorySge &local_mem, const MemorySge &remote_mem, bool signal)
    {
        return rc_qp_->read(local_mem, remote_mem, signal);
    }

    // async
    bool read(uint64_t local_addr, uint64_t remote_addr, uint32_t len)
    {
        pr_info("data-conn::read");
        return rc_qp_->read(local_mr_->pick_by_addr(local_addr, len),
                            remote_mr_->pick_by_addr(remote_addr, len), true);
    }

    bool write(const MemorySge &local_mem, const MemorySge &remote_mem, optional<uint32_t> imm = std::nullopt)
    {
        return rc_qp_->write(local_mem, remote_mem, true, imm);
    }

    bool write(uint64_t local_addr, uint64_t remote_addr, uint32_t len, optional<uint32_t> imm = std::nullopt)
    {
        return rc_qp_->write(local_mr_->pick_by_addr(local_addr, len),
                             remote_mr_->pick_by_addr(remote_addr, len), true, imm);
    }

    void post_send_wr(Wr *wr) { rc_qp_->post_send_wr(wr); }

    int wait_send(ibv_wc *wc = nullptr)
    {
        return rc_qp_->wait_send(wc);
    }

    auto rc_qp() { return rc_qp_; }

    auto * local_mr() { return local_mr_.get(); }
    auto * remote_mr() { return remote_mr_.get(); }
};
}
