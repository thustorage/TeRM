#pragma once

#include "rdma.hh"

namespace rdma {
    // attention! UD does not support connection.
    // we wrap an interface for better programming.
    // there is actually only one ud_qp per thread!
    class UdConnection {
        util::ObserverPtr<UdQp> ud_qp_;
        util::ObserverPtr<UdAddrHandle> remote_ud_ah_;
    public:
        UdConnection(util::ObserverPtr<UdQp> ud_qp, util::ObserverPtr<UdAddrHandle> remote_ud_ah)
            : ud_qp_(ud_qp), remote_ud_ah_(remote_ud_ah) {}

        void send(const MemorySge &local_mem, bool signaled)
        {
            ud_qp_->send(remote_ud_ah_.get(), local_mem, signaled);
        }

        auto ud_qp() { return ud_qp_; }
        auto remote_ud_ah() { return remote_ud_ah_; }
    };
};
