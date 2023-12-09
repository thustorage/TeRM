#pragma once
#include "rdma.hh"
#include "sms.hh"
#include "data-conn.hh"
#include "ud-conn.hh"

namespace rdma {

class Thread;

class Node {
private:
    const char *kNrNodesKey = "nr_nodes";
    const char *kNrPreparedKey = "nr_prepared";
    const char *kNrConnectedKey = "nr_connected";

public:
    using Uptr = std::unique_ptr<Node>;
    SharedMetaService sms_{"10.0.2.181", 23333}; // shared_meta_service
    int nr_nodes_;
    int node_id_;
    bool registered_ = false;
    vector<Thread> threads_;
    std::vector<std::unique_ptr<Context>> ctxs_;

    Node(int nr_nodes, int node_id = -1) : nr_nodes_(nr_nodes), node_id_(node_id) {}

    void register_node()
    {
        std::cout << "registering node..." << std::flush;
        if (node_id_ == -1)
            node_id_ = sms_.inc(kNrNodesKey) - 1;
            
        std::cout << node_id_ << std::endl;
        if (is_server()) {
            sms_.put(kNrPreparedKey, {'0'});
            sms_.put(kNrConnectedKey, {'0'});
        }
        registered_ = true;
    }

    void unregister_node()
    {
        if (registered_) {
            sms_.dec(kNrNodesKey);
            pr_info("unregister node %d", node_id_);
            registered_ = false;
        }
    }

    bool is_server()
    {
        return node_id_ == 0;
    }

    void wait_for_all_nodes_prepared()
    {
        std::cout << __func__ << "... " << std::flush;
        sms_.inc(kNrPreparedKey);
        while (sms_.get_int(kNrPreparedKey) < nr_nodes_) {
            usleep(10000);
        }
        std::cout << "done" << std::endl;
    }

    void wait_for_all_nodes_connected()
    {
        std::cout << __func__ << "... " << std::flush;
        sms_.inc(kNrConnectedKey);
        while (sms_.get_int(kNrConnectedKey) < nr_nodes_) {
            usleep(10000);
        }
        std::cout << "done" << std::endl;
    }
};

class Thread {
    util::ObserverPtr<Node> node_;
    util::ObserverPtr<Context> ctx_;
    int thread_id_;

    int nr_dst_nodes_; // including myself for simplicity
    int nr_dst_threads_;

    LocalMemoryRegion::Uptr local_mr_;

    struct RcInfo {
        MemoryRegion mr;
        QueuePair::Identifier data_addr;
    };

    // [dst_node_id][dst_thread_id]; for clients, dst_node_id=0 (connected to server)
    vector<vector<RcQp::Uptr>> data_qp_; // [node_id][thread_id]
    vector<vector<DataConnection::Uptr>> data_conn_; // [node_id][thread_id]

    std::unique_ptr<UdQp> ud_qp_;
    vector<vector<std::unique_ptr<UdAddrHandle>>> ud_ah_; // [dst_node_id][dst_thread_id]

    static string cat_to_string(int node_id, int thread_id)
    {
        return std::to_string(node_id) + ":" + std::to_string(thread_id);
    }

    string rc_push_key(int dst_node_id, int dst_thread_id)
    {
        return std::string("[rc-addr]") + cat_to_string(node_->node_id_, thread_id_) + "-" + cat_to_string(dst_node_id, dst_thread_id);
    }

    string rc_pull_key(int dst_node_id, int dst_thread_id)
    {
        return std::string("[rc-addr]") + cat_to_string(dst_node_id, dst_thread_id) + "-" + cat_to_string(node_->node_id_, thread_id_);
    }

    auto ud_push_key()
    {
        return std::string("[ud-addr]") + cat_to_string(node_->node_id_, thread_id_);
    }

    auto ud_pull_key(int dst_node_id, int dst_thread_id)
    {
        return std::string("[ud-addr]") + cat_to_string(dst_node_id, dst_thread_id);
    }

public:
    // for clients, dst_nr_nodes should be 1.
    // for the server, dst_nr_nodes should be node_->nr_nodes_.
    Thread(util::ObserverPtr<Node> node, util::ObserverPtr<Context> ctx, int thread_id, int dst_nr_nodes, int dst_nr_threads, LocalMemoryRegion::Uptr local_mr)
        : node_(std::move(node)), ctx_(ctx), 
          thread_id_(thread_id), nr_dst_nodes_(dst_nr_nodes), nr_dst_threads_(dst_nr_threads),
          local_mr_(std::move(local_mr))
    {
        data_qp_.resize(dst_nr_nodes);
        data_conn_.resize(dst_nr_nodes);
        ud_ah_.resize(dst_nr_nodes);
        for (int i = 0; i < dst_nr_nodes; i++) {
            for (int j = 0; j < dst_nr_threads; j++) {
                data_qp_[i].emplace_back();
                data_conn_[i].emplace_back();
                ud_ah_[i].emplace_back();
            }
        }
    }

    void prepare_and_push_rc()
    {
        for (int i = 0; i < nr_dst_nodes_; i++) {
            if (i == node_->node_id_) continue; // myself
            for (int j = 0; j < nr_dst_threads_; j++) {
                data_qp_[i][j] = std::make_unique<RcQp>(ctx_);
                RcInfo thread_info{*local_mr_, data_qp_[i][j]->identifier()};
                node_->sms_.put(rc_push_key(i, j), util::to_vec_char(thread_info));
            }
        }
    }

    void prepare_and_push_ud()
    {
        ud_qp_ = std::make_unique<UdQp>(util::make_observer(ctx_.get()));
        node_->sms_.put(ud_push_key(), util::to_vec_char(ud_qp_->identifier()));
    }

    void pull_and_connect_rc()
    {
        for (int i = 0; i < nr_dst_nodes_; i++) {
            if (i == node_->node_id_) continue; // myself
            for (int j = 0; j < nr_dst_threads_; j++) {
                auto rc_info = util::from_vec_char<RcInfo>(node_->sms_.get(rc_pull_key(i, j)));
                auto remote_mr = std::make_unique<RemoteMemoryRegion>(rc_info.mr);
                data_qp_[i][j]->connect_to(rc_info.data_addr);
                auto p = std::make_unique<DataConnection>(util::make_observer(data_qp_[i][j].get()),
                    util::make_observer(local_mr_.get()), std::move(remote_mr));
                data_conn_[i][j] = std::move(p);
            }
        }
    }

    void pull_and_setup_ud()
    {
        for (int i = 0; i < nr_dst_nodes_; i++) {
            if (i == node_->node_id_) continue;
            for (int j = 0; j < nr_dst_threads_; j++) {
                auto remote_addr = util::from_vec_char<QueuePair::Identifier>(node_->sms_.get(ud_pull_key(i, j)));
                ud_ah_[i][j] = std::make_unique<UdAddrHandle>(ctx_, remote_addr);
                ud_qp_->setup();
            }
        }
    }

    util::ObserverPtr<DataConnection> get_data_conn(int dst_node_id, int dst_thread_id)
    {
        return util::make_observer(data_conn_[dst_node_id][dst_thread_id].get());
    }

    auto get_ud_conn(int dst_node_id, int dst_thread_id)
    {
        return std::make_unique<UdConnection>(util::make_observer(ud_qp_.get()), util::make_observer(ud_ah_[dst_node_id][dst_thread_id].get()));
    }

    auto ud_qp() { return ud_qp_.get(); }
    auto local_mr() { return local_mr_.get(); }

};

}
