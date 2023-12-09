#pragma once

// my headers
#include "config.hh"

#include <util.hh>
#include <sms.hh>
#include <rdma.hh>
#include <lock.hh>

#include <better-enum.hh>
// system
#include <infiniband/verbs.h>

// c++ headers
#include <cassert>
#include <memory>
#include <thread>
#include <mutex>
#include <unordered_map>

#define PDP_ACCESS_PDP (IBV_ACCESS_RELAXED_ORDERING << 1)

namespace pdp
{
    using create_qp_t = struct ibv_qp *(*)(struct ibv_pd *pd, struct ibv_qp_init_attr *qp_init_attr);
                                                                    using modify_qp_t = int(*)(struct ibv_qp *qp, struct ibv_qp_attr *attr, int attr_mask);
    using poll_cq_t = int(*)(struct ibv_cq *cq, int num_entries, struct ibv_wc *wc);
    using post_send_t = int(*)(struct ibv_qp *qp, struct ibv_send_wr *wr, struct ibv_send_wr **bad_wr);
    using post_recv_t = int(*)(struct ibv_qp *qp, struct ibv_recv_wr *wr, struct ibv_recv_wr **bad_wr);
    using reg_mr_t = struct ibv_mr *(*)(struct ibv_pd *pd, void *addr, size_t length, int access);
    using reg_mr_iova2_t = struct ibv_mr *(*)(struct ibv_pd *pd, void *addr, size_t length,
				uint64_t iova, unsigned int access);

    class RcQp;
}

namespace pdp {
    template<typename Resource>
    class Manager {
    public:
        using resource_type = Resource;
    protected:
        std::unordered_map<typename Resource::Identifier, std::unique_ptr<Resource>> map_;
        ReaderFriendlyLock lock_;
        using Map = decltype(map_);

        virtual void do_in_reg(typename Map::iterator it) {}
    public:
        Manager() = default;
        virtual ~Manager() = default;

        // thread-safe register function protected by the write_single lock, in order to construct the value exactly once.
        // id: the key
        // args: to construct the value iff the key does not exist
        template <class... Args>
        util::ObserverPtr<Resource> reg_with_construct_args(const typename Resource::Identifier &id, Args&&... args) {
            std::unique_lock w_lk(lock_);

            if (const auto &p = unlocked_get(id)) {
                return p;
            }

            auto p = std::make_unique<Resource>(std::forward<Args>(args)...);
            auto [it, inserted] = map_.emplace(id, std::move(p));
            ASSERT(inserted, "fail to insert");

            do_in_reg(it);
            return util::make_observer(it->second.get());
        }

        template <typename Lambda>
        util::ObserverPtr<Resource> reg_with_construct_lambda(const typename Resource::Identifier &id, const Lambda &lmd)
        {
            std::unique_lock w_lk(lock_);

            if (auto p = unlocked_get(id)) {
                return p;
            }

            auto [it, inserted] = map_.emplace(id, lmd());
            ASSERT(inserted, "fail to insert");

            do_in_reg(it);
            return util::make_observer(it->second.get());
        }

        // unlocked get
        util::ObserverPtr<Resource> unlocked_get(const typename Resource::Identifier &id)
        {
            if (auto it = map_.find(id); it != map_.end()) {
                return util::make_observer(it->second.get());
            }
            return util::ObserverPtr<Resource>(nullptr);
        }

        util::ObserverPtr<Resource> get(const typename Resource::Identifier &id) {
            std::shared_lock r_lk(lock_);
            return unlocked_get(id);
        }
    };

//    class Resource {
//    public:
//        class Bytes {};
//        class Local {};
//        class Remote {};
//        class Manager {};
//    };

    class ContextManager: public Manager<rdma::Context> {
    public:
        util::ObserverPtr<rdma::Context> get_or_reg_by_pd(ibv_pd *pd)
        {
            util::ObserverPtr<rdma::Context> mr = get(pd);
            if (!mr) {
                mr = reg_with_construct_args(pd, pd->context, pd);
            }
            return mr;
        }
    };

    namespace PageState {
        using Val = u8;
        enum : Val {
            kUncached = 0,
            kCached = 1,
            kDirty = 2,
        };
    }

    class LocalMrManager;
    class RemoteMrManager;
    class QpManager;

    BETTER_ENUM(ServerIoPath, int, TIERING, MEMCPY, BUFFER, DIRECT);
    BETTER_ENUM(Mode, int, 
        PDP, ODP, PIN, 
        RPC, RPC_MEMCPY, RPC_BUFFER, RPC_DIRECT, RPC_TIERING, 
        RPC_TIERING_PROMOTE,
        MAGIC_MEMCPY, MAGIC_BUFFER, MAGIC_DIRECT, MAGIC_TIERING,
        PDP_MEMCPY);

    struct GlobalVariables {
    private:
        GlobalVariables();
    public:
        rdma::SharedMetaService sms;

        // configs
        struct {
            Mode mode;
            bool rpc_mode;

            const bool is_server;
            const std::string server_mmap_dev;
            ServerIoPath server_io_path;

            int server_memory_gb;
            const int server_rpc_threads;
            bool server_page_state;

            bool read_magic_pattern;
            bool promote_hotspot;
            const uint32_t promotion_window_ms;

            bool pull_page_bitmap;
            bool advise_local_mr;
            bool predict_remote_mr;

            std::string to_string() const {
                std::string out = "=== configs begin ===\n";
                fmt::format_to(std::back_inserter(out), "mode: {}\n", mode._to_string());
                fmt::format_to(std::back_inserter(out), "rpc_mode: {}\n", rpc_mode);
                fmt::format_to(std::back_inserter(out), "is_server: {}\n", is_server);
                fmt::format_to(std::back_inserter(out), "server_mmap_dev: {}\n", server_mmap_dev);
                fmt::format_to(std::back_inserter(out), "server_io_path: {}\n", server_io_path._to_string());
                fmt::format_to(std::back_inserter(out), "server_memory_gb: {}\n", server_memory_gb);
                fmt::format_to(std::back_inserter(out), "server_rpc_threads: {}\n", server_rpc_threads);
                fmt::format_to(std::back_inserter(out), "server_page_state: {}\n", server_page_state);
                fmt::format_to(std::back_inserter(out), "read_magic_pattern: {}\n", read_magic_pattern);
                fmt::format_to(std::back_inserter(out), "promote_hotspot: {}\n", promote_hotspot);
                fmt::format_to(std::back_inserter(out), "promotion_window_ms: {}\n", promotion_window_ms);
                fmt::format_to(std::back_inserter(out), "pull_page_bitmap: {}\n", pull_page_bitmap);
                fmt::format_to(std::back_inserter(out), "advise_local_mr: {}\n", advise_local_mr);
                fmt::format_to(std::back_inserter(out), "predict_remote_mr: {}\n", predict_remote_mr);
                fmt::format_to(std::back_inserter(out), "--- configs end ---\n");
                return out;
            }
        } configs;

        bool has_write_req = false;
        bool has_dirty_page = false;
        bool doing_promotion = false;

        // CTXs
        std::unique_ptr<ContextManager> ctx_mgr;

        // MRs
        std::unique_ptr<LocalMrManager> local_mr_mgr;
        std::unique_ptr<RemoteMrManager> remote_mr_mgr;

        // QPs
        std::unique_ptr<QpManager> qp_mgr;

        // original symbols from ibv
        struct {
            // QP-related: inited in create_qp()
            create_qp_t create_qp = nullptr;
            post_send_t post_send = nullptr;
            post_recv_t post_recv = nullptr;
            poll_cq_t poll_cq = nullptr;

            // inited in modify_qp
            modify_qp_t modify_qp = nullptr;

            // MR-related: initied in reg_mr_iova2()
            reg_mr_iova2_t reg_mr_iova2 = nullptr;
            reg_mr_t reg_mr = nullptr;
        } ori_ibv_symbols;

        static GlobalVariables& instance()
        {
            static GlobalVariables gv;
            return gv;
        }
    };
}
