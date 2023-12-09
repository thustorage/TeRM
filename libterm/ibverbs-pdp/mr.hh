#pragma once

#include "rpc.hh"
#include "global.hh"
#include <page-bitmap.hh>
// c
#include <cassert>
#include <cstdint>
// c++
#include <map>
#include <memory>
#include <mutex>
#include <functional>
// system
#include <infiniband/verbs.h>
// mine
#include <logging.hh>
#include <utility>

namespace pdp {
    // [lower, upper)
    class Interval {
    public:
        using data_t = uint64_t;
    private:
        std::pair<data_t, data_t> pair_;
        int guard_ = 0; // -1: min guard, 0: normal, 1: max guard

        Interval(data_t l, data_t r, int guard): pair_{l, r}, guard_(guard) {}

    public:
        static Interval max() { return {std::numeric_limits<data_t>::max(), std::numeric_limits<data_t>::max(), 1}; }
        static Interval min() { return {std::numeric_limits<data_t>::min(), std::numeric_limits<data_t>::min(), -1}; }

        Interval(data_t l, data_t r) : Interval(l, r, 0) {}

        auto left() const { return pair_.first; }
        auto right() const {  return pair_.second; }
        auto length() const { return right() - left(); }
        bool valid() const { return guard_ == 0; }

        bool intersect(const Interval &intv) const {
            if (guard_) return false;

            // [0, 10) and [10, 20) intersect in our setting.
            return !(right() < intv.left() || left() > intv.right());
        }

        bool operator<(const Interval &rhs) const {
            if (guard_ == 0 && rhs.guard_ == 0)
                return left() < rhs.left();
            return guard_ < rhs.guard_;
        }

        bool cover(const Interval &intv) const {
            return left() <= intv.left() && right() >= intv.right();
        }

        std::string to_string() const
        {
            switch (guard_) {
                case -1: return "[min,min)";
                case 0 : {
                    char cstr[123];
                    sprintf(cstr, "[0x%lx,0x%lx)", left(), right());
                    return cstr;
                }
                case 1: return "[max,max)";
            }
            return "[error,error)";
        }
    };

    class LocalMr;
    // interval merge tree
    // align to 4K
    class MrPinnedIntervalTree {
    public:
        using Uptr = std::unique_ptr<MrPinnedIntervalTree>;
    private:
        // pin in 4KB!
        static const inline int kPageShift = 12;
        static const inline int kPageSize = (1 << kPageShift);
        static const inline int kAccessFlags = IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ |
                                               IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_REMOTE_ATOMIC;
        util::ObserverPtr<LocalMr> local_mr_;
        std::map<Interval, std::unique_ptr<rdma::LocalMemoryRegion>> map_;
        std::mutex mutex_; // protect map_ for write_single
        mutable std::shared_mutex shared_mutex_; // read for map_

        void insert_min_max() {
            map_.emplace(Interval::min(), nullptr);
            map_.emplace(Interval::max(), nullptr);
        }

        // return value: of pinned region
        // 0: already pinned
        // 1: pin now
        int insert_without_pinning(uint64_t address, uint64_t length);

        int pin_all();

    public:
        MrPinnedIntervalTree(util::ObserverPtr<LocalMr> local_mr) : local_mr_(local_mr) {
            insert_min_max();
        }

        auto begin() { return map_.begin(); }

        auto end() { return map_.end(); }

        std::optional <uint32_t> get_covered_region_lkey(uint64_t address, uint64_t length) const {
            uint64_t first_idx = address >> kPageShift;
            uint64_t last_idx = (address + length - 1) >> kPageShift; // include the last byte!
            Interval intv{first_idx << kPageShift, (last_idx + 1) << kPageShift};
            std::shared_lock r_lk(shared_mutex_);

            // `it' won't be begin(), end(),  at most [min, min) and [max, max).
            auto it = map_.upper_bound(intv);
            if (std::prev(it)->first.cover(intv)) {
                return std::prev(it)->second->lkey_;
            } else {
                return {};
            }
        }

        bool cover_region(uint64_t address, uint64_t length) const {
            return get_covered_region_lkey(address, length).has_value();
        }

        // return # pinned regions
        int pin_regions(const std::vector <Interval> &intvs) {
            for (const auto &intv: intvs) {
                insert_without_pinning(intv.left(), intv.length());
            }
            return pin_all();
        }

        auto to_string() const {
            std::string str;
            for (const auto &u: map_) {
                char cstr[256];
                auto *mr = u.second.get();
                sprintf(cstr, "%s->%p(lkey=0x%x) ", u.first.to_string().c_str(), (void *)mr,
                        mr ? mr->lkey_ : 0);
                str += cstr;
            }
            return str;
        }

        ~MrPinnedIntervalTree() = default;
    };

    struct MrBytes {
        enum class Type {
            PDP,
            ODP,
            PIN,
        };
        rdma::Context::RemoteIdentifier ctx_id;
        Type type;
        rdma::MemoryRegion user_mr_;
        rdma::MemoryRegion bitmap_mr_;

        MrBytes() = default;
        explicit MrBytes(const std::vector<char> &vec_char)
        {
            ASSERT(vec_char.size() == sizeof(MrBytes),
                   "vec_char.size()=%lu, expected=%lu", vec_char.size(), sizeof(MrBytes));
            memcpy(this, vec_char.data(), sizeof(MrBytes));
            ASSERT(type >= Type::PDP && type <= Type::PIN, "type %u is out of bound.", (int)type);
        }

#if 1
        class Identifier {
            rdma::Context::RemoteIdentifier ctx_identifier_;
            uint32_t key_{};
        public:
            Identifier() = default;
            Identifier(const rdma::Context::RemoteIdentifier & ctx_identifier, uint32_t key)
                    : ctx_identifier_(ctx_identifier), key_(key) {}

            auto operator<=>(const Identifier &rhs) const = default;

            std::string to_string() const
            {
                return ctx_identifier_.to_string() + fmt::format("/[mr]{:x}", key_);
            }
            
            std::size_t hash() const
            {
                std::size_t seed = ctx_identifier_.hash();
                util::hash_combine(seed, key_);
                return seed;
            }
        };
#else
        class __cacheline_aligned Identifier {
            uint32_t key_;
        public:
            Identifier() = default;
            Identifier(const rdma::Context::RemoteIdentifier &ctx_identifier, uint32_t key)
                    : key_(key) {}

            auto operator<=>(const Identifier &rhs) const = default;

            std::string to_string() const
            {
                char str[256];
                sprintf(str, "[mr]0x%x", key_);
                return str;
            }
            
            std::size_t hash() const
            {
                // std::size_t seed = ctx_identifier_.hash();
                // util::hash_combine(seed, key_);
                return std::hash<uint32_t>{}(key_);
                // return seed;
            }
        };
#endif

        auto identifier() const
        {
            return Identifier{ctx_id, user_mr_.rkey_};
        }

        std::vector<char> to_vec_char() const
        {
            return util::to_vec_char(*this);
        }

        std::string to_string() const 
        {
            std::string str = ctx_id.to_string();
            fmt::format_to(std::back_inserter(str), ",type={:x},user_mr={{{}}},bitmap_mr={{{}}}",
                (int)type, user_mr_.to_string(), bitmap_mr_.to_string());
            return str;
        }
    };
}

template<>
struct std::hash<pdp::MrBytes::Identifier>
{
    std::size_t operator()(const pdp::MrBytes::Identifier &id) const noexcept
    {
        return id.hash();
    }
};

namespace pdp {
    class RemoteMr
    {
    public:
        using Identifier = MrBytes::Identifier;

    private:
        util::ObserverPtr<pdp::RcQp> pdp_rc_; // the qp to pull bitmap
        MrBytes mr_bytes_;

        MrBytes::Type type_;
        rdma::RemoteMemoryRegion user_mr_;
        uint64_t page_base_;
        uint64_t nr_pages_;
        uint64_t unit_base_;
        uint64_t nr_units_;

        RemotePageBitmap::Uptr page_bitmap_;
        util::Bitmap hot_unit_bitmap_;

    public:
        explicit RemoteMr(util::ObserverPtr<pdp::RcQp> pdp_rc, const MrBytes &mr_bytes);
        void page_bitmap_assign_batch(uint64_t mr_addr, uint64_t length, uint64_t *present_bitmap);
        bool page_bitmap_test_present(uint64_t page) const { return page_bitmap_->test_present(page); }

        const MrBytes & mr_bytes() const { return mr_bytes_; }
        auto user_mr() const { return user_mr_; }
        bool is_pin() const { return type_ == MrBytes::Type::PIN; }
        bool is_odp() const { return type_ == MrBytes::Type::ODP; }
        bool is_pdp() const { return type_ == MrBytes::Type::PDP; }

        bool cover_region(uint64_t addr, uint32_t len) const
        {
            return user_mr().cover_region(addr, len);
        }

        void pull_page_bitmap();
        uint32_t count_fault_pages(uint64_t addr, uint32_t len) const;
        bool has_fault_page(uint64_t addr, uint32_t len) const;

        std::tuple<uint64_t, uint64_t> get_unit_span(uint64_t addr, uint32_t len);
        std::tuple<uint64_t, uint64_t> get_page_span(uint64_t addr, uint64_t len);
        std::tuple<uint64_t, uint64_t> get_addr_span_of_page(uint64_t page);
        uint64_t nr_units() { return nr_units_; }
        uint64_t nr_pages() const  { return nr_pages_; }
    };

    class CounterForRemoteMr {
    private:
        util::ObserverPtr<pdp::RcQp> rc_qp_;
        util::ObserverPtr<pdp::RemoteMr> remote_mr_;

    public:
        std::vector<uint32_t> vec_; // first: rdma, second: rpc
        rdma::LocalMemoryRegion vec_mr_;

    public:
        CounterForRemoteMr(util::ObserverPtr<pdp::RcQp> rc_qp, util::ObserverPtr<pdp::RemoteMr> remote_mr);

        auto remote_mr() { return remote_mr_; }

        std::string identifier_string();
    };

    class RemoteMrManager : public Manager<RemoteMr> {
    private:
        std::unique_ptr<std::jthread> pull_page_bitmap_thread_;

        void pull_page_bitmap_func(const std::stop_token &stop_token);

        void do_in_reg(typename Map::iterator it) final;

    public:
        bool rdma_should_via_rpc(util::ObserverPtr<RcQp> rc, const rdma::Context::RemoteIdentifier & remote_ctx_id, ibv_send_wr *wr);

        util::ObserverPtr<RemoteMr> get_or_reg(const RemoteMr::Identifier &remote_mr_id, util::ObserverPtr<pdp::RcQp> pdp_rc);
    };

    class CounterForLocalMr
    {
    public:
        std::vector<uint32_t> vec_;
        rdma::LocalMemoryRegion vec_mr_;
        std::vector<u32> last_counter_;
        CounterForLocalMr(util::ObserverPtr<rdma::Context> ctx, uint64_t nr_units)
        : vec_(nr_units),
          vec_mr_(ctx, vec_.data(), vec_.size() * sizeof(vec_[0]), false, GlobalVariables::instance().ori_ibv_symbols.reg_mr),
          last_counter_(nr_units)
        {}
    };

    class CounterFromRemoteQp
    {
    public:
        rdma::RemoteMemoryRegion counter_mr_;

        CounterFromRemoteQp(const rdma::RemoteMemoryRegion &counter_mr)
            : counter_mr_(counter_mr) {}
    };

    class LocalMr
    {
    public:
        using PinnedIntvTreeVec = std::vector<MrPinnedIntervalTree::Uptr>;
        using Identifier = util::CachelineAligned<uint32_t>;
    private:
        util::ObserverPtr<rdma::Context> ctx_;
        MrBytes::Type type_;
        std::unique_ptr<rdma::LocalMemoryRegion> user_mr_;
        std::unique_ptr<rdma::LocalMemoryRegion> internal_odp_mr_; // only valid in a PDP mr.

        LocalPageBitmap::Uptr bitmap_;
        uint64_t page_base_;
        uint64_t nr_pages_;
        uint64_t unit_base_;
        uint64_t nr_units_;
        std::vector<PageState::Val> page_state_vec_;
        std::vector<u64> page_mapped_vec_;
        std::vector<bool> page_locked_vec_;
        std::vector<bool> unit_hot_vec_;
        uint64_t msync_unit_id_{};

        uint32_t nr_locked_units_{};

        std::map<pdp::RcQp *, CounterFromRemoteQp> counters_from_remote_qps_;
        std::unique_ptr<CounterForLocalMr> counter_;
        std::mutex counter_lock_;

        std::unique_ptr<PinnedIntvTreeVec> pinned_intv_tree_vec_;
        ReaderFriendlyLock tree_vec_lock_;

        std::unique_ptr<std::vector<SpinLock>> sector_lock_vec_;
        std::unique_ptr<std::vector<ReaderFriendlyLock>> unit_lock_vec_;
        std::vector<uint8_t> unit_buffer_;

        int pch_fd_ = -1;
        int pagemap_fd_ = -1;
        int nvme_fd_ = -1;

    public:
        // PIN
        LocalMr(std::unique_ptr<rdma::LocalMemoryRegion> pin_mr)
            : ctx_(pin_mr->ctx()),
            type_(MrBytes::Type::PIN),
            user_mr_(std::move(pin_mr))
            {}

        // ODP or PDP
        LocalMr(std::unique_ptr<rdma::LocalMemoryRegion> odp_mr, 
            std::unique_ptr<rdma::LocalMemoryRegion> pdp_mr);

        ~LocalMr()
        {
            close_pch(pch_fd_);
            close_pagemap(pagemap_fd_);
        }

        static void close_pagemap(int fd)
        {
            static auto &gv = GlobalVariables::instance();
            if (!gv.configs.is_server) return;
            if (fd >= 0) {
                close(fd);
            }
        }

        static void close_pch(int fd)
        {
            static auto &gv = GlobalVariables::instance();
            if (!gv.configs.is_server) return;
            if (fd >= 0) {
                close(fd);
            }
        }

        static void close_nvme(int fd)
        {
            if (fd >= 0) {
                close(fd);
            }
        }

        static int open_nvme()
        {
            static auto &gv = GlobalVariables::instance();
            if (!gv.configs.is_server) return -1;
            std::string path = "/dev/" + gv.configs.server_mmap_dev;

            int fd = open(path.c_str(), O_RDWR);
            ASSERT(fd >= 0, "fail to open nvme: %s", strerror(errno));

            return fd;
        }

        static int open_pch()
        {
            static auto &gv = GlobalVariables::instance();
            if (!gv.configs.is_server) return -1;

            std::string path = "/dev/" + gv.configs.server_mmap_dev;
            int fd = open("/proc/pch", O_RDWR);
            ASSERT(fd >= 0, "fail to open pch: %s", strerror(errno));
            auto ret = pwrite(fd, path.c_str(), path.length(), 0);
            ASSERT(ret == path.length(), "fail to write pch: %s", strerror(errno));

            return fd;
        }

        static int open_pagemap()
        {
            static auto &gv = GlobalVariables::instance();
            if (!gv.configs.is_server) return -1;

            int fd = open("/proc/self/pagemap", O_RDONLY);
            ASSERT(fd >= 0, "fail to open pagemap: %s", strerror(errno));

            return fd;
        }

        Identifier identifier() const
        {
            return user_mr_->lkey_;
        }

        auto ctx() const { return ctx_; }
        auto pinned_intv_trees() { return util::make_observer(pinned_intv_tree_vec_.get()); }
        auto tree_lock() { return util::make_observer(&tree_vec_lock_); }

        auto sector_lock_vec() { return util::make_observer(sector_lock_vec_.get()); }
        ReaderFriendlyLock *get_unit_lock(u64 unit_id)
        {
            return &unit_lock_vec_->at(util::hash_u64(unit_id) % unit_lock_vec_->size());
        }

        bool unit_is_hot(uint64_t unit_id) const { return unit_hot_vec_[unit_id]; }
        uint64_t advance_msync_unit_id() {
            do {
                msync_unit_id_ = (msync_unit_id_ + 1) % nr_units_;
            } while (!unit_hot_vec_[msync_unit_id_]);
            return msync_unit_id_;
        }

        auto user_mr() const { return util::make_observer(user_mr_.get()); }
        auto internal_odp_mr() const { return util::make_observer(internal_odp_mr_.get()); }
        uint64_t addr64() const { return user_mr_->addr64(); }
        uint64_t length() const { return user_mr_->length(); }
        uint64_t unit_base() const { return unit_base_; }
        std::tuple<uint64_t, uint64_t> get_unit_addr_span(uint64_t unit_id)
        {
            uint64_t addr_begin = std::max(addr64(), (unit_id + unit_base_) * UNIT_SIZE);
            uint64_t addr_end = std::min(addr64() + length(), (unit_id + unit_base_ + 1) * UNIT_SIZE);
            return std::make_tuple(addr_begin, addr_end);
        }

        bool is_pin() const { return type_ == MrBytes::Type::PIN; }
        bool is_odp() const { return type_ == MrBytes::Type::ODP; }
        bool is_pdp() const { return type_ == MrBytes::Type::PDP; }

        MrBytes to_mr_bytes() const
        {
            MrBytes bytes{};
            bytes.ctx_id = ctx_->remote_identifier();
            bytes.type = type_;
            bytes.user_mr_ = *user_mr_;
            if (!is_pin())
                bytes.bitmap_mr_ = *bitmap_->bitmap_mr();
            return bytes;
        }

        bool cover_region(uint64_t address, uint64_t length) const {
            return address >= (uint64_t) user_mr_->native_mr()->addr &&
                   address + length <= (uint64_t) user_mr_->native_mr()->addr + user_mr_->native_mr()->length;
        }

        void do_advise_region(uint64_t address, uint32_t length)
        {
            ibv_sge sg = {address, length, user_mr_->native_mr()->lkey};
            int ret = ibv_advise_mr(user_mr_->native_mr()->pd, IBV_ADVISE_MR_ADVICE_PREFETCH,
                                    IBV_ADVISE_MR_FLAG_FLUSH, &sg, 1);

            if (ret && ret != EBUSY) {
                pr_err("%s: errno in ibv_advise_mr", strerror(errno));
            }

//             if (internal_odp_mr_) {
//                 sg.lkey = internal_odp_mr_->native_mr()->lkey;
//                 int ret = ibv_advise_mr(internal_odp_mr_->native_mr()->pd, IBV_ADVISE_MR_ADVICE_PREFETCH_NO_FAULT,
//                                         IBV_ADVISE_MR_FLAG_FLUSH, &sg, 1);
//                 ASSERT(!ret || ret == EBUSY, "error=%s", strerror(ret));
//             }
        }

        void read_rnic_bitmap(u64 addr, u64 len, uint64_t *present_bitmap);
        size_t nr_cached_pages() const;
        bool range_has_uncached_page(u64 addr, u64 len) const;
        bool range_has_cached_page(u64 addr, u64 len) const;
        bool range_has_rnic_fault_page(uint64_t addr, uint64_t len) const;
        bool range_has_locked_page(u64 addr, u64 len) const;
        bool range_has_unlocked_page(u64 addr, u64 len) const;
        std::tuple<u64, u64> get_page_span(u64 addr, u64 len) const;
        std::tuple<u64, u64> get_unit_span(u64 addr, u64 len) const;

    public:
        void advise_region(uint64_t address, uint64_t length);
        void promote_hotspot();
        void update_page_state();
        void print_page_state();
        void register_counter(pdp::RcQp *pdp_rc, const rdma::RemoteMemoryRegion &counter_mr);
        void sync_cold_units();
    };

    class LocalMrManager : public Manager<LocalMr> {
    private:
        // mapping from a converted pinned mr to the original user mr.
        // This works because we generate pinned mrs for each user mr separately.
        // user_mr may be ODP or PDP, according to the PDP_MODE
        std::map<uint32_t, uint32_t> internal_pin_to_user_;

        // As we convert a PDP mr to an ODP mr in the send_wr chain.
        // we add a mapping here.
        std::map<uint32_t, uint32_t> internal_odp_to_pdp_;

        std::unique_ptr<std::jthread> promote_hotspot_thread_;
        std::unique_ptr<std::jthread> page_state_thread_;
        std::unique_ptr<std::jthread> sync_cold_thread_;

    private:
        // unlocked
        std::optional<uint32_t> find_internal_pin_to_user(uint32_t key)
        {
            auto it = internal_pin_to_user_.find(key);
            if (it == internal_pin_to_user_.end()) {
                return {};
            }
            return it->second;
        }

        // unlocked
        std::optional<uint32_t> find_internal_odp_to_pdp(uint32_t key)
        {
            auto it = internal_odp_to_pdp_.find(key);
            if (it == internal_odp_to_pdp_.end()) {
                return std::nullopt;
            }
            return it->second;
        }

        // unlocked
        bool is_internal_mr(uint32_t key)
        {
            return find_internal_pin_to_user(key).has_value() || find_internal_odp_to_pdp(key).has_value();
        }

        void do_in_reg(typename Map::iterator it) final;
        void promote_hotspot_func(const std::stop_token &stop_token);
        void page_state_func(const std::stop_token &stop_token);
        void sync_cold_func(const std::stop_token &stop_token);
    public:

        // pin for UD recv for now.
        // locked
        void pin_mr_in_recv_wr_chain(ibv_recv_wr *wr);

        int convert_pdp_mr_in_send_wr_chain(ibv_send_wr *wr);

        template <typename Wr>
        void advise_mr_in_wr_chain(const Wr *wr)
        {
//            std::shared_lock r_lk(shared_mutex_);
            for (const Wr *wr_iter = wr; wr_iter; wr_iter = wr_iter->next) {
                for (int i = 0; i < wr->num_sge; i++) {
                    ibv_sge &sge = wr->sg_list[i];
                    if (find_internal_pin_to_user(sge.lkey).has_value()) continue;

                    auto user_mr_key = sge.lkey;
                    if (auto opt_pdp_key = find_internal_odp_to_pdp(sge.lkey); opt_pdp_key.has_value()) {
                        user_mr_key = opt_pdp_key.value();
                    }
                    auto mr = unlocked_get(user_mr_key);
                    ASSERT(mr, "fail to find mr 0x%x", user_mr_key);
                    mr->advise_region(sge.addr, sge.length);
                }
            }
        }
    };
}
