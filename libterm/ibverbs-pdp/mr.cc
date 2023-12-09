#include "mr.hh"
#include "qp.hh"
#include <algorithm>
#include <numeric>

namespace pdp {
    // class MrPinnedIntervalTree
    int MrPinnedIntervalTree::insert_without_pinning(uint64_t address, uint64_t length)
    {
        static const auto &gv = GlobalVariables::instance();
        assert(gv.ori_ibv_symbols.reg_mr);
        // copied from the Linux kernel
        uint64_t first_idx = address >> kPageShift;
        uint64_t last_idx = (address + length - 1) >> kPageShift; // include the last byte!
        Interval intv{first_idx << kPageShift, (last_idx + 1) << kPageShift};

        // fast path
        std::shared_lock r_lk(shared_mutex_);
        {
            auto it = map_.upper_bound(intv);
            if (std::prev(it)->first.cover(intv)) {
                pr_debug("already pinned %s", std::prev(it)->first.to_string().c_str());
                return 0;
            }
        }

        // slow path
        std::unique_lock w_lk(mutex_);
        {
            auto it = map_.upper_bound(intv);
            if (std::prev(it)->first.cover(intv)) {
                pr_debug("already pinned %s", std::prev(it)->first.to_string().c_str());
                return 0;
            }
        }
        // we shall get the iterators again!
        auto it_left = map_.lower_bound(intv);
        auto it_right = map_.upper_bound({intv.right(), intv.right()});

        if (std::prev(it_left)->first.intersect(intv)) {
            it_left--;
        }

        auto new_left = std::min(intv.left(), it_left->first.left());
        auto new_right = std::max(intv.right(), std::prev(it_right)->first.right());
        auto new_intv = Interval{new_left, new_right};

        // when we release the rdma::LocalMemoryRegion::Sptr, the destructor will dereg the mr for us.
        map_.erase(it_left, it_right);
        map_.emplace(new_intv, nullptr);

        return 1;
    }

    int MrPinnedIntervalTree::pin_all()
    {
        static auto &gv = GlobalVariables::instance();
        ASSERT(gv.ori_ibv_symbols.reg_mr, "");
        int cnt = 0;

        for (auto &u : map_) {
            if (u.first.valid() && !u.second) {
                for (int i = 0; i < 3; i++) {
                    u.second = std::make_unique<rdma::LocalMemoryRegion>(local_mr_->ctx(), (void *)(u.first.left()),
                                                                         u.first.length(), false, gv.ori_ibv_symbols.reg_mr);
                    if (u.second) break;
                }
                ASSERT(u.second, "failed to register mr addr=0x%lx, length=0x%lx, error=%s", u.first.left(),
                       u.first.length(), strerror(errno));
                cnt++;
            }
        }
        return cnt;
    }

    // class RemoteMr
    RemoteMr::RemoteMr(util::ObserverPtr<pdp::RcQp> pdp_rc, const MrBytes &mr_bytes)
    : pdp_rc_(pdp_rc), mr_bytes_(mr_bytes),
      type_(mr_bytes.type), user_mr_{mr_bytes.user_mr_},
      page_base_{util::div_down(user_mr_.addr_, PAGE_SIZE)},
      nr_pages_{util::div_up(user_mr_.addr_ + user_mr_.length_, PAGE_SIZE) - page_base_},
      unit_base_{util::div_down(user_mr_.addr_, UNIT_SIZE)},
      nr_units_{util::div_up(user_mr_.addr_ + user_mr_.length_, UNIT_SIZE) - unit_base_},
      hot_unit_bitmap_(nr_units_, false)
    {
        if (type_ != MrBytes::Type::PIN) {
            size_t expected_bitmap_bytes = util::bits_to_long_aligned_bytes(nr_pages_);
            size_t actual_bitmap_bytes = mr_bytes_.bitmap_mr_.length();
            ASSERT(expected_bitmap_bytes == actual_bitmap_bytes, "bitmap_bytes error: expetected=0x%lx, actual=0x%lx",
                   expected_bitmap_bytes, actual_bitmap_bytes);
            page_bitmap_ = std::make_unique<RemotePageBitmap>(pdp_rc_->ctx(), rdma::RemoteMemoryRegion(mr_bytes.bitmap_mr_));
        }
        pr_info("mr_bytes={%s}", mr_bytes.to_string().c_str());
    }

    void RemoteMr::page_bitmap_assign_batch(uint64_t mr_addr, uint64_t length, uint64_t *present_bitmap)
    {
        static int single_thread_id = util::Schedule::thread_id();
        if (util::Schedule::thread_id() != single_thread_id) return;

        auto [page_begin, page_end] = get_page_span(mr_addr, length);
        for (uint64_t page = page_begin; page < page_end; page++) {
            uint64_t i = page - page_begin;
            if (util::test_bit(i, present_bitmap) != page_bitmap_->test_present(page)) {
                page_bitmap_->assign(page, util::test_bit(i, present_bitmap));
            }
        }
    }

    uint32_t RemoteMr::count_fault_pages(uint64_t addr, uint32_t len) const
    {
        uint64_t pg_id_begin = util::div_down(addr, PAGE_SIZE) - page_base_;
        uint64_t pg_id_end = util::div_up(addr + len, PAGE_SIZE) - page_base_;
        return page_bitmap_->range_count_fault(pg_id_begin, pg_id_end);
    }

    bool RemoteMr::has_fault_page(uint64_t addr, uint32_t len) const
    {
        uint64_t pg_id_begin = util::div_down(addr, PAGE_SIZE) - page_base_;
        uint64_t pg_id_end = util::div_up(addr + len, PAGE_SIZE) - page_base_;
        return page_bitmap_->range_has_fault(pg_id_begin, pg_id_end);
    }

    void RemoteMr::pull_page_bitmap()
    {
        ibv_wc wc{};
        rdma::Wr wr = page_bitmap_->pull_wr();

        u64 ts = util::wall_time_ns();
        pdp_rc_->mr_qp()->post_send_wr(&wr);
        pdp_rc_->mr_qp()->wait_send(&wc);

        ASSERT(wc.status == IBV_WC_SUCCESS, "pdp bitmap wc.status=%s", ibv_wc_status_str(wc.status));

        auto elapsed = util::wall_time_ns() - ts;

        auto nr_pages = util::mr_length_to_pgs(user_mr().length_);
        uint64_t nr_mapped_pages = 0;
        for (uint64_t i = 0; i < nr_pages; i++) {
            if (page_bitmap_->test_present(i)) nr_mapped_pages++;
        }
        double ratio = (double)nr_mapped_pages * 100 / nr_pages;
        pr_info("remote_mr(0x%x) #pages: total=%lu, mapped=%lu, ratio=%.2lf%%, elapsed=%luns",
                user_mr().rkey_, nr_pages, nr_mapped_pages, ratio, elapsed);
    }

    std::tuple<uint64_t, uint64_t> RemoteMr::get_unit_span(uint64_t addr, uint32_t len)
    {
        uint64_t unit_id_begin = util::div_down(addr, UNIT_SIZE) - unit_base_;
        uint64_t unit_id_end = util::div_up(addr + len, UNIT_SIZE) - unit_base_;
        return {unit_id_begin, unit_id_end};
    }

    std::tuple<uint64_t, uint64_t> RemoteMr::get_page_span(uint64_t addr, uint64_t len)
    {
        uint64_t page_begin = util::div_down(addr, PAGE_SIZE) - page_base_;
        uint64_t page_end = util::div_up(addr + len, PAGE_SIZE) - page_base_;
        return {page_begin, page_end};
    }

    std::tuple<uint64_t, uint64_t> RemoteMr::get_addr_span_of_page(uint64_t page)
    {
        uint64_t addr_begin = std::max(user_mr_.addr64(), (page + page_base_) * PAGE_SIZE);
        uint64_t addr_end = std::min(user_mr_.addr64() + user_mr_.length(), (page + 1 + page_base_) * PAGE_SIZE);
        return {addr_begin, addr_end};
    }

    // class CounterForRemoteMr
    CounterForRemoteMr::CounterForRemoteMr(util::ObserverPtr<pdp::RcQp> rc_qp, 
            util::ObserverPtr<pdp::RemoteMr> remote_mr)
        : rc_qp_(rc_qp), remote_mr_(remote_mr),
          vec_(remote_mr_->nr_units()),
          vec_mr_(rc_qp_->ctx(), vec_.data(), vec_.size() * sizeof(vec_[0]), false, GlobalVariables::instance().ori_ibv_symbols.reg_mr)
    {
    }

    std::string CounterForRemoteMr::identifier_string()
    {
        std::string str("[counter]");
        str += remote_mr_->mr_bytes().identifier().to_string();
        str += ":";
        str += rc_qp_->get_user_qp_remote_identifier().to_string();
        return str;
    }

    // class RemoteMrManager
    void RemoteMrManager::do_in_reg(typename Map::iterator it)
    {
        static auto &gv = GlobalVariables::instance();
        if (gv.configs.pull_page_bitmap && !pull_page_bitmap_thread_) {
            pull_page_bitmap_thread_ = std::make_unique<std::jthread>(
                [this](const std::stop_token &stop_token)
                {
                    pull_page_bitmap_func(stop_token);
                }
            );
        }
    }

    void RemoteMrManager::pull_page_bitmap_func(const std::stop_token &stop_token)
    {
        while (!stop_token.stop_requested()) {
            using namespace std::chrono_literals;

            auto tp = std::chrono::steady_clock::now();
            {
                std::shared_lock r_lk(lock_);
                for (auto &[id, mr]: map_) {
                    if (mr->is_pin()) continue;
                    mr->pull_page_bitmap();
                }
            }
            std::this_thread::sleep_until(tp + 1s);
        }
    }

    bool RemoteMrManager::rdma_should_via_rpc(util::ObserverPtr<RcQp> rc_qp,
                                              const rdma::Context::RemoteIdentifier &remote_ctx_id, 
                                              ibv_send_wr *wr)
    {
        static auto &gv = GlobalVariables::instance();
        switch (wr->opcode) {
            case IBV_WR_RDMA_WRITE:
            case IBV_WR_RDMA_READ: {
                static util::LatencyRecorder lr_get("remote_mr");
                lr_get.begin_one();
                RemoteMr::Identifier remote_mr_id{remote_ctx_id, wr->wr.rdma.rkey};
                util::ObserverPtr<RemoteMr> remote_mr = get_or_reg(remote_mr_id, rc_qp);
                lr_get.end_one();

                if (remote_mr->is_pin()) {
                    return false;
                }
                uint64_t remote_addr = wr->wr.rdma.remote_addr;
                uint32_t len = 0;
                for (int i = 0; i < wr->num_sge; i++) {
                    len += wr->sg_list[i].length;
                }

                return remote_mr->has_fault_page(remote_addr, len);
            }
            case IBV_WR_RDMA_WRITE_WITH_IMM:
            case IBV_WR_ATOMIC_CMP_AND_SWP:
            case IBV_WR_ATOMIC_FETCH_AND_ADD:
            default:
                // IBV_WR_SEND, IBV_WR_SEND_WITH_IMM, IBV_WR_LOCAL_INV
                // IBV_WR_BIND_MW, IBV_WR_SEND_WITH_INV, IBV_WR_TSO, IBV_WR_DRIVER1
                return false;
        }
    }

    util::ObserverPtr<RemoteMr> RemoteMrManager::get_or_reg(const RemoteMr::Identifier &remote_mr_id, util::ObserverPtr<pdp::RcQp> pdp_rc)
    {
        static auto &gv = GlobalVariables::instance();
        auto remote_mr = get(remote_mr_id); // raw pointer

        if (!remote_mr) [[unlikely]] {

            // lazy execution
            auto construct_lmd = [&remote_mr_id, pdp_rc]()
            {
                pr_info("get remote_mr.id=%s", remote_mr_id.to_string().c_str());
                auto vec_char = gv.sms.get(remote_mr_id.to_string());
                MrBytes mr_bytes{vec_char};
                return std::make_unique<RemoteMr>(pdp_rc, mr_bytes);
            };
            remote_mr = reg_with_construct_lambda(remote_mr_id, construct_lmd);
        }

        return remote_mr;
    }

    // class LocalMr
    // ODP or PDP
    LocalMr::LocalMr(std::unique_ptr<rdma::LocalMemoryRegion> odp_mr, std::unique_ptr<rdma::LocalMemoryRegion> pdp_mr)
    : ctx_(odp_mr->ctx()), pch_fd_(open_pch()), pagemap_fd_(open_pagemap()), nvme_fd_(open_nvme())
    {
        static const auto &gv = GlobalVariables::instance();
        if (pdp_mr) {
            type_ = MrBytes::Type::PDP;
            user_mr_ = std::move(pdp_mr);
            internal_odp_mr_ = std::move(odp_mr);
            bitmap_ = std::make_unique<LocalPageBitmap>(util::make_observer(user_mr_.get()));
        } else {
            type_ = MrBytes::Type::ODP;
            user_mr_ = std::move(odp_mr);
            bitmap_ = std::make_unique<LocalPageBitmap>(util::make_observer(user_mr_.get()));
        }

        pinned_intv_tree_vec_ = std::make_unique<std::vector<MrPinnedIntervalTree::Uptr>>();
        page_base_ = util::div_down(user_mr_->addr_, PAGE_SIZE);
        nr_pages_ = util::div_up(user_mr_->addr_ + user_mr_->length_, PAGE_SIZE) - page_base_;
        unit_base_ = util::div_down(user_mr_->addr_, UNIT_SIZE);
        nr_units_ = util::div_up(user_mr_->addr_ + user_mr_->length_, UNIT_SIZE) - unit_base_;
        page_state_vec_.resize(nr_pages_);
        page_mapped_vec_.resize(nr_pages_);
        page_locked_vec_.resize(nr_pages_);
        unit_hot_vec_.resize(nr_units_);

        counter_ = std::make_unique<CounterForLocalMr>(ctx_, nr_units_);
        sector_lock_vec_ = std::make_unique<std::vector<SpinLock>>(1024);
        unit_lock_vec_ = std::make_unique<std::vector<ReaderFriendlyLock>>(1024);
        unit_buffer_ = std::vector<uint8_t>(UNIT_SIZE);
    }

    size_t LocalMr::nr_cached_pages() const
    {
        if (is_pin()) return 0;
        size_t nr = 0;
        for (u64 page = 0; page < nr_pages_; page++) {
            if (page_state_vec_[page] & PageState::kCached) {
                nr++;
            }
        }
        return nr;
    }

    bool LocalMr::range_has_uncached_page(u64 addr, u64 len) const
    {
        if (is_pin()) return false;
        u64 page_begin = util::div_down(addr, PAGE_SIZE) - page_base_;
        u64 page_end = util::div_up(addr + len, PAGE_SIZE) - page_base_;
        for (u64 page = page_begin; page < page_end; page++) {
            if (page_state_vec_[page] == PageState::kUncached) {
                return true;
            }
        }
        return false;
    }

    bool LocalMr::range_has_cached_page(u64 addr, u64 len) const
    {
        if (is_pin()) return true;
        u64 page_begin = util::div_down(addr, PAGE_SIZE) - page_base_;
        u64 page_end = util::div_up(addr + len, PAGE_SIZE) - page_base_;
        for (u64 page = page_begin; page < page_end; page++) {
            if (page_state_vec_[page] & PageState::kCached) {
                return true;
            }
        }
        return false;
    }
    void LocalMr::read_rnic_bitmap(u64 addr, u64 len, uint64_t *present_bitmap)
    {
        auto [page_begin, page_end] = get_page_span(addr, len);
        for (u64 page = page_begin; page < page_end; page++) {
            u64 i = page - page_begin;
            if (bitmap_->test_present(page)) {
                util::set_bit(i, present_bitmap);
            } else {
                util::clear_bit(i, present_bitmap);
            }
        }
    }

    bool LocalMr::range_has_rnic_fault_page(uint64_t addr, uint64_t len) const
    {
        if (is_pin()) return false;
        uint64_t page_begin = util::div_down(addr, PAGE_SIZE) - page_base_;
        uint64_t page_end = util::div_up(addr + len, PAGE_SIZE) - page_base_;
        for (uint64_t page = page_begin; page < page_end; page++) {
            if (!bitmap_->test_present(page)) return true;
        }
        return false;
    }

    bool LocalMr::range_has_locked_page(u64 addr, u64 len) const
    {
        if (is_pin()) return true;
        std::tuple<u64, u64> span = get_page_span(addr, len);
        for (u64 page = std::get<0>(span); page < std::get<1>(span); page++) {
            if (page_locked_vec_[page]) return true;
        }
        return false;
    }

    bool LocalMr::range_has_unlocked_page(u64 addr, u64 len) const
    {
        if (is_pin()) return false;
        std::tuple<u64, u64> span = get_page_span(addr, len);
        for (u64 page = std::get<0>(span); page < std::get<1>(span); page++) {
            if (!page_locked_vec_[page]) return true;
        }
        return false;
    }

    std::tuple<u64, u64> LocalMr::get_page_span(u64 addr, u64 len) const
    {
        u64 page_begin = util::div_down(addr, PAGE_SIZE) - page_base_;
        u64 page_end = util::div_up(addr + len, PAGE_SIZE) - page_base_;
        return std::make_tuple(page_begin, page_end);
    }

    std::tuple<u64, u64> LocalMr::get_unit_span(u64 addr, u64 len) const
    {
        u64 unit_begin = util::div_down(addr, UNIT_SIZE) - unit_base_;
        u64 unit_end = util::div_up(addr + len, UNIT_SIZE) - unit_base_;
        return std::make_tuple(unit_begin, unit_end);
    }

    void LocalMr::advise_region(uint64_t address, uint64_t length)
    {
        if (!cover_region(address, length)) {
            return;
        }

        if (!range_has_rnic_fault_page(address, length)) {
            return;
        }

        for (uint64_t offset = 0; offset < length; offset += SZ_2G) {
            uint32_t iter_length = std::min<uint64_t>(SZ_2G, length - offset);
            do_advise_region(address + offset, iter_length);
        }
    }

    void LocalMr::print_page_state()
    {
        static auto &gv = GlobalVariables::instance();

        auto output_per_state = [](u64 length, auto fn_page_filter) {
            uint64_t iter_step = SZ_2G;
            uint64_t nr_total_present_pages = 0;

            std::string out_str;

            for (uint64_t iter_offset = 0; iter_offset < length; iter_offset += iter_step) {
                u64 iter_length = std::min(length - iter_offset, iter_step);
                uint32_t nr_present_pgs = 0;
                for (uint64_t i = 0; i < iter_length; i += PAGE_SIZE) {
                    if (fn_page_filter((iter_offset + i) / PAGE_SIZE)) {
                        nr_present_pgs++;
                    }
                }
                uint32_t present_percent = nr_present_pgs * 100 / (iter_length / PAGE_SIZE);
                if (present_percent == 100) {
                    fmt::format_to(std::back_inserter(out_str), "xx ");
                } else {
                    fmt::format_to(std::back_inserter(out_str), "{:02d} ", present_percent);
                }
                nr_total_present_pages += nr_present_pgs;
            }

            std::string out_str2 = fmt::format("{:3d}% ", nr_total_present_pages * 100 / util::div_up(length, PAGE_SIZE));
            return out_str2 + out_str;
        };
        std::ignore = system(R"(cat /proc/vmstat | egrep "writeback|dirty")");
        // uncached
        {
            auto fn_page_uncached = [this](u64 page_id) {
                return page_state_vec_[page_id] == PageState::kUncached;
            };

            std::string out_str = "[uncached] " + output_per_state(user_mr_->length(), fn_page_uncached);
            pr_info("%s", out_str.c_str());
        }

        // cached
        {
            auto fn_page_cached = [this](u64 page_id) -> bool {
                return page_state_vec_[page_id] & PageState::kCached;
            };

            std::string out_str = "[cached]   " + output_per_state(user_mr_->length(), fn_page_cached);
            pr_info("%s", out_str.c_str());
        }

        // mapped
        {
            auto fn_page_mapped = [this](u64 page_id) {
                u64 val = page_mapped_vec_[page_id];
                return val & (1ul << 63);
            };

            std::string out_str = "[mapped]   " + output_per_state(user_mr_->length(), fn_page_mapped);
            pr_info("%s", out_str.c_str());
        }

        // locked
        {
            auto fn_page_locked = [this](u64 page_id) -> bool
            {
                return page_locked_vec_[page_id];
            };

            std::string out_str = "[locked]   " + output_per_state(user_mr_->length(), fn_page_locked);
            pr_info("%s", out_str.c_str());
        }

        // innic
        {
            auto fn_page_innic = [this](u64 page_id) {
                return bitmap_->test_present(page_id);
            };

            std::string out_str = "[innic]    " + output_per_state(user_mr_->length(), fn_page_innic);
            pr_info("%s", out_str.c_str());
        }
    }

    void LocalMr::update_page_state()
    {
        static auto &gv = GlobalVariables::instance();
        // use pch or mincore
        {
            static util::LatencyRecorder<false> lr("lr_update_pch");
            util::LatencyRecorderHelper h(lr);
#if 1
            {
                auto ret = pread(pch_fd_, page_state_vec_.data(), nr_pages_, 0);
                ASSERT(ret == nr_pages_, "fail to read pch: %s", strerror(errno));
                for (uint64_t i = 0; i < nr_pages_; i++) {
                    if (page_state_vec_[i] & PageState::kDirty) {
                        if (!gv.has_dirty_page) {
                            gv.has_dirty_page = true;
                        }
                        break;
                    }
                }
            }
#else
            {
                std::vector<unsigned char> vec_mincore(nr_pages_);
                int ret = mincore(user_mr()->addr(), user_mr()->length(), vec_mincore.data());
                ASSERT(ret == 0, "fail to mincore: %s", strerror(errno));
                memcpy(page_state_vec_.data(), vec_mincore.data(), nr_pages_);
            }
#endif
        }

        {
            // static util::LatencyRecorder lr("lr_update_pagemap");
            // util::LatencyRecorderHelper h(lr);
            // auto ret = pread(pagemap_fd_, page_mapped_vec_.data(), nr_pages_ * sizeof(u64), page_base_ * sizeof(u64));
            // ASSERT(ret == nr_pages_ * sizeof(u64), "failed to read pagemap: %s", strerror(errno));
        }
    }

    void LocalMr::sync_cold_units()
    {
        for (uint64_t unit_id = 0; unit_id < nr_units_; unit_id++) {
            if (unit_is_hot(unit_id)) continue;
            auto [sync_addr_begin, sync_addr_end] = get_unit_addr_span(unit_id);
            uint64_t sync_offset_begin = sync_addr_begin - addr64();
            uint64_t sync_offset_end = sync_addr_end - addr64();
            int ret = sync_file_range(nvme_fd_, sync_offset_begin, sync_offset_end - sync_offset_begin, SYNC_FILE_RANGE_WRITE_AND_WAIT);
            if (ret) {
                pr_err("fail to sync: %s", strerror(errno));
            }
            if (unit_id % 1024 == 0) {
                pr_info("unit_id=%lu", unit_id);
            }
        }
    }

    void LocalMr::promote_hotspot()
    {
        static auto &gv = GlobalVariables::instance();
        size_t nr_target_units = nr_units_;
        std::vector<uint32_t> counter_this_period(nr_units_);

        if (is_pin()) return;

        {
            std::scoped_lock lk(counter_lock_);
            for (auto & [pdp_rc, remote_counter] : counters_from_remote_qps_) {
                bool ret = pdp_rc->mr_qp()->read_sync(counter_->vec_mr_.pick_all(),
                                        remote_counter.counter_mr_.pick_all());
                ASSERT(ret, "failed to read");

                for (size_t i = 0; i < nr_units_; i++) {
                    counter_this_period[i] += counter_->vec_[i];
                }
            }
        }

        for (size_t i = 0; i < nr_units_; i++) {
            u32 l = counter_->last_counter_[i];
            counter_->last_counter_[i] = counter_this_period[i];
            counter_this_period[i] -= l;
        }

        std::vector<uint32_t> vec(nr_units_);
        std::iota(vec.begin(), vec.end(), 0);

        // most rdma access
        std::sort(vec.begin(), vec.end(),
                  [&counter_this_period](uint32_t lhs, uint32_t rhs)
                  {
                        return std::tie(counter_this_period[lhs], lhs) > std::tie(counter_this_period[rhs], rhs);
                  });

        uint32_t avg = std::accumulate(counter_this_period.cbegin(), counter_this_period.cend(), 0) / nr_units_;
        if (counter_this_period[vec.front()] == 0) {
            gv.doing_promotion = false;
            return;
        } else {
            gv.doing_promotion = true;
        }

        if (counter_this_period[vec.front()] <= 10 * avg) {
            pr_info("detect uniform pattern.");
            int cached_off = 0;
            int uncached_off = nr_units_ - 1;

            for (int i = 0; i < nr_units_; i++) {
                u64 addr = std::max((vec[i] + unit_base_) * UNIT_SIZE, user_mr_->addr_);
                u32 length = std::min<uint64_t>(UNIT_SIZE, user_mr_->addr_ + user_mr_->length_ - addr);
                if (!range_has_uncached_page(addr, length)) {
                    vec[cached_off++] = i;
                } else {
                    vec[uncached_off--] = i;
                }
            }
            ASSERT(cached_off - uncached_off == 1, "");
            std::sort(vec.begin() + cached_off, vec.end());

            std::string out_str;
            for (int i = 0; i < 10; i++) {
                fmt::format_to(std::back_inserter(out_str), "({} {})", vec[i], counter_this_period[vec[i]]);
            }
            pr_info("first ones: %s", out_str.c_str());
        } else {
            std::string out_str;
            for (int i = 0; i < 10; i++) {
                fmt::format_to(std::back_inserter(out_str), "({} {})", vec[i], counter_this_period[vec[i]]);
            }
            pr_info("detect skewed pattern: hottest units: %s", out_str.c_str());
        }

        if (gv.configs.server_memory_gb > 0) {
            nr_target_units = gv.configs.server_memory_gb * SZ_1G / UNIT_SIZE * 85 / 100;
            nr_target_units = std::min(nr_target_units, nr_units_);
            if (!gv.doing_promotion) {
                size_t nr_cached_units = nr_cached_pages() * PAGE_SIZE / UNIT_SIZE;
                nr_target_units = nr_cached_units;
            }
//            nr_target_units = std::max(nr_target_units, nr_cached_units);
        }

        for (uint64_t i = 0; i < nr_target_units; i++) {
            unit_hot_vec_[vec[i]] = true;
        }
        for (uint64_t i = nr_target_units; i < nr_units_; i++) {
            unit_hot_vec_[vec[i]] = false;
        }

        if (nr_locked_units_ >= nr_target_units) {
            for (size_t i = nr_target_units; i < nr_units_; i++) {
                u64 addr = std::max((vec[i] + unit_base_) * UNIT_SIZE, user_mr_->addr_);
                u32 length = std::min<uint64_t>(UNIT_SIZE, user_mr_->addr_ + user_mr_->length_ - addr);
                if (!range_has_locked_page(addr, length)) continue; // the range is not locked.
                int ret = munlock((void *) addr, length);
                if (!ret) {
                    std::tuple<u64, u64> span = get_page_span(addr, length);
                    for (u64 page = std::get<0>(span); page < std::get<1>(span); page++) {
                        page_locked_vec_[page] = false;
                    }
                    nr_locked_units_--;
                } else {
                    pr_err("error in munlock: %s", strerror(errno));
                }
            }
        }

        size_t nr_promoted_units = 0;
        auto tp = std::chrono::steady_clock::now();
        std::string out;
        for (int i = 0; i < nr_target_units; i++) {
            u64 addr = std::max((vec[i] + unit_base_) * UNIT_SIZE, user_mr_->addr_);
            u32 length = std::min<uint64_t>(UNIT_SIZE, user_mr_->addr_ + user_mr_->length_ - addr);

            if (gv.doing_promotion && range_has_unlocked_page(addr, length)) {
                int ret = 0;
                bool need_lock = gv.has_write_req; // gv.has_write_req may be modified by others.

                if (need_lock) {
                    ReaderFriendlyLock *unit_lock_ptr = get_unit_lock(vec[i]);
                    std::unique_lock unique_lock_unit(*unit_lock_ptr);
                    ret = pread(nvme_fd_, unit_buffer_.data(), length, addr - user_mr_->addr_);
                    ASSERT(ret == length, "fail to read: %s", strerror(errno));
                }

                ret = mlock((void *)addr, length);
                if (!ret) {
                    std::tuple<u64, u64> span = get_page_span(addr, length);
                    for (u64 page = std::get<0>(span); page < std::get<1>(span); page++) {
                        page_locked_vec_[page] = true;
                    }
                    nr_locked_units_++;
                } else {
                    pr_err("error in mlock: %s", strerror(errno));
                }
            }

            if (!range_has_rnic_fault_page(addr, length)) continue; // the range has been registered
            nr_promoted_units++;
            if (nr_promoted_units <= 10) {
                fmt::format_to(std::back_inserter(out), "({} {} {})", i, vec[i], counter_this_period[vec[i]]);
            }
            // msync
            if (gv.doing_promotion && (gv.has_write_req || gv.has_dirty_page) && nr_target_units < nr_units_) {
                uint64_t msync_unit_id_first = advance_msync_unit_id();
                do {
                    u64 msync_addr = std::max((msync_unit_id_ + unit_base_) * UNIT_SIZE, user_mr_->addr_);
                    u32 msync_len = std::min<uint64_t>(UNIT_SIZE, user_mr_->addr_ + user_mr_->length_ - addr);
                    if (!range_has_cached_page(msync_addr, msync_len)) continue;
                    if (!gv.has_write_req) {
                        // There is RPC WRITE to flush pages. Let's do it here.
                        msync((void *) msync_addr, msync_len, MS_SYNC);
                    }
                    if (range_has_locked_page(msync_addr, msync_len)) {
                        int ret = munlock((void *) msync_addr, msync_len);
                        if (!ret) {
                            std::tuple<u64, u64> span = get_page_span(msync_addr, msync_len);
                            for (u64 page = std::get<0>(span); page < std::get<1>(span); page++) {
                                page_locked_vec_[page] = false;
                            }
                            nr_locked_units_--;
                        } else {
                            pr_err("error in munlock: %s", strerror(errno));
                        }
                    }
                    // madvise((void *) msync_addr, msync_len, MADV_DONTNEED);
                    if (nr_promoted_units <= 10) {
                        fmt::format_to(std::back_inserter(out), "(sync {})", msync_unit_id_);
                    }
                    break;
                } while(advance_msync_unit_id() != msync_unit_id_first);
            }

            do_advise_region(addr, length);

            if (nr_locked_units_ >= nr_target_units) break;
            if (std::chrono::steady_clock::now() - tp > std::chrono::milliseconds{1} * gv.configs.promotion_window_ms) break;
        }
        fmt_pr(info, "target_units: {}, locked_units: {}, promote {} units: {}",
               nr_target_units, nr_locked_units_, nr_promoted_units, out);

//        update_page_state();
//        print_page_state();
    }

    void LocalMr::register_counter(pdp::RcQp *pdp_rc, const rdma::RemoteMemoryRegion &counter_mr)
    {
        std::unique_lock lk(counter_lock_);

        if (counters_from_remote_qps_.contains(pdp_rc)) {
            ASSERT(false, "duplicated register_counter requests from the same QP!");
        }

        counters_from_remote_qps_.emplace(pdp_rc, counter_mr);
    }

    // class LocalMrManager
    void LocalMrManager::do_in_reg(typename Map::iterator it)
    {
        static auto &gv = GlobalVariables::instance();

        const auto &local_mr = it->second;
        if (local_mr->is_pdp()) {
            internal_odp_to_pdp_[local_mr->internal_odp_mr()->lkey_] = local_mr->user_mr()->lkey_;
        }

        MrBytes mr_bytes = local_mr->to_mr_bytes();
//        pr_info("put mr.id=%s", mr_bytes.identifier().to_string().c_str());
        gv.sms.put(mr_bytes.identifier().to_string(), mr_bytes.to_vec_char());

        if (!gv.configs.is_server) return;

        if (gv.configs.mode == +Mode::PDP && gv.configs.promote_hotspot && !promote_hotspot_thread_) {
            promote_hotspot_thread_ = std::make_unique<std::jthread>(
                    [this](const std::stop_token &stop_token) {
                        promote_hotspot_func(stop_token);
                    });
//            sync_cold_thread_ = std::make_unique<std::jthread>(
//                    [this](const std::stop_token &stop_token) {
//                        sync_cold_func(stop_token);
//                    });
        }

        if (gv.configs.mode == +Mode::PDP && gv.configs.server_page_state && !page_state_thread_) {
            page_state_thread_ = std::make_unique<std::jthread>(
                [this] (const std::stop_token &stop_token) {
                    page_state_func(stop_token);
                }
            );
        }
    }

    void LocalMrManager::pin_mr_in_recv_wr_chain(ibv_recv_wr *wr) {
        const static auto &gv = GlobalVariables::instance();
        assert(!gv.configs.mode == +Mode::PIN);
        thread_local util::LatencyRecorder lr(__func__);
        util::LatencyRecorderHelper lr_h(lr);

        uint32_t local_mr_lkey = 0;
        std::unique_ptr<MrPinnedIntervalTree> new_intv_tree;
        util::ObserverPtr <LocalMr::PinnedIntvTreeVec> pinned_intv_tree_vec;
        util::ObserverPtr <ReaderFriendlyLock> tree_lock;

        std::vector<Interval> new_pin_intvs;

        int nr_total_sges = 0, nr_odp_sges = 0;
        int nr_new_pin_regions = 0;
        for (ibv_recv_wr *wr_iter = wr; wr_iter; wr_iter = wr_iter->next) {
            nr_total_sges += wr_iter->num_sge;
            for (int i = 0; i < wr_iter->num_sge; i++) {
                struct ibv_sge &sge = wr_iter->sg_list[i];
                assert(sge.length);
                uint32_t user_sge_lkey;

                auto opt_pdp_key = find_internal_odp_to_pdp(sge.lkey);
                if (opt_pdp_key.has_value()) {
                    sge.lkey = opt_pdp_key.value();
                }

                util::ObserverPtr <LocalMr> mr = unlocked_get(sge.lkey);
                // mr: the mr is not an interval mr pinned by pdp.
                // mr->is_pin: the mr is registered by the user and is pinned.
                if (mr && mr->is_pin()) continue;

                /* *
                 * !mr: this is a converted pinned mr.
                 * We restore it to the original user mr with internal_pin_to_user_ for the following scenario found in practice.
                 * When we have modified the lkey of a wr->sg_list[i], the user may modify the addr of sg_list[i] again to post a new wr, but leaves the lkey unmodified.
                 * In this case, the converted lkey does not cover the new address, so we have to pin again.
                 * We insert assert expressions here to check potential corner cases not implemented.
                 */
                if (!mr) {
                    auto it = internal_pin_to_user_.find(sge.lkey);
                    ASSERT(it != internal_pin_to_user_.end(), "sge.lkey=0x%x is not intenal pin.", sge.lkey);
                    user_sge_lkey = it->second;

                    mr = unlocked_get(user_sge_lkey);
                } else {
                    user_sge_lkey = sge.lkey;
                }

                if (local_mr_lkey == 0) {
                    // not initialized
                    local_mr_lkey = user_sge_lkey;
                    pinned_intv_tree_vec = mr->pinned_intv_trees();
                    tree_lock = mr->tree_lock();
                    new_intv_tree = std::make_unique<MrPinnedIntervalTree>(mr);
                }

                ASSERT(user_sge_lkey == local_mr_lkey, "multiple mr_s in a wr chain detected.");

                nr_odp_sges++;

                std::shared_lock lk(*tree_lock);
                if (pinned_intv_tree_vec->empty()) {
                    new_pin_intvs.emplace_back(sge.addr, sge.addr + sge.length);
//                    pr_info("addr=0x%lx, length=0x%x, pinned_intv_tree_vec is empty");

                    continue;
                }

                bool found = false;
                for (const auto &tree: *pinned_intv_tree_vec) {
                    auto opt_pin_key = tree->get_covered_region_lkey(sge.addr, sge.length);
                    if (opt_pin_key.has_value()) {
                        if (sge.lkey != opt_pin_key.value()) {
                            internal_pin_to_user_[opt_pin_key.value()] = local_mr_lkey;
                            sge.lkey = opt_pin_key.value();
                        }
                        found = true;
                        break;
                    }
                }
                if (!found) {
                    new_pin_intvs.emplace_back(sge.addr, sge.addr + sge.length);
                }
            }
        }

        if (new_pin_intvs.empty()) return;
        
        {
            nr_new_pin_regions = new_intv_tree->pin_regions(new_pin_intvs);
            (void) nr_new_pin_regions;
            pr_info("new_intv_tree: %s", new_intv_tree->to_string().c_str());
            std::unique_lock lk(*tree_lock);
            pinned_intv_tree_vec->emplace_back(std::move(new_intv_tree));
            pr_info("# sges: total=%d, odp=%d, new_pin=%lu. # regions: new_pin=%d",
                nr_total_sges, nr_odp_sges, new_pin_intvs.size(), nr_new_pin_regions);
            pr_info("pinned_intv_tree_vec->size()=%lu", pinned_intv_tree_vec->size());
        }

        for (ibv_recv_wr *wr_iter = wr; wr_iter; wr_iter = wr_iter->next) {
            for (int i = 0; i < wr_iter->num_sge; i++) {
                struct ibv_sge &sge = wr_iter->sg_list[i];
                const auto &mr = unlocked_get(sge.lkey);
                if (mr && mr->is_pin()) continue;

                bool found = false;
                std::shared_lock lk(*tree_lock);
                for (const auto &tree : *pinned_intv_tree_vec) {
                    auto opt_pin_key = tree->get_covered_region_lkey(sge.addr, sge.length);
                    if (opt_pin_key.has_value()) {
                        if (sge.lkey != opt_pin_key.value()) {
                            internal_pin_to_user_[opt_pin_key.value()] = local_mr_lkey;
                            sge.lkey = opt_pin_key.value();
                        }
                        found = true;
                        break;
                    }
                }
                if (!found) {
                    pr_err("target region: %s", Interval(sge.addr, sge.addr + sge.length).to_string().c_str());
                    assert(false);
                }
            }
        }
    }

    int LocalMrManager::convert_pdp_mr_in_send_wr_chain(ibv_send_wr *wr)
    {
        const static auto &gv = GlobalVariables::instance();
        assert(gv.configs.mode == +Mode::PDP);

        int nr_converted_mrs = 0;
        for (ibv_send_wr *wr_iter = wr; wr_iter; wr_iter = wr_iter->next) {
            switch (wr_iter->opcode) {
                // OK! A READ wr writes to the local mr. it triggers PF for PDP.
                [[likely]] case IBV_WR_RDMA_READ:
                    break;
                    // A WRITE/SEND wr reads magic content from the local MR and writes to the remote mr.
                case IBV_WR_RDMA_WRITE:
                case IBV_WR_RDMA_WRITE_WITH_IMM:
                case IBV_WR_SEND:
                case IBV_WR_SEND_WITH_IMM: {
                    for (int i = 0; i < wr_iter->num_sge; i++) {
                        ibv_sge &sge = wr_iter->sg_list[i];
                        // an internal pin/odp mr.
                        if (is_internal_mr(sge.lkey)) continue;
                        const auto &mr = get(sge.lkey);
                        ASSERT(mr, "unknown mr=0x%x", sge.lkey);
                        if (!mr->is_pdp()) continue;
                        sge.lkey = mr->internal_odp_mr()->lkey_;
                        nr_converted_mrs++;
                    }
                }
                    break;
                default:
                    pr_err("unsupported op=0x%x!", wr->opcode);
            }
        }
        return nr_converted_mrs;
    }

    void LocalMrManager::promote_hotspot_func(const std::stop_token &stop_token)
    {
        using namespace std::chrono_literals;
        util::Schedule::bind_server_bg_cpu(0);
        std::this_thread::sleep_for(3s);
        while (!stop_token.stop_requested()) {
            auto tp = std::chrono::steady_clock::now();
            std::shared_lock lk(lock_);
            for (auto &[id, mr] : map_) {
                mr->promote_hotspot();
            }
            std::this_thread::sleep_until(tp + 1s);
        }
    }

    void LocalMrManager::page_state_func(const std::stop_token &stop_token)
    {
        static auto &gv = GlobalVariables::instance();
        static util::LatencyRecorder lr("lr_page_state");
        util::Schedule::bind_server_bg_cpu(1);
        auto last_print_tp = std::chrono::steady_clock::now();

        while (!stop_token.stop_requested()) {
            if (gv.doing_promotion) {
                util::LatencyRecorderHelper h(lr);
                std::shared_lock lk(lock_);
                for (auto &[id, mr] : map_) {
                    mr->update_page_state();
                    if (std::chrono::steady_clock::now() > last_print_tp + std::chrono::seconds{5}) {
                        mr->print_page_state();
                        last_print_tp = std::chrono::steady_clock::now();
                    }
                }
            }
            using namespace std::chrono_literals;
            std::this_thread::sleep_for(1s);
        }
    }

    void LocalMrManager::sync_cold_func(const std::stop_token &stop_token)
    {
        static util::LatencyRecorder<false> lr("lr_sync_cold");
        pr_info("starting sync_cold_func");
        return;
        while (!stop_token.stop_requested()) {
            std::shared_lock lk(lock_);
            using namespace std::chrono_literals;
            auto tp = std::chrono::steady_clock::now();
            lr.begin_one();
            for (auto &[id, mr] : map_) {
                mr->sync_cold_units();
            }
            lr.end_one();
            std::this_thread::sleep_until(tp + 1s);
        }
    }
}
