#include "qp.hh"
#include "mr.hh"
#include <algorithm>

namespace pdp {
    constexpr uint8_t MAGIC_BYTE = 0xfd;

    RcQp::RcQp(util::ObserverPtr<rdma::Context> ctx, struct ibv_qp *uqp, const ibv_qp_init_attr *init_attr)
    : ctx_(ctx), uqp_(uqp),
    uqp_sq_sig_all_(init_attr->sq_sig_all),
    uqp_cap_(init_attr->cap),
    send_bitmap_(uqp_cap_.max_send_wr)
    {
        static auto &gv = GlobalVariables::instance();
        user_send_wr_vec_.reserve(uqp_cap_.max_send_wr);
        for (int i = 0; i < uqp_cap_.max_send_wr; i++) {
            user_send_wr_vec_.emplace_back(this, uqp_cap_.max_send_sge);
        }

        sqp_ = std::make_unique<rdma::RcQp>(ctx, gv.ori_ibv_symbols.create_qp, RPC_SERVER_POLL_WITH_EVENT ? gv.configs.is_server : false);
        mr_qp_ = std::make_unique<rdma::RcQp>(ctx, gv.ori_ibv_symbols.create_qp);
    }

    RcBytes RcQp::to_rc_bytes() const
    {
        RcBytes rc_bytes;
        rc_bytes.user_qp = rdma::QueuePair::Identifier{ctx_->remote_identifier(), uqp_->qp_num};
        rc_bytes.shadow_qp = sqp_->identifier();
        rc_bytes.bitmap_qp = mr_qp_->identifier();
        return rc_bytes;
    }
    // test
    bool RcQp::connect_to(const RcBytes &rc_bytes)
    {
        static auto &gv = GlobalVariables::instance();
        assert(gv.ori_ibv_symbols.modify_qp);
        assert(gv.ori_ibv_symbols.reg_mr);

        bool ok = sqp_->connect_to(rc_bytes.shadow_qp, gv.ori_ibv_symbols.modify_qp);
        if (!ok) return false;
        ok = mr_qp_->connect_to(rc_bytes.bitmap_qp, gv.ori_ibv_symbols.modify_qp);
        if (!ok) return false;
        msg_conn_ = std::make_unique<pdp::MessageConnection>(util::make_observer(sqp_.get()));
        pr_info("%s => %s", this->to_rc_bytes().to_qp_string().c_str(), rc_bytes.to_qp_string().c_str());

        return true;
    }

    std::optional<uint32_t> RcQp::find_send_seq()
    {
        for (std::size_t i = 0; i < send_bitmap_.size(); i++) {
            if (!send_bitmap_[i])
                return i;
        }
        return {};
    }

    void RcQp::clear_send_seq(uint32_t seq)
    {
        send_bitmap_[seq] = false;
    }

    void RcQp::set_send_seq(uint32_t seq)
    {
        send_bitmap_[seq] = true;
    }

    bool RcQp::test_send_seq(uint32_t seq)
    {
        return send_bitmap_[seq];
    }

    bool RcQp::buffer_is_magic(const char *data, uint32_t len)
    {
        // bool is_filled_pattern = true;
        // for (uint32_t i = 0; i < len; i++) {
        //     if ((uint8_t)data[i] != MAGIC_BYTE + 1) {
        //         is_filled_pattern = false;
        //     }
        // }
        // ASSERT(!is_filled_pattern, "still filled pattern after RDMA READ! data=%p", data);

        for (uint32_t i = 0; i < len; i++) {
            if ((uint8_t)data[i] != MAGIC_BYTE) {
                return false;
            }
        }

        return true;
    }

    int RcQp::sge_identify_magic(const ibv_sge &sge, uint64_t remote_addr, uint64_t *fault_bitmap)
    {
        uint64_t begin_page_id = util::div_down(remote_addr, PAGE_SIZE);
        uint64_t end_page_id = util::div_up(remote_addr + sge.length, PAGE_SIZE);
        uint32_t nr_pages = end_page_id - begin_page_id;
        int nr_fault_pages = 0;

        for (uint32_t idx = 0; idx < nr_pages; idx++) {
            uint64_t iter_remote_addr = std::max(remote_addr, (begin_page_id + idx) * PAGE_SIZE);
            uint64_t iter_offset = iter_remote_addr - remote_addr;
            uint32_t iter_length = std::min(PAGE_SIZE - iter_remote_addr % PAGE_SIZE, sge.length - iter_offset);

            if (buffer_is_magic((const char *)sge.addr + iter_offset, iter_length)) {
                util::set_bit(idx, fault_bitmap);
                nr_fault_pages++;
            } else {
                util::clear_bit(idx, fault_bitmap);
            }
        }
        return nr_fault_pages;
    }

    void RcQp::rpc_wr_async(ibv_send_wr *wr)
    {
        msg_conn_->lock();
        pdp::Message *msg = msg_conn_->send_mr_ptr();

        switch (wr->opcode) {
            case IBV_WR_RDMA_READ: {
                msg->opcode = RpcOpcode::kReadSingle;
                msg->read_single.s_addr = wr->wr.rdma.remote_addr;
                msg->read_single.s_lkey = wr->wr.rdma.rkey;
                msg->read_single.c_addr = wr->sg_list[0].addr;
                msg->read_single.c_rkey = wr->sg_list[0].lkey;
                msg->read_single.len = wr->sg_list[0].length;
                ASSERT(wr->sg_list[0].length <= MAX_RDMA_LENGTH, "RDMA length (0x%x) is too long! (limit 0x%lx)", wr->sg_list[0].length, MAX_RDMA_LENGTH);
                msg->read_single.update_bitmap = false; // only in RPC mode, we can reach here.
                break;
            }
            case IBV_WR_RDMA_WRITE: {
                ASSERT(wr->sg_list[0].length <= MAX_RDMA_LENGTH, "RDMA length (0x%x) is too long! (limit 0x%lx)", wr->sg_list[0].length, MAX_RDMA_LENGTH);


                if (wr->sg_list[0].length <= WRITE_INLINE_SIZE) {
                    msg->opcode = RpcOpcode::kWriteInline;
                    msg->write_inline.s_addr = wr->wr.rdma.remote_addr;
                    msg->write_inline.s_lkey = wr->wr.rdma.rkey;
                    msg->write_inline.len = wr->sg_list[0].length;
                    memcpy(msg->write_inline.data, (void *)wr->sg_list[0].addr, wr->sg_list[0].length);
                    msg->write_inline.update_bitmap = PAGE_BITMAP_UPDATE;
                } else {
                    msg->opcode = RpcOpcode::kWriteSingle;
                    msg->write_single.s_addr = wr->wr.rdma.remote_addr;
                    msg->write_single.s_lkey = wr->wr.rdma.rkey;
                    msg->write_single.c_addr = wr->sg_list[0].addr;
                    msg->write_single.c_rkey = wr->sg_list[0].lkey;
                    msg->write_single.len = wr->sg_list[0].length;
                    msg->write_single.update_bitmap = PAGE_BITMAP_UPDATE;
                }
                break;
            }
            default:
                ASSERT(false, "unsupported wr->opcode=%u", wr->opcode);
        }

        msg_conn_->send(msg);
    }

    Message RcQp::rpc_wr_wait(ibv_send_wr *wr)
    {
        ibv_wc wc{};
        Message msg_return = msg_conn_->get(&wc);
//        ASSERT(wc.opcode == (wr->opcode == IBV_WR_RDMA_WRITE ? IBV_WC_RECV : IBV_WC_RECV_RDMA_WITH_IMM), "");
//        ASSERT(wc.opcode == IBV_WC_RECV, "expected: IBV_WC_RECV, got: 0x%x", wc.opcode);
        msg_conn_->unlock();

        return msg_return;
    }

    // post process a wc
    // return: whether it is signaled
    bool RcQp::post_process_wc(struct ibv_wc *wc)
    {
        const static auto &gv = GlobalVariables::instance();
        std::unique_lock<std::mutex> lk(mutex_);
        static util::Recorder<false> rec_all("rec_reqs_all");
        static util::LatencyRecorder<false> lr_rpc("lr_reqs_rpc_in_poll");
        static util::Recorder<false> rec_read_magic("rec_reqs_read_magic"); // we tried read, but got magic

        rec_all.record_one(1);

        WrId swr_id(wc->wr_id);
        uint32_t seq = swr_id.wr_seq();
        ASSERT(seq >= 0 && seq < 128, "seq=%u is out of bound (%lu)", seq, user_send_wr_vec_.size());
        bool signaled = swr_id.user_signaled();
        UserSendWr *user_send_wr = &user_send_wr_vec_[seq];
        // pr_info("qpn=0x%x, opcode=0x%x, wr_id=0x%lx, signaled=%u", uqp_->qp_num, wc->opcode, wc->wr_id, signaled);

        ASSERT(wc->status == IBV_WC_SUCCESS, "qpn=0x%x, wc->status=%s", uqp_->qp_num, ibv_wc_status_str(wc->status));
        ASSERT(test_send_seq(seq), "qpn=0x%x, seq(%u) is not set", uqp_->qp_num, seq);

        if (gv.configs.read_magic_pattern && wc->opcode == IBV_WC_RDMA_READ && !user_send_wr->via_rpc) {
            pr_once(info, "read magic");
            ibv_send_wr *uwr = &user_send_wr->wr;
            ASSERT(uwr->opcode == IBV_WR_RDMA_READ, "opcode=0x%x, expected RDMA READ!", uwr->opcode);
            ASSERT(uwr->num_sge == 1, "uwr->num_sge=%d", uwr->num_sge);
            ASSERT(uwr->sg_list[0].length, "uwr->sg_list[0]=0x%x", uwr->sg_list[0].length);

            pdp::Message msg = {
                .opcode = kReadSparce,
                .read_sparce = {
                    .s_addr = uwr->wr.rdma.remote_addr,
                    .s_lkey = uwr->wr.rdma.rkey,
                    .c_addr = uwr->sg_list[0].addr,
                    .c_rkey = uwr->sg_list[0].lkey,
                    .len = uwr->sg_list[0].length,
                    .update_bitmap = PAGE_BITMAP_UPDATE
                }
            };

#if PAGE_BITMAP_HINT_READ
            ASSERT(user_send_wr->remote_mr, "user_send_wr->remote_mr is nullptr!");
#endif

            if (sge_identify_magic(uwr->sg_list[0], uwr->wr.rdma.remote_addr, msg.read_sparce.fault_bitmap)) {
                if (user_send_wr->rdma_read) {
                    rec_read_magic.record_one(1);
                }
                lr_rpc.begin_one();
                ibv_wc msg_wc{};
                msg_conn_->lock();
                msg_conn_->send(&msg);
                Message msg_return = msg_conn_->get(&msg_wc);

#if PAGE_BITMAP_UPDATE
                ASSERT(msg_wc.opcode == IBV_WC_RECV, "error opcode=%d", msg_wc.opcode);
                user_send_wr->remote_mr->page_bitmap_assign_batch(msg.read_sparce.s_addr, msg.read_sparce.len,
                                                    msg_return.return_bitmap.present_bitmap);
#else
                ASSERT(msg_wc.opcode == IBV_WC_RECV_RDMA_WITH_IMM, "error opcode=%d", msg_wc.opcode);
#endif
                msg_conn_->unlock();
                if (sge_identify_magic(uwr->sg_list[0], uwr->wr.rdma.remote_addr, msg.read_sparce.fault_bitmap)) {
                    ASSERT(false, "magic again!!!");
                }


                lr_rpc.end_one();
            } else {
#if PAGE_BITMAP_UPDATE
                auto [page_begin, page_end] = user_send_wr->remote_mr->get_page_span(msg.read_sparce.s_addr, msg.read_sparce.len);
                // we borrow fault_bitmap, as it is not used now.
                for (uint64_t page = page_begin; page < page_end; page++) {
                    uint64_t i = page - page_begin;
                    util::set_bit(i, msg.read_sparce.fault_bitmap);
                }
                user_send_wr->remote_mr->page_bitmap_assign_batch(msg.read_sparce.s_addr, msg.read_sparce.len,
                                         msg.read_sparce.fault_bitmap);
#endif
            }
        }

        wc->wr_id = user_send_wr_vec_[seq].original_wr_id;
        clear_send_seq(seq);

        return signaled;
    }

    void RcQp::record_rdma_access(const ibv_send_wr *wr)
    {
        static auto &gv = GlobalVariables::instance();

        uint64_t addr = wr->wr.rdma.remote_addr;
        uint32_t len = wr->sg_list[0].length;
        uint32_t mr_key = wr->wr.rdma.rkey;

        CounterForRemoteMr *counter = nullptr;

        auto it = access_counters_.find(mr_key);
        if (it == access_counters_.end()) [[unlikely]] {
            RemoteMr::Identifier remote_mr_id{sqp_->remote_qp_identifier().ctx_addr(), mr_key};
            auto remote_mr = gv.remote_mr_mgr->get_or_reg(remote_mr_id, util::make_observer(this));

            if (remote_mr->is_pin()) return;

            auto counter_uptr = std::make_unique<CounterForRemoteMr>(util::make_observer(this), remote_mr);
            counter = counter_uptr.get();

            pdp::Message msg {
                .opcode = kRegisterCounter,
                .register_counter = {
                    .server_mr_key = mr_key,
                    .counter_mr = *(rdma::RemoteMemoryRegion *)(&counter->vec_mr_)
                }
            };
            access_counters_.emplace(mr_key, std::move(counter_uptr));

            msg_conn_->lock();
            msg_conn_->send(&msg);
            msg_conn_->get();
            msg_conn_->unlock();
        } else {
            counter = it->second.get();
            if (counter->remote_mr()->is_pin()) return;
        }

        auto [unit_id_begin, unit_id_end] = counter->remote_mr()->get_unit_span(addr, len);
        for (uint64_t unit_id = unit_id_begin; unit_id < unit_id_end; unit_id++) {
            counter->vec_[unit_id]++;
        }
    }

    int RcQp::post_send(struct ibv_send_wr *wr, struct ibv_send_wr **bad_wr)
    {
        static const auto &gv = GlobalVariables::instance();
        static util::LatencyRecorder<false> lr_rpc("rpc_in_post");
        // thread_local util::Recorder rec_nr_wr("nr_wr");
        // thread_local util::LatencyRecorder lr_post_send("RcQp::post_send");
        // util::LatencyRecorder::Helper h(lr_post_send); 

        std::unique_lock lk(mutex_);
        int nr_wr = 0;
        for (; wr; wr = wr->next, nr_wr++) {
            ASSERT(wr->num_sge == 1, "wr->num_sge=%d", wr->num_sge);

            uint32_t seq;
            {
                // std::unique_lock lk(mutex_);
                auto seq_opt = find_send_seq();
                ASSERT(seq_opt.has_value(), "post send is full!");

                seq = seq_opt.value();
                set_send_seq(seq);
            }

            // pr_info("qpn=0x%x, opcode=0x%x, wr_id=0x%lx, seq=0x%x, signaled=%u", uqp_->qp_num, wr->opcode, wr->wr_id, seq, wr->send_flags & IBV_SEND_SIGNALED);
            // store user's wr, for g_ori_poll_cq pdp
            auto &user_send_wr = user_send_wr_vec_[seq];
#if PAGE_BITMAP_HINT_READ
            RemoteMr::Identifier remote_mr_id{sqp_->remote_qp_identifier().ctx_addr(), wr->wr.rdma.rkey};
            util::ObserverPtr<RemoteMr> remote_mr = gv.remote_mr_mgr->get_or_reg(
                    remote_mr_id, util::make_observer(this));
            user_send_wr.remote_mr = remote_mr;
            user_send_wr.rdma_read = false;
#endif

            ibv_send_wr original_wr = *wr;

            WrId swr_id(seq, uqp_sq_sig_all_ || (wr->send_flags & IBV_SEND_SIGNALED));

            wr->next = nullptr;
            wr->send_flags |= IBV_SEND_SIGNALED;

            // modify
            if (gv.configs.read_magic_pattern) {
                user_send_wr.wr = *wr;
                user_send_wr.wr.sg_list = user_send_wr.sg.data();
                memcpy(user_send_wr.wr.sg_list, wr->sg_list, sizeof(ibv_sge) * wr->num_sge);
            }

            user_send_wr.original_wr_id = wr->wr_id;
            wr->wr_id = swr_id.u64_value();

            if (gv.configs.promote_hotspot) {
                if (wr->opcode == IBV_WR_RDMA_WRITE || wr->opcode == IBV_WR_RDMA_READ) {
                    record_rdma_access(wr);
                }
            }

            user_send_wr.via_rpc = false;
            if (gv.configs.rpc_mode) {
                if (wr->opcode == IBV_WR_RDMA_WRITE || wr->opcode == IBV_WR_RDMA_READ) {
                    user_send_wr.via_rpc = true;
                    lr_rpc.begin_one();
                    rpc_wr_async(wr);
                    wr->sg_list[0].length = 0;
                }
            } else if (gv.configs.predict_remote_mr) {
                if (wr->opcode == IBV_WR_RDMA_WRITE || wr->opcode == IBV_WR_RDMA_READ) {
                    // pdp strategy before submit.
                    bool via_rpc = gv.remote_mr_mgr->rdma_should_via_rpc(util::make_observer(this),
                                                                         sqp_->remote_qp_identifier().ctx_addr(), wr);
                    if (via_rpc) {
                        user_send_wr.via_rpc = true;
                        lr_rpc.begin_one();
                        rpc_wr_async(wr);
                        wr->sg_list[0].length = 0;
                    }
                }
            } else if (wr->opcode == IBV_WR_RDMA_WRITE) {
                user_send_wr.via_rpc = true;
                lr_rpc.begin_one();
                rpc_wr_async(wr);
                wr->sg_list[0].length = 0;
            }

#if PAGE_BITMAP_HINT_READ
            // pr_info();
            if (gv.configs.read_magic_pattern && wr->opcode == IBV_WR_RDMA_READ && !user_send_wr.via_rpc) {
                uint32_t length = wr->sg_list[0].length;
                uint64_t remote_addr_begin = wr->wr.rdma.remote_addr;
                uint64_t remote_addr_end = remote_addr_begin + length;
                uint64_t local_addr_begin = wr->sg_list[0].addr;
                uint32_t lkey = wr->sg_list[0].lkey;
                uint32_t rkey = wr->wr.rdma.rkey;

                memset((void *)local_addr_begin, MAGIC_BYTE, length); // set it to the magic
                static util::Recorder rec_request_pages("request_pages");
                static util::Recorder rec_read_pages("read_pages");
                auto [page_begin, page_end] = remote_mr->get_page_span(remote_addr_begin, length);
                for (uint64_t page = page_begin; page < page_end; page++) {
                    rec_request_pages.record_one(1);
                    user_send_wr.rdma_read = false;
                    if (!remote_mr->page_bitmap_test_present(page)) continue;
                    user_send_wr.rdma_read = true;
                    rec_read_pages.record_one(1);
                    // the addr span in the page
                    auto [page_addr_begin, page_addr_end] = remote_mr->get_addr_span_of_page(page);

                    // the addr span of this iter
                    uint64_t iter_remote_addr_begin = std::max(page_addr_begin, remote_addr_begin);
                    uint64_t iter_remote_addr_end = std::min(page_addr_end, remote_addr_end);
                    uint64_t iter_local_addr_begin = iter_remote_addr_begin - remote_addr_begin + local_addr_begin;
                    uint32_t iter_length = iter_remote_addr_end - iter_remote_addr_begin;

                    auto iter_wr = rdma::Wr().set_op_read().set_signaled(false)
                            .set_sg(iter_local_addr_begin, iter_length, lkey)
                            .set_rdma(iter_remote_addr_begin, rkey);
                    struct ibv_send_wr *iter_bad_wr;
                    // pr_info("qpn=0x%x, opcode=0x%x, wr_id=0x%lx, signaled=%u", uqp_->qp_num, iter_wr.native_wr()->opcode, 
                                // iter_wr.native_wr()->wr_id, iter_wr.native_wr()->send_flags & IBV_SEND_SIGNALED);
                    int ret = gv.ori_ibv_symbols.post_send(uqp_, iter_wr.native_wr(), &iter_bad_wr);
                    ASSERT(ret == 0, "failed to ibv_post_send: %s", strerror(errno));
                }
                // no need to read
                wr->sg_list[0].length = 0;
            }
#endif
            // post
            // pr_info("qpn=0x%x, opcode=0x%x, wr_id=0x%lx, signaled=%u", uqp_->qp_num, wr->opcode, wr->wr_id, wr->send_flags & IBV_SEND_SIGNALED);
            int ret = gv.ori_ibv_symbols.post_send(uqp_, wr, bad_wr); // 0 on success, errno on failure
            ASSERT(ret == 0, "fail to ibv_post_send: %s", strerror(errno));
            *wr = original_wr;

            if (user_send_wr.via_rpc) {
                Message msg_return = rpc_wr_wait(wr);
#if PAGE_BITMAP_UPDATE
                ASSERT(msg_return.opcode == kReturnBitmap, "msg_return.opcode != kReturnBitmap");
                auto [page_begin, page_end] = remote_mr->get_page_span(wr->wr.rdma.remote_addr, wr->sg_list[0].length);
                remote_mr->page_bitmap_assign_batch(
                        wr->wr.rdma.remote_addr, wr->sg_list[0].length, msg_return.return_bitmap.present_bitmap);
#endif
                lr_rpc.end_one();
            }
            // restore
        }

        // rec_nr_wr.record_one(nr_wr);

        return 0;
    }

    void QpManager::do_in_reg(typename Map::iterator it)
    {
        static auto &gv = GlobalVariables::instance();
        if (!gv.configs.is_server) {
            return;
        }
        RcQp *rc_qp = it->second.get();

        if (rpc_qps_for_threads_.size() <= rpc_server_thread_id_current_) {
            rpc_qps_for_threads_.resize(rpc_server_thread_id_current_ + 1);
        }
        if (rpc_server_threads_.size() <= rpc_server_thread_id_current_) {
            rpc_server_threads_.resize(rpc_server_thread_id_current_ + 1);
        }

        std::vector<pdp::RcQp *> *rpc_qps_for_thread = &rpc_qps_for_threads_[rpc_server_thread_id_current_];

        rpc_qps_for_thread->push_back(rc_qp);
#if MAX_NR_COROUTINES
        ASSERT(rpc_qps_for_thread->size() <= MAX_NR_COROUTINES, "%lu is more than MAX_NR_COROUTINES(%u)!", rpc_qps_for_thread->size(), MAX_NR_COROUTINES);
#endif

        if (rpc_server_threads_[rpc_server_thread_id_current_] == nullptr) {
            rpc_server_threads_[rpc_server_thread_id_current_] = std::make_unique<std::jthread>(
                    [this, rpc_thread_id = rpc_server_thread_id_current_] (const std::stop_token& stop_token) {
#if MAX_NR_COROUTINES
                        thread_local std::vector<coroutine<void>::pull_type> coroutine_vec;
                        for (int coro_id = 0; coro_id < MAX_NR_COROUTINES; coro_id++) {
                            coroutine_vec.emplace_back(
                                [this, &stop_token, rpc_thread_id, coro_id](coroutine<void>::push_type &sink) {
                                    rpc_worker_func(stop_token, rpc_thread_id, sink, coro_id);
                                }
                            );
                        }
                        while (!stop_token.stop_requested()) {
                            int nr_qps;
                            {
                                std::shared_lock lk(lock_);
                                nr_qps = rpc_qps_for_threads_[rpc_thread_id].size();
                            }
                            for (int coro_id = 0; coro_id < nr_qps; coro_id++) {
                                // pr_info("switch to coro_id=%d", coro_id);
                                coroutine_vec[coro_id]();
                            }
                        }
#else
                        coroutine<void>::push_type sink; // just a placeholder
                        rpc_worker_func(stop_token, id, sink, 0);
#endif
                    }
            );
        }

        rpc_server_thread_id_current_++;
        if (gv.configs.server_rpc_threads) {
            rpc_server_thread_id_current_ %= gv.configs.server_rpc_threads;
        }

    }

    void QpManager::rpc_worker_func(const std::stop_token &stop_token, int rpc_server_thread_id, coroutine<void>::push_type& sink, int coro_id)
    {
        static auto &gv = GlobalVariables::instance();
        static util::LatencyRecorder lr_rpc_all("rpc_all");
        static util::LatencyRecorder lr_rpc_get("rpc_get");

        util::Schedule::bind_server_bg_cpu(2 + rpc_server_thread_id);

#if MAX_NR_COROUTINES == 0
        size_t cur_qp_id = 0;
#endif

        while (!stop_token.stop_requested()) {
            pdp::RcQp *prc;
#if MAX_NR_COROUTINES
            while (true) {
                lock_.lock_shared();

                if (coro_id >= rpc_qps_for_threads_[rpc_server_thread_id].size()) {
                    lock_.unlock_shared();
                    // pr_info("coro_id=%d, vec_size=%lu", coro_id, rpc_qps_for_threads_[rpc_server_thread_id].size());
                    sink();
                } else {
                    prc = rpc_qps_for_threads_[rpc_server_thread_id][coro_id];
                    lock_.unlock_shared();
                    break;
                }
            }
            while (!prc->msg_conn()) {
                sink();
            }
#else
            {
                std::shared_lock lk(lock_);

                prc = rpc_qps_for_threads_[rpc_server_thread_id][cur_qp_id];
                cur_qp_id = (cur_qp_id + 1) % rpc_qps_for_threads_[rpc_server_thread_id].size();
            }
            if (!prc->msg_conn()) continue;
#endif


            const auto &msg_conn = prc->msg_conn();
            pdp::Message msg_req{};
            const auto &bounce_mr = msg_conn->rpc_io()->bounce_mr();
#if MAX_NR_COROUTINES
            while (!msg_conn->try_get(&msg_req, bounce_mr->addr())) {
                sink();
            }
#else
            if (!msg_conn->try_get(&msg_req, bounce_mr->addr())) continue;
#endif
            lr_rpc_get.end_one();

            // pr_coro(info, "req msg=%s", msg_req.to_string().c_str());
            lr_rpc_all.begin_one();
            switch (msg_req.opcode) {
                case RpcOpcode::kReadSingle: {
                    static util::LatencyRecorder lr_read_single("read_single_all");
                    static util::LatencyRecorder lr_io("read_single_io");
                    static util::LatencyRecorder lr_response("read_single_response");
                    lr_read_single.begin_one();
                    const ReadSingle &req = msg_req.read_single;

                    LocalMr *mr = gv.local_mr_mgr->get(req.s_lkey).get();
                    ASSERT(mr, "failed to get local_mr=0x%x", req.s_lkey);

                    lr_io.begin_one();
                    auto rpc_io = msg_conn->rpc_io();

                    uint64_t rw_offset = req.s_addr - mr->user_mr()->addr64();
                    uint64_t mr_offset_of_bounce = util::round_down(rw_offset, PAGE_SIZE);

                    std::span<std::tuple<uint64_t, uint32_t>> access_span
                            = rpc_io->io(rw_offset, req.len,
                                         nullptr, false, mr, sink, coro_id);
                    lr_io.end_one();
                    lr_response.begin_one();
                    for (int i = 0; i < access_span.size(); i++) {
                        auto &it = access_span[i];

                        auto wr = rdma::Wr().set_op_write().set_signaled(false)
                                .set_sg(rpc_io->bounce_mr()->pick_by_offset(std::get<0>(it) - mr_offset_of_bounce, std::get<1>(it)))
                                .set_rdma(req.c_addr + (std::get<0>(access_span[i]) - rw_offset), req.c_rkey);

                        if (!req.update_bitmap && i == access_span.size() - 1) {
                            wr.set_op_write_imm(0); // no wait. QD = 1 on the client.
                        }

                        msg_conn->post_send_wr_auto_wait(&wr);
                    }
                    if (req.update_bitmap) {
                        Message *msg_resp = msg_conn->send_mr_ptr();
                        msg_resp->opcode = kReturnBitmap;
                        mr->read_rnic_bitmap(req.s_addr, req.len,
                                             msg_resp->return_bitmap.present_bitmap);
                        msg_conn->send(msg_resp);
                    }

                    lr_response.end_one();

                    lr_read_single.end_one();
                    break;
                }
                case RpcOpcode::kWriteSingle: {
                    static util::LatencyRecorder lr_write_single("write_single_all");
                    static util::LatencyRecorder lr_fetch_data("write_single_fetch");
                    static util::LatencyRecorder lr_io("write_single_io");
                    static util::LatencyRecorder lr_response("write_single_response");
                    const WriteSingle &req = msg_req.write_single;

                    if (!gv.has_write_req) {
                        gv.has_write_req = true;
                    }

                    lr_write_single.begin_one();
                    lr_fetch_data.begin_one();

                    ASSERT(req.len <= bounce_mr->length_ && req.len > 0, "");

                    auto mr = gv.local_mr_mgr->get(req.s_lkey);
                    ASSERT(mr, "unknown mr");

                    u64 bounce_saddr = util::round_down(req.s_addr, PAGE_SIZE);

                    auto wr = rdma::Wr().set_op_read()
                            .set_rdma(req.c_addr, req.c_rkey)
                            .set_sg(bounce_mr->pick_by_offset(req.s_addr - bounce_saddr, req.len))
                            .set_signaled(true);
                    msg_conn->post_send_wr_auto_wait(&wr); // auto wait
                    lr_fetch_data.end_one();

                    lr_io.begin_one();
                    u64 rw_offset = req.s_addr - mr->user_mr()->addr64();

                    msg_conn->rpc_io()->io(rw_offset, req.len, nullptr, true, mr.get(), sink, coro_id);
                    lr_io.end_one();
                    lr_response.begin_one();

                    Message *msg_resp = msg_conn->send_mr_ptr();
                    if (req.update_bitmap) {
                        msg_resp->opcode = kReturnBitmap;
                        mr->read_rnic_bitmap(req.s_addr, req.len, msg_resp->return_bitmap.present_bitmap);
                    } else {
                        msg_resp->opcode = kReturnStatus;
                    }
                    msg_conn->send(msg_resp);

                    lr_response.end_one();
                    lr_write_single.end_one();
                    break;
                }
                case RpcOpcode::kWriteInline: {
                    static util::LatencyRecorder lr_write_inline("write_inline_all");
                    static util::LatencyRecorder lr_io("write_inline_io");
                    static util::LatencyRecorder lr_response("write_inline_response");
                    const WriteInline &req = msg_req.write_inline;

                    if (!gv.has_write_req) {
                        gv.has_write_req = true;
                    }

                    lr_write_inline.begin_one();
                    lr_io.begin_one();
                    LocalMr *mr = gv.local_mr_mgr->get(req.s_lkey).get();
                    ASSERT(mr, "");

                    u64 rw_offset = req.s_addr - mr->user_mr()->addr64();

                    msg_conn->rpc_io()->io(rw_offset, req.len, nullptr, true, mr, sink, coro_id);

                    lr_io.end_one();
                    lr_response.begin_one();

                    Message *msg_resp = msg_conn->send_mr_ptr();
                    if (req.update_bitmap) {
                        msg_resp->opcode = kReturnBitmap;
                        mr->read_rnic_bitmap(req.s_addr, req.len, msg_resp->return_bitmap.present_bitmap);
                    } else {
                        msg_resp->opcode = kReturnStatus;
                    }
                    msg_conn->send(msg_resp);

                    lr_response.end_one();
                    lr_write_inline.end_one();
                    break;
                }
                case RpcOpcode::kReadSparce: {
                    static util::LatencyRecorder lr_read_sparce("read_sparce_all");
                    static util::LatencyRecorder lr_io("read_sparce_io");
                    static util::LatencyRecorder lr_response("read_sparce_response");
                    const ReadSparce &req = msg_req.read_sparce;

                    lr_read_sparce.begin_one();
                    lr_io.begin_one();
                    LocalMr *mr = gv.local_mr_mgr->get(req.s_lkey).get();
                    ASSERT(mr, "failed to get local_mr=0x%x", req.s_lkey);

                    auto rpc_io = msg_conn->rpc_io();

                    uint64_t rw_offset = req.s_addr - mr->user_mr()->addr64();
                    uint64_t mr_offset_of_bounce = util::round_down(rw_offset, PAGE_SIZE);

                    std::span<std::tuple<uint64_t, uint32_t>> access_span
                        = rpc_io->io(rw_offset, req.len,
                                     req.fault_bitmap, false, mr, sink, coro_id);
                    lr_io.end_one();
                    lr_response.begin_one();
                    for (int i = 0; i < access_span.size(); i++) {
                        auto &it = access_span[i];

                        auto wr = rdma::Wr().set_op_write().set_signaled(false)
                            .set_sg(rpc_io->bounce_mr()->pick_by_offset(std::get<0>(it) - mr_offset_of_bounce, std::get<1>(it)))
                            .set_rdma(req.c_addr + (std::get<0>(access_span[i]) - rw_offset), req.c_rkey);

                        if (!req.update_bitmap && i == access_span.size() - 1) {
                            wr.set_op_write_imm(0); // no wait. client-side QD = 1
                        }

                        msg_conn->post_send_wr_auto_wait(&wr);
                    }
                    if (req.update_bitmap) {
                        Message *msg_resp = msg_conn->send_mr_ptr();
                        msg_resp->opcode = kReturnBitmap;
                        mr->read_rnic_bitmap(req.s_addr, req.len,
                                             msg_resp->return_bitmap.present_bitmap);
                        msg_conn->send(msg_resp);
                    }
                    lr_response.end_one();
                    lr_read_sparce.end_one();

                    break;
                }
                case RpcOpcode::kRegisterCounter: {
                    const RegisterCounter &req = msg_req.register_counter;
                    auto mr = gv.local_mr_mgr->get(req.server_mr_key);
                    mr->register_counter(prc, req.counter_mr);

                    pdp::Message resp_msg {
                            .opcode = kReturnStatus
                    };

                    msg_conn->send(&resp_msg);

                    break;
                }
                default:
                    printf("unknown rpc\n");
            }
            lr_rpc_all.end_one();
            // pr_coro(info, "done msg=%s", msg_req.to_string().c_str());
            lr_rpc_get.begin_one();

        }
    }
}
