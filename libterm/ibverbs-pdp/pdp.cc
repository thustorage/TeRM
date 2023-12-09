#include "mr.hh"
#include "qp.hh"
// c++ headers
#include <cassert>

// system headers
#include <infiniband/verbs.h>
#include <dlfcn.h>

// my headers
#include <logging.hh>
#include <util.hh>

namespace pdp
{
    struct ibv_mr *ori_reg_mr(struct ibv_pd *pd, void *addr, size_t length,
                              int access)
    {
        const static auto &gv = GlobalVariables::instance();
        assert(gv.ori_ibv_symbols.reg_mr_iova2);
        return gv.ori_ibv_symbols.reg_mr_iova2(pd, addr, length, (uintptr_t)addr, access);
    }
    
    /**
     * postprocess
     * All are the same as ibv_poll_cq() and only returns ibv_wc with IBV_SEND_SIGNALED set by the user.
     * Be careful that the CQ may not be empty when return < num_entries.
     */
    int poll_cq(struct ibv_cq *cq, int num_entries, struct ibv_wc *wc)
    {
        const static auto &gv = GlobalVariables::instance();
        assert(gv.ori_ibv_symbols.poll_cq);
//        thread_local util::Recorder rec_poll("poll_nr_entries");

        int ret = gv.ori_ibv_symbols.poll_cq(cq, num_entries, wc);
        if (ret <= 0) {
            return ret;
        }

        if (gv.configs.is_server) return ret;

        if (gv.configs.mode != +Mode::PDP) {
//            rec_poll.record_one(ret);
            return ret;
        }

        if (wc->opcode & IBV_WC_RECV) [[unlikely]] {
            // we do not modify post_recv
            return ret;
        }

#if 1
        // pr_info("poll ret=%d.", ret);
        int nr_user_entries = 0;
        for (int i = 0; i < ret; i++) {
            auto prc = gv.qp_mgr->get(wc[i].qp_num);

            if (!prc) [[unlikely]] {
                wc[nr_user_entries++] = wc[i];
            } else {
                bool signaled = prc->post_process_wc(wc);
                if (signaled) {
                    wc[nr_user_entries++] = wc[i];
                }
//                pr_debug("[pdp] a pdp wc done, signaled=%u, user_num_entries=%d", signaled, user_num_entries);
            }
        }
#else
        bool is_prc = gv.qp_mgr->is_managed_qp(wc[i].qp_num);
        if (!is_prc) [[unlikely]] {
            wc[user_num_entries++] = wc[i];
        } else {
            auto *user_send_wr = reinterpret_cast<RcQp::UserSendWr *>(wc->wr_id);
            bool signaled = user_send_wr->prc->post_process_wc(&wc[i]);
            if (signaled) {
                wc[user_num_entries++] = wc[i];
            }
        }
#endif

        return nr_user_entries;
    }

    // preprocess wr
    int post_send(struct ibv_qp *qp, struct ibv_send_wr *wr,
                          struct ibv_send_wr **bad_wr)
    {
        const static auto &gv = GlobalVariables::instance();
        static util::LatencyRecorder lr_post_send{"post_send"};
        util::LatencyRecorderHelper h{lr_post_send};
        // static util::LatencyRecorder lr_1("1");
        // pr_info("op=%u", wr->opcode);
        // lr_1.begin_one();
        if (gv.configs.mode != +Mode::PDP) [[unlikely]] {
            return gv.ori_ibv_symbols.post_send(qp, wr, bad_wr);
        }

        // !prc: a shadow RCQP or a user UDQP
        const auto &prc = gv.qp_mgr->get(qp->qp_num);
        bool is_shadow_rc = !prc && qp->qp_type == IBV_QPT_RC;
        if (is_shadow_rc) [[unlikely]] {
            return gv.ori_ibv_symbols.post_send(qp, wr, bad_wr);
        }

        // check for all UD and User RC.
        // do not check for Shadow RC.
        if (gv.configs.is_server) {
            gv.local_mr_mgr->convert_pdp_mr_in_send_wr_chain(wr);
        }

        if (gv.configs.advise_local_mr) {
            gv.local_mr_mgr->advise_mr_in_wr_chain(wr);
        }

        // this is UD.
        if (!prc || gv.configs.is_server) [[unlikely]] {
//            gv.local_mr_mgr->pin_mr_in_recv_wr_chain((ibv_recv_wr *)wr);
//            gv.local_mr_mgr->advise_mr_in_wr_chain(wr);

            return gv.ori_ibv_symbols.post_send(qp, wr, bad_wr);
        }
        // lr_1.end_one();
        return prc->post_send(wr, bad_wr);
    }

    int post_recv(struct ibv_qp *qp, struct ibv_recv_wr *wr,
            struct ibv_recv_wr **bad_wr)
    {
        const static auto &gv = GlobalVariables::instance();
        assert(gv.ori_ibv_symbols.post_recv);
        // thread_local util::LatencyRecorder lr_recv{"post_recv"};
        // util::LatencyRecorder::Helper h{lr_recv};

        if (gv.configs.mode == +Mode::PIN) {
            return gv.ori_ibv_symbols.post_recv(qp, wr, bad_wr);
        }

        if (qp->qp_type == IBV_QPT_UD) {
            // RECV: the remote writes to us. we pin mrs for UD so that the NIC does not drop the package.
            gv.local_mr_mgr->pin_mr_in_recv_wr_chain(wr);
            if (gv.configs.mode == +Mode::ODP) {
                return gv.ori_ibv_symbols.post_recv(qp, wr, bad_wr);
            }
        }

        if (gv.configs.advise_local_mr) {
            gv.local_mr_mgr->advise_mr_in_wr_chain(wr);
        }

        return gv.ori_ibv_symbols.post_recv(qp, wr, bad_wr);
    }

    struct ibv_qp *create_qp(struct ibv_pd *pd,
                                     struct ibv_qp_init_attr *qp_init_attr)
    {
        const static auto ori_ibv_create_qp = (create_qp_t)dlvsym(RTLD_NEXT, "ibv_create_qp", "IBVERBS_1.1");
        // static auto ori_ibv_create_qp = (create_qp_t)dlsym(RTLD_NEXT, "ibv_create_qp");
        static auto &gv = GlobalVariables::instance();
        static bool inited = false;

        if (!inited) {
            gv.ori_ibv_symbols.create_qp = ori_ibv_create_qp;
            gv.ori_ibv_symbols.post_send = pd->context->ops.post_send;
            gv.ori_ibv_symbols.post_recv = pd->context->ops.post_recv;
            gv.ori_ibv_symbols.poll_cq = pd->context->ops.poll_cq;
            inited = true;
        }

        assert(ori_ibv_create_qp);

        struct ibv_qp *uqp = ori_ibv_create_qp(pd, qp_init_attr); // man page: NULL if fails
        if (!uqp) return uqp; // fails
        
        assert(uqp->context == pd->context);
        if (gv.configs.mode == +Mode::PIN) return uqp;

        // a work-around patch for UD post_recv with ODP/PDP mrs.
        // Using a UD QP, a post_recv of ODP/PDP mr will drop a package directly during processing a page fault.
        // But many prototype systems use UD for RPC without considering package drops.
        // We pin memory in post_recv, so that the application can run.
        // Note! This works for ODP & PDP!
        uqp->context->ops.post_recv = post_recv;

        if (!gv.configs.mode == +Mode::PDP) return uqp;

        // we override post_send of all QPs, including UD, to check the mr.
        // Note! This works only for PDP!
        uqp->context->ops.poll_cq = poll_cq;
        uqp->context->ops.post_send = post_send;

        if (qp_init_attr->qp_type != IBV_QPT_RC) {
//            pr_info("create qp: type=0x%x, num=0x%x", uqp->qp_type, uqp->qp_num);
            return uqp;
        }

        // but we only create uqp for RC, to support READ/WRITE
        auto ctx = gv.ctx_mgr->get_or_reg_by_pd(uqp->pd);
        auto prc = gv.qp_mgr->reg_with_construct_args(uqp->qp_num, ctx, uqp, qp_init_attr);

        RcBytes rc_bytes = prc->to_rc_bytes();

        // w/ gid
        gv.sms.put(rc_bytes.identifier().to_string(), rc_bytes.to_vec_char());

        // w/o gid
        gv.sms.put(rc_bytes.identifier().wo_gid().to_string(), rc_bytes.to_vec_char());

        return uqp;
    }

    // postprocess. put uqp_addr, sqp_.
    int modify_qp(struct ibv_qp *qp, struct ibv_qp_attr *attr,
                          int attr_mask)
    {
        const static auto ori_ibv_modify_qp = (modify_qp_t)dlvsym(RTLD_NEXT, "ibv_modify_qp", "IBVERBS_1.1");
        assert(ori_ibv_modify_qp != nullptr);

        static auto &gv = GlobalVariables::instance();
        static bool inited = false;
        if (!inited) {
            gv.ori_ibv_symbols.modify_qp = ori_ibv_modify_qp;
            inited = true;
        }

        int ret = ori_ibv_modify_qp(qp, attr, attr_mask);
        if (!gv.configs.mode == +Mode::PDP) {
            return ret;
        }
        // man page: 0 on success, errno on failure
        if (ret) {
            return ret;
        }

        // change state?
        if ((attr_mask & IBV_QP_STATE) == 0) {
            return ret;
        }

        const auto &prc = gv.qp_mgr->get(qp->qp_num);
        if (!prc) return ret;

        // init -> recv -> send
        switch (attr->qp_state) {
            case IBV_QPS_INIT: {
                assert(attr->port_num == 1);
            }
            break;
            case IBV_QPS_RTR: {
                rdma::QueuePair::Identifier remote_uqp_addr{{attr->ah_attr.grh.dgid, attr->ah_attr.dlid}, attr->dest_qp_num};
                std::vector<char> vec_char;
                if (attr->ah_attr.is_global) {
                    vec_char = gv.sms.get(remote_uqp_addr.to_string());
                } else {
                    vec_char = gv.sms.get(remote_uqp_addr.wo_gid().to_string());
                }
                RcBytes remote_mr_bytes{vec_char};
                bool ok = prc->connect_to(remote_mr_bytes);
                assert(ok);
            }
            break;
            default: break;
        }

        return ret;
    }

    struct ibv_mr *reg_mr_iova2(struct ibv_pd *pd, void *addr, size_t length,
				uint64_t iova, unsigned int access)
    {
        const static auto ori_ibv_reg_mr_iova2 = (reg_mr_iova2_t)dlsym(RTLD_NEXT, "ibv_reg_mr_iova2");
        assert(ori_ibv_reg_mr_iova2);
        static auto &gv = GlobalVariables::instance();
        static bool inited = false;

        if (!inited) {
            gv.ori_ibv_symbols.reg_mr_iova2 = ori_ibv_reg_mr_iova2;
            gv.ori_ibv_symbols.reg_mr = ori_reg_mr;
            inited = true;
        }

        bool user_odp = access & IBV_ACCESS_ON_DEMAND;
        if (user_odp) {
            pr_info("user_odp!");
        }

        if (user_odp && gv.configs.mode == +Mode::PIN) {
            pr_warn("!!! ATTENTION !!! ODP mr is requested but pdp_mode is pin");
            access &= (~IBV_ACCESS_ON_DEMAND);
            user_odp = false;
        }

        struct ibv_mr *native_mr;
        for (int i = 0; i < 10; i++) {
            native_mr = gv.ori_ibv_symbols.reg_mr_iova2(pd, addr, length, iova, access);
            if (native_mr) break;
        }
        if (!native_mr) {
            pr_err("reg_mr: failed, addr=%p, len=0x%lx, error=%s!", addr, length, strerror(errno));
            return native_mr;
        }

        auto ctx = gv.ctx_mgr->get_or_reg_by_pd(pd);
        auto rdma_mr = std::make_unique<rdma::LocalMemoryRegion>(ctx, native_mr);

        // PIN
        if (!user_odp) {
//            pr_info("%s", fmt::format("reg_mr: pin,addr={},len={:#x},lkey={:#x},pd={}", addr, length, \
//                native_mr ? native_mr->lkey : -1, fmt::ptr(pd)).c_str());

            // if (!gv.configs.mode == +Mode::PIN) {
            gv.local_mr_mgr->reg_with_construct_args(rdma_mr->lkey_, std::move(rdma_mr));
            // }

            return native_mr;
        }

        // ODP
        if (gv.configs.mode == +Mode::ODP || !gv.configs.read_magic_pattern) {
            pr_info("reg_mr: odp, addr=%p, len=0x%lx, lkey=0x%x", addr, length,
                    native_mr ? native_mr->lkey : -1);
            
            gv.local_mr_mgr->reg_with_construct_args(rdma_mr->lkey_, std::move(rdma_mr), nullptr);
            pr_info();
            return native_mr;
        }

        // PDP
        // user_odp && gv.configs.mode.is_pdp

        assert((access & PDP_ACCESS_PDP) == 0);
        ASSERT((uint64_t)addr % PAGE_SIZE == 0, "addr must be aligned to 4KB");

        // we create a PDP native_mr for each ODP native_mr, and return the PDP one
        // only the ODP native_mr is registered!
        struct ibv_mr *pdp_native_mr;
        for (int i = 0; i < 10; i++) {
            pdp_native_mr = gv.ori_ibv_symbols.reg_mr_iova2(pd, addr, length, iova, access | PDP_ACCESS_PDP);
            if (pdp_native_mr) break;
            if (i) pr_info("retry %d, error=%s", i, strerror(errno));
        }
        assert(pdp_native_mr);

        // key-value
        // key: lkey of pdp_native_mr
        auto pdp_rdma_mr = std::make_unique<rdma::LocalMemoryRegion>(ctx, pdp_native_mr);
        pr_info();
        gv.local_mr_mgr->reg_with_construct_args(pdp_rdma_mr->lkey_, std::move(rdma_mr), std::move(pdp_rdma_mr));

        pr_info("reg_mr: pdp, addr=%p, len=0x%lx, pdp_lkey=0x%x, internal_odp_key=0x%x", addr, length,
                pdp_native_mr->lkey, native_mr->lkey);

        // return pdp_native_mr, so we can RDMA read!
        return pdp_native_mr;
    }
}

struct ibv_qp *ibv_create_qp(struct ibv_pd *pd,
                                    struct ibv_qp_init_attr *qp_init_attr)
{
    return pdp::create_qp(pd, qp_init_attr);
}

int ibv_modify_qp(struct ibv_qp *qp, struct ibv_qp_attr *attr,
                        int attr_mask)
{
    return pdp::modify_qp(qp, attr, attr_mask);
}

struct ibv_mr *ibv_reg_mr_iova2(struct ibv_pd *pd, void *addr, size_t length,
				uint64_t iova, unsigned int access)
{
    return pdp::reg_mr_iova2(pd, addr, length, iova, access);
}

#ifdef ibv_reg_mr
#undef ibv_reg_mr
#endif
struct ibv_mr *ibv_reg_mr(struct ibv_pd *pd, void *addr, size_t length,
			  int access)
{
    return pdp::reg_mr_iova2(pd, addr, length, (uintptr_t)addr, access);
}
