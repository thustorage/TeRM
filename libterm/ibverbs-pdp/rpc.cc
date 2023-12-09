#include "rpc.hh"
#include "qp.hh"
#include "mr.hh"
#include <libaio.h>

namespace pdp {
    // class RpcIo
    RpcIo::RpcIo(util::ObserverPtr<rdma::Context> ctx)
    {
        static const auto &gv = GlobalVariables::instance();

        rdma::Allocation bounce_alloc{kBounceMrSize};
        bounce_mr_ = std::make_unique<rdma::LocalMemoryRegion>(ctx, std::move(bounce_alloc), false, gv.ori_ibv_symbols.reg_mr);

        std::string dev_path = "/dev/" + gv.configs.server_mmap_dev;

        nvme_fd_buffer_ = open(dev_path.c_str(), O_RDWR);
        ASSERT(nvme_fd_buffer_ >= 0, "fail to open nvme_fd_buffer_=%d: %s", nvme_fd_buffer_, strerror(errno));
        posix_fadvise(nvme_fd_buffer_, 0, 64ul << 30, POSIX_FADV_RANDOM);

        nvme_fd_direct_ = open(dev_path.c_str(), O_RDWR | O_DIRECT);
        ASSERT(nvme_fd_direct_ >= 0, "nvme_fd_buffer_=%d: %s", nvme_fd_direct_, strerror(errno));
        posix_fadvise(nvme_fd_direct_, 0, 64ul << 30, POSIX_FADV_RANDOM);

        pch_fd_ = open("/proc/pch", O_RDWR);
        ASSERT(pch_fd_ >= 0, "fail to open pch:%s", strerror(errno));
        int ret = write(pch_fd_, dev_path.c_str(), dev_path.length());
        ASSERT(ret == dev_path.length(), "fail to set mmap file: %d", ret);

//        for (int i = 0; i < 3; i++) {
//            ret = io_uring_queue_init(kQueueDepth, &ring_, 0);
//            if (!ret) break;
//        }
//        ASSERT(ret == 0, "error in queue_init: %s\n", strerror(errno));

        io_req_vec_.resize(kQueueDepth);
        access_vec_.resize(kQueueDepth);

//        iovec vec = {
//                .iov_base = bounce_mr_->addr(),
//                .iov_len = bounce_mr_->length()
//        };
//        for (int i = 0; i < 3; i++) {
//            ret = io_uring_register_buffers(&ring_, &vec, 1);
//            if (!ret) break;
//        }
//        ASSERT(ret == 0, "error in register_buffers: %s\n", strerror(errno));
    }

#if 0
    void RpcIo::do_io_by_uring(int nr_io_reqs, bool write)
    {
        int ret;
        bool iou_submit = false;

        pr_once(info, "nr_io_reqs=%d", nr_io_reqs);
        for (int i = 0; i < nr_io_reqs; i++) {
            fmt_pr(info, "req[{}]: {}", i, io_req_vec_[i].to_string());
        }
        ASSERT(false, "");

        // prepare
        for (int i = 0; i < nr_io_reqs; i++) {
            auto &r = io_req_vec_[i];

            // if (!r.direct) {
            //     if (write) {
            //         memcpy((char *)mr_base_addr_ + r.offset, r.buffer, r.length);
            //     } else {
            //         memcpy(r.buffer, (char *)mr_base_addr_ + r.offset, r.length);
            //     }
            //     continue;
            // }

            if (!r.direct && write) {
                ret = pwrite(nvme_fd_buffer_, r.buffer, r.length, r.offset);
                ASSERT(ret == io_req_vec_[i].length, "fail to do io=%s", strerror(errno));
                continue;
            }

            iou_submit = true;

            io_uring_sqe *sqe = io_uring_get_sqe(&ring_);
            ASSERT(sqe, "failed to get sqe");

            auto fd = r.direct ? nvme_fd_direct_ : nvme_fd_buffer_;

            if (write) {
                io_uring_prep_write_fixed(sqe, fd, r.buffer, r.length, r.offset, 0);
            } else {
                io_uring_prep_read_fixed(sqe, fd, r.buffer, r.length, r.offset, 0);
            }

            io_uring_sqe_set_data(sqe, sqe);
        }

        if (!iou_submit) return;

        // submit
        ret = io_uring_submit(&ring_);
        if (ret < 0) {
            ASSERT(false, "error in io_uring_submit=%s", strerror(-ret));
        } else if (ret != nr_io_reqs) {
            ASSERT(false, "io_uring submits only %d requests, expected=%d", ret, nr_io_reqs);
        }

        // wait_cqe
        io_uring_cqe *cqe;
        for (int i = 0; i < nr_io_reqs; i++) {
            ret = io_uring_wait_cqe(&ring_, &cqe);
            ASSERT(ret == 0, "error in wait_cqe=%s", strerror(-ret));

            auto *sqe = (io_uring_sqe *)io_uring_cqe_get_data(cqe);
            ASSERT(cqe->res == sqe->len, "cqe->res=%u, expected=%u", cqe->res, sqe->len);
            io_uring_cqe_seen(&ring_, cqe);
        }
    }
#endif

    void RpcIo::do_io_by_psync(int nr_io_reqs, bool write, coroutine<void>::push_type& sink, int coro_id)
    {
        static auto &gv = GlobalVariables::instance();
#if MAX_NR_COROUTINES
        if (!inited_[coro_id]) {
            int ret = io_setup(1, &io_context_vec_[coro_id]);
            ASSERT(ret == 0, "error in io_setup: %s", strerror(errno));
            inited_[coro_id] = true;
        }

        io_context_t &aio_ctx = io_context_vec_[coro_id];

        struct iocb aio_req;
        struct io_event aio_event;
#endif
        for (int i = 0; i < nr_io_reqs; i++) {
            int ret;
            const auto &r = io_req_vec_[i];

            // pr_info("io_req_vec[%d]: %s", i, r.to_string().c_str());
            // pr_info("buffer content=0x%lx", *(uint64_t *)r.buffer);
#if MAX_NR_COROUTINES
            if (r.direct) {
                if (write) {
                    io_prep_pwrite(&aio_req, nvme_fd_direct_, r.buffer, r.length, r.offset);
                } else {
                    io_prep_pread(&aio_req, nvme_fd_direct_, r.buffer, r.length, r.offset);
                }
                iocb *aio_req_p = &aio_req;
                if (io_submit(aio_ctx, 1, &aio_req_p) != 1) {
                    ASSERT(false, "failed to io_submit.");
                }

                sink();
                while (true) {
                    int nr_events = io_getevents(aio_ctx, 0, 1, &aio_event, nullptr);
                    if (nr_events == 0) {
                        // pr_info("sink from coro_id=%d", coro_id);

                        sink();
                    } else {
                        break;
                    }
                }
                continue;
            }
#endif
            int fd = r.direct ? nvme_fd_direct_ : nvme_fd_buffer_;
            if (write) {
                ret = pwrite(fd, r.buffer, r.length, r.offset);
            } else {
                ret = pread(fd, r.buffer, r.length, r.offset);
            }

            ASSERT(ret == r.length, "fail to do io=%s, offset=0x%lx, length=0x%x, direct=%u",
                   strerror(errno), r.offset, r.length, r.direct);
        }
    }

    void RpcIo::subsector_write(void *buffer, u32 length, u64 offset, coroutine<void>::push_type& sink, int coro_id)
    {
        static auto &gv = GlobalVariables::instance();
#if MAX_NR_COROUTINES
        if (!inited_[coro_id]) {
            int ret = io_setup(1, &io_context_vec_[coro_id]);
            ASSERT(ret == 0, "error in io_setup: %s", strerror(errno));
            inited_[coro_id] = true;
        }
        io_context_t &aio_ctx = io_context_vec_[coro_id];

        struct iocb aio_req;
        struct io_event aio_event;
        {
            io_prep_pread(&aio_req, nvme_fd_direct_, direct_sector_buffer(), SECTOR_SIZE, util::round_down(offset, SECTOR_SIZE));
            iocb *aio_req_p = &aio_req;
            if (io_submit(aio_ctx, 1, &aio_req_p) != 1) {
                ASSERT(false, "failed to io_submit.");
            }
            sink();
            while (true) {
                int nr_events = io_getevents(aio_ctx, 0, 1, &aio_event, nullptr);
                if (nr_events == 0) {
                    sink();
                } else {
                    break;
                }
            }
        }
#else
        ssize_t ret = pread(nvme_fd_direct_, direct_sector_buffer(), SECTOR_SIZE, util::round_down(offset, SECTOR_SIZE));
        ASSERT(ret == SECTOR_SIZE, "fail to read: %s", strerror(errno));
#endif
        memcpy(direct_sector_buffer() + (offset % SECTOR_SIZE), buffer, length);

#if MAX_NR_COROUTINES
        {
            io_prep_pwrite(&aio_req, nvme_fd_direct_, direct_sector_buffer(), SECTOR_SIZE, util::round_down(offset, SECTOR_SIZE));
            iocb *aio_req_p = &aio_req;
            if (io_submit(aio_ctx, 1, &aio_req_p) != 1) {
                ASSERT(false, "failed to io_submit.");
            }

            sink();

            while (true) {
                int nr_events = io_getevents(aio_ctx, 0, 1, &aio_event, nullptr);
                if (nr_events == 0) {
                    sink();
                } else {
                    break;
                }
            }
        }
#else
        ret = pwrite(nvme_fd_direct_, direct_sector_buffer(), SECTOR_SIZE, util::round_down(offset, SECTOR_SIZE));
        ASSERT(ret == SECTOR_SIZE, "fail to write: %s", strerror(errno));
#endif
    }

    std::span<std::tuple<u64, u32>>
    RpcIo::io(u64 rw_offset, u32 rw_count, const uint64_t *fault_bitmap, bool write, LocalMr *mr, coroutine<void>::push_type& sink, int coro_id)
    {
        static auto &gv = GlobalVariables::instance();
        static util::LatencyRecorder lr_pch("lr pch");
        static util::LatencyRecorder lr_memcpy("lr memcpy");
        static util::LatencyRecorder<false> lr_direct_read("lr_direct_read");
        static util::LatencyRecorder<false> lr_buffer_read("lr_buffer_read");
        static util::LatencyRecorder<false> lr_direct_write("lr_direct_write");
        static util::LatencyRecorder<false> lr_buffer_write("lr_buffer_write");
        static util::Recorder<false> rec_length("rec_length");

        auto [rw_page_begin, rw_page_end] = mr->get_page_span(mr->user_mr()->addr64() + rw_offset, rw_count);
        u32 nr_pages = rw_page_end - rw_page_begin;
        auto [unit_begin, unit_end] = mr->get_unit_span(mr->user_mr()->addr64() + rw_offset, rw_count);
        int ret;

        for (u64 unit_id = unit_begin; unit_id < unit_end; unit_id++) {
#if MAX_NR_COROUTINES
            while (!mr->get_unit_lock(unit_id)->try_lock_shared()) {
                sink();
            }
#else
            mr->get_unit_lock(unit_id)->lock_shared();
#endif
        }

        if (!mr_base_addr_) {
            mr_base_addr_ = mr->user_mr()->addr();
        }
        ASSERT(mr_base_addr_ == mr->user_mr()->addr(), "");

        auto fn_get_iter_length = [rw_offset, rw_count] (uint64_t iter_offset) -> u64
        {
            return std::min(PAGE_SIZE - iter_offset % PAGE_SIZE, rw_offset + rw_count - iter_offset);
        };

        auto fn_offset_to_bounce_addr = [addr = bounce_mr_->addr(), rw_page_begin] (u64 offset) -> char *
        {
            return (char *)addr + (offset - rw_page_begin * PAGE_SIZE);
        };

        auto fn_offset_to_mr_addr = [addr = mr->user_mr()->addr()](u64 offset) -> char *
        {
            return (char *)addr + offset;
        };

        auto fn_sector_to_lock = [mr] (u64 sector_id)
        {
            u64 lock_id = util::hash_u64(sector_id) % mr->sector_lock_vec()->size();
            return &mr->sector_lock_vec()->at(lock_id);
        };

        // pch
        lr_pch.begin_one();
        #if 1
        std::vector<u8> vec_cached(nr_pages);
        ret = pread(pch_fd_, vec_cached.data(), nr_pages, rw_page_begin);
        ASSERT(ret == nr_pages, "fail to read pch_fd: %s", strerror(errno));
        #else
        std::vector<u8> vec_cached(nr_pages);
        ret = mincore(((char *)mr->user_mr()->addr() + rw_page_begin * PAGE_SIZE), nr_pages * PAGE_SIZE, vec_cached.data());
        ASSERT(ret == 0, "fail to mincore: %s", strerror(errno));
        #endif
        lr_pch.end_one();

        int nr_io_reqs = 0;
        int access_vec_size = 0;
        u64 head_offset = 0;
        u32 head_length = 0;
        u64 tail_offset = 0;
        u32 tail_length = 0;
        bool direct = false;

        if (gv.configs.server_io_path == +ServerIoPath::MEMCPY) {
            direct = (vec_cached[0] == PageState::kUncached);
            if (direct) {
                if (write)
                    lr_direct_write.begin_one();
                else {
                    lr_direct_read.begin_one();
                }
            } else {
                if (write) {
                    lr_buffer_write.begin_one();
                } else {
                    lr_buffer_read.begin_one();
                }
            }
        }
        for (u32 idx = 0; idx < nr_pages; idx++) {
            if (fault_bitmap && !util::test_bit(idx, fault_bitmap)) continue;

            u64 iter_offset = std::max(rw_offset, (idx + rw_page_begin) * PAGE_SIZE);
            u32 iter_count = fn_get_iter_length(iter_offset);

            if (auto s = std::span(access_vec_.begin(), access_vec_size);
                    s.size() && std::get<0>(s.back()) + std::get<1>(s.back()) == iter_offset) {
                std::get<1>(s.back()) += iter_count;
            } else {
                access_vec_[access_vec_size] = {iter_offset, iter_count};
                access_vec_size++;
            }

            PageState::Val state;

            switch (gv.configs.server_io_path) {
                case ServerIoPath::MEMCPY: {
                    char *mr_addr = fn_offset_to_mr_addr(iter_offset);
                    char *bounce_addr = fn_offset_to_bounce_addr(iter_offset);
                    if (write) {
                        memcpy(mr_addr, bounce_addr, iter_count);
                    } else {
                        memcpy(bounce_addr, mr_addr, iter_count);
                    }
                    rec_length.record_one(iter_count);
                    continue;
                }
                case ServerIoPath::TIERING: {
                    state = vec_cached[idx] ? PageState::kCached : PageState::kUncached;
                    break;
                }
                case ServerIoPath::BUFFER: {
                    state = PageState::kCached;
                    break;
                }
                case ServerIoPath::DIRECT: {
                    state = PageState::kUncached;
                    break;
                }
            }

//            PageState state = mr->page_state_vec().at(rw_page_begin + idx);

            io_req_t io_req{};
            if (state == PageState::kUncached) {
                direct = true;
                if (write && (iter_offset % SECTOR_SIZE || iter_count % SECTOR_SIZE)) {
                    if (iter_offset % SECTOR_SIZE) {
                        // the unaligned head
                        // must be in the first page.
                        //  direct_read_offset
                        //          ||           sector              ||
                        //                 |     read                       |h
                        //              iter_offset
                        ASSERT(idx == 0, "idx=%u, expected 0.", idx);
                        u64 sector = iter_offset / SECTOR_SIZE;

                        u32 offset_in_first_sector = iter_offset % SECTOR_SIZE;
                        u32 count_in_first_sector = SECTOR_SIZE - offset_in_first_sector;
                        u32 write_count = std::min(count_in_first_sector, iter_count);

                        head_offset = iter_offset;
                        head_length = write_count;

                        if (iter_count == write_count) {
                            // nothing is left to write.
                            continue;
                        } else {
                            // the latter part still needs writing.
                            iter_offset += count_in_first_sector;
                            iter_count -= count_in_first_sector;
                        }
                    } // if (iter_offset % SECTOR_SIZE)
                    ASSERT(iter_offset % SECTOR_SIZE == 0, "");

                    if (iter_count % SECTOR_SIZE) {
                        // the unaligned tail
                        // must be in the last rw page.
                        // even if it is in the first rw page, there is only one rw page.
                        //                                 direct_read_offset
                        //          ||           sector      ||           sector      ||
                        //           |           read                  |
                        //       iter_offset
                        ASSERT(idx == nr_pages - 1, "idx=%u, nr_pages=%u", idx, nr_pages);
                        u32 length_in_last_sector = iter_count % SECTOR_SIZE;

                        tail_offset = iter_offset + (iter_count - length_in_last_sector);
                        tail_length = length_in_last_sector;
                        iter_count -= length_in_last_sector;

                        if (iter_count == 0) {
                            break;
                        }
                    } //  if (iter_length % SECTOR_SIZE
                    ASSERT(iter_count % SECTOR_SIZE == 0, "");
                } //  if (write is unaligned.)
                io_req.direct = true;
                iter_offset = util::round_down(iter_offset, SECTOR_SIZE);
                iter_count = util::round_up(fn_get_iter_length(iter_offset), SECTOR_SIZE);

            } // page is direct. 

            io_req.offset = iter_offset;
            io_req.length = iter_count;
            io_req.buffer = fn_offset_to_bounce_addr(iter_offset);

            if (nr_io_reqs && io_req_vec_[nr_io_reqs - 1].try_merge(io_req)) {
            } else {
                io_req_vec_[nr_io_reqs] = io_req;
                nr_io_reqs++;
            }
        }

        if (gv.configs.server_io_path == +ServerIoPath::MEMCPY) {
            if (direct) {
                if (write)
                    lr_direct_write.end_one();
                else {
                    lr_direct_read.end_one();
                }
            } else {
                if (write) {
                    lr_buffer_write.end_one();
                } else {
                    lr_buffer_read.end_one();
                }
            }
        } else {
            if (direct) {
                if (write)
                    lr_direct_write.begin_one();
                else {
                    lr_direct_read.begin_one();
                }
            } else {
                if (write) {
                    lr_buffer_write.begin_one();
                } else {
                    lr_buffer_read.begin_one();
                }
            }
        }

        // ASSERT(!(head_length && tail_length), "head_length=0x%x, tail_length=0x%x", head_length, tail_length);

        if (head_length) {
            auto *sector_lock = fn_sector_to_lock(head_offset / SECTOR_SIZE);
#if MAX_NR_COROUTINES
            while (!sector_lock->try_lock()) {
                sink();
            }
#else
            sector_lock->lock();
#endif
            subsector_write(fn_offset_to_bounce_addr(head_offset), head_length, head_offset, sink, coro_id);
            sector_lock->unlock();
        }

        // if (nr_io_reqs > 3) {
        //     do_io_by_uring(nr_io_reqs, write);
        // } else {
            do_io_by_psync(nr_io_reqs, write, sink, coro_id);
        // }

        if (tail_length) {
            auto *sector_lock = fn_sector_to_lock(tail_offset / SECTOR_SIZE);
#if MAX_NR_COROUTINES
            while (!sector_lock->try_lock()) {
                sink();
            }
#else
            sector_lock->lock();
#endif
            subsector_write(fn_offset_to_bounce_addr(tail_offset), tail_length, tail_offset, sink, coro_id);
            sector_lock->unlock();
        }

        if (gv.configs.server_io_path != +ServerIoPath::MEMCPY) {
            if (direct) {
                if (write) {
                    lr_direct_write.end_one();
                } else {
                    lr_direct_read.end_one();
                }
            } else {
                if (write) {
                    lr_buffer_write.end_one();
                } else {
                    lr_buffer_read.end_one();
                }
            }
        }

        for (u64 unit_id = unit_begin; unit_id < unit_end; unit_id++) {
            static int msync_thread_id = util::Schedule::thread_id();
            if (util::Schedule::thread_id() == msync_thread_id && write && !direct && !mr->unit_is_hot(unit_id)) {
                auto [sync_addr_begin, sync_addr_end] = mr->get_unit_addr_span(unit_id);
                uint64_t sync_offset_begin = sync_addr_begin - mr->addr64();
                uint64_t sync_offset_end = sync_addr_end - mr->addr64();

                int ret = sync_file_range(nvme_fd_buffer_, sync_offset_begin, sync_offset_end - sync_offset_begin, SYNC_FILE_RANGE_WRITE_AND_WAIT);
                if (ret) {
                    pr_err("fail to sync: %s", strerror(errno));
                }
            }
            mr->get_unit_lock(unit_id)->unlock_shared();
        }
        
        return std::span(access_vec_.begin(), access_vec_size);
    }
}
