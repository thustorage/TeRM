#pragma once
// mine
#include <util.hh>
#include <bitmap.hh>
// linux
#include <fcntl.h>
#include <sys/mman.h>
// c++
#include <memory>
#include <new>
// c
#include <cassert>
#include <cstdint>
#include <cstdio>

namespace pdp {
    class PageBitmap {
    protected:
        util::ulong_t *bitmap_{}; // passed and freed by a derived class
        uint64_t bytes_{}; // bitmap mr_bytes_ (bits / 8)
    public:
        PageBitmap() = default;

        virtual ~PageBitmap()
        {
            // the derived class will set bitmap_ to nullptr in its dtor
            if (bitmap_) {
                delete[] bitmap_;
                bitmap_ = nullptr;
            }
        }

        bool test_present(uint64_t pg) const
        {
            ASSERT(pg / util::kBitsPerByte < bytes_, "pg=0x%lx is out of bound max_pages=0x%lx", pg, bytes_ * util::kBitsPerByte);
            return util::test_bit(pg, bitmap_);
        }

        void set_present(uint64_t pg)
        {
            ASSERT(pg / util::kBitsPerByte < bytes_, "pg=0x%lx is out of bound max_pages=0x%lx", pg, bytes_ * util::kBitsPerByte);

            util::set_bit(pg, bitmap_);
        }

        void clear_present(uint64_t pg)
        {
            ASSERT(pg / util::kBitsPerByte < bytes_, "pg=0x%lx is out of bound max_pages=0x%lx", pg, bytes_ * util::kBitsPerByte);

            util::clear_bit(pg, bitmap_);
        }

        void assign(uint64_t pg, bool set)
        {
            if (test_present(pg) == set) return;
            if (set) {
                set_present(pg);
            } else {
                clear_present(pg);
            }
        }

        bool range_has_fault(uint64_t pg_begin, uint64_t pg_end) const
        {
            ASSERT(pg_end / util::kBitsPerByte < bytes_, "pg=0x%lx is out of bound max_pages=0x%lx", pg_end, bytes_ * util::kBitsPerByte);


            // if (pg_end - pg_begin > 1) {
            //     pr_err("error");
            // }
            // return false;

            for (auto i = pg_begin; i < pg_end; i++) {
                if (!test_present(i)) {
                    // pr_err("true");   
                    return true;
                }
            }
            return false;
        }

        uint64_t range_count_fault(uint64_t pg_begin, uint64_t pg_end) const 
        {
            ASSERT(pg_end / util::kBitsPerByte < bytes_, "pg=0x%lx is out of bound max_pages=0x%lx", pg_end, bytes_ * util::kBitsPerByte);

            uint64_t count = 0;
            for (auto i = pg_begin; i < pg_end; i++) {
                if (!test_present(i)) {
                    count++;
                }
            }
            return count;
        }
    };

class LocalPageBitmap final: public PageBitmap {
    util::ObserverPtr<rdma::LocalMemoryRegion> local_mr_;
    rdma::LocalMemoryRegion::Uptr bitmap_mr_;

public:
    using Uptr = std::unique_ptr<LocalPageBitmap>;

    LocalPageBitmap(util::ObserverPtr<rdma::LocalMemoryRegion> local_mr)
        : local_mr_(local_mr)
    {
        static auto &gv = GlobalVariables::instance();
        char file[256];
        sprintf(file, "/proc/pdp_bitmap_0x%x", local_mr_->lkey_);
        int fd = open(file, O_RDWR);
        assert(fd > 0);

        uint32_t nr_pfns;
        auto ret = pread(fd, &nr_pfns, sizeof(uint32_t), 0);
        assert(ret == sizeof(uint32_t));

        bytes_ = util::bits_to_long_aligned_bytes(nr_pfns);
        bitmap_ = (util::ulong_t *)mmap(nullptr, bytes_, PROT_READ | PROT_WRITE,
            MAP_SHARED | MAP_POPULATE, fd, 0);
        assert(bitmap_ != MAP_FAILED);

        close(fd);

        assert(gv.ori_ibv_symbols.reg_mr);
        bitmap_mr_ = std::make_unique<rdma::LocalMemoryRegion>(local_mr->ctx(), (void *)bitmap_, bytes_, false, gv.ori_ibv_symbols.reg_mr);

        pr_info("mr: key=0x%x; bitmap: addr=%p, length=0x%lx, key=0x%x", local_mr->lkey_, bitmap_, bytes_, bitmap_mr_->lkey_);
    }

    ~LocalPageBitmap() final
    {
        munmap(bitmap_, bytes_);
        bitmap_ = nullptr;
    }

    auto bitmap_mr() const { return util::make_observer(bitmap_mr_.get()); }
};

class RemotePageBitmap final : public PageBitmap {
    rdma::RemoteMemoryRegion remote_bitmap_mr_; // RDMA read from this remote mr.
    std::unique_ptr<rdma::LocalMemoryRegion> local_bitmap_mr_; // RDMA read to this local mr.
    rdma::Wr pull_wr_;

public:
    using Uptr = std::unique_ptr<RemotePageBitmap>;

    DELETE_COPY_CONSTRUCTOR_AND_ASSIGNMENT(RemotePageBitmap);
    DELETE_MOVE_CONSTRUCTOR_AND_ASSIGNMENT(RemotePageBitmap);

    RemotePageBitmap(util::ObserverPtr<rdma::Context> ctx, rdma::RemoteMemoryRegion remote_bitmap_mr)
            : remote_bitmap_mr_(remote_bitmap_mr)
    {
        static auto &gv = GlobalVariables::instance();

        bytes_ = remote_bitmap_mr_.length_;
        pr_info("remote_bitmap_mr: %s", remote_bitmap_mr.to_string().c_str());
        
        ASSERT(remote_bitmap_mr_.addr64() % SZ_4K == 0, "remote_bitmap_mr.addr is not aligned to 4KB.");
        ASSERT(remote_bitmap_mr_.length() % sizeof(util::ulong_t) == 0, "remote_bitmap_mr.length is not aligned to 8 bytes.");

        bitmap_ = new util::ulong_t[bytes_ / sizeof(util::ulong_t)];
        ASSERT(gv.ori_ibv_symbols.reg_mr, "gv.ori_ibv_symbols.reg_mr");
        local_bitmap_mr_ = std::make_unique<rdma::LocalMemoryRegion>(ctx, bitmap_, bytes_, false, gv.ori_ibv_symbols.reg_mr);

        // we generate a wr and let the caller submit via a RC QP.
        pull_wr_ = rdma::Wr().set_op_read().set_signaled(true)
                .set_rdma(remote_bitmap_mr_.pick_all())
                .set_sg(local_bitmap_mr_->pick_all());
    }

    ~RemotePageBitmap() final
    {
        delete[] bitmap_;
        bitmap_ = nullptr;
    }

    rdma::Wr pull_wr() { return pull_wr_; }
};

}
