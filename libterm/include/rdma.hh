#pragma once
// c++
#include <memory>
#include <iostream>
#include <optional>
#include <thread>
#include <cassert>
#include <random>
#include <fmt/core.h>
// 3rd
#include <infiniband/verbs.h>
#include <libmemcached/memcached.hpp>
#include <utility>
// linux
#include <unistd.h>
#include <sys/mman.h>
#include <fcntl.h>
// mine
#include "logging.hh"
#include "util.hh"
#include "const.hh"

#define PSN 0
#define UD_QKEY 0

namespace rdma {

using std::vector;
using std::string;
using std::optional;

using create_qp_t = struct ibv_qp *(*)(struct ibv_pd *pd, struct ibv_qp_init_attr *qp_init_attr);
using modify_qp_t = int(*)(struct ibv_qp *qp, struct ibv_qp_attr *attr, int attr_mask);
using reg_mr_t = struct ibv_mr *(*)(struct ibv_pd *pd, void *addr, size_t length,
                                           int access);

// TODO: split it to Device, Context, and Protection Domain
class Context {
protected:
    ibv_context *native_ctx_ = nullptr;
    ibv_pd *native_pd_ = nullptr;

    const uint8_t port_num_ = 1;
    const uint8_t gid_index_ = 0;

    union ibv_gid gid_{};
    uint16_t lid_ = 0;

    bool need_free_ = true;
public:
    using Identifier = util::CachelineAligned<ibv_pd *>;
    Identifier identifier() const { return native_pd_; }
public:
    class __cacheline_aligned RemoteIdentifier {
    private:
        union ibv_gid gid_;
        uint16_t lid_;
    public:
        RemoteIdentifier() : gid_{}, lid_{} {}
        RemoteIdentifier(union ibv_gid gid, uint16_t lid) : gid_(gid), lid_(lid) {}
        constexpr std::strong_ordering operator<=>(const RemoteIdentifier &rhs) const
        {
            if (auto v = gid_.global.subnet_prefix <=> rhs.gid_.global.subnet_prefix; v != 0) {
                return v;
            }
            if (auto v = gid_.global.interface_id <=> rhs.gid_.global.interface_id; v != 0) {
                return v;
            }
            // if (int v = memcmp(gid_.raw, rhs.gid_.raw, sizeof(gid_)); v) {
            //     return v <=> 0;
            // }
            return lid_ <=> rhs.lid_;
        }
        // bool operator<(const RemoteIdentifier &rhs) const
        // {
        //     if (int cmp = memcmp(gid_.raw, rhs.gid_.raw, sizeof(gid_)); cmp != 0) {
        //         return cmp < 0;
        //     }
        //     return lid_ < rhs.lid_;
        // }

        auto gid() const { return gid_; }
        auto lid() const { return lid_; }

        // bool operator!=(const RemoteIdentifier &rhs) const
        // {
        //     return memcmp(gid_.raw, rhs.gid_.raw, sizeof(gid_)) || lid_ != rhs.lid_;
        // }

        bool operator==(const RemoteIdentifier &rhs) const 
        {
            return memcmp(gid_.raw, rhs.gid_.raw, sizeof(gid_)) == 0 && lid_ == rhs.lid_;
        }

        std::size_t hash() const
        {
            std::size_t seed{};
            util::hash_combine(seed, gid_.global.subnet_prefix);
            util::hash_combine(seed, gid_.global.interface_id);
            util::hash_combine(seed, lid_);
            return seed;
        }

        std::string to_string() const {
            char str[64];
            sprintf(str, "[ctx]0x%016llx:0x%016llx-0x%04x", gid_.global.interface_id,
                    gid_.global.subnet_prefix, lid_);
            return str;
        }

        RemoteIdentifier wo_gid() const
        {
            return {{}, lid_};
        }
    };

    auto remote_identifier() const
    {
        return RemoteIdentifier(gid_, lid_);
    }
public:
    Context(ibv_context *native_ctx, ibv_pd *native_pd)
        : native_ctx_(native_ctx), native_pd_(native_pd), need_free_(false)
    {
        if (ibv_query_gid(native_ctx_, port_num_, gid_index_, &gid_)) {
            pr_err("could not get gid for port: %d, gidIndex: %d", port_num_, gid_index_);
            assert(false);
        }

        struct ibv_port_attr port_attr{};
        if (ibv_query_port(native_ctx_, port_num_, &port_attr)) {
            pr_err("ibv_query_port failed");
            assert(false);
        }
        lid_ = port_attr.lid;
    }

    Context(int dev_idx): native_ctx_(nullptr), native_pd_(nullptr) {
        int nr_devices;
        struct ibv_port_attr port_attr{};

        struct ibv_device **dev_list = ibv_get_device_list(&nr_devices);
        if (!nr_devices || !dev_list) {
            pr_err("no device found");
            return;
        }

        int dev_id_in_list = -1;
        for (int i = 0; i < nr_devices; ++i) {
            // printf("Device %d: %s\n", i, ibv_get_device_name(deviceList[i]));
            if (ibv_get_device_name(dev_list[i])[5] == '0' + dev_idx) {
                dev_id_in_list = i;
                break;
            }
        }

        if (dev_id_in_list >= nr_devices) {
            ibv_free_device_list(dev_list);
            pr_err("ib device wasn't found");
            return;
        }

        struct ibv_device *dev = dev_list[dev_id_in_list];
        // printf("open nic: %s\n", ibv_get_device_name(dev));

        native_ctx_ = ibv_open_device(dev);
        if (!native_ctx_) {
            pr_err("failed to open device");
            return;
        }

        ibv_free_device_list(dev_list);

        if (ibv_query_port(native_ctx_, port_num_, &port_attr)) {
            pr_err("ibv_query_port failed");
            return;
        }

        lid_ = port_attr.lid;

        native_pd_ = ibv_alloc_pd(native_ctx_);
        if (!native_pd_) {
            pr_err("ibv_alloc_pd failed");
            return;
        }

        if (ibv_query_gid(native_ctx_, port_num_, gid_index_, &gid_)) {
            pr_err("could not get gid for port: %d, gidIndex: %d", port_num_, gid_index_);
            return;
        }
    }

    virtual ~Context()
    {
        if (!need_free_) return;
        if (native_pd_) ibv_dealloc_pd(native_pd_);
        if (native_ctx_) ibv_close_device(native_ctx_);
    }

    bool operator<(const Context &rhs) const {
        if (native_ctx_ != rhs.native_ctx_) {
            return native_ctx_ < rhs.native_ctx_;
        }
        return native_pd_ < rhs.native_pd_;
    }

    auto native_pd() const { return native_pd_; }
    auto native_ctx() const { return native_ctx_; }
    auto gid_index() const { return gid_index_; }
    auto port_num() const { return port_num_; }
    auto gid() const { return gid_; }
    auto lid() const { return lid_; }
};


}

template<>
struct std::hash<rdma::Context::RemoteIdentifier>
{
    std::size_t operator()(rdma::Context::RemoteIdentifier const &id) const noexcept
    {
        return id.hash();
    }
};

namespace rdma {
class Allocation {
    void *addr_{};
    size_t size_{};
    int type_{}; // 0: not free, 1: alloc, 2: mmap

public:
    Allocation() = default;
    Allocation(const Allocation &a) = delete;
    Allocation& operator=(const Allocation &a) = delete;

    Allocation(Allocation &&a) noexcept
    {
        *this = std::move(a);
    }

    Allocation& operator=(Allocation &&rhs) noexcept
    {
        std::swap(addr_, rhs.addr_);
        std::swap(size_, rhs.size_);
        std::swap(type_, rhs.type_);
        return *this;
    }

    ~Allocation()
    {
        switch (type_) {
            case 0: break;
            case 1:
                free(addr_);
                break;
            case 2:
                munmap(addr_, size_);
                break;
        }
    }

    Allocation(void *addr, size_t size) : addr_(addr), size_(size), type_(0) {}

    explicit Allocation(size_t size) : size_(size), type_(1)
    {
        addr_ = aligned_alloc(4096, size_);
        memset(addr_, 0, size_);
    }

    Allocation(size_t size, std::string mmap_dev, bool clear) : size_(size), type_(2)
    {
        int flags = 0;
        int fd;

        // shared
        #if 1
            flags = MAP_SHARED;
            // pr_info("MMAP SHARED");
        #else
            flags = MAP_PRIVATE;
            pr_info("MMAP PRITVATE");
        #endif

        // mem or dev
        if (mmap_dev.empty()) {
            fd = -1;
            flags |= MAP_ANONYMOUS;
            // pr_info("MMAP ANON");
        } else {
            std::string path = "/dev/" + mmap_dev;
            fd = open(path.c_str(), O_RDWR);
            ASSERT(fd >= 0, "fail to open %s: %s", path.c_str(), strerror(errno));
            pr_info("MMAP %s", mmap_dev.c_str());
            posix_fadvise(fd, 0, size_, POSIX_FADV_RANDOM);
        }

        // populate
        #if 0
            flags |= MAP_POPULATE;
            pr_info("POPULATE: ON");
        #else
            // pr_info("POPULATE: OFF");
        #endif

        // hugepage
        #if 0
            flags |= MAP_HUGETLB;
            pr_info("HUGEPAGE: ON");
        #else
            // pr_info("HUGEPAGE: OFF");
        #endif
        
        addr_ = mmap(nullptr, size_, PROT_READ | PROT_WRITE,
                        flags, fd, 0);
        ASSERT(addr_ != MAP_FAILED, "fail to mmap: %s", strerror(errno));

        if (clear) {
            pr_info("memset 0, size=0x%lx", size_);

            auto tp = std::chrono::steady_clock::now();
            for (uint64_t off = 0; off < size_; off += SZ_1G) {
                uint8_t *iter_addr = (uint8_t *)addr_ + off;
                uint64_t iter_length = std::min<uint64_t>(size_ - off, SZ_1G);
                memset(iter_addr, 0, iter_length);
                msync(iter_addr, iter_length, MS_SYNC);
                pr_info("memset %lu GB.", off / SZ_1G);
            }
            auto d = std::chrono::steady_clock::now() - tp;

            pr_info("%.2lfs", std::chrono::duration<double>(d).count());
        }
        madvise(addr_, size_, MADV_RANDOM);

        pr_info("done.");

        if (fd > 0) {
            close(fd);
        }
    }

//    void random_memset_0()
//    {
//        #define MEMORY_UNIT_SIZE (1ul << 20)
//        ASSERT(size_ % MEMORY_UNIT_SIZE == 0, "unsupported size");
//        size_t nr_units = size_ / MEMORY_UNIT_SIZE;
//        std::vector<size_t> vec(nr_units);
//        std::iota(vec.begin(), vec.end(), 0);
//        std::random_device rd;
//        std::mt19937 g(rd());
//
//        std::shuffle(vec.begin(), vec.end(), g);
//        for (size_t i = 0; i < nr_units; i++) {
//            if ((i * 10) % nr_units == 0) {
//                pr_info("%lu", i * 100 / nr_units);
//            }
//            memset((char *)addr_ + vec[i] * MEMORY_UNIT_SIZE, 0, MEMORY_UNIT_SIZE);
//        }
//    }

    void *addr() const { return addr_; }
    size_t size() const { return size_; }
};

// for function parameter
struct MemorySge {
    uint64_t addr;
    uint32_t rkey;
    uint32_t lkey;
    uint32_t length;

    bool operator<(const MemorySge &rhs) const {
        if (lkey != rhs.lkey) return lkey < rhs.lkey;
        if (addr != rhs.addr) return addr < rhs.addr;
        if (length != rhs.length) return length < rhs.length;
        return rkey < rhs.rkey;
    }

    auto to_sge()
    {
        return ibv_sge{
            .addr = addr, 
            .length = length,
            .lkey = lkey
        };
    }
};

class MemoryRegion {
public:
    uint64_t addr_;
    uint32_t rkey_;
    uint32_t lkey_;
    uint64_t length_;

    MemorySge pick_by_offset(uint64_t offset, uint32_t length) const
    {
        return MemorySge{addr_ + offset, rkey_, lkey_, length};
    }

    MemorySge pick_by_addr(uint64_t addr, uint32_t length) const
    {
        return MemorySge{addr, rkey_, lkey_, length};
    }

    MemorySge pick_by_offset0(uint32_t length) const
    {
        return MemorySge{addr_, rkey_, lkey_, length};
    }

    MemorySge pick_all() const
    {
        if (length_ > UINT32_MAX) {
            pr_err("length_ is too long!");
        }
        return MemorySge{addr_, rkey_, lkey_, (uint32_t)length_};
    }

    bool cover_region(uint64_t addr, uint64_t len) const
    {
        return addr >= addr_ && addr + len <= addr_ + length_;
    }

    auto to_string() const 
    {
        char str[256];
        sprintf(str, "addr=0x%lx, key=0x%x, length=0x%lx", addr_, lkey_, length_);
        return std::string{str};
    }

    auto to_vec_char() const
    {
        return util::to_vec_char(*this);
    }

    auto addr() const { return (void*)addr_; }
    auto addr64() const { return addr_; }
    auto length() const { return length_; }
    auto lkey() const { return lkey_; }
    auto rkey() const { return rkey_; }
};

class LocalMemoryRegion : public MemoryRegion {
private:
    util::ObserverPtr<Context> ctx_;
    Allocation alloc_{};

    ibv_mr *native_mr_ = nullptr;
    bool need_dereg_ = true;

public:
    using Uptr = std::unique_ptr<LocalMemoryRegion>;
    auto native_mr() { return native_mr_; }
    auto ctx() const { return ctx_; }

    LocalMemoryRegion(util::ObserverPtr<Context> ctx, ibv_mr *mr)
        : ctx_(ctx), native_mr_(mr), need_dereg_(false)
    {
        addr_ = (uint64_t)mr->addr;
        rkey_ = mr->rkey;
        lkey_ = mr->lkey;
        length_ = mr->length;
    }

    LocalMemoryRegion(util::ObserverPtr<Context> ctx, void *addr, size_t size, bool odp, reg_mr_t reg_mr = ibv_reg_mr)
        : LocalMemoryRegion(ctx, Allocation{addr, size}, odp, reg_mr) {}
    LocalMemoryRegion(util::ObserverPtr<Context> ctx, Allocation alloc, bool odp, reg_mr_t reg_mr = ibv_reg_mr)
    : ctx_(ctx), alloc_(std::move(alloc))
    {
        addr_ = (uint64_t)alloc_.addr();
        length_ = alloc_.size();

        int access = IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ | IBV_ACCESS_REMOTE_WRITE;
        if (odp) {
            access |= (IBV_ACCESS_ON_DEMAND);
            pr_info("MR with ODP");
        }

        // if (odp) {
        //     ibv_mr *pin_mr_in_recv_wr_chain = reg_mr(ctx->native_pd(), addr(), (16ul << 30), IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ | IBV_ACCESS_REMOTE_WRITE);
        //     ASSERT(pin_mr, "pin_mr_in_recv_wr_chain=%s", strerror(errno));
        // }

        native_mr_ = reg_mr(ctx->native_pd(), (void *)addr_, length_, access);
        if (!native_mr_) {
            pr_err("Memory registration failed 0x%lx 0x%lx", addr_, length_);
            perror("ibv_reg_mr");
            throw std::runtime_error("memory registration");
        }



        rkey_ = native_mr_->rkey;
        lkey_ = native_mr_->lkey;
    }

    ~LocalMemoryRegion() 
    {
        if (need_dereg_)
            ibv_dereg_mr(native_mr_);
    }
};

class RemoteMemoryRegion : public MemoryRegion {
public:
    explicit RemoteMemoryRegion(MemoryRegion mr) : MemoryRegion(mr) {}
};

    class CompletionQueue {
private:
    ibv_cq *native_cq_{};
    util::ObserverPtr<Context> ctx_;
    ibv_comp_channel *native_comp_channel_{};
    int nr_cq_events_{};

public:
    explicit CompletionQueue(util::ObserverPtr<Context> ctx, bool use_event = false)
        : ctx_(ctx)
    {
        if (use_event) {
            native_comp_channel_ = ibv_create_comp_channel(ctx_->native_ctx());
            if (!native_comp_channel_) {
                throw std::runtime_error("ibv_create_comp_channel.");
            }
            pr_info("ibv_create_comp_channel");
        }

        native_cq_ = ibv_create_cq(ctx_->native_ctx(), 128, nullptr, native_comp_channel_, 0);
        if (!native_cq_) {
            throw std::runtime_error("ibv_create_cq.");
        }

        if (use_event) {
            if (ibv_req_notify_cq(native_cq_, 0)) {
                throw std::runtime_error("ibv_req_notify_cq");
            }
        }
    }

    ~CompletionQueue()
    {
        if (nr_cq_events_) {
            ibv_ack_cq_events(native_cq_, nr_cq_events_);
        }

        if (native_comp_channel_) {
            ibv_destroy_comp_channel(native_comp_channel_);
        }

        if (native_cq_) {
            ibv_destroy_cq(native_cq_);
        }
    }

    auto native_cq() const { return native_cq_; }
    auto ctx() const { return ctx_; }

    int poll(int nr_entries, ibv_wc *wc = nullptr)
    {
        static util::LatencyRecorder lr_get_cq_event("get_cq_event");
        static util::LatencyRecorder lr_req_notify_cq("req_notify_cq");
        ibv_wc tmp_wc[nr_entries];

        if (wc == nullptr) {
            wc = tmp_wc;
        }        

        if (native_comp_channel_) {
            ibv_cq *ev_cq;
            void   *ev_ctx;

            lr_get_cq_event.begin_one();
            if (ibv_get_cq_event(native_comp_channel_, &ev_cq, &ev_ctx)) {
                throw std::runtime_error("ibv_get_cq_event");
            }
            lr_get_cq_event.end_one();

            nr_cq_events_++;

            if (ev_cq != native_cq_) {
                throw std::runtime_error("CQ event for unknown CQ");
            }

            lr_req_notify_cq.begin_one();
            if (ibv_req_notify_cq(native_cq_, 0)) {
                throw std::runtime_error("ibv_req_notify_cq");
            }
            lr_req_notify_cq.end_one();
        }

        int ret = ibv_poll_cq(native_cq_, nr_entries, wc);
        if (ret < 0) {
            pr_err("failed to poll, err=%s.", strerror(-ret));
        }

        if (nr_cq_events_ == 1'000'000) {
            ibv_ack_cq_events(native_cq_, nr_cq_events_);
            nr_cq_events_ = 0;
        }

        return ret;
    }

    // ret_val < 0: error
    // wait one
    int wait(ibv_wc *wc = nullptr, bool *should_stop = nullptr)
    {
        int ret;

        // <0: error
        // ==0: not yet
        // >0: # requests
        do {
            if (should_stop && *should_stop) break;
        } while ((ret = poll(1, wc)) == 0);
        
        return ret;
    }
};

    class Wr {
        ibv_send_wr wr_{};
        ibv_sge sge_{};

    public:
        ibv_send_wr* native_wr()
        {
            wr_.num_sge = 1;
            wr_.sg_list = &sge_;

            return &wr_;
        }

        Wr& set_op_read()
        {
            wr_.opcode = IBV_WR_RDMA_READ;
            return *this;
        }

        Wr& set_op_write()
        {
            wr_.opcode = IBV_WR_RDMA_WRITE;
            return *this;
        }

        Wr& set_op_write_imm(uint32_t imm)
        {
            wr_.opcode = IBV_WR_RDMA_WRITE_WITH_IMM;
            wr_.imm_data = imm;
            return *this;
        }

        Wr& set_op_send() 
        { 
            wr_.opcode = IBV_WR_SEND; 
            return *this; 
        }

        Wr& set_sg(uint64_t addr, uint32_t length, uint32_t lkey)
        {
            sge_.addr = addr;
            sge_.length = length;
            sge_.lkey = lkey;
            return *this;
        }

        Wr& set_sg(const MemorySge &sge)
        {
            return set_sg(sge.addr, sge.length, sge.lkey);
        }

        Wr& set_rdma(const MemorySge &sge)
        {
            return set_rdma(sge.addr, sge.rkey);
        }

        Wr& set_rdma(uint64_t addr, uint32_t rkey)
        {
            wr_.wr = {
                    .rdma = {
                            .remote_addr = addr,
                            .rkey = rkey
                    }
            };
            return *this;
        }

        Wr& set_ud(ibv_ah *ah, uint32_t remote_qpn)
        {
            wr_.wr = {
                    .ud = {
                            .ah = ah,
                            .remote_qpn = remote_qpn,
                            .remote_qkey = UD_QKEY
                    }
            };
            return *this;
        }

        bool signaled() const
        {
            return wr_.send_flags & IBV_SEND_SIGNALED;
        }

        Wr& set_signaled(bool set)
        {
            if (set)
                wr_.send_flags |= IBV_SEND_SIGNALED;
            else
                wr_.send_flags &= (~IBV_SEND_SIGNALED);
            return *this;
        }

//        Wr& set_next(Wr &next_wr)
//        {
//            wr_.next = next_wr.native_wr();
//            return *this;
//        }
//
//        Wr& set_no_next()
//        {
//            wr_.next = nullptr;
//            return *this;
//        }
    };

class QueuePair {
public:
    class __cacheline_aligned Identifier {
    private:
        Context::RemoteIdentifier ctx_identifier_;
        uint32_t qp_num_;
    public:
        Identifier() : ctx_identifier_{}, qp_num_{} {}
        Identifier(const Context::RemoteIdentifier &ctx_addr, uint32_t qp_num)
            : ctx_identifier_(ctx_addr), qp_num_(qp_num) {}

        auto qp_num() const { return qp_num_; }
        auto ctx_addr() const { return ctx_identifier_; }

        auto operator<=>(const Identifier &rhs) const = default;

        Identifier wo_gid() const {
            return {ctx_identifier_.wo_gid(), qp_num_};
        }

        std::string to_string() const
        {
            char str[64];
            sprintf(str, "/[qpn]0x%08x", qp_num_);
            return ctx_identifier_.to_string() + str;
        }
    };
protected:
    util::ObserverPtr<Context> ctx_;
    ibv_qp_type type_;
    std::unique_ptr<CompletionQueue> send_cq_;
    std::unique_ptr<CompletionQueue> recv_cq_;
    ibv_qp *native_qp_;

    // never use Qp directly!
    virtual bool modify_to_init(modify_qp_t modify_qp) = 0;
    virtual bool modify_to_rtr(modify_qp_t modify_qp) = 0;
    virtual bool modify_to_rts(modify_qp_t modify_qp) = 0;
    bool modify_to_init_rtr_rts(const modify_qp_t modify_qp)
    {
        return modify_to_init(modify_qp) && modify_to_rtr(modify_qp) && modify_to_rts(modify_qp);
    }

private:
    QueuePair(util::ObserverPtr<Context> ctx, ibv_qp_type type,
                std::unique_ptr<CompletionQueue> send_cq, std::unique_ptr<CompletionQueue> recv_cq,
              create_qp_t create_qp = ibv_create_qp)
            : ctx_(ctx), type_(type), send_cq_(std::move(send_cq)), recv_cq_(std::move(recv_cq))
    {
        const uint32_t kMaxWr = 128;
        const uint32_t kMaxInlineData = 0;
        struct ibv_qp_init_attr qp_init_attr = {
                .send_cq = send_cq_->native_cq(),
                .recv_cq = recv_cq_->native_cq(),
                .cap = {
                        .max_send_wr = kMaxWr,
                        .max_recv_wr = kMaxWr,
                        .max_send_sge = 1,
                        .max_recv_sge = 1,
                        .max_inline_data = kMaxInlineData
                },
                .qp_type = type
        };

        native_qp_ = create_qp(ctx_->native_pd(), &qp_init_attr);
        if (!native_qp_) {
            pr_err("Failed to create QP");
        } else {
            // pr_info("Create QueuePair %d", native_qp_->qp_num);
        }
    }

public:
    QueuePair(util::ObserverPtr<Context> ctx, ibv_qp_type type, create_qp_t create_qp = ibv_create_qp, bool use_event = false)
    : QueuePair(ctx, type,
                std::make_unique<CompletionQueue>(ctx), // send_cq
            std::make_unique<CompletionQueue>(ctx, use_event), // recv_cq
                    create_qp) {}

    virtual ~QueuePair() { ibv_destroy_qp(native_qp_); }
    
    auto native_qp() const { return native_qp_; }
    auto send_cq() const { return util::make_observer(send_cq_.get()); }
    auto ctx() const { return ctx_; }
    auto identifier() const
    {
        return Identifier{ctx_->remote_identifier(), native_qp_->qp_num};
    }

    int poll_send(int nr_entries, ibv_wc *wc = nullptr)
    {
        return send_cq_->poll(nr_entries, wc);
    }

    int poll_recv(int nr_entries, ibv_wc *wc = nullptr) 
    {
        return recv_cq_->poll(nr_entries, wc);
    }

    int wait_send(ibv_wc *wc = nullptr, bool *should_stop = nullptr)
    {
        return send_cq_->wait(wc, should_stop);
    }

    int wait_recv(ibv_wc *wc = nullptr, bool *should_stop = nullptr)
    {
        return recv_cq_->wait(wc, should_stop);
    }

    bool receive(const MemorySge &local_mem)
    {
        return receive(local_mem.addr, local_mem.lkey, local_mem.length);
    }

    bool receive(uint64_t addr, uint32_t lkey, uint32_t length)
    {
        struct ibv_sge sg = { addr, length, lkey };
        struct ibv_recv_wr wr = {
                .sg_list = &sg,
                .num_sge = 1
        };
        struct ibv_recv_wr *bad_wr;

        if (ibv_post_recv(native_qp_, &wr, &bad_wr)) {
            pr_err("receive");
            return false;
        }
        return true;
    }

    void post_send_wr(Wr *wr)
    {
        ibv_send_wr *wr_bad;
        if (ibv_post_send(native_qp_, wr->native_wr(), &wr_bad)) {
            ASSERT(false, "error in post_send=%s", strerror(errno));
        }
    }
};

// a wrapper of struct ibv_qp.
class RcQp : public QueuePair {
private:
    Identifier remote_qp_identifier_;
public:
    using Uptr = std::unique_ptr<RcQp>;
    using Optr = util::ObserverPtr<RcQp>;
    RcQp(util::ObserverPtr<rdma::Context> ctx, create_qp_t create_qp = ibv_create_qp, bool use_event = false)
        : QueuePair(ctx, IBV_QPT_RC, create_qp, use_event) 
        {
        }
protected:
    bool modify_to_init(modify_qp_t modify_qp = ibv_modify_qp) final
    {
        struct ibv_qp_attr attr{};
        attr.qp_state = IBV_QPS_INIT;
        attr.port_num = ctx_->port_num();
        attr.pkey_index = 0;

        switch (native_qp_->qp_type) {
        case IBV_QPT_RC:
            attr.qp_access_flags = IBV_ACCESS_REMOTE_READ | IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_REMOTE_ATOMIC;
            break;

        case IBV_QPT_UC:
            attr.qp_access_flags = IBV_ACCESS_REMOTE_WRITE;
            break;

        default:
            pr_err("implement me:)");
        }

        if (modify_qp(native_qp_, &attr, IBV_QP_STATE | IBV_QP_PKEY_INDEX | IBV_QP_PORT | IBV_QP_ACCESS_FLAGS)) {
            pr_err("Failed to modify QP state to INIT");
            return false;
        }
        return true;
    }

    // rtr: ready to receive
    bool modify_to_rtr(modify_qp_t modify_qp = ibv_modify_qp) final
    {
        struct ibv_qp_attr qp_attr = {};

        qp_attr.qp_state = IBV_QPS_RTR;
        qp_attr.path_mtu = IBV_MTU_4096;
        qp_attr.dest_qp_num = remote_qp_identifier_.qp_num();
        qp_attr.rq_psn = PSN;

        ibv_ah_attr *ah_attr = &qp_attr.ah_attr;
        
        ah_attr->dlid = remote_qp_identifier_.ctx_addr().lid();
        ah_attr->sl = 0;
        ah_attr->src_path_bits = 0;
        ah_attr->port_num = ctx_->port_num();
        ah_attr->is_global = 1;
        ah_attr->grh = {
            .dgid = remote_qp_identifier_.ctx_addr().gid(),
            .flow_label = 0,
            .sgid_index = ctx_->gid_index(),
            .hop_limit = 1,
            .traffic_class = 0
        };

        int flags = IBV_QP_STATE | IBV_QP_AV | IBV_QP_PATH_MTU | IBV_QP_DEST_QPN | IBV_QP_RQ_PSN;

        if (native_qp_->qp_type == IBV_QPT_RC) {
            qp_attr.max_dest_rd_atomic = 16;
            qp_attr.min_rnr_timer = 1; //12;
            flags |= IBV_QP_MAX_DEST_RD_ATOMIC | IBV_QP_MIN_RNR_TIMER;
        }

        if (modify_qp(native_qp_, &qp_attr, flags)) {
            pr_err("failed to modify QP state to RTR");
            return false;
        }
        return true;
    }

    // rts: ready to send
    bool modify_to_rts(modify_qp_t modify_qp = ibv_modify_qp) final
    {
        struct ibv_qp_attr qp_attr = {};
        qp_attr.qp_state = IBV_QPS_RTS;
        qp_attr.sq_psn = PSN;
        
        int flags = IBV_QP_STATE | IBV_QP_SQ_PSN;

        if (native_qp_->qp_type == IBV_QPT_RC) {
            qp_attr.timeout = 20;
            qp_attr.retry_cnt = 7;
            qp_attr.rnr_retry = 7;
            qp_attr.max_rd_atomic = 16;
            flags |= IBV_QP_TIMEOUT | IBV_QP_RETRY_CNT | IBV_QP_RNR_RETRY | IBV_QP_MAX_QP_RD_ATOMIC;
        }
        
        if (modify_qp(native_qp_, &qp_attr, flags)) {
            pr_err("failed to modify QP state to RTS");
            return false;
        }
        return true;
    }

public:
    auto remote_qp_identifier() { return remote_qp_identifier_; }
    bool connect_to(const Identifier &remote_qp_identifier, modify_qp_t modify_qp = ibv_modify_qp)
    {
        remote_qp_identifier_ = remote_qp_identifier;
        return modify_to_init_rtr_rts(modify_qp);
    }

    bool read(const MemorySge &local_mem, const MemorySge &remote_mem, bool signal)
    {
        assert(local_mem.length == remote_mem.length);
        return read(local_mem.addr, remote_mem.addr, local_mem.length, local_mem.lkey, remote_mem.rkey, signal);
    }

    bool read_sync(const MemorySge &local_mem, const MemorySge &remote_mem)
    {
        bool ret = read(local_mem, remote_mem, true);
        if (!ret) return ret;

        ibv_wc wc{};
        wait_send(&wc);

        return wc.status == IBV_WC_SUCCESS;
    }

    bool read(uint64_t local_mem_addr, uint64_t remote_mem_addr, 
        uint32_t length, uint32_t local_mr_lkey, uint32_t remote_mr_rkey,
        bool signal)
    {
        struct ibv_send_wr *wr_bad;

        struct ibv_sge sg = { local_mem_addr, length, local_mr_lkey };
        struct ibv_send_wr wr = {
            .sg_list = &sg,
            .num_sge = 1,
            .opcode = IBV_WR_RDMA_READ,
            .send_flags = signal ? (uint32_t)IBV_SEND_SIGNALED : 0,
            .wr { .rdma = {
                .remote_addr = remote_mem_addr,
                .rkey = remote_mr_rkey
            }}
        };

        if (ibv_post_send(native_qp_, &wr, &wr_bad)) {
            pr_err("Send with RDMA_RW failed.");
            throw std::runtime_error("ibv_post_send");
        }
        return true;
    }

    bool write(const MemorySge &local_mem, const MemorySge &remote_mem, bool signal, std::optional<uint32_t> imm = std::nullopt)
    {
        assert(local_mem.length == remote_mem.length);
        return write(local_mem.addr, remote_mem.addr, local_mem.length, local_mem.lkey, remote_mem.rkey,signal,  imm);
    }

    bool write(uint64_t local_mem_addr, uint64_t remote_mem_addr, 
        uint32_t length, uint32_t local_mr_lkey, uint32_t remote_mr_rkey, 
        bool signal, std::optional<uint32_t> imm = std::nullopt)
    {
        struct ibv_send_wr *wr_bad;

        struct ibv_sge sg = { local_mem_addr, length, local_mr_lkey };
        struct ibv_send_wr wr = {
            .sg_list = &sg,
            .num_sge = 1,
            .opcode = imm ? IBV_WR_RDMA_WRITE_WITH_IMM : IBV_WR_RDMA_WRITE,
            .send_flags = signal ? (uint32_t)IBV_SEND_SIGNALED : 0,
            .imm_data = imm.value_or(0),
            .wr { .rdma = {
                .remote_addr = remote_mem_addr,
                .rkey = remote_mr_rkey
            }}
        };

        if (ibv_post_send(native_qp_, &wr, &wr_bad)) {
            pr_err("Send with RDMA_RW failed.");
            throw std::runtime_error("ibv_post_send");
            return false;
        }
        return true;
    }

    // post without wait
    bool send(const MemorySge &local_mem, bool signal)
    {
        return send(local_mem.addr, local_mem.lkey, local_mem.length, signal);
    }

    bool send(uint64_t addr, uint32_t lkey, uint32_t length, bool signal)
    {
        struct ibv_sge sg = { addr, length, lkey };
        struct ibv_send_wr wr = {
            .sg_list = &sg,
            .num_sge = 1,
            .opcode = IBV_WR_SEND,
            .send_flags = signal ? (uint32_t)IBV_SEND_SIGNALED : 0
        };
        struct ibv_send_wr *bad_wr;

        if (ibv_post_send(native_qp_, &wr, &bad_wr)) {
            pr_err("send: post_send.");
            return false;
        }

        return true;
    }

};

// a wrapper of struct ibv_ah & qpn.
class UdAddrHandle
{
    util::ObserverPtr<Context> ctx_;
    ibv_ah *native_ah_ {nullptr};
    uint32_t qpn_;
public:
    using Uptr = std::unique_ptr<UdAddrHandle>;
    UdAddrHandle(util::ObserverPtr<Context> ctx, const QueuePair::Identifier &qp_addr) : ctx_(std::move(ctx))
    {
        struct ibv_ah_attr ah_attr = {
            .grh = {
                    .dgid = qp_addr.ctx_addr().gid(),
                    .flow_label = 0,
                    .sgid_index = ctx_->gid_index(),
                    .hop_limit = 1,
                    .traffic_class = 0
            },
            .dlid = qp_addr.ctx_addr().lid(),
            .sl = 0,
            .src_path_bits = 0,
            .static_rate = 0,
            .is_global = 1,
            .port_num = ctx_->port_num()
        };
        native_ah_ = ibv_create_ah(ctx_->native_pd(), &ah_attr);
        assert(native_ah_);
        qpn_ = qp_addr.qp_num();
    }

    ~UdAddrHandle() { ibv_destroy_ah(native_ah_); }
    auto native_ah() { return native_ah_; }
    auto qpn() { return qpn_; }
};

class UdQp : public QueuePair {
public:
    UdQp(util::ObserverPtr<Context> ctx, create_qp_t create_qp = ibv_create_qp)
        : QueuePair(ctx, IBV_QPT_UD, create_qp) {}
protected:
    bool modify_to_init(const modify_qp_t modify_qp = ibv_modify_qp) final
    {
        struct ibv_qp_attr attr = {
                .qp_state = IBV_QPS_INIT,
                .qkey = UD_QKEY,
                .pkey_index = 0,
                .port_num = ctx_->port_num()
        };

        if (modify_qp(native_qp_, &attr, IBV_QP_STATE | IBV_QP_PKEY_INDEX |
                                         IBV_QP_PORT | IBV_QP_QKEY)) {
            pr_err("Failed to modify QP state to INIT");
            return false;
        }
        return true;
    }

    bool modify_to_rtr(const modify_qp_t modify_qp = ibv_modify_qp) final
    {
        struct ibv_qp_attr attr = {
                .qp_state = IBV_QPS_RTR
        };
        if (modify_qp(native_qp_, &attr, IBV_QP_STATE)) {
            pr_err("failed to modify QP state to RTR");
            return false;
        }
        return true;
    }

    bool modify_to_rts(const modify_qp_t modify_qp = ibv_modify_qp) final
    {
        struct ibv_qp_attr attr = {
                .qp_state = IBV_QPS_RTS,
                .sq_psn = PSN
        };
        if (modify_qp(native_qp_, &attr, IBV_QP_STATE | IBV_QP_SQ_PSN)) {
            pr_err("failed to modify QP state to RTS");
            return false;
        }
        return true;
    }
public:
    bool setup(modify_qp_t modify_qp = ibv_modify_qp)
    {
        return modify_to_init_rtr_rts(modify_qp);
    }

    void send(UdAddrHandle *ud_ah, const MemorySge &local_mem, bool signaled)
    {
        Wr wr = Wr().set_sg(local_mem.addr, local_mem.length, local_mem.lkey)
            .set_op_send()
            .set_signaled(signaled)
            .set_ud(ud_ah->native_ah(), ud_ah->qpn());

        return post_send_wr(&wr);
    }
};

    int count_wr_chain(auto *wr)
    {
        int cnt = 0;
        for (; wr; wr = wr->next) {
            cnt++;
        }
        return cnt;
    }
} // namespace rdma


