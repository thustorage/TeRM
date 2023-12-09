#include "global.hh"
#include <rdma.hh>

namespace pdp {
    struct ContextBytes {
        ibv_gid gid;
        uint16_t lid;
        ContextBytes(ibv_gid gid, uint16_t lid) : gid(gid), lid(lid) {}
        explicit ContextBytes(const std::vector<char> &vec_char)
        {
            memcpy(this, vec_char.data(), sizeof(ContextBytes));
        }

        // for memcached KV
        std::string key() const
        {
            auto id = rdma::Context::RemoteIdentifier(gid, lid);
            return id.to_string();
        }

        auto to_vec_char() const
        {
            return util::to_vec_char(*this);
        }
    };

    class RemoteContext {
    public:
        using Identifier = rdma::Context::RemoteIdentifier;

        RemoteContext() = default;
        explicit RemoteContext(const ContextBytes &bytes) : gid_(bytes.gid), lid_(bytes.lid) {}

        auto identifier() const
        {
            return Identifier{gid_, lid_};
        }

    private:
        ibv_gid gid_;
        uint16_t lid_;
    };

    class RemoteContextManager : public Manager<RemoteContext> {};

    class LocalContext : public rdma::Context {
    public:
        using Identifier = util::CachelineAligned<ibv_pd *>;
        auto identifier() const {
            return native_pd();
        }
    public:
        LocalContext(ibv_context *native_ctx, ibv_pd *native_pd)
                : Context(native_ctx, native_pd) {}

        ContextBytes to_bytes() const
        {
            return ContextBytes(gid_, lid_);
        }
    };

    class LocalContextManager : public Manager<LocalContext> {};
}
