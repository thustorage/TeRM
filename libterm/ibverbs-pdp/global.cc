#include "global.hh"
#include "mr.hh"
#include "qp.hh"

namespace pdp {
    GlobalVariables::GlobalVariables()
        : sms {"10.0.2.181", 23333},
        configs {
            .mode = Mode::_from_string_nocase(util::getenv_string("PDP_mode").value_or("PDP").c_str()),
            .rpc_mode = util::getenv_bool("PDP_rpc_mode").value_or(false),
            .is_server = util::getenv_bool("PDP_is_server").value_or(false),
            .server_mmap_dev = util::getenv_string("PDP_server_mmap_dev").value_or(""),
            .server_io_path = ServerIoPath::_from_string_nocase(util::getenv_string("PDP_server_io_path").value_or("tiering").c_str()),
            .server_memory_gb = util::getenv_int("PDP_server_memory_gb").value_or(0),
            .server_rpc_threads = util::getenv_int("PDP_server_rpc_threads").value_or(32),
            .server_page_state = util::getenv_bool("PDP_server_page_state").value_or(true),
            .read_magic_pattern = util::getenv_bool("PDP_read_magic_pattern").value_or(true),
            .promote_hotspot = util::getenv_bool("PDP_promote_hotspot").value_or(true),
            .promotion_window_ms = (uint32_t)util::getenv_int("PDP_promotion_window_ms").value_or(1000),
            .pull_page_bitmap = util::getenv_bool("PDP_pull_page_bitmap").value_or(true),
            .advise_local_mr = util::getenv_bool("PDP_advise_local_mr").value_or(false),
            .predict_remote_mr = util::getenv_bool("PDP_predict_remote_mr").value_or(false)
        }
    {
        // modify
        switch (configs.mode) {
            case Mode::PIN:
            case Mode::ODP: {
                configs.rpc_mode = false;
                configs.read_magic_pattern = false;
                configs.promote_hotspot = false;
                break;
            }
            case Mode::PDP: {
                configs.server_io_path = ServerIoPath::TIERING;
                configs.mode = Mode::PDP;
                configs.rpc_mode = false;
                configs.read_magic_pattern = true;
                configs.promote_hotspot = true;
                break;
            }
            case Mode::RPC: {
                configs.mode = Mode::RPC_MEMCPY;
                // fallthrough here
            }
            case Mode::RPC_MEMCPY: {
                configs.server_io_path = ServerIoPath::MEMCPY;
                configs.mode = Mode::PDP;
                configs.rpc_mode = true;
                configs.read_magic_pattern = true;
                configs.promote_hotspot = false;
                break;
            }
            case Mode::RPC_BUFFER: {
                configs.server_io_path = ServerIoPath::BUFFER;
                configs.mode = Mode::PDP;
                configs.rpc_mode = true;
                configs.read_magic_pattern = true;
                configs.promote_hotspot = false;
                break;
            }
            case Mode::RPC_DIRECT: {
                configs.server_io_path = ServerIoPath::DIRECT;
                configs.mode = Mode::PDP;
                configs.rpc_mode = true;
                configs.read_magic_pattern = true;
                configs.promote_hotspot = false;
                break;
            }
            case Mode::RPC_TIERING: {
                configs.server_io_path = ServerIoPath::TIERING;
                configs.mode = Mode::PDP;
                configs.rpc_mode = true;
                configs.read_magic_pattern = true;
                configs.promote_hotspot = false;
                break;
            }
            case Mode::RPC_TIERING_PROMOTE: {
                configs.server_io_path = ServerIoPath::TIERING;
                configs.mode = Mode::PDP;
                configs.rpc_mode = true;
                configs.read_magic_pattern = true;
                configs.promote_hotspot = true;
                break;
            }
            case Mode::MAGIC_MEMCPY: {
                configs.server_io_path = ServerIoPath::MEMCPY;
                configs.mode = Mode::PDP;
                configs.rpc_mode = false;
                configs.read_magic_pattern = true;
                configs.promote_hotspot = false;
                break;
            }
            case Mode::MAGIC_BUFFER: {
                configs.server_io_path = ServerIoPath::BUFFER;
                configs.mode = Mode::PDP;
                configs.rpc_mode = false;
                configs.read_magic_pattern = true;
                configs.promote_hotspot = false;
                break;
            }
            case Mode::MAGIC_DIRECT: {
                configs.server_io_path = ServerIoPath::DIRECT;
                configs.mode = Mode::PDP;
                configs.rpc_mode = false;
                configs.read_magic_pattern = true;
                configs.promote_hotspot = false;
                break;
            }
            case Mode::MAGIC_TIERING: {
                configs.server_io_path = ServerIoPath::TIERING;
                configs.mode = Mode::PDP;
                configs.rpc_mode = false;
                configs.read_magic_pattern = true;
                configs.promote_hotspot = false;
                break;
            }
            case Mode::PDP_MEMCPY: {
                configs.server_io_path = ServerIoPath::MEMCPY;
                configs.mode = Mode::PDP;
                configs.rpc_mode = false;
                configs.read_magic_pattern = true;
                configs.promote_hotspot = true;
            }

            default: break;
        }

        // if (configs.mode == +Mode::PIN && configs.server_memory_gb) {
        //     pr_err("ignore PDP_server_memory_gb in PIN mode!");
        //     configs.server_memory_gb = 0;
        // }
        if (configs.mode == +Mode::PIN) {
            ASSERT(configs.server_memory_gb == 0, "PDP_server_memory_gb should not be set in PIN mode.");
        }

        if (configs.mode == +Mode::PDP) {
            ASSERT(!configs.server_mmap_dev.empty(), "PDP_server_mmap_dev must be set!");
        }

        if (configs.promote_hotspot) {
            pr_warn("auto set PDP_server_page_state for PDP_promote_hotspot");
            configs.server_page_state = true;
        }

        if (configs.read_magic_pattern) {
            pr_warn("auto set PDP_pull_page_bitmap for PDP_read_magic_pattern");
            configs.pull_page_bitmap = true;
        }


        // print
        fmt_pr(info, "memcached: {}", sms.to_string());
        fmt_pr(info, "{}", configs.to_string());

        ctx_mgr = std::make_unique<ContextManager>();
        local_mr_mgr = std::make_unique<LocalMrManager>();
        remote_mr_mgr = std::make_unique<RemoteMrManager>();
        qp_mgr = std::make_unique<QpManager>();
    }
}
