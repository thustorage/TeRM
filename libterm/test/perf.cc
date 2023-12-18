#include "rdma.hh"
#include "util.hh"
#include "node.hh"
#include "zipf.hh"

#include <gflags/gflags.h>
#include <fmt/core.h>

#include <csignal>

DEFINE_int32(nr_nodes, 2, "number of total nodes, including the server and clients.");
DEFINE_bool(verify, false, "");
DEFINE_int32(nr_client_threads, 1, "# client threads");
DEFINE_string(sz_unit, "64", "access unit in bytes (OK with k,m,g suffix)");
DEFINE_string(sz_server_mr, "32g", "server mr size in bytes (OK with k,m,g suffix)");
DEFINE_int32(running_seconds, 120, "running seconds");
DEFINE_int32(node_id, -1, "node id (0: server, ...: client)");
DEFINE_int32(skewness_100, 99, "skewness * 100, -1: seq");
DEFINE_uint32(write_percent, 100, "write percent");
DEFINE_uint32(hotspot_switch_second, 0, "the second to switch hotspot");

constexpr int kNrServerThreads = 1;
constexpr size_t kClientMrSize = SZ_4M;
constexpr bool kClientMmap = false;
constexpr bool kClientOdp = false;
constexpr bool kServerMmap = true;
constexpr bool kServerOdp = true;

std::atomic_bool g_should_stop{};
std::string g_server_mmap_dev = util::getenv_string("PDP_server_mmap_dev").value_or("");
std::unique_ptr<util::LatencyDistribution> g_lat_dis;
rdma::Node::Uptr g_node;

void signal_int_handler(int sig)
{
    g_should_stop = true;
    util::print_stacktrace();

    if (g_node) {
        if (!g_node->is_server()) {
        }
        g_node->unregister_node();
        g_node = nullptr;
    }

    // reset to the default for clean-up
    signal(sig, SIG_DFL);
    raise(sig);
}

static bool check_data(void *addr, size_t length, uint64_t expected_base)
{
    ASSERT((uint64_t)addr % 8 == 0, "addr is not aligned to 8.");

    for (size_t i = 0; i < length; i += sizeof(uint64_t)) {
        uint64_t value = *(uint64_t *)((char *)addr + i);
        uint64_t expected = expected_base + i;
        if (value != expected) {
            return false;
        }
    }
    return true;
}

static void set_data(void *addr, size_t length, uint64_t base, bool print_progress)
{
    ASSERT((uint64_t)addr % 8 == 0, "addr is not aligned to 8.");
    // #pragma omp parallel for
    for (size_t i = 0; i < length; i += sizeof(uint64_t)) {
        if (print_progress && i % SZ_1G == 0) {
            pr_info("set %lu GB.", i / SZ_1G);
        }
        *(uint64_t *)((char *)addr + i) = base + i;
    }
}

void initialize()
{
    g_node = std::make_unique<rdma::Node>(FLAGS_nr_nodes, FLAGS_node_id);
    g_node->register_node();

    if (g_node->is_server()) {
        std::cout << "Server" << std::endl;
        std::cout << "prepare" << std::endl;
        auto ctx = std::make_unique<rdma::Context>(0);

        ASSERT(!g_server_mmap_dev.empty(), "PDP_server_mmap_dev must be set!");
        for (int i = 0; i < kNrServerThreads; i++) {
            rdma::Allocation alloc{(size_t)util::stoll_suffix(FLAGS_sz_server_mr), g_server_mmap_dev, !FLAGS_verify};
            if (FLAGS_verify) { 
                madvise(alloc.addr(), alloc.size(), MADV_SEQUENTIAL);
                set_data((char *)alloc.addr(), alloc.size(), 0, true);
                madvise(alloc.addr(), alloc.size(), MADV_RANDOM);
            }
            auto local_mr = std::make_unique<rdma::LocalMemoryRegion>(util::make_observer(ctx.get()), std::move(alloc), kServerOdp);
            g_node->threads_.emplace_back(util::make_observer(g_node.get()), util::make_observer(ctx.get()), i, FLAGS_nr_nodes, FLAGS_nr_client_threads, std::move(local_mr));
            g_node->threads_.back().prepare_and_push_rc();
        }

        g_node->ctxs_.emplace_back(std::move(ctx));

        g_node->wait_for_all_nodes_prepared();
        for (int i = 0; i < kNrServerThreads; i++) {
            g_node->threads_[i].pull_and_connect_rc();
        }
    } else {
        std::cout << "Client" << std::endl;
        std::cout << "prepare" << std::endl;

        for (int i = 0; i < FLAGS_nr_client_threads; i++) {
            auto ctx = std::make_unique<rdma::Context>(0);
            auto local_mr = std::make_unique<rdma::LocalMemoryRegion>(util::make_observer(ctx.get()), rdma::Allocation{kClientMrSize}, kClientOdp);
            g_node->threads_.emplace_back(util::make_observer(g_node.get()), util::make_observer(ctx.get()), i, 1, kNrServerThreads, std::move(local_mr));
            g_node->threads_.back().prepare_and_push_rc();
            g_node->ctxs_.emplace_back(std::move(ctx));
        }
        g_node->wait_for_all_nodes_prepared();
        for (int i = 0; i < FLAGS_nr_client_threads; i++) {
            g_node->threads_[i].pull_and_connect_rc();
        }
    }

    g_node->wait_for_all_nodes_connected();
}

void cleanup()
{
    // clean-up for the next execution
    g_node->unregister_node();
}

void client_test(const std::stop_token &stop_token, int thread_id)
{
    auto &thread = g_node->threads_[thread_id];
    auto conn = thread.get_data_conn(0, 0);
    auto *local_mr = conn->local_mr();
    auto *remote_mr = conn->remote_mr();
    auto sz_unit = util::stoll_suffix(FLAGS_sz_unit);
    static util::LatencyRecorder lr_send("send");
    static util::LatencyRecorder lr_wait("wait");

    uint64_t nr_units = remote_mr->length() / sz_unit;

    util::Schedule::bind_client_fg_cpu(thread_id);

    ASSERT(FLAGS_skewness_100 < 100, "FLAGS_skewness_100 must < 100!");
    bool seq = false;
    if (FLAGS_skewness_100 < 0) {
        seq = true;
        FLAGS_skewness_100 = 0;
    }
    double theta = double(FLAGS_skewness_100) / 100;
    util::ZipfGen zipf_gen(nr_units, theta, FLAGS_node_id * FLAGS_nr_client_threads + thread_id);

    unsigned int seed = thread_id;

    auto switch_tp = std::chrono::steady_clock::now() + std::chrono::seconds(FLAGS_hotspot_switch_second);

    for (uint64_t i = 0; /*i < 16 * sz_unit*/; i += sz_unit) {
        lr_send.begin_one();
        uint64_t ts = util::wall_time_ns();

        if (stop_token.stop_requested()) break;

        uint64_t v = zipf_gen.next();
        if (FLAGS_hotspot_switch_second) {
            if (std::chrono::steady_clock::now() > switch_tp) {
                v = nr_units - 1 - v;
            }
        }
        v = util::hash_u64(v) % nr_units;
        if (seq) {
            v = (i / sz_unit) % nr_units;
        }

        uint64_t remote_offset = v * sz_unit;

        uint64_t local_addr = local_mr->addr64() + i % local_mr->length();
        uint64_t remote_addr = remote_mr->addr64() + remote_offset;

        auto wr = rdma::Wr()
            .set_sg(local_addr, sz_unit, local_mr->lkey())
            .set_rdma(remote_addr, remote_mr->rkey())
            .set_signaled(true);
        
        uint64_t expected_base = util::hash_u64(remote_offset);

        bool is_read = (rand_r(&seed) % 100) >= FLAGS_write_percent;
        if (is_read) {
            wr.set_op_read();
        } else {
            wr.set_op_write();
            if (FLAGS_verify) {
                set_data((void *)local_addr, sz_unit, expected_base, false);
            }
        }

        conn->post_send_wr(&wr);
        lr_wait.begin_one();

        lr_send.end_one();
        struct ibv_wc wc{};
        conn->wait_send(&wc);
        assert(wc.status == IBV_WC_SUCCESS);

        if (FLAGS_verify) {
            if (is_read) {
                bool ok = check_data((void *)local_addr, sz_unit, remote_offset);
                ASSERT(ok, "check error");
            } else {
                memset((void *)local_addr, 0x34, sz_unit);
                auto read_wr = rdma::Wr().set_op_read()
                    .set_sg(local_addr, sz_unit, local_mr->lkey())
                    .set_rdma(remote_addr, remote_mr->rkey())
                    .set_signaled(true);
                conn->post_send_wr(&read_wr);
                conn->wait_send(&wc);
                ASSERT(wc.status == IBV_WC_SUCCESS, "");

                bool ok = check_data((void *)local_addr, sz_unit, expected_base);
                ASSERT(ok, "check error");
            }
        }

        lr_wait.end_one();

        uint64_t lat = util::wall_time_ns() - ts;
        g_lat_dis->record(thread_id, lat);
    }
}

static void check_flags()
{
    ASSERT(FLAGS_skewness_100 < 100, "skewness_100 must < 100");
    ASSERT(FLAGS_write_percent <= 100, "write_percent must <= 100");
    ASSERT(FLAGS_node_id >= 0 && FLAGS_node_id < FLAGS_nr_nodes, "FLAGS_node_id must between 0 and nr_nodes");
}

static void system_cmd_func(const std::stop_token &stop_token, const std::string &cmd)
{

    while (!stop_token.stop_requested()) {
        using namespace std::chrono_literals;
        auto tp = std::chrono::steady_clock::now();
        std::ignore = system(cmd.c_str());
        std::this_thread::sleep_until(tp + 1s);
    }
}

int main(int argc, char *argv[])
{

    gflags::SetUsageMessage(argv[0]);
    gflags::ParseCommandLineFlags(&argc, &argv, true);

    signal(SIGINT, signal_int_handler);
    signal(SIGSEGV, signal_int_handler);

    check_flags();

    initialize();

    if (g_node->is_server()) {
        pr_info("running...");
        const std::string cmd = fmt::format(R"(top -bc -n1 -w512 | grep -e "{}" -e "mlx5_ib_page" | grep -v grep; cat /proc/vmstat | egrep "writeback|dirty")", argv[0]);

        // std::jthread system_cmd_thread([&cmd](const std::stop_token &stop_token) {
        //     system_cmd_func(stop_token, cmd);
        // });
        sleep(FLAGS_running_seconds + 2);
    } else {
        pr_info("# client threads=%d", FLAGS_nr_client_threads);

        std::vector<std::jthread> test_threads(FLAGS_nr_client_threads);
        g_lat_dis = std::make_unique<util::LatencyDistribution>(FLAGS_nr_client_threads);

        for (int i = 0; i < FLAGS_nr_client_threads; i++) {
            test_threads[i] = std::jthread(client_test, i);
        }

        double elapsed = 0.0;
        util::Schedule::bind_client_fg_cpu(FLAGS_nr_client_threads + 1);

        uint32_t running_second = 0;
        while (!g_should_stop) {
            using namespace std::chrono_literals;
            auto tp = std::chrono::steady_clock::now();
            if (running_second) {
                pr_emph("epoch %u (%.2lfs): %s", running_second, elapsed, g_lat_dis->report_and_clear().c_str());
            }
            running_second++;
            if (running_second > FLAGS_running_seconds) {
                break;
            }
            std::this_thread::sleep_until(tp + 1s);

            auto d = std::chrono::steady_clock::now() - tp;
            elapsed = std::chrono::duration<double>(d).count();
        }
    }

    cleanup();
    return 0;
}
