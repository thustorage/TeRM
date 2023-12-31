import os
import re

def run_test(write_percent = 0, skewness_100 = 99, server_memory_gb = 32, ssd = "nvme4n1", 
             mode = "pdp", nr_server_threads = 16, nr_client_threads = 64, sz_unit = "4k", 
             verify = 0, dynamic = False, log_dir="."):
    if mode == "pin":
        server_memory_gb = 0
    running_seconds = 120 if dynamic else 70
    
    os.system(f"mkdir -p {log_dir}")
    log_file = f"{log_dir}/write.{write_percent},skew.{skewness_100},pm.{server_memory_gb},ssd.{ssd},mode.{mode},sth.{nr_server_threads},cth.{nr_client_threads},unit.{sz_unit}.log"
    
    comment_mark = "#" if nr_client_threads == 1 else ""
    
    str = f"""
        init_cmd = "/home/yz/workspace/TeRM/ae/scripts/reset-memc.sh"
        prefix_cmd = \"\"\"echo 3 > /proc/sys/vm/drop_caches; 
        echo 100 > /proc/sys/vm/dirty_ratio; 
        echo 0 > /proc/sys/kernel/numa_balancing; 
        systemctl start opensmd;
        export LD_PRELOAD=/home/yz/workspace/TeRM/ae/bin/libpdp.so; 
        export PDP_server_rpc_threads={nr_server_threads} PDP_server_mmap_dev={ssd} PDP_server_memory_gb={server_memory_gb};
        export PDP_mode={mode};\"\"\"
        suffix_cmd = "--nr_nodes={2 if nr_client_threads == 1 else 3} --running_seconds={running_seconds} --sz_server_mr=64g --write_percent={write_percent} --verify={verify} --skewness_100={skewness_100} --sz_unit={sz_unit} --nr_client_threads={1 if nr_client_threads == 1 else int(nr_client_threads / 2)} --hotspot_switch_second={60 if dynamic else 0};"

        ## server process
        [[pass]]
        name = "s0"
        host = "node184"
        path = "/home/yz/workspace/TeRM/ae"
        cmd = \"\"\"./scripts/ins-pch.sh;
        systemctl restart openibd;
        ./scripts/limit-mem.sh 0;
        export PDP_is_server=1;
        ./bin/perf --node_id=0\"\"\"

        ## below are clients
        [[pass]]
        name = "c0"
        host = "node166"
        path = "/home/yz/workspace/TeRM/ae"
        cmd = "./bin/perf --node_id=1"

        {comment_mark} [[pass]]
        {comment_mark} name = "c1"
        {comment_mark} host = "node168"
        {comment_mark} path = "/home/yz/workspace/TeRM/ae"
        {comment_mark} cmd = "./bin/perf --node_id=2"

    """
    with open(f"{log_dir}/test.toml", "w") as f:
        f.write(str)
        
    cmd = f"""
        date | tee -a {log_file};
        /home/yz/workspace/TeRM/ae/scripts/bootstrap.py -f {log_dir}/test.toml 2>&1 | tee -a {log_file}
    """
    os.system(cmd)
    
def run_octopus(mode = "pdp", rw = 0, unit_size = "4k", log_dir = "."):
    server_memory_gb = 0 if mode == "pin" else 18
    
    os.system(f"mkdir -p {log_dir}")
    log_file = f"{log_dir}/write.{rw},mode.{mode},unit.{unit_size}.log"
    
    test_file_content = f"""
        init_cmd = "/home/yz/workspace/TeRM/ae/scripts/reset-memc.sh"
        prefix_cmd = \"\"\"echo 3 > /proc/sys/vm/drop_caches; 
        echo 100 > /proc/sys/vm/dirty_ratio; 
        echo 0 > /proc/sys/kernel/numa_balancing; 
        export LD_PRELOAD="/home/yz/workspace/TeRM/ae/bin/libpdp.so /home/yz/workspace/TeRM/ae/bin/octopus/libnrfsc.so"; 
        export PDP_server_rpc_threads=16 PDP_server_mmap_dev=nvme4n1 PDP_server_memory_gb={server_memory_gb};
        export PDP_mode={mode};\"\"\"
        suffix_cmd = ""

        ## server process
        [[pass]]
        name = "s0"
        host = "node184"
        path = "/home/yz/workspace/TeRM/ae/bin/octopus"
        cmd = \"\"\"../../scripts/ins-pch.sh;
        systemctl restart openibd;
        ../../scripts/limit-mem.sh;
        export PDP_is_server=1;
        ./dmfs\"\"\"

        ## below are clients
        [[pass]]
        name = "c0"
        host = "node166"
        path = "/home/yz/workspace/TeRM/ae/bin/octopus"
        cmd = "mpiexec --allow-run-as-root --np 32 /bin/bash -c './mpibw --write={rw} --unit_size={unit_size} --seconds=70 --zipf_theta=0.99 --file_size=1g' "
    """
    with open(f"{log_dir}/test.toml", "w") as f:
        f.write(test_file_content)
        
    cmd = f"""
        date | tee -a {log_file};
        /home/yz/workspace/TeRM/ae/scripts/bootstrap-octopus.py -f {log_dir}/test.toml 2>&1 | tee -a {log_file}
    """
    os.system(cmd)
    
def extract_throughput(write_percent = 0, skewness_100 = 99, server_memory_gb = 32, 
                    ssd = "nvme4n1", mode = "pdp", nr_server_threads = 16, 
                    nr_client_threads = 64, sz_unit = "4k", dynamic = False, log_dir = "."):
    
    if mode == "pin":
        server_memory_gb = 0
        
    sampling_start = 0 if dynamic else 5
    sampling_seconds = 120 if dynamic else 60

    log_file = f"{log_dir}/write.{write_percent},skew.{skewness_100},pm.{server_memory_gb},ssd.{ssd},mode.{mode},sth.{nr_server_threads},cth.{nr_client_threads},unit.{sz_unit}.log"
    with open(log_file, "r") as f:
        thpt_list = []
        current_thpt_list = []
        
        new_flag = False
        dual_client = False
        while True:
            line = f.readline()
            if not line:
                break
            if "epoch" not in line:
                continue
            if "epoch 1 " in line:
                if not new_flag:
                    thpt_list = current_thpt_list
                    current_thpt_list = []
                else:
                    dual_client = True
                new_flag = True
            else:
                new_flag = False

            line = line.replace(",", "")
            tokens = re.findall("cnt=(\d+)", line)
            current_thpt_list.append(int(tokens[0]))
        thpt_list = current_thpt_list
        if dual_client:
            thpt_list = [thpt_list[i] + thpt_list[i + 1] for i in range(0, 2 * (sampling_start + sampling_seconds), 2)]
        thpt_list = thpt_list[(sampling_start) : (sampling_start + sampling_seconds)]
        return thpt_list
    
def extract_latency(write_percent = 0, skewness_100 = 99, server_memory_gb = 32, 
                    ssd = "nvme4n1", mode = "pdp", nr_server_threads = 16, 
                    nr_client_threads = 64, sz_unit = "4k", 
                    sampling_start = 5, sampling_seconds = 60, label="p99", log_dir = "."):
    
    if mode == "pin":
        server_memory_gb = 0
    
    log_file = f"{log_dir}/write.{write_percent},skew.{skewness_100},pm.{server_memory_gb},ssd.{ssd},mode.{mode},sth.{nr_server_threads},cth.{nr_client_threads},unit.{sz_unit}.log"
    with open(log_file, "r") as f:
        result_list = []
        current_result_list = []
        
        new_flag = False
        dual_client = False
        while True:
            line = f.readline()
            if not line:
                break
            if "epoch" not in line:
                continue
            if "epoch 1 " in line:
                if not new_flag:
                    result_list = current_result_list
                    current_result_list = []
                else:
                    dual_client = True
                new_flag = True
            else:
                new_flag = False

            line = line.replace(",", "")
            tokens = re.findall(f"{label}=(\d+)", line)
            current_result_list.append(int(tokens[0]))
        result_list = current_result_list
        if dual_client:
            result_list = [(result_list[i] + result_list[i + 1]) / 2 for i in range(0, 2 * (sampling_start + sampling_seconds), 2)]
        result_list = result_list[(sampling_start) : (sampling_start + sampling_seconds)]
        result_list = [i for i in result_list if i < 120_000_000_000]
        return result_list
    

def avg(l):
    v = sum(l) / len(l)
    return v

def format_digits(n):
    return f"{n:,.2f}"

def split_kwargs(kwargs):
    vector_kwargs = {}
    scalar_kwargs = {}
    for k, v in kwargs.items():
        if type(v) == list:
            vector_kwargs[k] = v
        else:
            scalar_kwargs[k] = v
    return vector_kwargs, scalar_kwargs
    

def run_batch(name : str, **kwargs):
    log_dir = f"./output/log/{name}"
    os.system(f"mkdir -p {log_dir}")
        
    vector_kwargs, scalar_kwargs = split_kwargs(kwargs)
    
    from itertools import product
    l = list(dict(zip(vector_kwargs.keys(), values)) for values in product(*vector_kwargs.values()))
    for idx, kv in enumerate(l):
        str = f"### Running ({idx + 1}/{len(l)}): {kv} ###"
        with open("./output/current-running.txt", "w") as f:
            f.write(f"{name}\n{str}\n")
        print(str)
        run_test(**kv, **scalar_kwargs, log_dir=log_dir)
    
    
def output_batch(name : str, dynamic = False, ydata = [],
                  **kwargs):
    log_dir = f"./output/log/{name}"
    vector_kwargs, scalar_kwargs = split_kwargs(kwargs)
    from itertools import product
    l = list(dict(zip(vector_kwargs.keys(), values)) for values in product(*vector_kwargs.values()))
    output = ""
    
    for idx, kv in enumerate(l):
        thpt_list = extract_throughput(**kv, **scalar_kwargs, dynamic=dynamic, log_dir=log_dir)
        str = f"({idx + 1}/{len(l)}) {kv}: throughput (ops/s)="
        if dynamic:
            str += f"{thpt_list}"
            ydata.append(thpt_list)
        else:
            avg_v = avg(thpt_list)
            ydata.append(avg_v)
            str += f"{format_digits(avg_v)}"
        output += str + "\n"
    
    print(output)
    with open(f"./output/{name}.txt", "w") as f:
        f.write(output)


def draw_figure(name : str, xlabel : str, xdata : list, ydata : list, legend : list[str]):
    import matplotlib.pyplot as plt
    xlen = len(xdata)
    if name == "figure-11":
        plt.plot(xdata, ydata[0], label = legend[0])
        plt.plot(xdata, ydata[1], label = legend[1])
        plt.plot(xdata, ydata[2], label = legend[2])
        plt.plot(xdata, ydata[3], label = legend[3])
    else:
        for idx, label in enumerate(legend):
            match label:
                case "PIN":
                    plt.plot(xdata, ydata[idx * xlen : (idx + 1) * xlen], "-D", mfc="w", label = "PIN")
                case "ODP":
                    plt.plot(xdata, ydata[idx * xlen : (idx + 1) * xlen], "-^", mfc="w", label = "ODP")
                case "RPC":
                    plt.plot(xdata, ydata[idx * xlen : (idx + 1) * xlen], "-x", mfc="w", label = "RPC")
                case "TeRM":
                    plt.plot(xdata, ydata[idx * xlen : (idx + 1) * xlen], "-o", label = "TeRM")
                case _:
                    plt.plot(xdata, ydata[idx * xlen : (idx + 1) * xlen], "-o", label = label)
    plt.xlabel(xlabel)
    plt.ylabel("Throughput (ops/s)")
    plt.legend()
    print(f"written {name}.png")
    plt.savefig(f"./output/{name}.png")
    
class Experiment:
    def __init__(self, name : str,
                 xlabel: str, xdata: list, legend: list[str],
                 **kwargs):
        self.name = name
        self.xlabel = xlabel
        self.xdata = xdata
        self.ydata = []
        self.legend = legend
        self.kwargs = kwargs

    def run(self):
        run_batch(name=self.name, **self.kwargs)
    
    def output(self):
        output_batch(name=self.name, ydata=self.ydata, **self.kwargs)
        draw_figure(name=self.name, xlabel=self.xlabel, xdata=self.xdata, ydata=self.ydata, legend=self.legend)
    