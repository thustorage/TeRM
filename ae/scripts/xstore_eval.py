import os
import re
    
def run_test(mode : str = "pdp", zipf_theta = 0, log_dir : str = "."):
    server_memory_gb = 0 if mode == "pin" else 16
    
    os.system(f"mkdir -p {log_dir}")
    log_file = f"{log_dir}/mode.{mode},zipf_theta.{zipf_theta}.log"
    
    test_file_content = f"""
        init_cmd = "/home/yz/workspace/TeRM/ae/scripts/reset-memc.sh"
        prefix_cmd = \"\"\"echo 3 > /proc/sys/vm/drop_caches; 
        echo 100 > /proc/sys/vm/dirty_ratio; 
        echo 0 > /proc/sys/kernel/numa_balancing; 
        export LD_PRELOAD="/home/yz/workspace/TeRM/ae/bin/libpdp.so";
        export LD_LIBRARY_PATH="/home/yz/workspace/TeRM/ae/bin/xstore"; 
        export PDP_server_rpc_threads=16 PDP_server_mmap_dev=nvme4n1 PDP_server_memory_gb={server_memory_gb};
        export PDP_mode={mode};\"\"\"
        suffix_cmd = "--need_hash=false --cache_sz_m=327680 --server_host=node184 --total_accts=100'000'000 --eval_type=sc --workloads=ycsbc --concurrency=1 --use_master=true --uniform=false --running_time=60 --zipf_theta={zipf_theta} \
--undefok=concurrency,workloads,eval_type,total_accts,server_host,cache_sz_m,need_hash,use_master,uniform,running_time,zipf_theta"

        ## server process
        [[pass]]
        name = "s0"
        host = "node184"
        path = "/home/yz/workspace/TeRM/ae/bin/xstore"
        cmd = \"\"\"../../scripts/ins-pch.sh;
        systemctl restart openibd;
        ../../scripts/limit-mem.sh;
        export PDP_is_server=1;
        ./fserver --threads=16 --db_type=ycsb --id=0 --ycsb_num=100'000'000 --no_train=false --step=2 --model_config=./ycsb-model.toml --enable_odp=true\"\"\"

        ## clients
        [[pass]]
        name = "c0"
        host = "node166"
        path = "/home/yz/workspace/TeRM/ae/bin/xstore"
        cmd = "./ycsb --threads=32 --id=1"

        [[pass]]
        name = "c1"
        host = "node168"
        path = "/home/yz/workspace/TeRM/ae/bin/xstore"
        cmd = "./ycsb --threads=32 --id=2"

        # master process to collect results
        [[pass]]
        name = "m0"
        host = "node181"
        path = "/home/yz/workspace/TeRM/ae/bin/xstore"
        cmd = "sleep 5; ./master --client_config=./cs.toml --epoch=90 --nclients=2"
    """
    with open(f"{log_dir}/test.toml", "w") as f:
        f.write(test_file_content)
        
    cmd = f"""
        date | tee -a {log_file};
        /home/yz/workspace/TeRM/ae/scripts/bootstrap.py -f {log_dir}/test.toml 2>&1 | tee -a {log_file}
    """
    os.system(cmd)
    
def extract_result(mode : str = "pdp", zipf_theta = 0, log_dir : str = "."):
    log_file = f"{log_dir}/mode.{mode},zipf_theta.{zipf_theta}.log"
    with open(log_file, "r") as f:
        result_list = []
        current_result_list = []
        
        while True:
            line = f.readline()
            if not line:
                break
            if "at epoch" not in line:
                continue
            if "epoch 0 " in line:
                result_list = current_result_list
                current_result_list = []

            line = line.replace(",", "")
            tokens = re.findall(f"thpt: (\d+\.\d+)", line)
            current_result_list.append(float(tokens[0]))
        result_list = current_result_list
        avg_v = avg(result_list)
        
    return avg_v

def avg(l : list):
    return sum(l) / len(l)

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
        print(f"### Running ({idx + 1}/{len(l)}): {kv} ###")
        run_test(**kv, **scalar_kwargs, log_dir=log_dir)
    
    
def output_batch(name : str, ydata = [], **kwargs):
    log_dir = f"./output/log/{name}"
    vector_kwargs, scalar_kwargs = split_kwargs(kwargs)
    from itertools import product
    l = list(dict(zip(vector_kwargs.keys(), values)) for values in product(*vector_kwargs.values()))
    output = ""
    
    for idx, kv in enumerate(l):
        result = extract_result(**kv, **scalar_kwargs, log_dir=log_dir)
        str = f"({idx + 1}/{len(l)}) {kv}: throughput (ops/s)="
        ydata.append(result)
        str += f"{format_digits(result)}"
        output += str + "\n"
    
    print(output)
    with open(f"./output/{name}.txt", "w") as f:
        f.write(output)


def draw_figure(name : str, xlabel : str, xdata : list, ydata : list, legend : list[str]):
    import matplotlib.pyplot as plt
    xlen = len(xdata)

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
    