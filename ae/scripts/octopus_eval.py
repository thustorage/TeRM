import os
import re
    
def run_test(mode = "pdp", write = 0, unit_size = "4k", log_dir = "."):
    server_memory_gb = 0 if mode == "pin" else 18
    
    os.system(f"mkdir -p {log_dir}")
    log_file = f"{log_dir}/write.{write},mode.{mode},unit.{unit_size}.log"
    
    test_file_content = f"""
        init_cmd = "/home/yz/workspace/TeRM/ae/scripts/reset-memc.sh"
        prefix_cmd = \"\"\"echo 3 > /proc/sys/vm/drop_caches; 
        echo 100 > /proc/sys/vm/dirty_ratio; 
        echo 0 > /proc/sys/kernel/numa_balancing; 
        systemctl start opensmd;
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
        cmd = "mpiexec --allow-run-as-root --np 32 /bin/bash -c './mpibw --write={write} --unit_size={unit_size} --seconds=70 --zipf_theta=0.99 --file_size=1g' "
    """
    with open(f"{log_dir}/test.toml", "w") as f:
        f.write(test_file_content)
        
    cmd = f"""
        date | tee -a {log_file};
        /home/yz/workspace/TeRM/ae/scripts/bootstrap-octopus.py -f {log_dir}/test.toml 2>&1 | tee -a {log_file}
    """
    os.system(cmd)
    
def extract_result(mode = "pdp", write = 0, unit_size = "4k", log_dir = "."):
    result = 0.0
        
    log_file = f"{log_dir}/write.{write},mode.{mode},unit.{unit_size}.log"
    with open(log_file, "r") as f:
        while True:
            line = f.readline()
            if not line:
                break
            if "Bandwidth = " not in line:
                continue

            tokens = re.findall("Bandwidth = (\d+\.\d+)", line)
            result = float(tokens[0])
            
        return result

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
    
    
def output_batch(name : str, ydata = [], **kwargs):
    log_dir = f"./output/log/{name}"
    vector_kwargs, scalar_kwargs = split_kwargs(kwargs)
    from itertools import product
    l = list(dict(zip(vector_kwargs.keys(), values)) for values in product(*vector_kwargs.values()))
    output = ""
    
    for idx, kv in enumerate(l):
        result = extract_result(**kv, **scalar_kwargs, log_dir=log_dir)
        str = f"({idx + 1}/{len(l)}) {kv}: bandwidth (MB/s)="
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
    plt.ylabel("Bandwidth (MB/s)")
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
    