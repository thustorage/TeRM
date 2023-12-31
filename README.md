# TeRM: Extending RDMA-Attached Memory with SSD

This is the open-source repository for our paper
 **TeRM: Extending RDMA-Attached Memory with SSD** to appear on [FAST'24](https://www.usenix.org/conference/fast24/presentation/yang-zhe).

Notably, the codename of TeRM is `PDP`.

# Directory structure
- `ae`: artifact evaluation scripts. `figure-*.py` scripts reproduce results in our paper.
- `driver`: modification to the ofed driver. The patch should be parsed and applied manually.
- `libterm`: the userspace shared library. We provide `CMakeLists.txt` to build it.

# How to build
## Environment

- OS: Ubuntu 22.04.2 LTS
- Kernel: Linux 5.19.0-50-generic
- OFED driver: 5.8-2.0.3

We recommend the same environment used in our development. 
You may need to customize the source code for different enviroment.

## Dependencies
```
sudo apt install libfmt-dev libaio-dev libboost-coroutine-dev libmemcached-dev libgoogle-glog-dev libgflags-dev
```

## Settings

We hard coded some settings in the source code. Please modify them according to your cluster settings.

1. memcached.
TeRM uses memcached to synchronize cluster metadata.
Please install memcached in your cluster and modify the ip and port in `ae/bin/reset-memc.sh`, `libterm/ibverbs-pdp/global.cc`, and `libterm/include/node.hh`.

2. CPU affinity.
The source code is in `class Schedule` of file `libterm/include/util.hh`.
Please modify the constants according to your CPU hardware.

## Build the driver
There are two ways to build the driver. We provide an out-of-the-box driver zip file in the second choice.

1. Download the source code of the driver from the official website.
Apply official backport batches first and then patch the modifications listed in `driver/driver.patch`.
Then, build the driver.
Please note that, we apply minimum number of patches, instead of all patches, that make it work for our environment. One shall not `git apply` the `driver/driver.patch` directly, because line numbers may differ. One should parse and patch it manually.

2. Use `driver/mlnx-ofed-kernel-5.8-2.0.3.0.zip`. Unzip it and run the contained `build.sh`.

## Build libterm
We provide `CMakeLists.txt` for building.
It produces two outputs, the userspace shared library `libpdp.so` and a program `perf`.
Please copy two files to `ae/bin` before running AE scripts.
```
$ cd libterm
$ mkdir -p build && cd build
$ cmake .. -DCMAKE_BUILD_TYPE=Release # Release for compiler optimizations and high performance
$ make -j
```

# How to use
1. Replace the modified driver `*.ko` files and restart the `openibd` service.
2. Restart the `memcached` instance. We provide a script `ae/bin/reset-memc.sh` to do so.
3. `mmap` an SSD in the RDMA program with `MAP_SHARED` and `ibv_reg_mr` the memory area as an `ODP MR`.
4. Set `LD_PRELOAD=libpdp.so` to enable TeRM. Also set enviroment variables `PDP_server_mmap_dev=nvmeXnY` for the SSD backend and `PDP_server_memory_gb=Z` for the size of the mapped area. Set `PDP_is_server=1` if and only if for the server side.
5. Run the RDMA application.

libterm accepts a series of environment variables for configuration. Please refer to `libterm/ibverbs-pdp/global.cc` for more details.

If you have further questions and interests about the repository, please feel free to propose an issue or contact me via email (zheyang.zy AT outlook.com). You can find my github at [yzim](https://github.com/yzim).
