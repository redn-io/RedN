RedN is a framework that allows arbitrary offloads to RDMA NICs showcasing that they are, in fact, Turing complete. Our NSDI 2022 [paper](https://wreda.github.io/papers/redn-nsdi22.pdf) describes this framework in detail.

To cite us, you can use the following BibTex entry:
```
@inproceedings {redn,
author = {Waleed Reda and Marco Canini and Dejan Kosti{\'c} and Simon Peter},
title = {{RDMA} is Turing complete, we just did not know it yet!},
booktitle = {19th USENIX Symposium on Networked Systems Design and Implementation (NSDI 22)},
year = {2022},
address = {Renton, WA},
url = {https://www.usenix.org/conference/nsdi22/presentation/reda},
publisher = {USENIX Association},
month = apr,
}
```

## Features of RedN:
 * Turing complete: Provides RDMA implementations for high-level programming constructs (e.g. if and while statements)
 * Fully offloads access to popular data structures on remote servers (e.g. hash tables, linked lists)
 * Accelerates Memcached lookup performance (by up to 35x)
 * Supports Infiniband and RoCE

## Requirements

 * Hardware: Mellanox ConnectX-5 / ConnectX-6 NIC
    * Other models that use the `mlx5` driver may also be supported (subject to further testing).
 * Drivers: MLNX_OFED 4.7-1.0.0.1 installation
    * `TODO:` We are currently working on supporting other versions.  
 * Toolchain: GCC compiler (version 4.7+)

## RedN quickstart
 * Build the project:
    * Go to the root directory and run `make`.
* Modify the NIC's firmware (warning: use this at your own discretion)
    * `cd conf` and run `./disable_wqe_checks.sh <device_name>`
    * By default the `device_name` is set to `mlx5_1`
*  Build and run benchmarks:
    * `cd bench/micro` and then run `make`. To execute a hash table lookup offload:
    * On the server run `./hash_bench`
    * On the client run `./hash_bench <peer_address> <iters>`


## Contact
Waleed Reda (waleedreda@hotmail.com)

## License
This software is provided under the terms of the [GNU General Public License 2](https://www.gnu.org/licenses/gpl-2.0.html).

