#!/usr/bin/env python3
import scripts.term_eval as term_eval

kwargs = {
    "name": "figure-10d",
    "mode": ["rpc_memcpy", "rpc_buffer", "rpc_direct", "rpc_tiering", "pdp"],
    "sz_unit": "4k",
    "write_percent" : 100,
    "xlabel": "",
    "xdata": ["Write 4KB"],
    "legend": ["RPC", "RPC_buffer", "RPC_direct", "+tiering", "+hotspot"]
}

e = term_eval.Experiment(**kwargs)
e.run()
e.output()
