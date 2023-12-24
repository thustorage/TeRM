#!/usr/bin/env python3
import scripts.term_eval as term_eval

kwargs = {
    "name": "figure-10c",
    "mode": ["rpc_memcpy", "rpc_buffer", "rpc_direct", "rpc_tiering", "pdp"],
    "sz_unit": "256",
    "write_percent" : 100,
    "xlabel": "",
    "xdata": ["Write 256B"],
    "legend": ["RPC", "RPC_buffer", "RPC_direct", "+tiering", "+hotspot"]
}

e = term_eval.Experiment(**kwargs)
e.run()
e.output()