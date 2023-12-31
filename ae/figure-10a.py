#!/usr/bin/env python3
import scripts.term_eval as term_eval

kwargs = {
    "name": "figure-10a",
    "mode": ["rpc_memcpy", "rpc_buffer", "rpc_direct", "rpc_tiering", "rpc_tiering_promote", "pdp"],
    "sz_unit": "256",
    "xlabel": "",
    "xdata": ["Read 256B"],
    "legend": ["RPC", "RPC_buffer", "RPC_direct", "+tiering", "+hotspot", "+magic"]
}

e = term_eval.Experiment(**kwargs)
e.run()
e.output()
