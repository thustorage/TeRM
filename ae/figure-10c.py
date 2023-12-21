#!/usr/bin/env python3
import scripts.term_eval as term_eval

args = {
    "name" : "figure-10c",
    "mode" : ["rpc_memcpy", "rpc_buffer", "rpc_direct", "rpc_tiering", "pdp"],
    "sz_unit": ["256"],
    "write_percent" : [100],
    "xlabel": "",
    "xdata": ["Write 256B"]
}

e = term_eval.Experiment(args)
e.run()
e.output()