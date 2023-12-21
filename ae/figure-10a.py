#!/usr/bin/env python3
import scripts.term_eval as term_eval

args = {
    "name" : "figure-10a",
    "mode" : ["rpc_memcpy", "rpc_buffer", "rpc_direct", "rpc_tiering", "rpc_tiering_promote", "pdp"],
    "sz_unit": ["256"],
    "xlabel": "",
    "xdata": ["Read 256B"],
}

e = term_eval.Experiment(args)
e.run()
e.output()