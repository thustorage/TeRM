#!/usr/bin/env python3
import scripts.term_eval as term_eval

kwargs = {
    "name" : "figure-14b",
    "mode" : ["odp", "rpc", "pdp"],
    "server_memory_gb": [13, 26, 38, 51, 58, 0],
    "skewness_100": 99,
    "xlabel": "DRAM Ratio (%)",
    "xdata": ["20", "40", "60", "80", "90", "100"],
    "legend": ["ODP", "RPC", "TeRM"]
}

e = term_eval.Experiment(**kwargs)
e.run()
e.output()
