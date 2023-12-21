#!/usr/bin/env python3
import scripts.term_eval as term_eval

args = {
    "name" : "figure-14b",
    "mode" : ["odp", "rpc", "pdp"],
    "server_memory_gb": [13, 26, 38, 51, 58, 0],
    "skewness_100": [99],
    "xlabel": "DRAM Ratio (%)",
    "xdata": ["20", "40", "60", "80", "90", "100"]
}

e = term_eval.Experiment(args)
e.run()
e.output()
