#!/usr/bin/env python3
import scripts.term_eval as term_eval

args = {
    "name" : "figure-9-full",
    "mode" : ["pin", "odp", "rpc", "pdp"],
    "sz_unit": ["64", "128", "256", "512", "1k", "2k", "4k", "8k", "16k"],
    "write_percent" : [100],
    "xlabel": "Write Size",
    "xdata": ["64", "128", "256", "512", "1k", "2k", "4k", "8k", "16k"]
}

e = term_eval.Experiment(args)
e.run()
e.output()