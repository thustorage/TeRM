#!/usr/bin/env python3
import scripts.term_eval as term_eval

args = {
    "name" : "figure-15a",
    "mode" : ["odp", "rpc", "pdp"],
    "ssd" : ["nvme4n1", "nvme1n1", "nvme2n1"],
    "xlabel": "Read 4KB",
    "xdata": ["SSD 1", "SSD 2", "SSD 3"]
}

e = term_eval.Experiment(args)
e.run()
e.output()
