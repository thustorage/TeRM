#!/usr/bin/env python3
import scripts.term_eval as term_eval

kwargs = {
    "name" : "figure-15b",
    "mode" : ["odp", "rpc", "pdp"],
    "ssd" : ["nvme4n1", "nvme1n1", "nvme2n1"],
    "xlabel": "Read 4KB",
    "xdata": ["SSD 1", "SSD 2", "SSD 3"],
    "legend": ["ODP", "RPC", "TeRM"]
}

e = term_eval.Experiment(**kwargs)
e.run()
e.output()
