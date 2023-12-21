#!/usr/bin/env python3
import scripts.term_eval as term_eval

args = {
    "name" : "figure-12b",
    "mode" : ["pin", "odp", "rpc", "pdp"],
    "write_percent" : [0, 25, 50, 75, 100],
    "xlabel": "Write Ratio",
    "xdata": ["0%", "25%", "50%", "75%", "100%"]
}

e = term_eval.Experiment(args)
e.run()
e.output()

