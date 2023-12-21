#!/usr/bin/env python3
import scripts.term_eval as term_eval

args = {
    "name" : "figure-8",
    "mode" : ["pin", "odp", "rpc", "pdp"],
    "sz_unit": ["256", "4k"],
    "xlabel": "Read Size",
    "xdata": ["256", "4k"],
}

e = term_eval.Experiment(args)
e.run()
e.output()
