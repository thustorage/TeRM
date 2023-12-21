#!/usr/bin/env python3
import scripts.term_eval as term_eval

args = {
    "name" : "figure-11",
    "dynamic" : True,
    "mode" : ["pin", "odp", "rpc", "pdp"],
    "xlabel": "Time (s)",
    "xdata": [i for i in range(1, 121)]
}
e = term_eval.Experiment(args)
e.run()
e.output()