#!/usr/bin/env python3
import scripts.term_eval as term_eval

kwargs = {
    "name" : "figure-13b",
    "mode" : ["rpc", "pdp"],
    "nr_server_threads": [1, 2, 4, 8, 16, 32],
    "xlabel" : "Number of Server Threads",
    "xdata" : ["1", "2", "4", "8", "16", "32"],
    "legend": ["RPC", "TeRM"]
}

e = term_eval.Experiment(**kwargs)
e.run()
e.output()
