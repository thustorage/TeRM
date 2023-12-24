#!/usr/bin/env python3
import scripts.term_eval as term_eval

kwargs = {
    "name": "figure-13a",
    "mode": ["pin", "odp", "rpc", "pdp"],
    "nr_client_threads": [1, 2, 4, 8, 16, 32, 64],
    "xlabel": "Number of Client Threads",
    "xdata": ["1", "2", "4", "8", "16", "32", "64"],
    "legend": ["PIN", "ODP", "RPC", "TeRM"]
}

e = term_eval.Experiment(**kwargs)
e.run()
e.output()
