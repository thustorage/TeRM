#!/usr/bin/env python3
import scripts.term_eval as term_eval

kwargs = {
    "name": "figure-11",
    "mode": ["pin", "odp", "rpc", "pdp"],
    "dynamic": True,
    "xlabel": "Time (s)",
    "xdata": [i for i in range(1, 121)],
    "legend": ["PIN", "ODP", "RPC", "TeRM"],
}

e = term_eval.Experiment(**kwargs)
e.run()
e.output()
