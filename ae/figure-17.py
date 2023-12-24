#!/usr/bin/env python3
import scripts.xstore_eval as xstore_eval

kwargs = {
    "name": "figure-17",
    "mode": ["pin", "odp", "rpc", "pdp"],
    "zipf_theta": [0, 0.4, 0.8, 0.9, 0.99],
    "xlabel": "Zipfian theta",
    "xdata": ["0", "0.4", "0.8", "0.9", "0.99"],
    "legend": ["PIN", "ODP", "RPC", "TeRM"]
}

e = xstore_eval.Experiment(**kwargs)
e.run()
e.output()
