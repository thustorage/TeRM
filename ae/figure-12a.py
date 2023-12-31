#!/usr/bin/env python3
import scripts.term_eval as term_eval

kwargs = {
    "name": "figure-12a",
    "mode": ["pin", "odp", "rpc", "pdp"],
    "skewness_100": [0, 40, 80, 90, 99],
    "xlabel": "Zipfian theta",
    "xdata": ["0", "0.4", "0.8", "0.9", "0.99"],
    "legend": ["PIN", "ODP", "RPC", "TeRM"]
}

e = term_eval.Experiment(**kwargs)
e.run()
e.output()
