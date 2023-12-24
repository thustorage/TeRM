#!/usr/bin/env python3
import scripts.term_eval as term_eval

kwargs = {
    "name": "figure-8",
    "mode": ["pin", "odp", "rpc", "pdp"],
    "sz_unit": ["64", "256", "1k", "4k", "16k"],
    "xlabel": "Read Size",
    "xdata": ["64", "256", "1k", "4k", "16k"],
    "legend": ["PIN", "ODP", "RPC", "TeRM"],
}

e = term_eval.Experiment(**kwargs)
e.run()
e.output()
