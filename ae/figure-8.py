#!/usr/bin/env python3
import scripts.term_eval as term_eval

kwargs = {
    "name": "figure-8",
    "mode": ["pin", "odp", "rpc", "pdp"],
    "sz_unit": ["64", "128", "256", "512", "1k", "2k", "4k", "8k", "16k"],
    "xlabel": "Read Size",
    "xdata": ["64", "128", "256", "512", "1k", "2k", "4k", "8k", "16k"],
    "legend": ["PIN", "ODP", "RPC", "TeRM"],
}

e = term_eval.Experiment(**kwargs)
e.run()
e.output()
