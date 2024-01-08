#!/usr/bin/env python3
import scripts.octopus_eval as octopus_eval

kwargs = {
    "name": "figure-16",
    "mode": ["pin", "odp", "rpc", "pdp"],
    "write": [0, 1],
    "unit_size": ["4k", "16k"],
    "xlabel": "",
    "xdata": ["Read 4KB", "Read 16KB", "Write 4KB", "Write 16KB"],
    "legend": ["PIN", "ODP", "RPC", "TeRM"]
}


e = octopus_eval.Experiment(**kwargs)
e.run()
e.output()

# rerun a data point
# kwargs = {
#     "name": "figure-16",
#     "mode": ["rpc"],
#     "write": [1],
#     "unit_size": ["16k"],
#     "xlabel": "", # for output(), no need to edit
#     "xdata": ["Write 16KB"], # for output(), no need to edit
#     "legend": ["RPC"] # for output(), no need to edit
# }
# e = octopus_eval.Experiment(**kwargs)
# e.run()
