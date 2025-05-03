import utils as u
from contextlib import nullcontext

# just combine ... a bit different than templates, so no boilerplate this time
def _indicators(cat, cfg): 
    return cat.to_dataset(decode_time_delta=False)

def indicators(pcat, id0, cfg, task):
    save_kwargs = {
        "schemas": cfg["schemas"]["schema_xrfreq_no_var"],
        "simple_saving": True,  # to address some problems with timedelta and xscen chunking ... :(((
    }
    for xrfreq in set(pcat.search(**cfg[task]["input"]).df.xrfreq):
        id0f = {"id": id0, "xrfreq": xrfreq}
        u.template_func(pcat, id0f, cfg, task, _indicators, input_type = "cat", save_kwargs=save_kwargs)
