import utils as u
import xscen as xs
from copy import deepcopy

def individual_indicator(ds, ind):
    attrs = deepcopy(ds.attrs)
    # hard-coded 365-day on indicators, I think this is fine
    _, out = xs.compute_indicators(
        ds.convert_calendar("365_day", align_on="date"), indicators=[ind]
    ).popitem()
    out.attrs = {**attrs, **out.attrs}
    return out

def main(pcat,cfg,task,wildcards):
    mod = xs.indicators.load_xclim_module(**cfg[task]["load_xclim_module"])
    for _, ind in mod.iter_indicators():
        wildcards.update({
            "xrfreq":ind.injected_parameters["freq"],
            "var"  :ind.cf_attrs[0]["var_name"]
            })
        dd, cfg0 = u.dynamic_io(pcat,cfg, task, wildcards)
        out = individual_indicator(dd["input"],ind)
        u.save_tmp_update_path(out, pcat, cfg=cfg0, task=task)

        
