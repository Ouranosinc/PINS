import utils as u
import xscen as xs
from copy import deepcopy

def _individual_indicator(ds, cfg, ind):
    attrs = deepcopy(ds.attrs)
    # hard-coded 365-day on indicators, I think this is fine
    _, out = xs.compute_indicators(
        ds.convert_calendar("365_day", align_on="date"), indicators=[ind]
    ).popitem()
    out.attrs = {**attrs, **out.attrs}
    return out

def individual_indicator(pcat, id0, cfg, task):
    mod = xs.indicators.load_xclim_module(**cfg[task]["load_xclim_module"])
    for _, ind in mod.iter_indicators():
        func_kwargs = {"ind": ind}
        save_kwargs = {"schemas": cfg["schemas"]["schema_xrfreq"]}
        var_name = ind.cf_attrs[0]["var_name"]
        xrfreq = ind.injected_parameters["freq"]
        # look, indicator is complicated, so we modify a bit the config to be able
        # the use the general template func ... nothing so bad here
        cfg.set(f"{task}.output.variable", var_name)
        cfg.set(f"{task}.output.xrfreq", xrfreq)
        u.template_1d_func(
            pcat,
            id0,
            cfg,
            task,
            _individual_indicator,
            func_kwargs=func_kwargs,
            save_kwargs=save_kwargs,
        )
