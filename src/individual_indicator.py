from __future__ import annotations
import utils as u
import xscen as xs
from copy import deepcopy
import xclim 
import xarray 
import pins_utils as pu 

# Maybe this will be part of xclim in the future
# There is already a normal snw_season_length. 
# Here, we needed to compute start/end with different frequencies, and 
# and a suitable snw_season_length to go with this
snw_season_length_twofreqs = xclim.indicators.land._snow.SnowWithIndexing(
    identifier="snw_season_length_twofreqs",
    units="days",
    long_name="Snow cover duration",
    description=(
        "The duration of the snow season, starting with at least {window} days with snow amount above {thresh} "
        "and ending with at least {window} days with snow amount under {thresh}."
    ),
    compute=pu.snw_season_length_twofreqs,
)
xclim.indicators.snw_season_length_twofreqs = snw_season_length_twofreqs

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
    return out 
        # u.save_tmp_update_path(out, pcat, cfg=cfg0, task=task)

        
