import numpy as np
import utils as u
import xscen as xs
import pins_utils as pu


def _climatology(ds, cfg):
    # some indicators (season_start & season_end) need to be transformed
    # so that taking means make some sense
    ds = pu.doy_to_days_since(ds)
    ds_mean = xs.climatological_op(ds=ds.copy(), **cfg["climatological_op"])
    # I think some attributes are lost
    ds_mean = u.intake_to_cat(ds_mean)
    ds_mean = pu.timedelta_to_datetime(ds_mean)
    ds_mean = pu.days_since_to_doy(ds_mean)
    ds_mean = ds_mean.chunk(cfg["chunks"])
    if "calendar" in ds_mean["time"].attrs:
        del ds_mean["time"].attrs["calendar"]
    # Just to be safe, clear all problematic attrs
    for key in ["calendar", "units"]:
        ds_mean["time"].attrs.pop(key, None)
    for v in ds_mean.data_vars:
        if "timedelta" in str(ds_mean[v].dtype) or "datetime" in str(ds_mean[v].dtype):
            ds_mean[v] = ds_mean[v].astype("datetime64[D]").astype(np.float32)
    return ds_mean


def climatology(pcat, id0, cfg, task):
    xrfreqs = set(pcat.search(**cfg[task]["input"]).df.xrfreq)
    for xrfreq in xrfreqs:
        # at this point, id is not a unique identifier
        id0_xrfreq = {"id": id0, "xrfreq": xrfreq}
        save_kwargs = {
            "schemas": cfg["schemas"]["schema_xrfreq"],
            "simple_saving": True,
        }
        u.template_1d_func(
            pcat, id0_xrfreq, cfg, task, _climatology, save_kwargs=save_kwargs
        )
