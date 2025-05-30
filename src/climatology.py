import numpy as np
import utils as u
import xscen as xs
import pins_utils as pu


def climatology(ds, cfg, task):
    # some indicators (season_start & season_end) need to be transformed
    # so that taking means make some sense
    ds = pu.doy_to_days_since(ds)
    ds_mean = xs.climatological_op(ds=ds.copy(), **cfg[task]["climatological_op"])
    # I think some attributes are lost
    ds_mean = u.intake_to_cat(ds_mean)
    ds_mean = pu.timedelta_to_datetime(ds_mean)
    ds_mean = pu.days_since_to_doy(ds_mean)
    ds_mean = ds_mean.chunk(cfg[task]["chunks"])
    for v in ds_mean.data_vars:
        if "timedelta" in str(ds_mean[v].dtype) or "datetime" in str(ds_mean[v].dtype):
            ds_mean[v] = ds_mean[v].astype("datetime64[D]").astype(np.float32)
    return ds_mean


def main(pcat, cfg, task, wildcards):
    dd, cfg0 = u.dynamic_io(pcat, cfg,task,wildcards)
    out = climatology(dd["input"], cfg0,task)
    u.save_tmp_update_path(out, pcat, cfg=cfg0, task=task)
