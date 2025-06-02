import numpy as np
import utils as u
import xscen as xs

def train(ds, dsref, cfg, task, var):
    # quantiles may be in a weird form
    key = f"{task}.variables.{var}.training_args.xsdba_train_args.nquantiles"
    if isinstance(u.nget(cfg, key), dict):
        cfg.set(key, np.arange(**u.nget(cfg, key)))
    ds_tr = xs.train(
        dref=dsref, dhist=ds, **u.nget(cfg, f"{task}.variables.{var}.training_args")
    )
    ds_tr = ds_tr.assign_attrs(ds.attrs)
    return ds_tr

def main(pcat, cfg, task, wildcards):
    dd, cfg0 = u.dynamic_io(pcat,cfg, task, wildcards)
    sim_cal, ref_cal = [dd[k].time.dt.calendar for k in ["input", "input_ref"]]
    if ref_cal != sim_cal: 
        dd["input_ref"] = dd["input_ref"].convert_calendar(sim_cal)
    out = train(dd["input"],dd["input_ref"], cfg0, task, var=wildcards["var"])
    u.save_tmp_update_path(out, pcat, cfg=cfg0, task=task) 
