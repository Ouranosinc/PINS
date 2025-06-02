import pins_util    s as pu

def decay(ds, cfg, task):
    ds["snw"] = pu.decay_snow_season_end(ds.snw, **cfg[task]["decay_snow_season_end"])
    return ds 

def main(pcat, cfg,task, wildcards):
    dd, cfg0 = u.dynamic_io(pcat,cfg, task, wildcards)
    out = decay(dd["input"], cfg0, task)
    u.save_tmp_update_path(out, pcat, cfg=cfg0, task=task) 