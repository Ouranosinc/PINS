import utils as u
import xscen as xs 

def regrid(dsim, dref, cfg, task):
    ds_regrid = xs.regrid_dataset(ds=dsim, ds_grid=dref, **cfg[task]["regrid_dataset"])
    # ds_regrid = ds_regrid.convert_calendar("365_day", align_on="year")
    ds_regrid.attrs["cat:domain"] = dref.attrs["cat:domain"]
    return ds_regrid

def main(pcat,cfg,task, wildcards):
    dd, cfg0 = u.dynamic_io(pcat,cfg, task, wildcards)
    out = regrid(dd["input"], dd["input_ref"], cfg0, task)
    u.save_tmp_update_path(out, pcat, cfg=cfg0, task=task)
