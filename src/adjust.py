import utils as u
import xscen as xs


def adjust(ds, ds_tr, cfg, task, var):
    adj = xs.adjust(dsim=ds, dtrain=ds_tr, **cfg[task]["variables"][var]["adjusting_args"])
    return adj

def main(pcat, cfg,task, wildcards):
    dd, cfg0 = u.dynamic_io(pcat,cfg, task, wildcards)
    out = adjust(dd["input"],dd["input_ref"], cfg0, task, var=wildcards["var"])
    u.save_tmp_update_path(out, pcat, cfg=cfg0, task=task) 