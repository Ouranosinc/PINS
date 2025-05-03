import utils as u
import xscen as xs


def _adjust(ds, ds_tr, cfg, var):
    adj = xs.adjust(dsim=ds, dtrain=ds_tr, **cfg["variables"][var]["adjusting_args"])
    return adj


def adjust(pcat, id0, cfg, task):
    for var in cfg[task]["variables"]:
        # 2d_func will manage getting the ref
        u.template_2d_func(pcat, id0, cfg, task, _adjust, func_kwargs={"var":var})
