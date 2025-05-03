import numpy as np
import utils as u
import xscen as xs

def _train(ds, dsref, cfg, var):
    # quantiles may be in a weird form
    key = f"variables.{var}.training_args.xsdba_train_args.nquantiles"
    if isinstance(u.nget(cfg, key), dict):
        cfg.set(key, np.arange(**u.nget(cfg, key)))
    ds_tr = xs.train(
        dref=dsref, dhist=ds, **u.nget(cfg, f"variables.{var}.training_args")
    )
    ds_tr = ds_tr.assign_attrs(ds.attrs)
    return ds_tr

def train(pcat, id0, cfg, task, dsref=None):
    dsref = dsref or pcat.search(**cfg[task]["input_ref"]).to_dask()
    for var in cfg[task]["variables"]:
        u.template_2d_func(pcat, id0, cfg, task, _train, dsref, func_kwargs={"var":var})
