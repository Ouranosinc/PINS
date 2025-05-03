import numpy as np
import utils as u

def _rechunk(ds, cfg):
    return ds.chunk(cfg["chunks"])

def rechunk(pcat, id0, cfg, task):
    u.template_1d_func(pcat, id0, cfg, task, _rechunk)
