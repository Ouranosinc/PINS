import numpy as np
import utils as u


def _rechunk(ds, cfg):
    xdims = np.array(["x", "lon", "rlon"])
    xdim = xdims[[xdim in ds.dims for xdim in xdims]].item()
    ds = ds.chunk(cfg["chunks"][xdim])
    return ds


def rechunk(pcat, id0, cfg, task):
    u.template_1d_func(pcat, id0, cfg, task, _rechunk)
