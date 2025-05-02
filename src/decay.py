import utils as u
import pins_utils as pu


def _decay(ds, cfg):
    ds["snw"] = pu.decay_snow_season_end(ds.snw, **cfg["decay_snow_season_end"])
    return ds


def decay(pcat, id0, cfg, task):
    u.template_1d_func(pcat, id0, cfg, task, _decay)
