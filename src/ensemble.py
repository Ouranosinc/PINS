import numpy as np
import utils as u
import xscen as xs
import pins_utils as pu


def _ensemble(dsd, cfg):
    # some indicators (season_start & season_end) need to be transformed
    # so that taking means make some sense
    for k in dsd.keys():
        dsd[k] = u.convert_cf_time(dsd[k])
        for v in dsd[k].data_vars:
            # necessary to compute std properly
            if "timedelta" in str(dsd[k][v].dtype) or "datetime" in str(
                dsd[k][v].dtype
            ):
                dsd[k][v] = dsd[k][v].astype("datetime64[D]").astype(np.float32)
        dsd[k] = pu.days_since_to_doy(dsd[k])
    return xs.ensembles.ensemble_stats(datasets=dsd, **cfg["ensemble_stats"])


def ensemble(pcat, id0, cfg, task):
    xrfreqs = set(pcat.search(**cfg[task]["input"]).df.xrfreq)
    for xrfreq in xrfreqs:
        # at this point, id is not a unique identifier
        id0f = {"id": id0, "xrfreq": xrfreq}
        save_kwargs = {
            "schemas": cfg["schemas"]["schema_xrfreq_no_var"],
            "simple_saving": True,
        }
        u.template_dict_func(pcat, id0f, cfg, task, _ensemble, save_kwargs=save_kwargs)
