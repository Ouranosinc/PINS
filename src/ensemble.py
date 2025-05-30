import numpy as np
import utils as u
import xscen as xs
import pins_utils as pu


def ensemble(dsd, cfg,task):
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
    return xs.ensembles.ensemble_stats(datasets=dsd, **cfg[task]["ensemble_stats"])

def main(pcat, cfg, task, wildcards):
    cfg0 = u.dynamic_cfg(cfg,task,wildcards)
    dsd = pcat.search(**cfg0[task]["io"]["input"]).to_dataset_dict()
    out = ensemble(dsd,cfg, task)
    u.save_tmp_update_path(out, pcat, cfg=cfg0, task=task)

