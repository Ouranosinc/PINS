import utils as u
from contextlib import nullcontext

# just combine ... a bit different than templates, so no boilerplate this time
def indicators(pcat, id0, cfg, task):
    save_kwargs = {
        "schemas": cfg["schemas"]["schema_xrfreq_no_var"],
        "simple_saving": True,  # to address some problems with timedelta and xscen chunking ... :(((
    }
    save_kwargs = u._add_defaults_save_kwargs(save_kwargs, cfg)
    for xrfreq in set(pcat.search(**cfg[task]["input"]).df.xrfreq):
        with (
            Client(**dask_kwargs)
            if cfg["dask"]["use_dask"]
            else nullcontext() as client
        ):
            id0f = {"id": id0, "xrfreq": xrfreq}
            ds = pcat.search(**id0f, **cfg[task]["input"]).to_dataset(
                decode_time_delta=False
            )
            target = {**id0f, **cfg[task]["output"]}
            if u.check_existence_and_log(pcat, target, save_kwargs["overwrite"]):
                return
            ds = u.fill_cat_attrs(ds, cfg[task]["output"])
            u.save_tmp_update_path(ds, pcat=pcat, **save_kwargs)
            if client:
                client.close()
