
import utils as u
from dask.distributed import Client
from contextlib import nullcontext
from xscen import search_data_catalogs

def _extract(ds, cfg): 
    out = ds.convert_calendar("365_day")
    out["time"] = out.time.astype("datetime64[ns]")
    out = out.chunk(cfg["chunks"])
    return out

def extract(pcat, cfg, task):
    cat_sim = search_data_catalogs(**(cfg[task]["search_data_catalogs"]))
    for sim_id, dc_id in cat_sim.items():
        for region_name, region_dict in cfg['custom']['regions'].items():
            save_kwargs = {**cfg["save_kwargs"], **(cfg[task].get("save_kwargs",{})), "simple_saving":True}
            with Client(**(u.nget(cfg, f"{task}.dask_kwargs"))) if cfg["dask"]["use_dask"] else nullcontext() as client:
                target = {"id": sim_id, **cfg[task]["output"]}
                if u.check_existence_and_log(pcat, target, save_kwargs["overwrite"]):
                    return
                ds_sim = extract_dataset(catalog=dc_id, **(cfg[task]["extract_dataset"]), region=region_dict)['D']
                out = _extract(ds_sim, cfg[task])
                
                out = u.intake_to_cat(out)
                out = u.fill_cat_attrs(out, cfg["cat_attrs"])
                out = u.fill_cat_attrs(out, cfg[task]["output"])
                
                u.save_tmp_update_path(out, pcat, **save_kwargs)
                if client:
                    client.close()