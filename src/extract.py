
import utils as u
from dask.distributed import Client
from contextlib import nullcontext
from xscen import search_data_catalogs, extract_dataset

def extract(ds, cfg): 
    out = ds
    out["time"] = out.time.astype("datetime64[ns]")
    out = out.chunk(cfg["chunks"])
    return out


def main(pcat, cfg, task):
    cat_sim = search_data_catalogs(**(cfg[task]["search_data_catalogs"]))
    for sim_id, dc_id in cat_sim.items():
        print(sim_id)
        for region_name, region_dict in cfg['custom']['regions'].items():
            save_kwargs = {**cfg["save_kwargs"], **(cfg[task].get("save_kwargs",{})), "simple_saving":True}
            with Client(**(u.nget(cfg, f"{task}.dask_kwargs"))) if cfg["dask"]["use_dask"] else nullcontext() as client:
                target = {"id": sim_id, **cfg[task]["io"]["output"]}
                if u.check_existence_and_log(pcat, target, save_kwargs["overwrite"]):
                    return
                ds_sim = extract_dataset(catalog=dc_id, **(cfg[task]["extract_dataset"]), region=region_dict)['D']
                out = extract(ds_sim, cfg[task])
                
                out = u.intake_to_cat(out)
                out = u.fill_cat_attrs(out, cfg["cat_attrs"])
                out = u.fill_cat_attrs(out, cfg[task]["io"]["output"])
                out = u.fill_empty_facets(out, ['member', 'mip_era', 'activity', 'experiment'])
                
                u.save_tmp_update_path(out, pcat, cfg,task)
                if client:
                    client.close()