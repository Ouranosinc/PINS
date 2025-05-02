import utils as u
from dask.distributed import Client
from contextlib import nullcontext
from xscen import search_data_catalogs


# loads from catalogs and not pcat, so a bit different than other boilplates functions
# explicit implementation of utils.template_1d_func
def extract_reference(pcat, cfg, task):
    overwrite = cfg["workflow"]["overwrite"]
    # task = extract.reference
    with Client(**dask_kwargs) if cfg["dask"]["use_dask"] else nullcontext() as client:
        cat_ref = search_data_catalogs(**u.nget(cfg, f"{task}.search_data_catalogs"))
        id0, dc = cat_ref.popitem()
        target = {"id": id0, **u.nget(cfg, f"{task}.output")}
        if not cfg["custom"]["overwrite"] and u.check_existence_and_log(pcat, target):
            u.logger(f"{target} already computed")
            return
        ds_ref = extract_dataset(catalog=dc, **u.nget(cfg, f"{task}.extract_dataset"))[
            "D"
        ]
        ds_ref = ds_ref.chunk({d: cfg[task]["chunks"][d] for d in ds_ref.dims})
        ds_ref = u.fill_cat_attrs(ds_ref, u.nget(cfg, f"{task}.output"))
        u.save_tmp_update_path(
            ds_ref,
            schemas=cfg["schemas"]["schema"],
            pcat=pcat,
            save_dir=cfg["paths"]["exec_wdir"],
            tmp_dir=None,
            overwrite=cfg["custom"]["overwrite"],
        )
        if client:
            client.close()
