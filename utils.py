import time
import os
import shutil
import xscen as xs
import xarray as xr
from xscen.io import save_to_zarr
from pathlib import Path
import itertools
from dask.distributed import Client
from contextlib import nullcontext


# good ol' logger
def logger(message):
    print(f"{time.strftime('%H:%M:%S')} -- {message}")


def check_existence_and_log(pcat, target, overwrite=False):
    if overwrite: 
        logger(target)
        return False
    if (exists := pcat.exists_in_cat(**target)) == False:
        logger( target)
    else:
        logger(f"already computed { target} ")
    return exists


def convert_cf_time(ds):
    try:
        ds["time"] = ds.indexes["time"].to_datetimeindex()
    except:
        ValueError
    return ds


def _add_defaults_save_kwargs(save_kwargs, cfg):
    save_kwargs = save_kwargs or {}
    save_kwargs_defaults = {
        "schemas": cfg["schemas"]["schema"],
        "save_dir": cfg["paths"]["exec_wdir"],
        "tmp_dir": None,
        "overwrite": cfg["workflow"]["overwrite"],
    }
    return {**save_kwargs_defaults, **save_kwargs}


def template_1d_func(pcat, id0, cfg, task, func, func_kwargs=None, save_kwargs=None):
    func_kwargs = func_kwargs or {}
    save_kwargs = _add_defaults_save_kwargs(save_kwargs, cfg)
    if isinstance(id0, str): 
        id0 = {"id": id0}
    with Client(**dask_kwargs) if cfg["dask"]["use_dask"] else nullcontext() as client:
        if isinstance(id0, dict):
            target = {**id0, **cfg[task]["output"]}
            if check_existence_and_log(pcat, target, save_kwargs["overwrite"]):
                return
            ds = pcat.search(**id0, **cfg[task]["input"]).to_dask(decode_time_delta=False)
        elif isinstance(id0, xr.Dataset): 
            ds = id0
        else: 
            raise ValueError(f"id0 should be a `str`,`dict`, or `xr.Dataset`. Got {type(id0)}")
        ds = func(ds, cfg[task], **func_kwargs)
        ds = fill_cat_attrs(ds, cfg[task]["output"])
        save_tmp_update_path(ds, pcat=pcat, **save_kwargs)
        if client:
            client.close()


def template_dict_func(pcat, id0, cfg, task, func, func_kwargs=None, save_kwargs=None):
    func_kwargs = func_kwargs or {}
    save_kwargs = _add_defaults_save_kwargs(save_kwargs, cfg)
    id0 = id0 if isinstance(id0, dict) else {"id": id0}
    with Client(**dask_kwargs) if cfg["dask"]["use_dask"] else nullcontext() as client:
        target = {**id0, **cfg[task]["output"]}
        if check_existence_and_log(pcat, target, save_kwargs["overwrite"]):
            return
        dsd = pcat.search(**id0, **cfg[task]["input"]).to_dataset_dict()
        ds = func(dsd, cfg[task], **func_kwargs)
        ds = fill_cat_attrs(ds, cfg[task]["output"])
        save_tmp_update_path(ds, pcat=pcat, **save_kwargs)
        if client:
            client.close()


def template_2d_func(
    pcat, id0, cfg, task, func, dsref=None, func_kwargs=None, save_kwargs=None
):
    func_kwargs = func_kwargs or {}
    save_kwargs = _add_defaults_save_kwargs(save_kwargs, cfg)
    id0 = id0 if isinstance(id0, dict) else {"id": id0}
    with Client(**dask_kwargs) if cfg["dask"]["use_dask"] else nullcontext() as client:
        target = {**id0, **cfg[task]["output"]}
        if not save_kwargs["overwrite"] and check_existence_and_log(pcat, target):
            logger(f"{target} already computed")
            return
        ds = pcat.search(**id0, **cfg[task]["input"]).to_dask()
        if dsref is None:
            dsref = pcat.search(**id0, **cfg[task]["input_ref"]).to_dask()
        ds = func(ds, dsref, cfg[task], **func_kwargs)
        ds = fill_cat_attrs(ds, cfg[task]["output"])
        save_tmp_update_path(ds, pcat=pcat, **save_kwargs)
        if client:
            client.close()


# def template_dict_func(pcat, id0, cfg, task, func, func_kwargs = None, save_kwargs = None):
#     func_kwargs = func_kwargs or {}
#     save_kwargs = _add_defaults_save_kwargs(save_kwargs , cfg)
#     id0 = id0 if isinstance(id0, dict) else {"id":id0}
#     with Client(**dask_kwargs) if cfg["dask"]["use_dask"] else nullcontext() as client:
#         target = {**id0, **cfg[task]["output"]}
#         if not save_kwargs["overwrite"] and check_existence_and_log(pcat, target):
#             logger(f"{target} already computed")
#             return
#         dsd = pcat.search(**id0, **cfg[task]["input"]).to_dataset_dict()
#         ds = func(dsd, cfg[task], **func_kwargs)
#         ds = fill_cat_attrs(ds, cfg[task]["output"])
#         save_tmp_update_path(ds, pcat=pcat, **save_kwargs)
#         if client:
#             client.close()


def template_debug(pcat, id0, cfg, task, func, overwrite=False, **kwargs):
    with Client(**dask_kwargs) if cfg["dask"]["use_dask"] else nullcontext() as client:
        target = {"id": id0, **cfg[task]["output"]}
        if not overwrite and u.check_existence_and_log(pcat, target):
            u.logger(f"{target} already computed")
            return
        ds = pcat.search(id=id0, **cfg[task]["input"]).to_dask()
        ds = func(ds, cfg[task], **kwargs)
        if client:
            client.close()
        return ds


# =======================================================================================
# combine DataArray utils
# =======================================================================================
def cbc(dsl):
    ds = dsl[0]
    for dsi in dsl[1:]:
        ds = ds.combine_first(dsi)
    return ds


def cbcl(dsl, lab="_combine_dim"):
    if isinstance(dsl, dict):
        out = cbc([ds.expand_dims({lab: [k]}) for k, ds in dsl.items()])
    if isinstance(dsl, list):
        out = cbc([ds.expand_dims({lab: [k]}) for k, ds in zip(*dsl)])
    return out


def get_common_dict(dsl):
    ll = [list(i.attrs.keys()) for i in list(dsl)]
    # flatten
    ll = list(itertools.chain.from_iterable(ll))
    all_keys = set(ll)
    keys = []
    for k in list(all_keys):
        ll = [i.attrs.get(k, None) for i in list(dsl)]
        # work-around to use set, need unmutable objects
        ll = [tuple(i) if isinstance(i, list) else i for i in ll]
        s = set(ll)
        if len(s) == 1:
            keys.append(k)
    ind0 = list(dsl)[0]
    return {k: v for k, v in ind0.attrs.items()}


# =======================================================================================
# pcat, gen utls
# =======================================================================================
def fill_empty_facets(ds, facets):
    for f in facets:
        v = ds.attrs.get(f"cat:{f}", "na")
        ds.attrs[f"cat:{f}"] = v
    return ds


def fill_cat_attrs(ds, dd):
    for k, a in dd.items():
        ds.attrs[f"cat:{k}"] = a
    return ds


def nget(d, key):
    if len(keyl := key.split(".")) > 1:
        return nget(d.get(keyl[0]), ".".join(keyl[1:]))
    return d[keyl[0]]


def _delete_last_pcat_entry_(pcat):
    """with great powers comes great responsibility"""
    p = list(pcat.df.path)[-1]
    rem_zarr(p)
    pcat.update()


def _delete_kws_(pcat, search_kws):
    """with great powers comes great responsibility"""
    for p in pcat.search(**search_kws).df.path:
        rem_zarr(p)
    pcat.update()


def create_cat_labels(ds):
    exceptions = ["mip_era"]
    for lab in [
        "activity",
        "domain",
        "driving_experiment",
        "driving_institution",
        "driving_source",
        #'experiment',
        "frequency",
        #'id',
        "institution",
        #'member',
        "mip_era",
        "source",
        #'type',
        "variable",
        "xrfreq",
    ]:
        try:
            ds.attrs[f"cat:{lab}"]

        except:
            if lab == "frequency":
                ds.attrs[f"cat:{lab}"] = "day"
            elif lab == "xrfreq":
                ds.attrs[f"cat:{lab}"] = "D"
            elif lab == "variable":
                ds.attrs["cat:variable"] = ["tasmax", "tasmin", "pr"]
            else:
                if lab in exceptions:
                    att = ds.attrs[lab]
                else:
                    att = ds.attrs[f"{lab}_id"]
                ds.attrs[f"cat:{lab}"] = att
    ds.attrs["cat:id"] = "_".join(
        [
            ds.attrs[f"cat:{lab}"]
            for lab in ["mip_era", "source", "driving_experiment", "driving_source"]
        ]
    )
    ds.attrs["cat:processing_level"] = "extracted"
    return ds


def intake_to_cat(ds):
    for lab in [
        "_data_format_",
        "activity",
        "domain",
        "driving_model",
        "experiment",
        "frequency",
        "id",
        "institution",
        "member",
        "mip_era",
        "processing_level",
        "source",
        "type",
        "variable",
        # 'version',
        "xrfreq",
        # 'date_start',
        # 'date_end',
        # 'path',
    ]:
        try:
            ds.attrs[f"cat:{lab}"]
        except:
            if lab == "frequency":
                ds.attrs[f"cat:{lab}"] = "day"
            elif lab == "xrfreq":
                ds.attrs[f"cat:{lab}"] = "D"
            else:
                try:
                    ds.attrs[f"cat:{lab}"] = ds.attrs[f"intake_esm_attrs:{lab}"]
                except:
                    pass
    return ds


def rem_encoding(ref):
    for v in ref.data_vars:
        try:
            del ref[v].encoding["chunks"]
        except:
            pass
    for c in ref.coords:
        try:
            del ref[c].encoding["chunks"]
        except:
            pass
    return ref


def rem_zarr(path):
    if os.path.exists(path):
        shutil.rmtree(path)


def rem_all_chunks(ds):
    for v in ds.coords:
        try:
            del ds[v].encoding["chunks"]
        except:
            pass
    for v in ds.data_vars:
        try:
            del ds[v].encoding["chunks"]
        except:
            pass
    return ds


def move_then_delete(dirs_to_delete, moving_files, pcat):
    """
    First, move the moving_files. If they are zarr, update catalog
    with new path.
    Then, delete everything in dir_to_delete
    :param dirs_to_delete: list of directory where all content will be deleted
    :param moving_files: list of lists of path of files to move with format: [[source 1, destination1], [source 2, destination2],...]
    :param pcat: project catalog to update
    """

    for files in moving_files:
        source, dest = files[0], files[1]
        if Path(source).exists():
            shutil.move(source, dest)
            if dest[-5:] == ".zarr":
                ds = xr.open_zarr(dest)
                pcat.update_from_ds(ds=ds, path=dest)

    # erase workdir content if this is the last step
    for dir_to_delete in dirs_to_delete:
        dir_to_delete = Path(dir_to_delete)
        if dir_to_delete.exists() and dir_to_delete.is_dir():
            shutil.rmtree(dir_to_delete)
            os.mkdir(dir_to_delete)


def save_move_update(
    ds,
    pcat,
    init_path,
    final_path,
    info_dict=None,
    encoding=None,
    mode="o",
    itervar=False,
    rechunk=None,
    build_path_ext=None,
    simple_saving=False,
):
    if build_path_ext is not None:
        filepath = build_path(ds)
        init_path = f"{init_path}/{filepath}.{build_path_ext}"
        final_path = f"{final_path}/{filepath}.{build_path_ext}"
    if simple_saving:
        if encoding or rechunk:
            print("Simple saving is on, ignoring encoding/rechunk options")
        ds.to_zarr(init_path)
    else:
        encoding = encoding or {var: {"dtype": "float32"} for var in ds.data_vars}
        save_to_zarr(
            ds,
            init_path,
            encoding=encoding,
            mode=mode,
            itervar=itervar,
            rechunk=rechunk,
        )
    shutil.move(init_path, final_path)
    pcat.update_from_ds(ds=ds, path=str(final_path), info_dict=info_dict)


def save_tmp_update_path(
    ds,
    schemas,
    pcat,
    save_dir,
    tmp_dir=None,
    filename=None,
    overwrite=False,
    simple_saving=False,
):
    if filename is None:
        filepath = xs.catutils.build_path(ds, schemas=schemas)
    else:
        filepath = filename
    tmp_dir = tmp_dir or save_dir
    path = f"{tmp_dir}/{filepath}.tmp.zarr"
    final_path = path.replace(".tmp.", ".").replace(str(tmp_dir), str(save_dir))
    if os.path.exists(final_path) and overwrite is False:
        print(f"{final_path} already exists")
    else:
        rem_zarr(path)
        rem_zarr(final_path)
        save_move_update(
            ds=ds,
            pcat=pcat,
            init_path=path,
            final_path=final_path,
            simple_saving=simple_saving,
        )


def process_level(
    ds0,
    func,
    kwargs,
    chunks,
    pcat,
    lev0,
    out_dir,
    key,
    extra_lab=None,
    extra_fields=None,
    move_and_delete=[],
):
    """
    process

    Parameters
    ----------
        move_and_delete: tuple[Str, Str]
            Original dir and new dir
    """
    logger(key)
    ds0 = func(ds0, **kwargs)
    if isinstance(ds0, tuple) is False:
        ds0 = (ds0,)
    if isinstance(lev0, tuple) is False:
        if lev0 is not None:
            lev0 = (lev0,)
        else:
            lev0 = [None] * len(ds0)
    if len(ds0) != len(lev0):
        raise ValueError("`lev` and the output of func(`ds`) do not have equal length")

    for ds, lev in zip(ds0, lev0):
        filename = get_filename(ds, out_dir, extra_fields)
        print(filename)
        rem_zarr(filename)
        logger("File removed")
        if "target_mb" in chunks.keys():
            chunks = xs.io.estimate_chunks(
                ds, dims=chunks["dims"], target_mb=chunks["target_mb"]
            )
        ds = rem_all_chunks(ds)
        ds = ds.chunk(chunks)
        # xs.save_to_zarr(ds, filename, rechunk=chunks,  mode='o')
        xs.save_to_zarr(ds, filename, mode="o")
        logger("File saved")
        pcat.update_from_ds(ds=ds, path=filename)
        if len(move_and_delete) == 2:
            xs.scripting.move_and_delete(
                [[filename, filename.replace(move_and_delete[0], move_and_delete[1])]],
                pcat,
            )
