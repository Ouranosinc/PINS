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
from collections.abc import MutableMapping
import collections 
from copy import deepcopy
import pandas as pd
# =======================================================================================
# Template functions: Manages I/O and dask clients
# =======================================================================================

# good ol' logger
def logger(message):
    print(f"{time.strftime('%H:%M:%S')} -- {message}")

def flatten_dict(d: dict, sep: str= '.') -> MutableMapping:
    [flat_dict] = pd.json_normalize(d, sep=sep).to_dict(orient='records')
    return flat_dict

def build_path(dd, cfg0,task):
    # dd = collections.OrderedDict(sorted(dd.items()))
    path = "_".join(list(dd.values()))
    tmpdir = str(cfg0["paths"]["tmp"])
    path = f"{tmpdir}/{path}.zarr"
    return path

def check_existence_and_log(pcat, target, overwrite=False):
    if overwrite: 
        logger(target)
        return False
    if (exists := pcat.exists_in_cat(**target)) == False:
        logger( target)
    else:
        logger(f"already computed { target} ")
    return exists


def check_existence_and_log0(pcat, cfg, task):
    overwrite = {**cfg["save_kwargs"],**cfg[task].get("save_kwargs",{})}["overwrite"]
    target = cfg[task]["io"]["output"]
    if overwrite: 
        logger(target)
        return False
    if (exists := pcat.exists_in_cat(**target)) == False:
        logger(target)
    else:
        logger(f"already computed { target} ")
    return exists

def _add_defaults_save_kwargs(save_kwargs, cfg):
    save_kwargs = save_kwargs or {}
    save_kwargs_defaults = {
        "schemas": cfg["schemas"]["schema"],
        "save_dir": cfg["paths"]["exec_wdir"],
        "tmp_dir": None,
        "overwrite": cfg["workflow"]["overwrite"],
    }
    return {**save_kwargs_defaults, **save_kwargs}


def dynamic_cfg(cfg, task, wildcards): 
    cfg0 = deepcopy(cfg)
    template_wildcards = flatten_dict(cfg0[task]["io"]["wildcards"])
    for k,v in template_wildcards.items(): 
        cfg0.set(f"{task}.io.{k}", wildcards[v])
    return cfg0

def dynamic_io(pcat, cfg, task, wildcards): 
    cfg0 = dynamic_cfg(cfg,task,wildcards)
    if check_existence_and_log0(pcat, cfg0, task):
        return None,None
    inpd = {}
    for k in cfg0[task]["io"].keys():
        if k.startswith("input"): 
            if pcat is not None: 
                inpd[k] = pcat.search(**cfg0[task]["io"][k]).to_dask()
            else: 
                path = build_path(cfg0[task]["io"][k], cfg0, task)
                inpd[k] = xr.open_zarr(path)
    return inpd, cfg0

def template_func(pcat, id0, cfg, task, func, input_type = "dataset", func_kwargs=None, save_kwargs=None):
    print(func_kwargs)
    func_kwargs = {} if func_kwargs is None  else func_kwargs
    save_kwargs = _add_defaults_save_kwargs(save_kwargs, cfg)
    if isinstance(id0, xr.Dataset):
        if input_type != "dataset":
            raise ValueError(f"If `id0` is a xr.Dataset, `input_type` should be 'dataset'. Got {input_type}")
        input = id0
    else: 
        id0 = id0 if isinstance(id0, dict) else {"id": id0}
        if check_existence_and_log(pcat, {**id0, **cfg[task]["output"]}, save_kwargs["overwrite"]):
            return
        print(cfg[task]["input"])
        cat = pcat.search(**{**id0, **cfg[task]["input"]})
        if input_type == "dataset": 
            input = cat.to_dask(decode_time_delta=False)
        elif input_type == "dict": 
            input = cat.to_dataset_dict(decode_time_delta=False)
        elif input_type == "cat": 
            input = cat
    with Client(**dask_kwargs) if cfg["dask"]["use_dask"] else nullcontext() as client:
        ds = func(input, cfg[task], **func_kwargs)
        ds = fill_cat_attrs(ds, cfg[task]["output"])
        save_tmp_update_path(ds, pcat=pcat, **save_kwargs)
        if client:
            client.close()

def template_1d_func(pcat, id0, cfg, task, func, func_kwargs=None, save_kwargs=None):
    input_type = "dataset"
    return template_func(pcat, id0, cfg, task, func, input_type, func_kwargs, save_kwargs)

def template_dict_func(pcat, id0, cfg, task, func, func_kwargs=None, save_kwargs=None):
    input_type = "dict"
    return template_func(pcat, id0, cfg, task, func, input_type,  func_kwargs, save_kwargs)

def template_cat_func(pcat, id0, cfg, task, func, func_kwargs=None, save_kwargs=None):
    input_type = "cat"
    return template_func(pcat, id0, cfg, task, func, input_type,  func_kwargs, save_kwargs)


def template_2d_func(
    pcat, id0, cfg, task, func, dsref=None, func_kwargs=None, save_kwargs=None
):
    func_kwargs = func_kwargs or {}
    save_kwargs = _add_defaults_save_kwargs(save_kwargs, cfg)
    if isinstance(id0, xr.Dataset):
        input = id0
        
    else: 
        id0 = id0 if isinstance(id0, dict) else {"id": id0}
        if check_existence_and_log(pcat, {**id0, **cfg[task]["output"]}, save_kwargs["overwrite"]):
            return
        print(cfg[task]["input"])
        cat = pcat.search(**{**id0, **cfg[task]["input"]})
        if input_type == "dataset": 
            input = cat.to_dask(decode_time_delta=False)
        elif input_type == "dict": 
            input = cat.to_dataset_dict(decode_time_delta=False)
        elif input_type == "cat": 
            input = cat

    with Client(**dask_kwargs) if cfg["dask"]["use_dask"] else nullcontext() as client:
        ds = pcat.search(**id0, **cfg[task]["input"]).to_dask()
        if dsref is None:
            dsref = pcat.search(**id0, **cfg[task]["input_ref"]).to_dask()
        ds = func(ds, dsref, cfg[task], **func_kwargs)
        ds = fill_cat_attrs(ds, cfg[task]["output"])
        save_tmp_update_path(ds, pcat=pcat, **save_kwargs)
        if client:
            client.close()

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

# =======================================================================================
# saving utils
# =======================================================================================
def convert_cf_time(ds):
    try:
        ds["time"] = ds.indexes["time"].to_datetimeindex()
    except:
        ValueError
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
    pcat,
    cfg = None, 
    task= None,
):  
    ds = fill_cat_attrs(ds, cfg[task]["io"]["output"])
    save_kwargs = {**cfg["save_kwargs"], **cfg[task].get("save_kwargs",{})}
    overwrite = save_kwargs["overwrite"]
    if pcat is not None:
        filepath = xs.catutils.build_path(ds, schemas=save_kwargs["schemas"])
    else: 
        path = build_path(cfg[task]["io"]["output"], cfg, task)
        print(path)
        if os.path.exists(path) and overwrite is False:
            print(f"{path} already exists")
        else:        
            rem_zarr(path)
        ds.to_zarr(path)
        return 
    tmp_dir, save_dir = save_kwargs["tmp_dir"], save_kwargs["save_dir"]
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
            simple_saving=save_kwargs["simple_saving"],
        )


def save_tmp_update_path0(
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

def my_to_dataset(pcat, search_kws, coord_keys): 
    paths = pcat.search(**search_kws).df.path
    dsl = []
    coordsl = []
    for p in paths: 
        ds = xr.open_zarr(p)
        # add necessary coords
        c0s = {c:[ds.attrs[f"cat:{c}"]] for c in coord_keys}
        ds = ds.expand_dims(c0s)
        ds["time"] = ds.time.astype("datetime64[ns]")
        dsl.append(ds)
        # keep track of coords to ensure they are all unique
        coordsl.append("_".join([c0s[c][0] for c in coord_keys]))
    # u.cbc : Combine by coords. I add all necessary coords
    ds = cbc(dsl)
    assert len(set(coordsl))==len(paths)
    return ds