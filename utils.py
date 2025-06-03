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

def logger(message):
    print(f"{time.strftime('%H:%M:%S')} -- {message}")

def flatten_dict(d: dict, sep: str= '.') -> MutableMapping:
    [flat_dict] = pd.json_normalize(d, sep=sep).to_dict(orient='records')
    return flat_dict

def build_path(dd, cfg0,task):
    path = "_".join(list(dd.values()))
    tmpdir = str(cfg0["paths"]["tmp"])
    path = f"{tmpdir}/{path}.zarr"
    return path

def check_existence_and_log(pcat, cfg, task):
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

def dynamic_cfg(cfg, task, wildcards): 
    cfg0 = deepcopy(cfg)
    template_wildcards = flatten_dict(cfg0[task]["io"]["wildcards"])
    for k,v in template_wildcards.items(): 
        cfg0.set(f"{task}.io.{k}", wildcards[v])
    return cfg0

def dynamic_io(pcat, cfg, task, wildcards): 
    cfg0 = dynamic_cfg(cfg,task,wildcards)
    if check_existence_and_log(pcat, cfg0, task):
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
    rmrf(p)
    pcat.update()

def _delete_kws_(pcat, search_kws):
    """with great powers comes great responsibility"""
    for p in pcat.search(**search_kws).df.path:
        rmrf(p)
    pcat.update()

# =======================================================================================
# saving utils
# =======================================================================================
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


def rmrf(path):
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
            rmrf(path)
        ds.to_zarr(path)
        return 
    tmp_dir, save_dir = save_kwargs["tmp_dir"], save_kwargs["save_dir"]
    tmp_dir = tmp_dir or save_dir
    path = f"{tmp_dir}/{filepath}.tmp.zarr"
    final_path = path.replace(".tmp.", ".").replace(str(tmp_dir), str(save_dir))
    if os.path.exists(final_path) and overwrite is False:
        print(f"{final_path} already exists")
    else:
        rmrf(path)
        rmrf(final_path)
        save_move_update(
            ds=ds,
            pcat=pcat,
            init_path=path,
            final_path=final_path,
            simple_saving=save_kwargs["simple_saving"],
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