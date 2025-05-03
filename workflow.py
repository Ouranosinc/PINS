# ---
# jupyter:
#   jupytext:
#     text_representation:
#       extension: .py
#       format_name: percent
#       format_version: '1.3'
#       jupytext_version: 1.17.1
#   kernelspec:
#     display_name: Python 3 (ipykernel)
#     language: python
#     name: python3
# ---

# %% [markdown]
# # TODO: Find a mask? 
#
# We used to apply a mask for lakes I believe. Do we have a similar mask for CMIP6-MRCC5?
# ```python
#     file_snc = "/.../bbo/series/200601/snc_bbo_200601_se.nc"
#     daSnc = xr.open_dataset(file_snc)["snc"].isel(time=0)
#     mask_snc  = daSnc.sel(rlat = dsOur["snw"].rlat, rlon = dsOur["snw"].rlon)
# ```

# %%
### INIT
from xscen import (
    ProjectCatalog,
    load_config,
    CONFIG,
    measure_time, send_mail, send_mail_on_exit, timeout, TimeoutException,
    clean_up
)
import utils as u 
from src import rechunk, regrid, decay, train, adjust, extract,  individual_indicator, indicators, climatology, ensemble
import importlib 

MODULES = [u, extract, rechunk, regrid, decay, train, adjust, individual_indicator, indicators, climatology, ensemble]
# reload, used for convienience in jupyter mode, so I want to ignore it in 'script' mode
# Not really a problem if we forget to specify that we are in a script and not in a notebook
cfgfiles = ['config/paths.yml', "config/config.yml", "config/schemas.yml"]
load_config(*cfgfiles, verbose=(__name__ == '__main__'), reset=True)
def reload(modules=MODULES, skip=(CONFIG["workflow"]["jupy"]==False)): 
    if skip: 
        return 
    global PATHS, cfg
    modules = modules if isinstance(modules, list) else [modules]
    [importlib.reload(m) for m in modules]
    load_config(*cfgfiles, verbose=(__name__ == '__main__'), reset=True)
reload(skip=False)
# Get project catalog
if "initialize_pcat" in CONFIG["tasks"]:
    pcat = ProjectCatalog.create(PATHS['project_catalog'], project=CONFIG['project'], overwrite=True)
pcat = ProjectCatalog(PATHS['project_catalog'])
# get sim_ids, if they exist (only works if extraction
s_ids  = list(pcat.search(processing_level="extracted", type="simulation").df.id)
id0 = None if len(s_ids)==0 else s_ids[0]  # interactive debugging


# %% [markdown]
# # Main

# %%
if __name__ == '__main__':

# %%
if instance(slice("1950", "1980")

# %% [markdown]
# # Extract (ref)

    # %%
    reload() 
    if "makeref" in CONFIG["tasks"]: 
        task = "extract.reference"
        extract.extract_reference(pcat, CONFIG, task)

    # a quick one and done, should maybe be in makeref
    reload()
    task = "regrid"
    ds = pcat.search(processing_level="extracted", type="reconstruction").to_dask()
    ds = u.fill_empty_facets(ds, ["experiment", "mip_era", "member", "activity", "bias_adjust_project", "version"])
    rechunk.rechunk(pcat, ds, CONFIG, task)


# %% [markdown]
# ## Perform extraction (sims)

# %%
# to be done


# %% [markdown]
# # Regrid

    # %%
    reload()
    if (task := "regrid") in CONFIG["tasks"]: 
        regrid.regrid(pcat, id0, CONFIG, task)

# %% [markdown]
# # Rechunk

    # %%
    reload()
    if (task := "rechunk") in CONFIG["tasks"]: 
        rechunk.rechunk(pcat, id0, CONFIG, task)


# %% [markdown]
# # Decay snow

    # %%
    reload()
    if (task := "decay") in CONFIG["tasks"]: 
        decay.decay(pcat, id0, CONFIG, task)

# %% [markdown]
# # Train

    # %%
    reload()
    if (task := "train") in CONFIG["tasks"]:  
        dsref = pcat.search(**CONFIG[task]["input_ref"]).to_dask()
        train.train(pcat, id0, CONFIG, task, dsref)

# %% [markdown]
# # Adjust

    # %%
    reload()
    if (task := "adjust") in CONFIG["tasks"]: 
        adjust.adjust(pcat, id0, CONFIG, task)

# %% [markdown]
# # Individual indicator

    # %%
    reload()
    if (task := "individual_indicator") in CONFIG["tasks"]: 
        individual_indicator.individual_indicator(pcat, id0, CONFIG, task)

# %% [markdown]
# # Indicators

    # %%
    reload()
    if (task := "indicators") in CONFIG["tasks"]: 
        indicators.indicators(pcat, id0, CONFIG, task)

# %% [markdown]
# # Climatology

    # %%
    reload()
    if (task := "climatology") in CONFIG["tasks"]: 
        climatology.climatology(pcat, id0, CONFIG, task)

# %% [markdown]
# # Ensemble

    # %%
    reload()
    if (task := "ensemble") in CONFIG["tasks"]: 
        ensemble.ensemble(pcat, id0, CONFIG, task)
