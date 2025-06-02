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
# # Init

# %%
from xscen import ProjectCatalog, load_config,CONFIG
import importlib 
from pathlib import Path
import os 
from copy import deepcopy
from itertools import product

src_mods = ["extract", "rechunk", "regrid", "train", "adjust", "individual_indicator", "indicators", "climatology", "ensemble"]
r_mods = [("src." + m,m) for m in src_mods] + [("utils","u"), ("pins_utils","pu")]
cfgfiles = ['config/paths.yml', "config/config.yml", "config/schemas.yml"]

# load
modules = {short_key:importlib.import_module(mod_name) for mod_name,short_key in r_mods}
globals().update(modules)
load_config(*cfgfiles, verbose=(__name__ == '__main__'), reset=True)

def reload(mods=modules): 
    global CONFIG,cfg
    for mod in mods.values():
        importlib.reload(mod)
    load_config(*cfgfiles, verbose=(__name__ == '__main__'), reset=True)
    cfg = deepcopy(CONFIG)

if not os.path.isfile(CONFIG["paths"]['project_catalog'])  or  "initialize_pcat" in CONFIG["tasks"]:
    pcat = ProjectCatalog.create(CONFIG["paths"]['project_catalog'], project=CONFIG['project'], overwrite=True)
    for k in CONFIG["paths"].keys(): 
        if CONFIG["paths"][k] is not None and k.endswith("dir"): 
            Path(CONFIG["paths"][k]).mkdir(parents=True, exist_ok=True)
pcat = ProjectCatalog(CONFIG["paths"]['project_catalog'])

sim_ids  = list(pcat.search(processing_level="extracted", type="simulation").df.id)
xrfreqs = set(pcat.search(processing_level="individual_indicator").df.xrfreq)


# %% [markdown]
# # Main

# %%
if __name__ == '__main__':

# %% [markdown] jp-MarkdownHeadingCollapsed=true
# # Extract

    # %%
    reload()
    if (task := "extract_reference") in CONFIG["tasks"]: 
        extract.main(pcat, CONFIG, task)

    if (task := "extract_simulation") in CONFIG["tasks"]: 
        extract.main(pcat, CONFIG, task)
        sim_ids =  list(pcat.search(processing_level="extracted", type="simulation").df.id)


# %% [markdown] jp-MarkdownHeadingCollapsed=true
# # Regrid

    # %%
    reload()
    if (task := "regrid") in CONFIG["tasks"]: 
        for sim_id in sim_ids:
            regrid.main(pcat,CONFIG,task, wildcards= {"sim_id":sim_id})

# %% [markdown]
# # Rechunk

    # %%
    reload()
    if (task := "rechunk") in CONFIG["tasks"]: 
        for sim_id in sim_ids:
            rechunk.main(pcat,CONFIG,task, wildcards={"sim_id":sim_id})

# %% [markdown] jp-MarkdownHeadingCollapsed=true
# # Raw indicators

# %%
# u._delete_kws_(pcat, {"processing_level":["raw_individual_indicator", "raw_indicators"]})

    # %%
    # Maybe just restrict to SSS and SSE, currently all raw indicators are computed
    reload()
    if (task := "raw_individual_indicator") in CONFIG["tasks"]: 
        for sim_id in sim_ids+["ECMWF_ERA5-Land_NAM"]: 
            individual_indicator.main(pcat, CONFIG, task, wildcards={"sim_id":sim_id})
    if (task := "raw_indicators") in CONFIG["tasks"]: 
        xrfreqs0 = set(pcat.search(processing_level="raw_individual_indicator").df.xrfreq)
        for sim_id, xrfreq in product(sim_ids+["ECMWF_ERA5-Land_NAM"], xrfreqs0):
            indicators.main(pcat,CONFIG, task, wildcards={"sim_id":sim_id, "xrfreq":xrfreq})

# %% [markdown] jp-MarkdownHeadingCollapsed=true
# # Decay snow

    # %%
    reload()
    if (task := "decay") in CONFIG["tasks"]: 
        for sim_id in sim_ids:
            decay.main(pcat, CONFIG, task, wildcards={"sim_id":sim_id})

# %% [markdown]
# # Train

    # %%
    reload()
    if (task := "train") in CONFIG["tasks"]:  
        for sim_id,var in product(sim_ids, CONFIG[task]["variables"]):
            # train.main(pcat, CONFIG, task,  wildcards={"sim_id":sim_id, "var":var})

# %%
sim_ids

# %% [markdown] jp-MarkdownHeadingCollapsed=true
# # Adjust

    # %%
    reload()
    if (task := "adjust") in CONFIG["tasks"]:  
        for sim_id,var in product(sim_ids, CONFIG[task]["variables"]):
            adjust.main(pcat, CONFIG, task,  wildcards={"sim_id":sim_id, "var":var})

# %% [markdown] jp-MarkdownHeadingCollapsed=true
# # Individual indicator

    # %%
    reload()
    if (task := "individual_indicator") in CONFIG["tasks"]: 
        for sim_id in sim_ids: 
            individual_indicator.main(pcat, CONFIG, task, wildcards={"sim_id":sim_id})
        xrfreqs = set(pcat.search(processing_level="individual_indicator").df.xrfreq)


# %% [markdown] jp-MarkdownHeadingCollapsed=true
# # Indicators

    # %%
    reload()
    if (task := "indicators") in CONFIG["tasks"]: 
        for sim_id, xrfreq in product(sim_ids, xrfreqs):
            indicators.main(pcat,CONFIG, task, wildcards={"sim_id":sim_id, "xrfreq":xrfreq})

# %% [markdown] jp-MarkdownHeadingCollapsed=true
# # Climatology

    # %%
    reload()
    if (task := "climatology") in CONFIG["tasks"]: 
        for sim_id, xrfreq in product(sim_ids, xrfreqs):
            climatology.main(pcat,CONFIG, task, wildcards={"sim_id":sim_id, "xrfreq":xrfreq})


# %% [markdown] jp-MarkdownHeadingCollapsed=true
# # Ensemble

    # %%
    reload()
    if (task := "ensemble") in CONFIG["tasks"]: 
        for xrfreq,experiment in product(xrfreqs,["rcp45"]): 
            ensemble.main(pcat, CONFIG, task, wildcards={"experiment":experiment, "xrfreq":xrfreq, "experiment":experiment})
