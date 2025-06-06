# ---
# jupyter:
#   jupytext:
#     text_representation:
#       extension: .py
#       format_name: nomarker
#       format_version: '1.0'
#       jupytext_version: 1.17.1
#   kernelspec:
#     display_name: Python 3 (ipykernel)
#     language: python
#     name: python3
# ---

# # Init

from __future__ import annotations
from xscen import ProjectCatalog, load_config,CONFIG
import importlib 
from pathlib import Path
import os 
from copy import deepcopy
from itertools import product
from omegaconf import OmegaConf

src_mods = ["extract", "rechunk", "regrid", "train", "adjust", "individual_indicator", "indicators", "climatology", "ensemble"]
r_mods = [("src." + m,m) for m in src_mods] + [("utils","u"), ("pins_utils","pu")]
cfgfiles = ['config/paths.yml', "config/config.yml", "config/schemas.yml"]

# load
modules = {short_key:importlib.import_module(mod_name) for mod_name,short_key in r_mods}
globals().update(modules)

def reload(mods=modules): 
    global CONFIG
    for mod in mods.values():
        importlib.reload(mod)
    load_config(*cfgfiles, verbose=(__name__ == '__main__'), reset=True)
    cfg = dict(deepcopy(CONFIG))
    cfg = OmegaConf.create(dict(cfg))
    OmegaConf.resolve(cfg)
    CONFIG.update(cfg) 
reload()

if not os.path.isfile(CONFIG["paths"]['project_catalog'])  or  "initialize_pcat" in CONFIG["tasks"]:
    pcat = ProjectCatalog.create(CONFIG["paths"]['project_catalog'], project=CONFIG['project'], overwrite=True)
    for k in CONFIG["paths"].keys(): 
        if CONFIG["paths"][k] is not None and k.endswith("dir"): 
            Path(CONFIG["paths"][k]).mkdir(parents=True, exist_ok=True)
pcat = ProjectCatalog(CONFIG["paths"]['project_catalog'])

sim_ids  = list(pcat.search(processing_level="extracted", type="simulation").df.id)
# Select only simulations with a seasonality similar to the reference. 
# This is determined using the raw indicators in analysis.ipynb
# The major part of the workflow is then only performed on simulations deemed acceptable
sim_ids0  = CONFIG["selected_ids"]
xrfreqs = set(pcat.search(processing_level="individual_indicator").df.xrfreq)


if __name__ == '__main__':

# # Extract

    reload()
    if (task := "extract_reference") in CONFIG["tasks"]: 
        extract.main(pcat, CONFIG, task)

    if (task := "extract_simulation") in CONFIG["tasks"]: 
        extract.main(pcat, CONFIG, task)
        sim_ids =  list(pcat.search(processing_level="extracted", type="simulation").df.id)


# # Regrid

    reload()
    if (task := "regrid") in CONFIG["tasks"]: 
        for sim_id in sim_ids:
            regrid.main(pcat,CONFIG,task, wildcards= {"sim_id":sim_id})

# # Rechunk

    reload()
    if (task := "rechunk") in CONFIG["tasks"]: 
        for sim_id in sim_ids:
            rechunk.main(pcat,CONFIG,task, wildcards={"sim_id":sim_id})

# # Raw indicators

    # Maybe just restrict to SSS and SSE, currently all raw indicators are computed
    # Using these raw indicators, it is now possible to see which simulation have very bad seasonality 
    # and reject them. How it is done is shown in analysis.ipynb
    reload()
    if (task := "raw_individual_indicator") in CONFIG["tasks"]: 
        for sim_id in sim_ids+["ECMWF_ERA5-Land_NAM"]: 
            individual_indicator.main(pcat, CONFIG, task, wildcards={"sim_id":sim_id})
    if (task := "raw_indicators") in CONFIG["tasks"]: 
        xrfreqs0 = set(pcat.search(processing_level="raw_individual_indicator").df.xrfreq)
        for sim_id, xrfreq in product(sim_ids+["ECMWF_ERA5-Land_NAM"], xrfreqs0):
            indicators.main(pcat,CONFIG, task, wildcards={"sim_id":sim_id, "xrfreq":xrfreq})

# # Decay snow

    reload()
    if (task := "decay") in CONFIG["tasks"]: 
        for sim_id in sim_ids0:
            decay.main(pcat, CONFIG, task, wildcards={"sim_id":sim_id})

# # Train

    reload()
    if (task := "train") in CONFIG["tasks"]:  
        for sim_id,var in product(sim_ids0, CONFIG[task]["variables"]):
            train.main(pcat, CONFIG, task,  wildcards={"sim_id":sim_id, "var":var})

# # Adjust

    reload()
    if (task := "adjust") in CONFIG["tasks"]:  
        for sim_id,var in product(sim_ids0, CONFIG[task]["variables"]):
            adjust.main(pcat, CONFIG, task,  wildcards={"sim_id":sim_id, "var":var})

# # Individual indicator

    reload()
    if (task := "individual_indicator") in CONFIG["tasks"]: 
        for sim_id in sim_ids0: 
            individual_indicator.main(pcat, CONFIG, task, wildcards={"sim_id":sim_id})
        xrfreqs = set(pcat.search(processing_level="individual_indicator").df.xrfreq)


# # Indicators

    reload()
    if (task := "indicators") in CONFIG["tasks"]: 
        for sim_id, xrfreq in product(sim_ids0, xrfreqs):
            indicators.main(pcat,CONFIG, task, wildcards={"sim_id":sim_id, "xrfreq":xrfreq})

# # Climatology

    reload()
    if (task := "climatology") in CONFIG["tasks"]: 
        for sim_id, xrfreq in product(sim_ids0, xrfreqs):
            climatology.main(pcat,CONFIG, task, wildcards={"sim_id":sim_id, "xrfreq":xrfreq})


# # Ensemble

    reload()
    if (task := "ensemble") in CONFIG["tasks"]: 
        for xrfreq,experiment in product(xrfreqs,["rcp45"]): 
            ensemble.main(pcat, CONFIG, task, wildcards={"experiment":experiment, "xrfreq":xrfreq})
