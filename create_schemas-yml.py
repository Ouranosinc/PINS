# Schema for filenaming
import xscen as xs
from omegaconf import OmegaConf
from copy import deepcopy

# reuse existing schema
dd = xs.catutils._read_schemas("resources/file_schema.yml")
research_schema = dd["original-sims-ba"]
research_schema.pop("with")
research_schema_no_var = deepcopy(research_schema)
for k in research_schema.keys():
    research_schema_no_var[k].remove("variable")
reconstruction_schema = dd["derived-reconstruction"]
research_schema_xrfreq = deepcopy(research_schema)
research_schema_no_var_xrfreq = deepcopy(research_schema_no_var)
research_schema_xrfreq["filename"] = [
    v.replace("frequency", "xrfreq") for v in research_schema_xrfreq["filename"]
]
research_schema_no_var_xrfreq["filename"] = [
    v.replace("frequency", "xrfreq") for v in research_schema_no_var_xrfreq["filename"]
]
dd = {
    "schemas": {
        "schema": research_schema,
        "schema_no_var": research_schema_no_var,
        "schema_xrfreq": research_schema_xrfreq,
        "schema_xrfreq_no_var": research_schema_no_var_xrfreq,
    }
}

dd = OmegaConf.create(dd)
OmegaConf.save(dd, "config/schemas.yml")
