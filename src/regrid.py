import utils as u
import xscen as xs 

def _regrid(ds, dsref, cfg):
    ds_regrid = xs.regrid_dataset(ds=ds, ds_grid=dsref, **cfg["regrid_dataset"])
    ds_regrid = ds_regrid.convert_calendar("365_day", align_on="year")
    ds_regrid.attrs["cat:domain"] = dsref.attrs["cat:domain"]
    return ds_regrid


def regrid(pcat, id0, cfg, task, dsref=None):
    dsref = dsref or pcat.search(**cfg[task]["input_ref"]).to_dask()
    u.template_2d_func(pcat, id0, cfg, task, _regrid, dsref)
