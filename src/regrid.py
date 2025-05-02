import utils as u


def _regrid(ds, cfg, dsref):
    ds_regrid = xs.regrid_dataset(ds=ds, ds_grid=dsref, **cfg["regrid_dataset"])
    ds_regrid = ds_regrid.convert_calendar("365_day", align_on="year")
    ds_regrid.attrs["cat:domain"] = dsref.attrs["cat:domain"]
    # decoy values for reconstruction ... I just want a string there, why not?
    ds_regrid = u.fill_empty_facets(
        ds_regrid,
        ["experiment", "mip_era", "member", "activity", "bias_adjust_project"],
    )
    ds_regrid["lat"] = dsref.lat
    ds_regrid["lon"] = dsref.lon
    return ds_regrid


def regrid(pcat, id0, cfg, task, dsref=None):
    dsref = dsref or pcat.search(**cfg[task]["input_ref"]).to_dask()
    u.template_1d_func(pcat, id0, cfg, task, _regrid, dsref)
