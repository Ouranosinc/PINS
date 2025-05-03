""" Utils specific to PINS """

import warnings
from xclim.core.units import convert_units_to
from xclim.indices import run_length as rl
import xarray as xr
import numpy as np
import pandas as pd
from xclim.core import calendar

def decay_snow_season_end(
    snw,
    decay_factor=0.5,
    thresh_decay="1e-3 kg m-2",
    thresh_sse=None,
    window_sse=None,
    dim="time",
):
    """
    Add snow data points in the end of snow seasons for a smoother transition.

    Parameters
    ----------
    snw : xr.DataArray
        Snow amount.
    thresh_decay : str, optional
        Threshold below which snow is considered to be zero.
    decay_factor : float
        Snow decay factor.
    thresh_sse : str, optional
        Threshold for the end of the season (Snow-Season-End).
    window_sse: int, optional
        Window for the end of the season (Snow-Season-End).
    dim : str
        Dimension on which to perform the decaying. (This was put in when I was thinking of a general decay function
        agnostic to snow. With all the snow season end arguments, it would not make much sense to take anything else
        than "time", but I will left this in anyways)
    
    Returns
    -------
    xr.DataArray
        Snow amount with a smooth exponential transition.

    Notes
    -----
    * Values below `thresh_decay` are set to zero twice. Once before the decay occurs. This means that very small non-zero
    values will be on the same footing as zeroes of the series and can also be part of the smoother transition. After applying
    the smooth exponential decay, newly non-zero values that are below `thresh_decay` will be set to zero once and for all.
    * Both `thresh_sse` and `window_sse` need to be specified to apply inject decayed beyond
    the first sequence of 0's in the end season. Otherwise, the snow injection will occur everywhere.
    Recommended values are `thresh_sse="1 kg m-2"` and `window_sse="14"`.

    References
    ----------
    Michel et al, 2023 (SnowQM package)
    """
    attrs = snw.attrs
    thresh_decay = convert_units_to(thresh_decay, snw)
    snw = snw.where(snw > thresh_decay, 0)
    previous_non_zero_values = snw.where(snw > 0).ffill(dim=dim)
    # The further you are from a previous non-zero value, the largest the decay exponent is
    # If you were already non-zero, the exponent is 0, you receive a factor of 1
    decay_powers = rl._cumsum_reset(xr.where(snw == 0, 1, 0), dim=dim)
    # Apply decay factor (with appropriate power) on the non-zero values we just filled in place of zeroes
    decayed_da = previous_non_zero_values * (
        decay_factor ** (decay_powers.where(decay_powers > 0))
    )

    if None in (s := {thresh_sse, window_sse}):
        if len(s) > 1:
            warnings.warn(
                "`thresh_sse` and `window_sse` should be both `None`, or both specified, got only one `None`."
                "Proceeding as if they were both `None`."
            )
        # This will add snow anywhere possible
        mask = decayed_da > 0
    else:
        # This will add snow only find the first sequence of 0's in the end season
        thresh_sse = convert_units_to(thresh_sse, snw)
        run_lengths = rl.rle(snw < thresh_sse, dim="time", index="first").ffill(
            dim="time"
        )
        temp = xr.where((decayed_da > 0) & (run_lengths >= window_sse), 1, np.nan)
        temp = xr.where(temp.time.dt.dayofyear == 1, 0, temp)
        temp = rl._cumsum_reset(temp, dim=dim)
        temp = temp.where(temp == 1)
        temp = xr.where(snw > 0, 0, temp)
        mask = temp.ffill(dim=dim) == 1

    # Is this step only useful for the snow season branch? I have a doubt
    out = xr.where(mask, decayed_da, snw)
    out = out.where(out > thresh_decay, 0).assign_attrs(attrs).astype(np.float32)
    return out.where(snw)


def _get_date(xrfreq):
    month = pd.to_datetime(xrfreq, format="AS-%b", exact=True).month
    return f"{month:02d}-01"


def doy_to_days_since(ds: xr.Dataset):
    if isinstance(ds, xr.Dataset):
        for v in ds.data_vars:
            if ds[v].attrs.get("standard_name", "") == "day_of_year":
                ds[v] = calendar.doy_to_days_since(ds[v])
        return ds


def days_since_to_doy(ds: xr.Dataset):
    if isinstance(ds, xr.Dataset):
        for v in ds.data_vars:
            if ds[v].attrs.get("standard_name", "") == "day_of_year":
                ds[v] = calendar.days_since_to_doy(ds[v])
        return ds


def timedelta_to_datetime(ds: xr.Dataset):
    if isinstance(ds, xr.Dataset):
        for v in ds.data_vars:
            if "timedelta" in str(ds[v].dtype):
                ds[v] = ds[v].astype("datetime64[ns]")
        return ds
