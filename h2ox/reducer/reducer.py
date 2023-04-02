from typing import List, Dict
from datetime import datetime, timedelta

from gcsfs import GCSFileSystem
import pandas as pd
import geopandas as gpd
import xarray as xr

from h2ox.reducer import XRReducer


def reduce_timeperiod_to_df(
    start_dt: datetime, 
    end_dt: datetime, 
    target_spec: dict,  
    gdf: gpd.GeoDataFrame,
):
    
    # map the zxr
    mapper = GCSFileSystem(requester_pays=True).get_mapper
    zx_arr = xr.open_zarr(mapper(target_spec['url']))

    # reduce the xarray object for each variable - geometry
    reduced_var_arrays: Dict[str, xr.DataArray] = {}
    for variable in target_spec['variables']:
        reduced_geom_arrays = {}
        ds = XRReducer(
            array=zx_arr[variable],
            lat_variable=target_spec['lat_col'], 
            lon_variable=target_spec['lon_col'],
        )

        # for each geometry in the gdf
        for idx, row in gdf.iterrows():
            reduced_geom_arrays[idx] = ds.reduce(
                row["geometry"], start_dt, end_dt
            )

        reduced_var_arrays[variable] = xr.concat(
            list(reduced_geom_arrays.values()),
            pd.Index(list(reduced_geom_arrays.keys()), name="reservoir"),
        )
        reduced_var_arrays[variable].name = variable

    # merge back along the variable dimension
    array = xr.merge(list(reduced_var_arrays.values()))

    # force daily time dimension
    array = array.resample({"time": "1D"}).mean("time")
    
    if 'step' in array.coords.keys():
        array = array.resample({'step':timedelta(days=1)}).mean('step')
    
    # compute from dask
    array = array.compute()

    # cast to dataframe
    df = array.to_dataframe()

    # add date and timestamp
    df['date'] = df.reset_index()['time'].dt.date.values
    df['date'] = df['date'].apply(lambda el: el.strftime("%Y-%m-%d"))
    df['timestamp'] = datetime.now()
    df['timestamp'] = df['timestamp'].dt.strftime("%Y-%m-%dT%H:%M:%S.%f")

    df = df.rename(columns=dict(zip(target_spec['variables'], target_spec['variables_rename']))).reset_index().drop(columns=['time'])
    
    if 'step' in df.columns:
        df = pd.concat([df.groupby(['reservoir','date','timestamp'])[variable].apply(list) for variable in target_spec['variables_rename']], axis=1)
        df = df.reset_index()

    return df