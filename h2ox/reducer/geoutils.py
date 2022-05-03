"""
These geotools were adapted from tools developed for the Wave2Web Hackathon.
Developed with permission from license holders World Resources Institute and H2Ox (Lucas Kruitwagen, Chris Arderne, Thomas Lees, and Lisa Thalheimer)
"""

import geopandas as gpd
import numpy as np
import pandas as pd
from area import area
from shapely import geometry
from shapely.affinity import affine_transform

def binary_dilate(arr):
    if arr.shape[0]==1:
        return arr>0
    add_right = np.convolve(arr>0,[-1,1], mode='same')>0
    add_left = np.convolve(arr[::-1]>0,[-1,1],mode='same')[::-1]>0
    return (arr>0)+add_left+add_right

"""
def get_mask(lons, lats, geom, weighted=True):

    if lats[-1] < lats[0]:
        descending = True
    else:
        descending = False

    if ((lons.min() >= 0) and (lons.max() > 180)) and (geom.bounds[0] < 0):
        # pacific-centric projection 0-360deg
        # TODO: Greenwich intersections

        geotransform = [1, 0, 0, 1, 360, 0]  # [a, b, d, e, xoff, yoff]
        affine_geom = affine_transform(geom, geotransform)
        
        lon_idx = (lons>affine_geom.bounds[0]) & (lons < affin_geom.bounds[2])
        lon_idx = binary_dilate(lon_idx)
        #lower_lon_idx = np.where(lons <= affine_geom.bounds[0])[0].max()
        #upper_lon_idx = np.where(lons >= affine_geom.bounds[2])[0].min()

    else:
        lon_idx = (lons>geom.bounds[0]) & (lons < geom.bounds[2])
        lon_idx = binary_dilate(lon_idx)
        
        #lower_lon_idx = np.where(lons <= geom.bounds[0])[0].max()
        #upper_lon_idx = np.where(lons >= geom.bounds[2])[0].min()

    if descending:
        lat_idx = (lats > geom.bounds[1]) & (lats<geom.bounds[3])
        lat_idx = binary_dilate(lat_idx)
        # upper_lat_idx = np.where(lats < geom.bounds[1])[0].min()
        # lower_lat_idx = np.where(lats > geom.bounds[3])[0].max()

    else:
        lat_idx = (lats > geom.bounds[1]) & (lats<geom.bounds[3])
        lat_idx = binary_dilate(lat_idx)
        #lower_lat_idx = np.where(lats < geom.bounds[1])[0].max()
        #upper_lat_idx = np.where(lats > geom.bounds[3])[0].min()

    bounding_lons = lons[lon_idx]
    bounding_lats = lats[lat_idx]
    
    print ('bounding', bounding_lons.shape, bounding_lats.shape)

    if ((lons.min() >= 0) and (lons.max() > 180)) and (bounding_lons.min() > 180):
        # pacific-centric projection 0-360deg
        bounding_lons = bounding_lons - 360

    llons, llats = np.meshgrid(bounding_lons, bounding_lats)

    min_x = llons[:-1, :-1].flatten()
    max_x = llons[:-1, 1:].flatten()
    min_y = llats[:-1, :-1].flatten()
    max_y = llats[1:, :-1].flatten()

    gdf = (
        gpd.GeoDataFrame(
            pd.DataFrame(dict(minx=min_x, maxx=max_x, miny=min_y, maxy=max_y)).apply(
                lambda row: geometry.box(**row), axis=1
            )
        )
        .rename(columns={0: "geometry"})
        .set_geometry("geometry")
    )
    
    print (gdf)

    llon_idx, llat_idx = np.meshgrid(
        np.where(lon_idx)[0][:-1], np.where(lat_idx)[0][:-1]
    )
    
    print ('meshidx',llon_idx.shape, llat_idx.shape)

    gdf["lon_idx"] = llon_idx.flatten()
    gdf["lat_idx"] = llat_idx.flatten()

    gdf["geoarea"] = gdf.geometry.apply(lambda geom: area(geometry.mapping(geom)))

    gdf["intersection_area"] = gdf.intersection(geom).apply(
        lambda geom: area(geometry.mapping(geom))
    )

    gdf["area_weight"] = gdf["intersection_area"] / gdf["geoarea"]

    mask = np.zeros((lats.shape[0], lons.shape[0]))

    mask[
        tuple(gdf["lat_idx"].values.tolist()), tuple(gdf["lon_idx"].values.tolist())
    ] = gdf["area_weight"].values

    extents = (
        gdf.bounds.minx.min(),
        gdf.bounds.maxx.max(),
        gdf.bounds.miny.min(),
        gdf.bounds.maxy.max(),
    )
    
    mask = mask[
        np.where(lat_idx)[0][0]:np.where(lat_idx)[0][-1],
        np.where(lon_idx)[0][0]:np.where(lon_idx)[0][-1],
    ]
    
    print ('extents',extents)
    
    print ('mask shape',mask.shape, lat_idx.shape, lon_idx.shape)

    bounds = (lon_idx, lat_idx)

    return (
        mask,
        bounds,
        extents,
    )
"""


def get_mask(lons, lats, geom, weighted=True):

    if lats[-1] < lats[0]:
        descending = True
    else:
        descending = False

    if ((lons.min() >= 0) and (lons.max() > 180)) and (geom.bounds[0] < 0):
        # pacific-centric projection 0-360deg
        # TODO: Greenwich intersections

        geotransform = [1, 0, 0, 1, 360, 0]  # [a, b, d, e, xoff, yoff]
        affine_geom = affine_transform(geom, geotransform)
        lower_lon_idx = np.where(lons <= affine_geom.bounds[0])[0].max()
        upper_lon_idx = np.where(lons >= affine_geom.bounds[2])[0].min()

    else:
        lower_lon_idx = np.where(lons <= geom.bounds[0])[0].max()
        upper_lon_idx = np.where(lons >= geom.bounds[2])[0].min()

    if descending:
        upper_lat_idx = np.where(lats < geom.bounds[1])[0].min()
        lower_lat_idx = np.where(lats > geom.bounds[3])[0].max()

    else:

        lower_lat_idx = np.where(lats < geom.bounds[1])[0].max()
        upper_lat_idx = np.where(lats > geom.bounds[3])[0].min()

    bounding_lons = lons[lower_lon_idx : upper_lon_idx + 1]
    bounding_lats = lats[lower_lat_idx : upper_lat_idx + 1]

    if ((lons.min() >= 0) and (lons.max() > 180)) and (bounding_lons.min() > 180):
        # pacific-centric projection 0-360deg
        bounding_lons = bounding_lons - 360

    llons, llats = np.meshgrid(bounding_lons, bounding_lats)

    min_x = llons[:-1, :-1].flatten()
    max_x = llons[:-1, 1:].flatten()
    min_y = llats[:-1, :-1].flatten()
    max_y = llats[1:, :-1].flatten()

    gdf = (
        gpd.GeoDataFrame(
            pd.DataFrame(dict(minx=min_x, maxx=max_x, miny=min_y, maxy=max_y)).apply(
                lambda row: geometry.box(**row), axis=1
            )
        )
        .rename(columns={0: "geometry"})
        .set_geometry("geometry")
    )

    lon_idx, lat_idx = np.meshgrid(
        range(lower_lon_idx, upper_lon_idx), range(lower_lat_idx, upper_lat_idx)
    )

    gdf["lon_idx"] = lon_idx.flatten()
    gdf["lat_idx"] = lat_idx.flatten()

    gdf["geoarea"] = gdf.geometry.apply(lambda geom: area(geometry.mapping(geom)))

    gdf["intersection_area"] = gdf.intersection(geom).apply(
        lambda geom: area(geometry.mapping(geom))
    )

    gdf["area_weight"] = gdf["intersection_area"] / gdf["geoarea"]

    mask = np.zeros((lats.shape[0], lons.shape[0]))

    mask[
        tuple(gdf["lat_idx"].values.tolist()), tuple(gdf["lon_idx"].values.tolist())
    ] = gdf["area_weight"].values

    extents = (
        gdf.bounds.minx.min(),
        gdf.bounds.maxx.max(),
        gdf.bounds.miny.min(),
        gdf.bounds.maxy.max(),
    )

    return (
        mask[lower_lat_idx:upper_lat_idx, lower_lon_idx:upper_lon_idx],
        (lower_lon_idx, lower_lat_idx, upper_lon_idx, upper_lat_idx),
        extents,
    )