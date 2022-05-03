import datetime
import json
import time
from math import ceil

import pandas as pd
import geopandas as gpd
from shapely import geometry, wkt
import requests
from google.cloud import bigquery
from loguru import logger
from tqdm import tqdm


class BQClient:
    def __init__(self):

        self.client = bigquery.Client()

        self.min_dt = datetime.datetime(2010, 1, 1)

        self.tables = {
            'tracked_reservoirs':"oxeo-main.wave2web.tracked-reservoirs",
             'reservoir_data':"oxeo-main.wave2web.reservoir-data",
             'forecast_data':"oxeo-main.wave2web.forecast",
             'precip_data':"oxeo-main.wave2web.precipitation",
        }


    def check_errors(self, errors):

        if errors != []:
            raise ValueError(
                f"there where {len(errors)} error when inserting. " + str(errors),
            )

        return True

    def get_reservoirs(self):

        Q = f"""
            SELECT uuid, name
            FROM `{self.tables["tracked_reservoirs"]}`
        """

        df = self.client.query(Q).result().to_dataframe()

        return df
    
    def get_reservoir_gdf(self):

        Q = f"""
            SELECT *
            FROM `{self.tables["tracked_reservoirs"]}`
        """

        df = self.client.query(Q).result().to_dataframe()
        
        df['upstream_geom'] = df['upstream_geom'].apply(wkt.loads)
        
        gdf = gpd.GeoDataFrame(df, geometry='upstream_geom',crs='EPSG:4326').rename(columns={'upstream_geom':'geometry'}).set_index('name')

        return gdf

    def get_most_recent_date(self, table_str: str, uuid: str):

        Q = f"""
            SELECT MAX(DATETIME)
            FROM `{self.tables[table_str]}`
            WHERE RESERVOIR_UUID = '{uuid}'
        """

        df = self.client.query(Q).result().to_dataframe()

        return df.iloc[0]["f0_"].replace(tzinfo=None)
    
    def get_most_recent_dates(self, table_str: str):

        Q = f"""
            SELECT t.*
            FROM (SELECT t.*,
                         ROW_NUMBER() OVER (PARTITION BY reservoir
                                            ORDER BY date DESC
                                           ) as seqnum
                  FROM `{self.tables[table_str]}` as t
                 ) t
            WHERE seqnum = 1;
        """

        df = self.client.query(Q).result().to_dataframe()
        df['date'] = pd.to_datetime(df['date'])

        return df

    def push_data(self, table_str: str, df: pd.DataFrame):
        """
        df: a long-form dataframe indexed by (date, reservoir_name), remaining columns are data columns
        """

        errors = self.client.insert_rows_json(
            self.tables[table_str], df.to_dict(orient="records")
        )

        self.check_errors(errors)

        return True

