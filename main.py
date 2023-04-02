#!/usr/bin/python3

import json
import logging
import os
import sys
import time
import traceback
from datetime import datetime, timedelta
from typing import Optional

from flask import Flask, request
from loguru import logger
import numpy as np

from h2ox.reducer import XRReducer, BQClient, reduce_timeperiod_to_df
from h2ox.reducer.slackbot import SlackMessenger
from h2ox.reducer.gcp_utils import download_blob_to_filename, upload_blob, create_task, deploy_task

logger.remove()
logger.add(sys.stdout, colorize=False, format="{time:YYYYMMDDHHmmss}|{level}| {message}")


app = Flask(__name__)


if __name__ != "__main__":
    # Redirect Flask logs to Gunicorn logs
    gunicorn_logger = logging.getLogger("gunicorn.error")
    app.logger.handlers = gunicorn_logger.handlers
    app.logger.setLevel(gunicorn_logger.level)
    app.logger.info("Service started...")
else:
    app.run(debug=True, host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))


def format_stacktrace():
    parts = ["Traceback (most recent call last):\n"]
    parts.extend(traceback.format_stack(limit=25)[:-2])
    parts.extend(traceback.format_exception(*sys.exc_info())[1:])
    return "".join(parts)


@app.route("/", methods=["POST"])
def main():
    """Receive a request and queue downloading CHIRPS data

    Request params:
    ---------------

        today: str


    # download forecast (tigge or HRES)
    # ingest to zarr

    #if pubsub:
    envelope = request.get_json()
    if not envelope:
        msg = "no Pub/Sub message received"
        print(f"error: {msg}")
        return f"Bad Request: {msg}", 400

    if not isinstance(envelope, dict) or "message" not in envelope:
        msg = "invalid Pub/Sub message format"
        print(f"error: {msg}")
        return f"Bad Request: {msg}", 400

    request_json = envelope["message"]["data"]

    if not isinstance(request_json, dict):
        json_data = base64.b64decode(request_json).decode("utf-8")
        request_json = json.loads(json_data)

    logger.info('request_json: '+json.dumps(request_json))

    # parse request
    today_str = request_json['today']

    """

    time.time()

    payload = request.get_json()

    if not payload:
        msg = "no message received"
        print(f"error: {msg}")
        return f"Bad Request: {msg}", 400

    logger.info("payload: " + json.dumps(payload))
    logger.info("environ")
    logger.info(f"{os.environ.keys()}")

    if not isinstance(payload, dict):
        msg = "invalid task format"
        print(f"error: {msg}")
        return f"Bad Request: {msg}", 400
    
    token=os.environ.get("SLACKBOT_TOKEN")
    target=os.environ.get("SLACKBOT_TARGET")
    
    if token is not None and target is not None:

        slackmessenger = SlackMessenger(
            token=token,
            target=target,
            name="h2ox-reduction",
        )
    else:
        slackmessenger=None

    today_str = payload["today"]

    today = datetime.strptime(today_str, "%Y-%m-%d").replace(tzinfo=None)

    return main_loop(today, slackmessenger)


def main_loop(
    today: datetime,
    slackmessenger: Optional[SlackMessenger] = None,
    tigge_token_path: Optional[str] = None,
    chirps_token_path: Optional[str] = None,
    target_spec: Optional[dict] = None,
    requeue: Optional[bool] = True
):

    # 1. get variables from environment or args
    if tigge_token_path is None:
        tigge_token_path = os.environ.get("TIGGE_TOKEN_PATH")
    if chirps_token_path is None:
        chirps_token_path = os.environ.get("CHIRPS_TOKEN_PATH")
    if target_spec is None:
        target_spec = json.loads(os.environ.get("TARGET_SPEC"))
    if requeue is None:
        requeue  = os.environ.get("REQUEUE")=="true"
        
    
        
    # 2. get tokens from storage
    logger.info('Downloading tokens')
    tigge_token_local_path = os.path.join(os.getcwd(),'tigge_token.json')
    chirps_token_local_path = os.path.join(os.getcwd(),'chirps_token.json')
    
    download_blob_to_filename(tigge_token_path, tigge_token_local_path)
    download_blob_to_filename(chirps_token_path, chirps_token_local_path)
    
    # 3. get max-date from tokens
    tigge_token = json.load(open(tigge_token_local_path,'r'))
    chirps_token = json.load(open(chirps_token_local_path,'r'))
    
    tigge_dt = datetime.strptime(tigge_token['most_recent_tigge'],'%Y-%m-%d')
    chirps_dt = datetime.strptime(chirps_token['last_prelim'],'%Y-%m-%d') 
    logger.info(f'Got most recent datetimes: tigge: {tigge_dt}, chirps: {chirps_dt}')
    
    # 4. get all geometries
    client = BQClient()
    
    gdf = client.get_reservoir_gdf()
    
    # 5. get max-dates from BQ
    
    max_date_df = client.get_most_recent_dates('forecast_data')

    # 6. if archive dates > max-dates from BQ run reduction and upload
    # 6a. Tigge:
    forecast_rows = 0
    for unq_dt in max_date_df['date'].unique():
        if unq_dt< np.datetime64(tigge_dt):
            
            sites = max_date_df.loc[max_date_df['date']==unq_dt,'reservoir'].values
            logger.info(f'Doing Tigge {unq_dt}-{tigge_dt} with {len(sites)} sites')
            df = reduce_timeperiod_to_df(unq_dt, tigge_dt, target_spec['tigge'], gdf.loc[sites,:])
            # upload reduction
            client.push_data(table_str='forecast_data', df=df)
            forecast_rows += len(df)
          
    # 6b. chirps:
    max_date_df = client.get_most_recent_dates('precip_data')
    precip_rows = 0
    for unq_dt in max_date_df['date'].unique():
        # 6b. chirps:
        if unq_dt< np.datetime64(chirps_dt):
            
            sites = max_date_df.loc[max_date_df['date']==unq_dt,'reservoir'].values
            logger.info(f'Doing CHIRPS {unq_dt}-{chirps_dt} with {len(sites)} sites')
            df = reduce_timeperiod_to_df(unq_dt, chirps_dt, target_spec['chirps'], gdf.loc[sites,:])
            # upload reduction
            client.push_data(table_str='precip_data', df=df)
            precip_rows += len(df)
    
    # 6. enqueue tomorrow

    logger.info(
        f'Done reducing data. Pushed {forecast_rows} forecast rows and {precip_rows} precip rows.'
    )

    if slackmessenger is not None:
        slackmessenger.message(
            f"REDUCE ::: {today} pushed {forecast_rows} forecast rows and {precip_rows} precip rows"
        )

    if requeue:
        enqueue_tomorrow(today)

    return "Reduction complete", 200


def enqueue_tomorrow(today):

    tomorrow = today + timedelta(hours=24)

    cfg = dict(
        project=os.environ["project"],
        queue=os.environ["queue"],  # queue name
        location=os.environ["location"],  # queue
        url=os.environ["url"],  # service url
        service_account=os.environ["service_account"],  # service acct
    )

    task = create_task(
        cfg=cfg,
        payload=dict(today=tomorrow.isoformat()[0:10]),
        task_name=tomorrow.isoformat()[0:10],
        delay=24 * 3600,
    )

    deploy_task(cfg, task)