[<img alt="Wave2Web Hack" width="1000px" src="https://github.com/H2Oxford/.github/raw/main/profile/img/wave2web-banner.png" />](https://www.wricitiesindia.org/content/wave2web-hack)

H2Ox is a team of Oxford University PhD students and researchers who won first prize in the[Wave2Web Hackathon](https://www.wricitiesindia.org/content/wave2web-hack), September 2021, organised by the World Resources Institute and sponsored by Microsoft and Blackrock. In the Wave2Web hackathon, teams competed to predict reservoir levels in four reservoirs in the Kaveri basin West of Bangaluru: Kabini, Krishnaraja Sagar, Harangi, and Hemavathy. H2Ox used sequence-to-sequence models with meterological and forecast forcing data to predict reservoir levels up to 90 days in the future.

The H2Ox dashboard can be found at [https://h2ox.org](https://h2ox.org). The data API can be accessed at [https://api.h2ox.org](https://api.h2ox.org/docs#/). All code and repos can be [https://github.com/H2Oxford](https://github.com/H2Oxford). Our Prototype Submission Slides are [here](https://docs.google.com/presentation/d/1J_lmFu8TTejnipl-l8bXUZdKioVseRB4tTzqK6sEokI/edit?usp=sharing). The H2Ox team is [Lucas Kruitwagen](https://github.com/Lkruitwagen), [Chris Arderne](https://github.com/carderne), [Tommy Lees](https://github.com/tommylees112), and [Lisa Thalheimer](https://github.com/geoliz).

# H2Ox - Reducer
This repo is for a dockerised service to reduce geospatial [Zarr archives](https://zarr.readthedocs.io/en/stable/) using [shapely](https://shapely.readthedocs.io/en/stable/manual.html) geometries. The Zarr data is rechunked in the time domain in blocks of four years. It is reduced geospatially using [h2ox/reducer/xr_reducer.py](https://github.com/H2Oxford/h2ox-reducer/blob/main/h2ox/reducer/xr_reducer.py) and [h2ox/reducer/geoutils.py](https://github.com/H2Oxford/h2ox-reducer/blob/main/h2ox/reducer/geoutils.py). This reduces a three- or four-dimensional data block (lon-lat-time(-forecast_step)) to a one- or two-dimension (time(-forecast_step)) timeseries using either weighted `sum` or `mean` of intersected pixels. 

This repo is made to be setup as a containerised service for reducing `TIGGE` and `CHIRPS` data for use in the [h2ox-wave2web](https://github.com/H2Oxford) pipeline.
The respective zarr archives are reduced over the scope water basins.
The reduced timeseries data is then pushed to a [BigQuery](https://cloud.google.com/bigquery) table for further use downstream.

## Installation

This repo can be `pip` installed:

    pip install https://github.com/H2Oxford/h2ox-reducer.git

For development, the repo can be pip installed with the `-e` flag and `[dev]` options:

    git clone https://github.com/H2Oxford/h2ox-reducer.git
    cd h2ox-reducer
    pip install -e .[dev]

For containerised deployment, a docker container can be built from this repo:

    docker build -t <my-tag> .

Cloudbuild container registery services can also be targeted at forks of this repository.

## Useage

### Containerised Cloud Service

The Flask app in `main.py` listens for a POST http request and then triggers the ingestion workflow.
The http request must have a json payload with a YYYY-mm-dd datetime string keyed to "today": `{"today":"<YYYY-mm-dd>"}`.
The reduction script then:

1. gets tokens for the most recent TIGGE and CHIRPS data
2. gets all waterbasin geometries
3. reduces both CHIRPS and TIGGE data over the geometries
4. pushes reduced timeseries to rows in BigQuery tables.

A slackbot messenger is also implemented to post updates to a slack workspace.
Follow [these](https://api.slack.com/bot-users) instuctions to set up a slackbot user, and then set the `SLACKBOT_TOKEN` and `SLACKBOT_TARGET` environment variables.

Parameters for `tigge` and `chirps` ingestion are passed in a json-parseable `target-spec` environment variable. This might look like:

    json.dumps({
        "chirps": {
            "url": "<gs://path/to/chirps/archive>", 
            "lat_col": "latitude", 
            "lon_col": "longitude", 
            "variables": ["precip"], 
            "variables_rename": ["value"]
        }, 
        "tigge": {
            "url": "<gs://path/to/tigge/archive>", 
            "lat_col": "latitude", 
            "lon_col": "longitude", 
            "variables": ["tp", "t2m"], 
            "variables_rename": ["values_precip", "values_temp"]
        }
    })
    
The spec will look for the field `variables_rename` to rename the archive variables to match the BigQuery schema.

The following environment variables are required:

    SLACKBOT_TOKEN=<my-slackbot-token>                  # a token for a slack-bot messenger
    SLACKBOT_TARGET=<my-slackbot-target>                # target channel to issue ingestion updates
    TIGGE_TOKEN_PATH=<gs://path/to/tigge/token.json>    # the directory to store the tigge ingestion token
    CHIRPS_TOKEN_PATH=<gs://path/to/chirps/token.json>  # the path to the chirps ingestion token
    target_spec=<json-parseable-string>               # a target spec with the parameters for ingesting tigge and chirps data
    requeue=<true|false>                                # boolean variable to requeue the next day's reduction task

If `requeue` is set to `TRUE`, to requeue the next day's ingestion, the ingestion script will push a task to a [cloud task queue](https://cloud.google.com/tasks/docs/creating-queues) to enqueue ingestion for tomorrow. This way a continuous service is created that runs daily. The additional environment variables will be required:

    project=<my-gcp-project>            # gcp project associated with queue and cloud storage
    queue=<my-queue-name>               # queue name where pending tasks can be places
    location=<my-queue-region>          # location name for task queue
    url=<http://my/dockerised/service>  # url of the entrypoint for the docker container to be run
    service_account=<myacct@email.com>  # service account for submitting tasks and http request


Environment variables can be put in a `.env` file and passed to the docker container at runtime:

    docker run --env-file=.env -t <my-tag>

### Prototyping and Development

[xarray](https://docs.xarray.dev/en/stable/) can be used with a zarr backend to lazily access very large zarr archives.
`h2ox-reducer` can then be used to select slices of a large zarr archive for prototyping, development, and data science.
Reduced xarray `arrays` are lazily computed, they need to be called with `.compute()` to execute.

<img alt="Zarr Xarray" width="600px" src="https://github.com/H2Oxford/.github/raw/main/profile/img/zarr_reducer.png"/>


## Citation

Our Wave2Web submission can be cited as:

    <citation here>

