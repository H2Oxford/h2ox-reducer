# Use the official osgeo/gdal image.
FROM osgeo/gdal:ubuntu-small-latest

# Allow statements and log messages to immediately appear in the Knative logs
ENV PYTHONUNBUFFERED True


# Set default env vars to clear CI
#ENV PROVIDER GCP


#python -m pip install -U pip or python3 -m pip install -U pip
#pip3 install --upgrade setuptools

ENV APP_HOME /app

COPY ./main.py $APP_HOME/
COPY ./batch.py $APP_HOME/

WORKDIR $APP_HOME

# Copy local code to the container image.
# __context__ to __workdir__
COPY . ./h2ox-reducer
# Install GDAL dependencies

RUN echo $PWD

RUN echo $(ls)


RUN apt-get update
RUN apt-get install -y build-essential
RUN apt-get install -y python3 python3-pip
#RUN python3 -m pip install -U pip
RUN pip install --upgrade setuptools
#
#RUN pip install --upgrade pip

RUN echo $(which pip)
RUN echo $(which pip3)
RUN echo $(which python)
RUN echo $(which python3)
RUN echo $USER

# install binaries for cfgrib
# RUN apt-get install -y libeccodes0

# Install production dependencies.
RUN pip3 install ./h2ox-reducer

# Run the web service on container startup. Here we use the gunicorn
# webserver, with one worker process and 8 threads.
# For environments with multiple CPU cores, increase the number of workers
# to be equal to the cores available.
# Timeout is set to 0 to disable the timeouts of the workers to allow Cloud Run to handle instance scaling.
# CMD exec gunicorn --chdir /app --bind :$PORT --workers 1 --threads 8 --timeout 0 main:app
CMD python batch.py