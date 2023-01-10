# SCAMP-ML EDE-Service

This service is a part of the SCAMP-ML project. It is a RESTful service that provides a way to run 
EDE-Service jobs on a cluster. It is a part of the SCAMP-ML project. It is a RESTful service that provides a way to 
run cycle detection/identification and analysis tasks.

## Architecture

ToDo

## REST API

The REST API is split up into 4 distinct sections, each dealing with a particular functionality.

### Status
This resource provides information about the current state of the service.

`GET /v1/ede`

Returns core ML library versions. This is useful for debugging and troubleshooting. An example response
can be found bellow:
```json
{
  "libraries": [
    {
      "module": "sklearn",
      "version": "1.2.0"
    },
    {
      "module": "stumpy",
      "version": "1.11.1"
    },
    {
      "module": "shap",
      "version": "0.41.0"
    }
  ]
}
```

### Engine

These resource deal with the execution engine. The engine is responsible for executing detection jobs and
configuring Redis workers.

`GET /v1/ede/config`

Returns the current configuration of the engine. An example response can be found bellow:
```json
{
  "operators": {
    "anomaly": {
      "iforest.IForest": {
        "behaviour": "new",
        "bootstrap": 0,
        "contamination": 0.05,
        "max_features": 1,
        "max_samples": "auto",
        "n_estimators": 100,
        "n_jobs": -1,
        "random_state": null,
        "verbose": 0
      },
      "mark": 0,
      "model": "iforest_d355.joblib"
    },
    "cluster": {
      "HDSCAN": {
        "algorithm": "best",
        "leaf_size": 30,
        "min_cluster_size": 30,
        "min_samples": 5
      },
      "bootstrap": 0,
      "model": "hdbscan_d355.joblib"
    },
    "cycle_detect": {
      "checkpoint": 1,
      "delta_bias": 10,
      "max_distance": 10,
      "pattern": "df_pattern_d355.csv"
    },
    "scaler": {
      "MinMaxScaler": {
        "clip": 0,
        "copy": 0
      }
    }
  },
  "out": {
    "grafana": {},
    "kafka": {
      "broker": "kafka:9092",
      "topic": "ede_events"
    }
  },
  "source": {
    "local": "dev355.csv"
  }
}
```

The configuration file is split up into several subsections:

* __source__ - Deals with the datasource to be used. In the current example a local datasource is defined. Data sources are defined using other REST resources (see source section). These sources can be:
  * __ts_source__ - Time Series Database source, currently we support InfluxDB.
  * __kafka_source__ - Using a Kafka topic as source.
  * __minio_source__ - Using a Minio bucket as source.
* __out__ - Deals with the output of the engine. Currently we support:
  * __kafka__ - Output to a Kafka topic.
  * __grafana__ - Output to a Grafana dashboard.
* __operators__ - Deals with the operators that are used in the engine. Currently we support:
  * __scaler__ - Scaling operator as defined in scikit-learn [documentation](https://scikit-learn.org/stable/modules/preprocessing.html). All scaling and normalization methods are supported.
  * __anomaly__ - Anomaly detection methods. All methods supported by PyOD. In the current example we see how an Isolation Forest model can be trained. For further details please consult the offical PyOD [documentation](https://pyod.readthedocs.io/en/latest/index.html).
  * __cluster__ - Clustering methods. Current version supports only DBSCAN, Optics and HDBSCAN.
  * __cycle_detect__ - Cycle detection method. The above example show how that users have to define a known good pattern and the _max_distance_ and _delta_bias_ parameters used for filtering of overlapping cycles.

`PUT /v1/ede/config`

Is used for submitting a new configuration. See `GET /v1/ede/config` for more details.

`POST /v1/ede/config/revert`

Reverts the configuration to the default version as detailed in the `GET /v1/ede/config` response.

`POST /v1/ede/detect`

Start the detection based around the engine configuration. There are several prerequistes which have to be met before this resource can be used:
* All sources have to be defined. See __Source__ section for more details.
* REDIS workers have to be up and running. See resource `/v1/ede/worker` for more details.
* A pattern has to be defined. See resource `/v1/ede/pattern` for more details.

`GET /v1/ede/jobs`

Return all jobs of the detection engine. There are 4 states in which a job can be: _failed_, _finished_, _queued_ and _started_.
An example response can be seen bellow:

```json
{
  "failed": [
    "0daefeef-766d-4581-bda1-80aa5d3da910"
  ],
  "finished": [
    "6cf4bafc-fad4-40dd-bcd8-e719d0772159",
    "6b9a9706-acb8-4084-93b4-7288d70e58e5",
    "fab19623-4bc4-40f8-b0bb-dcbca47b9911",
    "3ecb430c-313b-4802-8764-0812422c7e83",
  ],
  "queued": [
    "d118ca0c-e890-420d-a82b-a19476412582"
  ],
  "started": [
    "09703919-0ff9-442b-a21e-3bdb00a0b8e4",
    "c5f03c77-0f30-45d9-b170-ec46f5c926ba",
    "8dae27cc-3a68-447b-97dd-0ecd0f123b91"
  ]
}
```

`GET /v1/ede/jobs/<job_id>`

This resource returns information about a job based on the jobs uuid. Each analysis and detection step is logged in and an appropriate message will appear in the _meta_ section of the JSON response.
An example can be seen bellow:

```json
{
  "finished": false,
  "meta": {
    "progress": "Computing heuristic overlap"
  },
  "status": "started"
}
```

`GET /v1/ede/pattern`

This resource can be used to fetch the currently used pattern for cycle detection:

```json
{
  "patterns": [
    "df_pattern_d355.csv",
    "pattern_d355.csv"
  ]
}
```

__NOTE__: Currently we support only csv defined patterns. Future version we will support time based indexing for the definition of patterns.

`PUT /v1/ede/pattern/<pattern>`

This resource is used to upload a pattern file to the service. The __pattern__ parameter is used as the name of the csv file.

`DELETE /v1/ede/pattern/<pattern>`

This resource deletes the pattern as defined by the __pattern__ parameter.

`GET /v1/ede/workers`

This resource returns a descripion of all of the REDIS workers curently registered to the service. An example response can be found bellow:

```json
{
  "workers": [
    {
      "id": "277f66c6068a4ca89af3a0c65153401c",
      "pid": 68,
      "status": false
    },
    {
      "id": "b31f0f0e57654f87880c274cd666c2ff",
      "pid": 38,
      "status": false
    }
  ]
}
```

__NOTE__: The _status_ parameter is used to indicate if the worker is currently operational or not. The maximum number of workers is based around the number of physical CPU cores available.

`POST /v1/ede/workers`

This resource is used to start REDIS workers. Each request starts a worker until the maximum number of workers has been reached.

`DELETE /v1/ede/workers`

Stops REDIS workers for each request. If there are workers which are registered but are not running they will be deregistered and removed.

### Source

These resources are used to define data source which can be used by the detection service.

`GET /v1/source`

Returns the current configuration for each datasource registered:

```json
{
  "source": {
    "kafka_source": {
      "host": "kafka.example.com",
      "port": 9092,
      "topic": "scamp-ede"
    },
    "local_source": "/data",
    "minio_source": {
      "access_key": "<access_key>",
      "host": "hal.example.com:9000",
      "secret_key": "<secret_key>",
      "secure": false
    },
    "ts_source": {
      "bucket": "sensors",
      "host": "https://influxdb.services.example.com",
      "org": "scamp",
      "port": 8086,
      "token": "<token>"
    }
  }
}
```

`PUT /v1/source` 

Is used to define data source as shown for `GET /v1/source`.

`GET /v1/source/<source>`

Return the information for each data source. The example from bellow has the __source__ parameter set to __minio__:

```json
{
  "access_key": "<access_keys>",
  "host": "hal.example.com:9000",
  "secret_key": "<secret_key>",
  "secure": false
}
```

__Note__: The supported sources are as follows: _minio_, _kafka_, _influxdb_ and _local_.

`PUT /v1/source/<source>`

Used to modify source configuration based on the __source__ parameter. See `GET /v1/source/<source>` for more details.

`GET /v1/source/local/<data>`

This resource is used to return the local data source file as defined by the __data__ parameter. The __data__ parameter is the name of the file without the extension. The file extension is automatically added based on the type of data source.

`GET /v1/source/local/<data>`

This resource is used to upload a data file to the local data source.

### Data

These resources are used to upload and modify data which can be used by the detection service.

`PUT /v1/source/remote/<source>`

This resource is used to upload data to the remote data source. The __source__ parameter is used to define the data source. The supported sources are as follows: _minio_, _kafka_ and _local_.

__NOTE__: Additional resource for listing data available in remote data sources are not yet implemented. Data source should be configured separately. 

## Environment Variables

We use several environment variables to configure the service. The following variables are required:


* EDE_HOST - the host of the EDE-Service
* EDE_PORT - the port of the EDE-Service
* EDE_DEBUG - the debug level of the EDE-Service
* EDE_USER - the user to use for the EDE-Service
* REDIS_END - the endpoint for redis queue
* REDIS_PORT - the port for redis queue

All environment variables have default values in acrodance with the libraries used.