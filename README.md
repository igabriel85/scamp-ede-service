# SCAMP-ML EDE-Service

This service is a part of the SCAMP-ML project. It is a RESTful service that provides a way to run 
EDE-Service jobs on a cluster. It is a part of the SCAMP-ML project. It is a RESTful service that provides a way to 
run cycle detection/identification and analysis tasks.

## Architecture

ToDo

## REST API

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
      "topic": "ads_events"
    },
    "influxdb": "ede"
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
  * __local__ - Using a local file as source.
  * __resample__ - Resample the data to a specific time interval. This is useful for data which is not sampled at a constant rate.
* __out__ - Deals with the output of the engine. Currently we support:
  * __kafka__ - Output to a Kafka topic.
  * __grafana__ - Output to a Grafana dashboard.
  * __influxdb__ - Output to an InfluxDB database. Must specify bucket for output.
* __operators__ - Deals with the operators that are used in the engine. Currently we support:
  * __scaler__ - Scaling operator as defined in scikit-learn [documentation](https://scikit-learn.org/stable/modules/preprocessing.html). All scaling and normalization methods are supported.
  * __anomaly__ - Anomaly detection methods. All methods supported by PyOD. In the current example we see how an Isolation Forest model can be trained. For further details please consult the offical PyOD [documentation](https://pyod.readthedocs.io/en/latest/index.html).
  * __cluster__ - Clustering methods. Current version supports only DBSCAN, Optics and HDBSCAN.
  * __cycle_detect__ - Cycle detection method. The above example show how that users have to define a known good pattern and the _max_distance_ and _delta_bias_ parameters used for filtering of overlapping cycles. It is possible to add a query string in case of _ts_source_. This will allow on the fly pattern definition.
    * __dtw__ - Dynamic Time Warping based cycle detection method will be used if this parameter is set to True. Initialy if scoring is different _max_distance_ had to have a larger value. If the value was to small the job would fail. To limit this issue now _max_distance_ reprezents a percentage of the median score. This will allow for more flexibility in the cycle detection process.

`PUT /v1/ede/config`

Is used for submitting a new configuration. See `GET /v1/ede/config` for more details.

`POST /v1/ede/config/revert`

Reverts the configuration to the default version as detailed in the `GET /v1/ede/config` response.

`POST /v1/ede/detect`

Start the detection based around the engine configuration. There are several prerequistes which have to be met before this resource can be used:
* All sources have to be defined. See __Source__ section for more details.
* REDIS workers have to be up and running. See resource `/v1/ede/worker` for more details.
* A pattern has to be defined. See resource `/v1/ede/pattern` for more details.

Resource payload has contains if only a single detection cycle should be executed or a continuaously
execution. In the case of contonuous execution the polling period has to be defined. The JSON payload example can be found bellow:

```json
    {
    "loop": true,
    "period": 60
    }
```

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
    "3ecb430c-313b-4802-8764-0812422c7e83"
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

`DELETE /v1/ede/jobs`

Deletes all jobs including currently executing ones which it terminates before removing from all registries.

`GET /v1/ede/jobs/<job_id>`

This resource returns information about a job based on the jobs uuid. Each analysis and detection step is logged in and an appropriate message will appear in the _meta_ section of the JSON response.
The _meta_ section also contains the configuration used for the job.

An example can be seen bellow:

```json
{
  "finished": false,
  "meta": {
    "progress": "Computing heuristic overlap",
    "config" : {
        "source": {
            "local": "dev355.csv"
        },
        "out": {
            "grafana": {},
            "kafka": {
            "broker": "kafka:9092",
            "topic": "ads_events"
            },
            "influxdb": "ede"
        },
        "operators": {
            "scaler": {
            "method": "minmax"
            },
            "anomaly": {
            "method": "iforest",
            "params": {
                "n_estimators": 100,
                "max_samples": 256,
                "contamination": 0.1
            }
            },
            "cluster": {
            "method": "dbscan",
            "params": {
                "eps": 0.5,
                "min_samples": 5
            }
            },
            "cycle_detect": {
            "pattern": "pattern_d355.csv",
            "max_distance": 0.5,
            "delta_bias": 0.1
            }
        }
    }
  },
  "status": "started"
}
```

`DELETE /v1/ede/jobs/<job_id>`

Deletes a job based on the jobs uuid including currently executing ones which it terminates before removing from all registries.

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
      "topic": "bd4nrg-ads"
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
      "org": "bd4mnrg",
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

`GET /v1/source/local/cycles`

This resource fetches only the locally stored cycles. During output configuration the maximum number of cycle detection instances is defined.

`GET /v1/source/local/cycles/<uuid>`

This resource lists all cycles detected locally with a particular UUID. If the loop parameter for detection is set to false then only one cycle per job UUID will be stored. 
If the loop parameter is set to true then all cycles will be stored using the same UUID.

`GET /v1/source/local/cycles/<uuid>/latest`

This resource returns the latest cycle detected for a particular job UUID. For other cycles please use the generic file fetching mechanism; ```/v1/source/local/<data>```


### Data

These resources are used to upload and modify data which can be used by the detection service.

`PUT /v1/source/remote/<source>`

This resource is used to upload data to the remote data source. The __source__ parameter is used to define the data source. The supported sources are as follows: _minio_, _kafka_ and _local_.

__NOTE__: Additional resource for listing data available in remote data sources are not yet implemented. Data source should be configured separately. 


## Usage

### Setting up the service

We use several environment variables to configure the service. The following variables are required:
* `EDE_HOST` - the host of the EDE-Service
* `EDE_PORT` - the port of the EDE-Service
* `EDE_DEBUG` - the debug level of the EDE-Service
* `EDE_USER` - the user to use for the EDE-Service
* `REDIS_END` - the endpoint for redis queue
* `REDIS_PORT` - the port for redis queue
* `WORKER_THRESHOLD` - threshold modifier for number of supported workers, be default it is twice the number of CPU cores
* `EDE_ENH` - select enhanced version of windowing for cycle detection, by default legacy version is used
* `EDE_PARALLEL` - select if parallel version of cycle detection should be used, by default it is set to false. The number of workers is defined by setting this variable

Note: If both `EDE_ENH` and `EDE_PARALLEL` are set to true then the parallel version of the enhanced windowing will be used.

All environment variables have default values in acrodance with the libraries used.

## Suported detection methods

In order to ensure that the service is as flexible as possible we support several detection methods. In order to
acomplish this we use the [sklearn](https://scikit-learn.org/stable/) API conventions as a guideline, thus all methods which 
use this convention are compatible with our service. Of particular note is the [PyOD](https://pyod.readthedocs.io/en/latest/) library which we support
in its entirety. The following methods are currently supported:

| Type              | Acronim           | Method                                                                                                  |
|-------------------|-------------------|---------------------------------------------------------------------------------------------------------|
| Probabilistic     | ECOD              | Unsupervised Outlier Detection Using   Empirical Cumulative Distribution Functions                      |
| Probabilistic     | ABOD              | Angle-Based Outlier Detection                                                                           |
| Probabilistic     | FastABOD          | Fast Angle-Based Outlier Detection using   approximation                                                |
| Probabilistic     | COPOD             | COPOD: Copula-Based Outlier Detection                                                                   |
| Probabilistic     | MAD               | Median Absolute Deviation (MAD)                                                                         |
| Probabilistic     | SOS               | Stochastic Outlier Selection                                                                            |
| Probabilistic     | QMCD              | Quasi-Monte Carlo Discrepancy outlier   detection                                                       |
| Probabilistic     | KDE               | Outlier Detection with Kernel Density   Functions                                                       |
| Probabilistic     | Sampling          | Rapid distance-based outlier detection   via sampling                                                   |
| Probabilistic     | GMM               | Probabilistic Mixture Modeling for   Outlier Analysis                                                   |
| Linear Model      | PCA               | Principal Component Analysis (the sum of   weighted projected distances to the eigenvector hyperplanes) |
| Linear Model      | KPCA              | Kernel Principal Component Analysis                                                                     |
| Linear Model      | MCD               | Minimum Covariance Determinant (use the   mahalanobis distances as the outlier scores)                  |
| Linear Model      | CD                | Use Cook's distance for outlier detection                                                               |
| Linear Model      | OCSVM             | One-Class Support Vector Machines                                                                       |
| Linear Model      | LMDD              | Deviation-based Outlier Detection (LMDD)                                                                |
| Proximity-Based   | LOF               | Local Outlier Factor                                                                                    |
| Proximity-Based   | COF               | Connectivity-Based Outlier Factor                                                                       |
| Proximity-Based   | (Incremental) COF | Memory Efficient Connectivity-Based   Outlier Factor (slower but reduce storage complexity)             |
| Proximity-Based   | CBLOF             | Clustering-Based Local Outlier Factor                                                                   |
| Proximity-Based   | LOCI              | LOCI: Fast outlier detection using the   local correlation integral                                     |
| Proximity-Based   | HBOS              | Histogram-based Outlier Score                                                                           |
| Proximity-Based   | kNN               | k Nearest Neighbors (use the distance to   the kth nearest neighbor as the outlier score)               |
| Proximity-Based   | AvgKNN            | Average kNN (use the average distance to   k nearest neighbors as the outlier score)                    |
| Proximity-Based   | MedKNN            | Median kNN (use the median distance to k   nearest neighbors as the outlier score)                      |
| Proximity-Based   | SOD               | Subspace Outlier Detection                                                                              |
| Proximity-Based   | ROD               | Rotation-based Outlier Detection                                                                        |
| Outlier Ensembles | IForest           | Isolation Forest                                                                                        |
| Outlier Ensembles | INNE              | Isolation-based Anomaly Detection Using   Nearest-Neighbor Ensembles                                    |
| Outlier Ensembles | FB                | Feature Bagging                                                                                         |
| Outlier Ensembles | LSCP              | LSCP: Locally Selective Combination of   Parallel Outlier Ensembles                                     |
| Outlier Ensembles | XGBOD             | Extreme Boosting Based Outlier Detection   (Supervised)                                                 |
| Outlier Ensembles | LODA              | Lightweight On-line Detector of Anomalies                                                               |
| Outlier Ensembles | SUOD              | SUOD: Accelerating Large-scale   Unsupervised Heterogeneous Outlier Detection (Acceleration)            |
| Neural Networks   | AutoEncoder       | Fully connected AutoEncoder (use   reconstruction error as the outlier score)                           |
| Neural Networks   | VAE               | Variational AutoEncoder (use   reconstruction error as the outlier score)                               |
| Neural Networks   | Beta-VAE          | Variational AutoEncoder (all customized   loss term by varying gamma and capacity)                      |
| Neural Networks   | SO_GAAL           | Single-Objective Generative Adversarial   Active Learning                                               |
| Neural Networks   | MO_GAAL           | Multiple-Objective Generative Adversarial   Active Learning                                             |
| Neural Networks   | DeepSVDD          | Deep One-Class Classification                                                                           |
| Neural Networks   | AnoGAN            | Anomaly Detection with Generative   Adversarial Networks                                                |
| Neural Networks   | ALAD              | Adversarially learned anomaly detection                                                                 |
| Graph-based       | R-Graph           | Outlier detection by R-graph                                                                            |
| Graph-based       | LUNAR             | LUNAR: Unifying Local Outlier Detection   Methods via Graph Neural Networks                             |

