# SCAMP-ML EDE-Service

This service is a part of the SCAMP-ML project. It is a RESTful service that provides a way to run 
EDE-Service jobs on a cluster. It is a part of the SCAMP-ML project. It is a RESTful service that provides a way to 
run cycle detection/identification and analysis tasks.

## Architecture


## REST API

## Usage

### Setting up the service

We use several environment variables to configure the service. The following variables are required:
* `EDE_HOST` - the host of the EDE-Service
* `EDE_PORT` - the port of the EDE-Service
* `EDE_DEBUG` - the debug level of the EDE-Service
* `EDE_USER` - the user to use for the EDE-Service
* `REDIS_END` - the endpoint for redis queue
* `REDIS_PORT` - the port for redis queue

All environment variables have default values in acrodance with the libraries used.

