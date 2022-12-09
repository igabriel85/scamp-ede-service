"""

Copyright 2022, IeAT, Timisoara
Developers:
 * Gabriel Iuhasz, iuhasz.gabriel@e-uvt.ro

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at:
    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
"""
from flask import request, jsonify, send_file
from werkzeug.utils import secure_filename
from minio import Minio
import subprocess
import logging
import jsonschema
from flask_restful import Resource
import marshmallow
from webargs import fields
from flask_apispec import marshal_with, use_kwargs
from flask_apispec.views import MethodResource
from flask_apispec.annotations import doc
import psutil
from redis import Redis
import rq
from rq.job import Job
from app import *
from utils import *
import json

#directory locations
data_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'data')
etc_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'etc')

#file location
log_file = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'ede.log')
config_file = os.path.join(etc_dir, 'source_cfg.yaml')
ede_cfg_file = os.path.join(etc_dir, 'ede_cfg.yaml')

# predefined variables
ALLOWED_DATA_EXTENSIONS = {'csv', 'json'}
allowed_source = ['minio', 'ts', 'kafka', 'local']

# redis connection
r_connection = Redis(host='redis', port=6379)
queue = rq.Queue('ede', connection=r_connection)  # TODO create 3 priority queues and make them selectable from REST call


################# Schemas ################
# For Response

class StatusSchema(marshmallow.Schema):
    version = marshmallow.fields.Str()
    module = marshmallow.fields.Str()


class StatusSchemaList(marshmallow.Schema):
    libraries = marshmallow.fields.Nested(StatusSchema, many=True)


class SourceSchemaMinio(marshmallow.Schema):
    host = marshmallow.fields.Str()
    access_key = marshmallow.fields.Str()
    secret_key = marshmallow.fields.Str()


class SourceSchemaInflux(marshmallow.Schema):
    host = marshmallow.fields.Str()
    port = marshmallow.fields.Int()
    token = marshmallow.fields.Str()
    bucket = marshmallow.fields.Str()


class SourceSchemaKafka(marshmallow.Schema):
    host = marshmallow.fields.Str()
    port = marshmallow.fields.Int()
    topic = marshmallow.fields.Str()


class SourceSchema(marshmallow.Schema):
    local_source = marshmallow.fields.Str()
    minio_source = marshmallow.fields.Nested(SourceSchemaMinio)
    ts_source = marshmallow.fields.Nested(SourceSchemaInflux)
    kafka_source = marshmallow.fields.Nested(SourceSchemaKafka)

class SourceSchemaReq(marshmallow.Schema):
    source = marshmallow.fields.Nested(SourceSchema)



################# Old ################
class DataListSchema(marshmallow.Schema):
    # data = marshmallow.fields.Str()
    data = marshmallow.fields.List(fields.Dict(keys=fields.Str(), values=fields.Str()))


class DataHandlerRemoteSchema(marshmallow.Schema):
    message = marshmallow.fields.Str()


class DataHandlerConfigSchemaInner(marshmallow.Schema):
    access_key = fields.String()
    endpoint = fields.String()
    secret_key = fields.String()
    secure = fields.String()


class DataHandlerConfigSchema(marshmallow.Schema):
    minio = marshmallow.fields.Nested(DataHandlerConfigSchemaInner)

# For payload
aug_sequencer_resp = {
    'ads': marshmallow.fields.Nested(
        {
            'operators': marshmallow.fields.Dict(keys=marshmallow.fields.Str(), values=marshmallow.fields.Str()),
            'random_order': marshmallow.fields.Boolean()}),

    'source': marshmallow.fields.Nested({'bucket_in': marshmallow.fields.Str(),
                                           'bucket_out': marshmallow.fields.Str(),
                                           'type': marshmallow.fields.Str()})
}

@doc(description='Library backend versions', tags=['status'])
@marshal_with(StatusSchemaList(), code=200)
class ScampStatus(Resource, MethodResource):
    def get(self):
        import sklearn, stumpy
        resp = jsonify({
            'libraries': [
                {
                    "version": str(sklearn.__version__),
                    "module": "sklearn",
                },
                {
                    "version": str(stumpy.__version__),
                    "module": "stumpy",
                },
                # {
                #     "version": str(shap.__version__),
                #     "module": "shap",
                # }
            ]
        })
        resp.status_code = 200
        return resp


@doc(description='List and update all data sources', tags=['source'])
# @marshal_with(DataListSchema(), code=200)
class ScampDataSource(Resource, MethodResource):
    @marshal_with(SourceSchemaReq(), code=200)
    def get(self):
        config_dict = load_yaml(config_file)
        resp = jsonify(config_dict)
        resp.status_code = 200
        return resp

    @marshal_with(SourceSchemaReq(), code=200)
    @use_kwargs(SourceSchemaReq(), apply=False)
    def put(self):
        if not request.is_json:
            log.error(f"Payload is not JSON for source configuration")
            resp = jsonify({
                'error': f"Payload is not JSON!"
            })
            resp.status_code = 400
            return resp
        else:
            schema = load_schema('source_schema.json')
            try:
                jsonschema.validate(request.json, schema)
            except jsonschema.exceptions.ValidationError as e:
                log.error(f"Payload is not valid for source configuration: {e}")
                resp = jsonify({
                    'error': f"Payload is not valid: {e}"
                })
                resp.status_code = 400
                return resp
            check_and_rename(config_file)
            with open(config_file, 'w') as f:
                yaml.dump(request.json, f)
            resp = jsonify({
                'message': f"Source configuration updated!"
            })
            resp.status_code = 201
            return resp


@doc(description='Check and query each data source', tags=['source'])
class ScampDataSourceDetails(Resource, MethodResource):
    def get(self, source):
        allowed_source = ['minio', 'ts', 'kafka', 'local']
        if source not in allowed_source:
            resp = jsonify({
                'error': f"Source {source} not supported!"
            })
            resp.status_code = 400
            return resp
        config_dict = load_yaml(config_file)
        if source == 'ts':
            ts_source = config_dict['source']['ts_source']
            resp = jsonify(ts_source)
            resp.status_code = 200
        elif source == 'minio':
            minio_source = config_dict['source']['minio_source']
            resp = jsonify(minio_source)
            resp.status_code = 200
        elif source == 'kafka':
            kafka_source = config_dict['source']['kafka_source']
            resp = jsonify(kafka_source)
            resp.status_code = 200
        elif source == 'local':
            # local_source = config_dict['source']['local_source'] # TODO only data folder is usable as local storage
            # print(local_source)
            resp = jsonify({"local": get_list_data_files(data_dir)})
            resp.status_code = 200
        return resp

    def post(self, source): # TODO used to getch data from source
        if source not in allowed_source:
            resp = jsonify({
                'error': f"Source {source} not supported!"
            })
            resp.status_code = 400
            return resp
        config_dict = load_yaml(config_file)
        if source == 'ts':
            return False
        elif source == 'minio':
            return False
        elif source == 'kafka':
            return False
        elif source == 'local':
            # local_source = config_dict['source']['local_source'] # TODO only data folder is usable as local storage
            # print(local_source)
            resp = jsonify({"local": get_list_data_files(data_dir)})
            resp.status_code = 200
        return resp


@doc(description='Fetch and Add local data', tags=['source'])
class DataHandlerLocal(Resource, MethodResource):
    def get(self, data):
        data_file = os.path.join(data_dir, data)
        if os.path.isfile(data_file):
            return send_file(data_file, mimetype="application/octet-stream")
        else:
            log.error(f"Data {data} not found!")
            resp = jsonify({
                "error": f"Data {data} not found!"
            })
            resp.status_code = 404
            return resp

    def put(self, data):
        if 'file' not in request.files:
            log.error('No file in request!')
            resp = jsonify(
                            {
                                'error': 'no file in request'
                            }
                        )
            resp.status_code = 400
            return resp
        file = request.files['file']
        if file.filename == '':
            log.error('No file selected for upload')
            resp = jsonify(
                {
                    'message': 'no file selected for upload'
                }
            )
            resp.status_code = 400

        if file and allowed_file(file.filename, ALLOWED_DATA_EXTENSIONS):
            filename = secure_filename(file.filename)
            file.save(os.path.join(data_dir, filename))
            if file.filename != data:
                log.warning(f"File name mismatch from resource {file.filename} and file {data}")
            log.info(f"Data {data} upload succesfull")
            resp = jsonify({
                'message': "data upload successfull"
            })
            resp.status_code = 201
        else:
            log.error(f"Unsupported data content_type, supported are {list(ALLOWED_DATA_EXTENSIONS)}")
            resp = jsonify(
                {
                    'message': f'allowed data file types are: {list(ALLOWED_DATA_EXTENSIONS)}'
                }
            )
            resp.status_code = 400
        return resp


@doc(description='Engine worker status', tags=['engine'])
class EngineWorkers(Resource, MethodResource):
    def get(self):
        list_workers = get_list_workers()
        worker_list = []
        for l in list_workers:
            worker = {}
            pid = get_pid_from_file(l)
            worker['id'] = l.split('/')[-1].split('.')[0].split('_')[-1]
            worker['pid'] = int(pid)
            worker['status'] = check_pid(pid)
            worker_list.append(worker)
        return jsonify({'workers': worker_list})

    def post(self):
        list_workers = get_list_workers()
        logic_cpu = psutil.cpu_count(logical=True)
        if len(list_workers) > logic_cpu - 1:
            resp = jsonify({'warning': 'maximum number of workers active!',
                                'workers': logic_cpu})
            log.warning('Maximum number of aug workers reached: {}'.format(logic_cpu))
            resp.status_code = 200
            return resp
        subprocess.Popen(['python', 'ede_worker.py'])
        log.info("Starting aug worker {}".format(len(list_workers)))
        resp = jsonify({'status': 'workers started'})
        resp.status_code = 201
        return resp

    def delete(self):
        list_workers = get_list_workers()
        pid_dict = {}
        failed_lst = []
        for worker in list_workers:
            pid = get_pid_from_file(worker)
            pid_dict[worker.split('/')[-1].split('.')[0]] = pid
            # kill pid
            try:
                kill_pid(pid)
            except Exception as inst:
                log.error(f"Failed to stop pid {pid} with {inst}")
            # check if pid is killed
            if not check_pid(pid):
                log.error(f"Failed to stop rq workers with pid {pid}")
                failed_lst.append(pid)
            # cleanup pid files
            log.info(f"Cleaning up pid file for worker {worker}")
            clean_up_pid(worker)
        if failed_lst:
            resp = jsonify({
                'message': f'failed to stop rq workers',
                'worker_pids': failed_lst
            })
            resp.status_code = 500
            return resp
        resp = jsonify({
            'message': 'all rq workers stopped',
            'workers': pid_dict
        })
        resp.status_code = 200
        return resp

##################


@doc(description='List all data from local or remote', tags=['data'])
@marshal_with(DataListSchema(), code=200)
class DataHandlerViz(Resource, MethodResource):
    @use_kwargs({'bucket': marshmallow.fields.Str()}, apply=False)
    def post(self, source):
        if source == 'remote':
            if check_file(config_file):
                # check if request is json
                if not request.is_json:
                    log.error(f"Payload is not JSON!")
                    resp = jsonify({
                        'error': f"Payload is not JSON!"
                    })
                    resp.status_code = 400
                    return resp
                else:
                    bucket = request.json.get('bucket', None)
                    if bucket is None:
                        log.warning(f"Payload must contain bucket name")
                        resp = jsonify({
                            'warn': 'Payload must contain bucket name'
                        })
                        resp.status_code = 400
                        return resp
                config_dict = load_yaml(config_file)
                try:
                    client = Minio(
                        config_dict['minio']['endpoint'],
                        access_key=config_dict['minio']['access_key'],
                        secret_key=config_dict['minio']['secret_key'],
                        secure=config_dict['minio']['secure']
                    )
                    objects = client.list_objects(bucket)
                    l_objects = []
                    for obj in objects:
                        obj_dict = {'bucket': obj.bucket_name,
                                    'obj_enc': str(obj.object_name.encode('utf-8')),
                                    'last_modified': obj.last_modified,
                                    'etag': obj.etag,
                                    'size': obj.size,
                                    'content_type': obj.content_type}
                        l_objects.append(obj_dict)
                    log.info(f'Remote contains {len(l_objects)}')
                    resp = jsonify({
                        'data': l_objects
                    })
                    resp.status_code = 200
                    return resp
                except Exception as inst:
                    log.error(f"Error while connecting to remote with {type(inst)} and {inst.args}")
                    resp = jsonify({
                        'error': f"Error while connecting to remote with {type(inst)} and {inst.args}"
                    })
                    resp.status_code = 500
                    return resp
            else:
                log.error(f"No configuration file located")
                resp = jsonify({
                    'error': 'missing configuration file'
                })
                resp.status_code = 404
                return resp
        elif source == 'local':
            objects = [i.split('/')[-1] for i in glob.glob(f"{data_dir}/*.*")]
            log.info(f'Local contains {len(objects)}')
            resp = jsonify(
                {
                    "data": objects,
                }
            )
            resp.status_code = 200
            return resp

        else:
            log.error(f'Unsupported source. Only local or remote supported set {source}.')
            resp = jsonify({
                "error": f'Unsupported source. Only local or remote supported set {source}.'
            })
            resp.status_code = 404
            return resp


@doc(description='Fetch remote data', tags=['data'])
class DataHandlerRemote(Resource, MethodResource):
    @use_kwargs({'data': marshmallow.fields.List(marshmallow.fields.Str())}, apply=False)
    @marshal_with(DataHandlerRemoteSchema(), code=200)
    def put(self, bucket):
        if check_file(config_file):
            # check if request is json
            if not request.is_json:
                log.error(f"Payload is not JSON!")
                resp = jsonify({
                    'error': f"Payload is not JSON!"
                })
                resp.status_code = 400
                return resp
            else:
                data = request.json.get('data', None)
                if data is None:
                    log.warning(f"Payload must contain data name")
                    resp = jsonify({
                        'warn': 'Payload must contain data name'
                    })
                    resp.status_code = 400
                    return resp
            config_dict = load_yaml(config_file)
            try:
                client = Minio(
                    config_dict['minio']['endpoint'],
                    access_key=config_dict['minio']['access_key'],
                    secret_key=config_dict['minio']['secret_key'],
                    secure=config_dict['minio']['secure']
                )
                for data_object in data:
                    bdata = client.get_object(bucket, data_object)
                    with open(os.path.join(data_dir, data_object), 'wb') as file_data:
                        for d in bdata.stream(32*1024):
                            file_data.write(d)
                log.info(f"Data  saved to local")
                resp = jsonify(
                    {
                        'message': f"Data saved to local"
                    }
                )
                resp.status_code = 200
                return resp
            except Exception as inst:
                log.error(f"Error while connecting to remote with {type(inst)} and {inst.args}")
                resp = jsonify({
                    'error': f"Error while connecting to remote with {type(inst)} and {inst.args}"
                })
                resp.status_code = 500
                return resp
        else:
            log.error(f"No configuration file located")
            resp = jsonify({
                'error': 'missing configuration file'
            })
            resp.status_code = 500
            return resp


# @doc(description='Fetch remote data handler config', tags=['status'])
# class DataHandlerConfig(Resource, MethodResource):
#     @marshal_with(DataHandlerConfigSchema(), code=200)
#     def get(self):  # TODO make it save by adding token based auth
#         if check_file(config_file):
#             config_dict = load_yaml(config_file)
#             resp = jsonify(config_dict)
#             resp.status_code = 200
#             return resp
#         else:
#             log.error(f"No configuration file located")
#             resp = jsonify({
#                 'error': 'missing configuration file'
#             })
#             resp.status_code = 500
#
#     @use_kwargs({'access_key': marshmallow.fields.Str(),
#                  'endpoint': marshmallow.fields.Str(),
#                  'secret_key': marshmallow.fields.Str(),
#                  'secure': marshmallow.fields.Str()
#                  }, apply=False)
#     @marshal_with(DataHandlerConfigSchema(), code=201)
#     def put(self):
#         if not request.is_json:
#             log.error(f"Payload is not JSON!")
#             resp = jsonify({
#                 'error': f"Payload is not JSON!"
#             })
#             resp.status_code = 400
#             return resp
#         else:
#             try:
#                 new_config = {
#                     'minio':
#                         {
#                             'access_key': request.json.get('access_key'),
#                             'endpoint': request.json.get('endpoint'),
#                             'secret_key': request.json.get('secret_key'),
#                             'secure': request.json.get('secure')
#                         }
#                 }
#             except Exception as inst:
#                 log.error(f"Error while parsing new config with {type(inst)} and {inst.args}")
#                 resp = jsonify({
#                     'error': f"Error while parsing new config with {type(inst)} and {inst.args}"
#                 })
#                 resp.status_code = 400
#                 return resp
#         save_config_yaml(new_config)
#         log.info(f"Saved new configuration file")
#         resp = jsonify(new_config)
#         resp.status_code = 201
#         return resp


# Rest API routing
api.add_resource(ScampStatus, "/v1/ede")
api.add_resource(ScampDataSource, "/v1/source")
api.add_resource(ScampDataSourceDetails, "/v1/source/<string:source>") # local, influxdb, minio, kafka
api.add_resource(DataHandlerLocal, "/v1/source/local/<string:data>")


# api.add_resource(ScampEdeEngine, "/v1/ede/config")
# api.add_resource(ScampEdeEngine, "/v1/ede/jobs")
api.add_resource(EngineWorkers, "/v1/ede/workers")

api.add_resource(DataHandlerViz, "/v1/source/<string:source>")
api.add_resource(DataHandlerRemote, "/v1/source/remote/<string:data>")
# api.add_resource(DataHandlerConfig, "/v1/source/remote/config")


# Rest API docs, Swagger
docs.register(ScampStatus)
docs.register(ScampDataSource)
docs.register(ScampDataSourceDetails)
docs.register(EngineWorkers)
docs.register(DataHandlerViz)
docs.register(DataHandlerLocal)
docs.register(DataHandlerRemote)
# docs.register(DataHandlerConfig)




