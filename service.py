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
from rq import cancel_job
from rq.command import send_stop_job_command
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
ALLOWED_PATTERN_EXTENSIONS = {'csv'}
allowed_source = ['minio', 'ts', 'kafka', 'local']

# redis connection
redis_end = os.getenv('REDIS_END', 'redis')
redis_port = os.getenv('REDIS_PORT', 6379)
r_connection = Redis(host=redis_end, port=redis_port)
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
    secure = marshmallow.fields.Int()


class SourceSchemaInflux(marshmallow.Schema):
    host = marshmallow.fields.Str()
    port = marshmallow.fields.Int()
    token = marshmallow.fields.Str()
    bucket = marshmallow.fields.Str()
    query = marshmallow.fields.Str()


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


class PatternListSchema(marshmallow.Schema):
    patterns = marshmallow.fields.List(marshmallow.fields.Str())


class LocalSource(marshmallow.Schema):
    local = marshmallow.fields.Str()


class OutKafka(marshmallow.Schema):
    topic = marshmallow.fields.Str()
    broker = marshmallow.fields.Str()


class EngineOut(marshmallow.Schema):
    kafka = marshmallow.fields.Nested(OutKafka)
    grafana = marshmallow.fields.Dict()


class EngineOutkafka(marshmallow.Schema):
    kafka = marshmallow.fields.Nested(OutKafka)


class MMScaler(marshmallow.Schema):
    copy = marshmallow.fields.Float()
    clip = marshmallow.fields.Float()


class EngineScalerMM(marshmallow.Schema):
    MinMaxScaler = marshmallow.fields.Nested(MMScaler)


class EngineCycleDetect(marshmallow.Schema):
    pattern = marshmallow.fields.Str()
    max_distance = marshmallow.fields.Float()
    checkpoint = marshmallow.fields.Int()
    delta_bias = marshmallow.fields.Float()
    dtw = marshmallow.fields.Bool()


class HDBSCANSchema(marshmallow.Schema):
    min_cluster_size = marshmallow.fields.Int()
    min_samples = marshmallow.fields.Int()
    cluster_selection_epsilon = marshmallow.fields.Float()
    cluster_selection_method = marshmallow.fields.Str()
    allow_single_cluster = marshmallow.fields.Int()
    leaf_size = marshmallow.fields.Int()


class EngineClusterHDBSCAN(marshmallow.Schema):
    HDBSCAN = marshmallow.fields.Nested(HDBSCANSchema)
    bootstrap = marshmallow.fields.Int()
    model = marshmallow.fields.Str()


class IFSchema(marshmallow.Schema):
    n_estimators = marshmallow.fields.Int()
    max_samples = marshmallow.fields.Float()
    contamination = marshmallow.fields.Float()
    max_features = marshmallow.fields.Float()
    bootstrap = marshmallow.fields.Int()
    n_jobs = marshmallow.fields.Int()
    behaviour = marshmallow.fields.Str()
    random_state = marshmallow.fields.Int()
    versbose = marshmallow.fields.Int()


class EngineAnomalyIF(marshmallow.Schema):
    IForest = marshmallow.fields.Nested(IFSchema)
    mark = marshmallow.fields.Int()
    model = marshmallow.fields.Str()


class EngineOperators(marshmallow.Schema):
    scaler = marshmallow.fields.Nested(EngineScalerMM)
    cluster = marshmallow.fields.Nested(EngineClusterHDBSCAN)
    anomaly = marshmallow.fields.Nested(EngineAnomalyIF)
    cycle_detect = marshmallow.fields.Nested(EngineCycleDetect)


class EngineSchema(marshmallow.Schema):
    source = marshmallow.fields.Nested(LocalSource)
    out = marshmallow.fields.Nested(EngineOut)
    operators = marshmallow.fields.Nested(EngineOperators)


class DataListSchema(marshmallow.Schema):
    # data = marshmallow.fields.Str()
    data = marshmallow.fields.List(fields.Dict(keys=fields.Str(), values=fields.Str()))


class DataHandlerRemoteSchema(marshmallow.Schema):
    message = marshmallow.fields.Str()


class DetectionSchema(marshmallow.Schema):
    loop = marshmallow.fields.Int()
    period = marshmallow.fields.Int()


@doc(description='Library backend versions', tags=['status'])
@marshal_with(StatusSchemaList(), code=200)
class ScampStatus(Resource, MethodResource):
    def get(self):
        import sklearn, stumpy, shap
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
                {
                    "version": str(shap.__version__),
                    "module": "shap",
                }
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


@doc(description='List registered patterns', tags=['engine'])
class ScampPattern(Resource, MethodResource):
    @marshal_with(PatternListSchema(), code=200)
    def get(self):
        patterns = [i.split('/')[-1] for i in glob.glob(f"{etc_dir}/*.csv")]
        log.info(f'Patterns found: {len(patterns)}')
        resp = jsonify(
            {
                "patterns": patterns,
            }
        )
        resp.status_code = 200
        return resp


@doc(description='Add/remove patterns', tags=['engine'])
class ScampPatternDetails(Resource, MethodResource):
    def get(self, pattern):
        pattern_file = os.path.join(etc_dir, pattern)
        if os.path.isfile(pattern_file):
            return send_file(pattern_file, mimetype="application/octet-stream")
        else:
            log.error(f"Pattern {pattern} not found!")
            resp = jsonify({
                "error": f"Pattern {pattern} not found!"
            })
            resp.status_code = 404
            return resp

    def put(self, pattern):
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

        if file and allowed_file(file.filename, ALLOWED_PATTERN_EXTENSIONS):
            filename = secure_filename(file.filename)
            file.save(os.path.join(etc_dir, filename))
            if file.filename != pattern:
                log.warning(f"File name mismatch from resource {file.filename} and file {pattern}")
            log.info(f"Pattern {pattern} upload succesfull")
            resp = jsonify({
                'message': "pattern upload successfull"
            })
            resp.status_code = 201
        else:
            log.error(f"Unsupported pattern content_type, supported are {list(ALLOWED_PATTERN_EXTENSIONS)}")
            resp = jsonify(
                {
                    'message': f'allowed pattern file types are: {list(ALLOWED_PATTERN_EXTENSIONS)}'
                }
            )
            resp.status_code = 400
        return resp

    def delete(self, pattern):
        pattern_file = os.path.join(etc_dir, pattern)
        if os.path.isfile(pattern_file):
            os.remove(pattern_file)
            log.info(f"Pattern {pattern} deleted")
            resp = jsonify({
                'message': f"Pattern {pattern} deleted"
            })
            resp.status_code = 200
            return resp
        else:
            log.error(f"Pattern {pattern} not found!")
            resp = jsonify({
                "error": f"Pattern {pattern} not found!"
            })
            resp.status_code = 404
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
                log.error(f"Failed to stop pid {pid} with {inst}, deleting file")
                delete_pid_file(worker)
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


@doc(description='Manage Engine Config', tags=['engine'])
class ScampEdeEngine(Resource, MethodResource):
    @marshal_with(EngineSchema(), code=200)
    def get(self):
        config_file = os.path.join(etc_dir, 'ede_engine.yaml')
        config_dict = load_yaml(config_file)
        resp = jsonify(config_dict)
        resp.status_code = 200
        return resp

    @marshal_with(EngineSchema(), code=200)
    @use_kwargs(EngineSchema(), apply=False)
    def put(self):
        if not request.is_json:
            log.error('No JSON in request!')
            resp = jsonify(
                {
                    'error': 'No JSON in request'
                }
            )
            resp.status_code = 400
            return resp
        else:
            schema = load_schema('ede_engine_schema.json')
            config_file = os.path.join(etc_dir, 'ede_engine.yaml')
            # config_dict = load_yaml(config_file)
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
                'message': f"Engine configuration updated!"
            })
            resp.status_code = 201
            return resp


@doc(description='Revert Engine Configuration to last config', tags=['engine'])
class EdeEngineRevertConfig(Resource, MethodResource):
    def post(self):
        config_file_old = os.path.join(etc_dir, 'ede_engine.yaml.old')
        config_file = os.path.join(etc_dir, 'ede_engine.yaml')
        if os.path.isfile(config_file_old):
            os.replace(config_file_old, config_file)
            resp = jsonify({
                'message': f"Engine configuration reverted!"
            })
            resp.status_code = 201
            return resp
        else:
            resp = jsonify({
                'error': 'No old config file found!'
            })
            resp.status_code = 404
            return resp


@doc(description='Detection Handling', tags=['engine'])
@use_kwargs(DetectionSchema(), apply=False)
class EdeEngineDetect(Resource, MethodResource):
    def post(self):
        with open(os.path.join(etc_dir, 'ede_engine.yaml')) as f:
            config_dict = yaml.safe_load(f)
        with open(os.path.join(etc_dir, 'source_cfg.yaml')) as f:
            source_dict = yaml.safe_load(f)

        from ede_handler import ede_detect_handler
        if request.is_json:
            loop = request.json['loop']
            period = request.json['period']
        else:
            loop = False # for testing is set to false
            period = 0 # for testinf it is set to 20
        mconf = {
            'ede_cfg': config_dict,
            'source_cfg': source_dict,
            'loop': loop,
            'period': period
        }
        job = queue.enqueue(ede_detect_handler, mconf, job_timeout=-1)
        resp = jsonify({
            'job_id': job.get_id(),
            'config': mconf
        })
        resp.status_code = 201
        return resp


@doc(description='Create Anomaly Detection Model', tags=['engine'])
class EdeEngineAnomalyModel(Resource, MethodResource):
    def post(self):
        with open(os.path.join(etc_dir, 'ede_engine.yaml')) as f:
            config_dict = yaml.safe_load(f)
        with open(os.path.join(etc_dir, 'source_cfg.yaml')) as f:
            source_dict = yaml.safe_load(f)

            from ede_handler import ede_anomaly_trainer_handler
            mconf = {
                'ede_cfg': config_dict,
                'source_cfg': source_dict,
            }
            job = queue.enqueue(ede_anomaly_trainer_handler, mconf)
            resp = jsonify({
                'job_id': job.get_id(),
                'config': mconf
            })
            resp.status_code = 201
            return resp


@doc(description='EDE Engine Jobs', tags=['engine'])
class EdeEngineJobQueueStatus(Resource, MethodResource):
    def get(self):
        try:
            jobs = queue.get_job_ids()
        except Exception as inst:
            log.error(f'Error connecting to redis with {type(inst)} and {inst.args}')
            resp = jsonify({
                'error': f'Error connecting to redis with {type(inst)} and {inst.args}'
            })
            resp.status_code = 500
            return resp
        failed = queue.failed_job_registry.get_job_ids()
        jqueued = []
        started = queue.started_job_registry.get_job_ids()
        finished = queue.finished_job_registry.get_job_ids()
        for rjob in jobs:
            try:
                job = Job.fetch(rjob, connection=r_connection)
            except:
                log.error("No job with id {}".format(rjob))
                response = {'error': 'no such job'}
                return response
            if job.is_finished:
                finished.append(rjob)
            elif job.is_failed:
                failed.append(rjob)
            elif job.is_started:
                started.append(rjob)
            elif job.is_queued:
                jqueued.append(rjob)

        resp = jsonify(
            {
                'started': started,
                'finished': finished,
                'failed': failed,
                'queued': jqueued
            }
        )
        resp.status_code = 200
        return resp

    def delete(self):
        try:
            jobs = queue.get_job_ids()
        except Exception as inst:
            log.error(f'Error connecting to redis with {type(inst)} and {inst.args}')
            resp = jsonify({
                'error': f'Error connecting to redis with {type(inst)} and {inst.args}'
            })
            resp.status_code = 500
            return resp
        for rjob in jobs:
            try:
                job = Job.fetch(rjob, connection=r_connection)
            except:
                log.error("No job with id {}".format(rjob))
                response = {'error': 'no such job'}
                return response
            job.delete()

        failed_registry = queue.failed_job_registry
        started_registry = queue.started_job_registry

        for rjob in failed_registry.get_job_ids():
            failed_registry.remove(rjob, delete_job=True)

        for rjob in started_registry.get_job_ids():
            send_stop_job_command(r_connection, rjob)
            cancel_job(rjob, connection=r_connection)
            # job = Job.fetch(rjob, connection=r_connection)
            # job.cancel()
            started_registry.remove(rjob, delete_job=True)
        resp = jsonify(
            {
                'message': 'All jobs deleted'
            }
        )
        resp.status_code = 200
        return resp


@doc(description='EDE Engine Job details', tags=['engine'])
class EdeEngineJobStatus(Resource, MethodResource):
    def get(self, job_id):
        try:
            job = Job.fetch(job_id, connection=r_connection)
        except Exception as inst:
            log.error("No job with id {}".format(job_id))
            response = jsonify({'error': 'no job',
                                'job_id': job_id})
            response.status_code = 404
            return response
        job.refresh()
        status = job.get_status()
        finished = job.is_finished
        meta = job.meta
        response = jsonify({'status': status,
                            'finished': finished,
                            'meta': meta})
        response.status_code = 200
        return response

    def delete(self, job_id):
        try:
            job = Job.fetch(job_id, connection=r_connection)
        except Exception as inst:
            log.error("No job with id {}".format(job_id))
            response = jsonify({'error': 'no job',
                                'job_id': job_id})
            response.status_code = 404
            return response
        send_stop_job_command(r_connection, job_id)
        job.delete()
        response = jsonify({'status': 'deleted',
                            'job_id': job_id})
        response.status_code = 200
        return response


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


# Rest API routing
api.add_resource(ScampStatus, "/v1/ede")
api.add_resource(ScampDataSource, "/v1/source")
api.add_resource(ScampDataSourceDetails, "/v1/source/<string:source>") # local, influxdb, minio, kafka
api.add_resource(DataHandlerLocal, "/v1/source/local/<string:data>")


api.add_resource(ScampEdeEngine, "/v1/ede/config")
api.add_resource(EdeEngineRevertConfig, "/v1/ede/config/revert")
api.add_resource(EdeEngineJobQueueStatus, "/v1/ede/jobs")
api.add_resource(EdeEngineJobStatus, "/v1/ede/jobs/<string:job_id>")
api.add_resource(EdeEngineDetect, "/v1/ede/detect")
api.add_resource(EdeEngineAnomalyModel, "/v1/ede/anomaly")
api.add_resource(ScampPattern, "/v1/ede/pattern")
api.add_resource(ScampPatternDetails, "/v1/ede/pattern/<string:pattern>")
api.add_resource(EngineWorkers, "/v1/ede/workers")

api.add_resource(DataHandlerViz, "/v1/source/<string:source>")
api.add_resource(DataHandlerRemote, "/v1/source/remote/<string:data>")



# Rest API docs, Swagger
docs.register(ScampStatus)
docs.register(ScampDataSource)
docs.register(ScampDataSourceDetails)
docs.register(ScampPattern)
docs.register(ScampPatternDetails)
docs.register(ScampEdeEngine)
docs.register(EdeEngineDetect)
docs.register(EdeEngineAnomalyModel)
docs.register(EdeEngineJobQueueStatus)
docs.register(EdeEngineJobStatus)
docs.register(EdeEngineRevertConfig)
docs.register(EngineWorkers)

docs.register(DataHandlerViz)
docs.register(DataHandlerLocal)
docs.register(DataHandlerRemote)
# docs.register(DataHandlerConfig)




