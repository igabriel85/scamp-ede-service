import os
import numpy as np
np.random.seed(42)
from numpy.lib.stride_tricks import sliding_window_view
import importlib
import yaml
import pandas as pd
import json
import stumpy
from minio import Minio
from minio.error import InvalidResponseError
from kafka import KafkaProducer
from dtaidistance import dtw
import hdbscan
from joblib import dump, load, Parallel, delayed
from statistics import mean, median
from utils import percentage
import datetime
import time
import glob

# from logging import getLogger
# log = getLogger(__name__)


# Influx Connection
import influxdb_client
from influxdb_client import InfluxDBClient, WriteOptions, WritePrecision, Point
import warnings
from influxdb_client.client.warnings import MissingPivotFunction
warnings.simplefilter("ignore", MissingPivotFunction)

from ede_schema import ede_kafka_detection

class EDEScampEngine():
    def __init__(self,
                 ede_cfg,
                 source_cfg,
                 data_dir,
                 etc_dir,
                 models_dir,
                 minio_bucket='scamp-models',
                 job={}
                 # pattern_file
                 ):
        self.ede_cfg = ede_cfg
        self.source_cfg = source_cfg
        self.data_dir = data_dir
        self.etc_dir = etc_dir
        self.models_dir = models_dir
        self.pattern = []
        self.mod_name = 'sklearn.preprocessing'
        self.anom_name = 'pyod.models'
        self.minio_bucket = minio_bucket
        self.allowed_extensions = {'joblib', 'pickle'}
        self.fillna = True
        self.cdata = 0
        self.job = job

    def __job_stat(self, message):
        if isinstance(self.job, dict):
            self.job['progress'] = message
        else:
            self.job.meta['progress'] = message
            self.job.save_meta()

    def __job_config(self, cfg):
        self.job.meta['config'] = cfg
        self.job.save_meta()

    def load_data(self, resample=None):
        '''
        Load the data from different sources as stated in ede engine config
        and defiend in source_cfg
        '''
        # Load the data
        if 'local' in self.ede_cfg['source'].keys():
            df = self.__local_data()
        elif 'ts_source' in self.ede_cfg['source'].keys():
            df = self.__influxdb_data()
        elif 'minio_source' in self.ede_cfg['source'].keys():
            df = self.__minio_data()
        elif 'kafka_source'in self.ede_cfg['source'].keys():
            pass # TODO: Implement this
        else:
            # print(ede_cfg['source'].keys())
            raise Exception('Unknown source type')
        if resample is not None:
            df = df.resample(resample).mean().to_frame()
        self.cdata = df.copy(deep=True)
        return df

    def __local_data(self):
        '''
        Load local data
        '''
        self.__job_stat('Loading data')
        df = pd.read_csv(os.path.join(self.data_dir, self.ede_cfg['source']['local']), index_col=0)
        df.index = pd.to_datetime(df.index)
        df = self.__scale_data(df)
        # print(df)
        return df

    def __minio_data(self):
        '''
        Load data from minio
        '''
        try:
            # Connect to Minio
            client = Minio(
                self.source_cfg['source']['minio_source']['host'],
                access_key=self.source_cfg['source']['minio_source']['access_key'],
                secret_key=self.source_cfg['source']['minio_source']['secret_key'],
                secure=False) # TODO fetch secure flag from config, string to boo
            bucket = self.ede_cfg['source']['minio_source']['bucket']
            data_object = self.ede_cfg['source']['minio_source']['data']
            # Check if bucket exists
            # Get the data
            local_data = os.path.join(data_dir, data_object)
            bdata = client.get_object(bucket, data_object)
            self.__job_stat('Loading minio data')
            # self.job.meta['status'] = 'Loading minio data'
            with open(local_data, 'wb') as file_data:
                for d in bdata.stream(32 * 1024):
                    file_data.write(d)
        except Exception as inst:
            print(f'Error loading data from minio with {type(inst)} and {inst.args}')
            # raise Exception('Error loading data from minio')
            return pd.DataFrame()
        df = pd.read_csv(local_data, index_col=0)
        df.index = pd.to_datetime(df.index)
        df = self.__scale_data(df)
        return df

    def __influxdb_data(self):
        '''
        Load data from influxdb
        '''
        try:
            client = InfluxDBClient(url=self.source_cfg['source']['ts_source']['host'],
                                    token=self.source_cfg['source']['ts_source']['token'],
                                    org=self.source_cfg['source']['ts_source'].get('org', 'scampml'))
            query = self.ede_cfg['source']['ts_source']['query']
            feature = self.ede_cfg['source']['ts_source'].get("feature", "_value")
            self.__job_stat('Loading influxdb data')

            df = client.query_api().query_data_frame(query)
            if df.empty:
                return df
            df['_time'] = pd.to_datetime(df['_time'])
            df.set_index('_time', inplace=True)
            # df = self.__scale_data(df[feature])
            return df
        except Exception as inst:
            self.__job_stat('Error loading data from influxdb')
            print(f'Error loading data from influxdb with {type(inst)} and {inst.args}')
            return pd.DataFrame()

    def __local_out(self, body):
        # fixed the date format
        processed_cycles = []
        for cycle in body['cycles']:
            cycle['start'] = cycle['start'].strftime("%Y-%m-%d %H:%M:%S")
            cycle['end'] = cycle['end'].strftime("%Y-%m-%d %H:%M:%S")
            processed_cycles.append(cycle)
        processed_body = {}
        processed_body['cycles'] = processed_cycles
        # check for number of files
        file_list = glob.glob(os.path.join(self.data_dir, 'cycles_*'))
        if len(file_list) > self.ads_cfg['out']['local']:
            # remove the oldest file
            oldest_file = min(file_list, key=os.path.getctime)
            os.remove(oldest_file)
        timestamp = datetime.datetime.fromtimestamp(time.time())
        with open(os.path.join(self.data_dir, f"cycles_{timestamp.strftime('%Y-%m-%d_%H:%M:%S')}_{self.job.id}.json"),
                  "w") as fp:
            json.dump(body, fp)

    def __kafka_out(self, body):
        # Output the results to kafka
        print('Outputting to kafka')
        try:
            producer = KafkaProducer(value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                                          bootstrap_servers=["{}".format(self.ede_cfg['out']['kafka']['broker'])],
                                          retries=5)
            query = self.ede_cfg['source']['ts_source']['query'] # make independent of influxdb query
            device_id = query.split("r[\"device_id\"] ==")[1].split(")")[0].strip().replace("\"", "")
            for cycle in body['cycles']:
                # cycle['device_id'] = device_id
                cycle['node'] = device_id
                cycle['cycle_start'] = cycle['start'].timestamp()
                cycle['cycle_end'] = cycle['end'].timestamp()
                if cycle['anomaly'] is None:
                    cycle['anomaly'] = ''
                if cycle['cluster'] is None:
                    cycle['cluster'] = ''
                del cycle['start']
                del cycle['end']
                ede_kafka_detection['payload'] = cycle
                producer.send(self.ede_cfg['out']['kafka']['topic'], ede_kafka_detection)
            self.__job_stat('Outputting to kafka')
            # self.job.meta['status'] = 'Output to kafka'
        except Exception as inst:
            self.__job_stat(f'Error outputting to kafka with {type(inst)} and {inst.args}')
            # self.job.meta['status'] = 'Error output to kafka'
            print('Error outputting to kafka with {} and {}'.format(type(inst), inst.args))

    def __influxdb_out(self, body):
        print("Outputting to influxdb")
        try:
            client = InfluxDBClient(url=self.source_cfg['source']['ts_source']['host'],
                                    token=self.source_cfg['source']['ts_source']['token'],
                                    org=self.source_cfg['source']['ts_source'].get('org', 'scampml'))
            query = self.ede_cfg['source']['ts_source']['query']
            if not self.check_bucket_exists(client, 'ede'):
                print("Creating bucket ede")
                self.__job_stat('Creating Influxdb bucket')
                self.create_influxdb_bucket(client=client, bucket_name='ede',
                                            org=self.source_cfg['source']['ts_source'].get('org', 'scampml'))
                print("Bucket created")
            device_id = query.split("r[\"device_id\"] ==")[1].split(")")[0].strip().replace("\"", "")
            self.__job_stat('Pushing data to influxdb')
            write_client = client.write_api(write_options=WriteOptions(batch_size=2,
                                                                       flush_interval=10_000,
                                                                       jitter_interval=2_000,
                                                                       retry_interval=5_000,
                                                                       max_retries=5,
                                                                       max_retry_delay=30_000,
                                                                       exponential_base=2))
            for detect in body['cycles']:
                print(f"-----> Cycle start {detect['start']}")
                write_client.write(bucket="ede",
                                   record=Point(device_id).tag("device_id", device_id).tag("cycle", "start").field(
                                       "value", 1.0).time(detect['start'],
                                                          WritePrecision.NS
                                                          ))
                print(f"-----> Cycle end {detect['end']}")
                write_client.write(bucket="ede",
                                   record=Point(device_id).tag("device_id", device_id).tag("cycle", "end").field(
                                       "value", 2.0).time(detect['end'],
                                                          WritePrecision.NS
                                                          ))
            client.close()
        except Exception as inst:
            self.__job_stat(f'Error outputting to influxdb with {type(inst)} and {inst.args}')
            return 0

        # write_client = client.write_api(write_options=WriteOptions(batch_size=1000,
        #                                                            flush_interval=10_000,
        #                                                            jitter_interval=2_000,
        #                                                            retry_interval=5_000,
        #                                                            max_retries=5,
        #                                                            max_retry_delay=30_000,
        #                                                            exponential_base=2))
        # _now = datetime.utcnow()
        # wrt_resp_start = write_client.write(bucket="ede",
        #                                     record=Point("B827EB4165DC").tag("device_id", "B827EB4165DC").tag("cycle",
        #                                                                                                       "start").field(
        #                                         "value", 1.0).
        #                                     time(_now, WritePrecision.NS))
        # time.sleep(60)
        # _now = datetime.utcnow()
        # wrt_res_end = write_client.write(bucket="ede",
        #                                  record=Point("B827EB4165DC").tag("device_id", "B827EB4165DC").tag("cycle",
        #                                                                                                    "end").field(
        #                                      "value", 1.0).
        #                                  time(_now, WritePrecision.NS))
        return 0

    def get_influx_org_id(self, client, org_name):
        influxdb_org_api = influxdb_client.OrganizationsApi(client)
        orgs = influxdb_org_api.find_organizations()
        for org in orgs:
            if org.name == org_name:
                print(f"ORG_ID: {org.id}")
                return org.id
        return None

    def create_influxdb_bucket(self,
                               client,
                               bucket_name,
                               org):
        try:
            new_bucket = influxdb_client.domain.bucket.Bucket(
                name=bucket_name,
                retention_rules=[],
                org_id=self.get_influx_org_id(client, org)
            )
            client.buckets_api().create_bucket(new_bucket)
        except Exception as inst:
            print(f'Error creating bucket {bucket_name} with {type(inst)} and {inst.args}')
            import sys
            sys.exit()

    def check_bucket_exists(self, client, bucket_name):
        bucket = client.buckets_api().find_bucket_by_name(bucket_name)
        if bucket:
            return True
        else:
            return False

    def __output(self, data):
        # Output the results
        if 'grafana' in self.ede_cfg['out'].keys():
            print('todo grafana output')
        if 'kafka' in self.ede_cfg['out'].keys():
            self.__kafka_out(data)
        if 'influxdb' in self.ede_cfg['out'].keys():
            self.__influxdb_out(data)
        if 'local' in self.ede_cfg['out'].keys():
            self.__local_out(data)

    def __scale_data(self, df):
        # scale the data
        if self.fillna:
            df.fillna(self.fillna, inplace=True)
        scaler = self.ede_cfg['operators'].get('scaler', None)
        if scaler: # if no scaler defined then return org data
            for k, v in scaler.items(): # TODO only one scaler is supported
                scaler = getattr(importlib.import_module(self.mod_name), k)(**v)
                self.__job_stat(f'Scaling data using {k}')
                # self.job.meta['status'] = f'Scaling data using {k}'
                resp_scaled = scaler.fit_transform(np.asarray(df).reshape(-1, 1))
            df_resp_scaled = pd.DataFrame(resp_scaled, index=df.index, columns=df.columns)
        else:
            return df
        return df_resp_scaled

    def __fetch_pattern_from_influxdb(self):
        default_pattern_file = os.path.join(self.etc_dir, 'df_pattern.csv')
        try:
            client = InfluxDBClient(url=self.source_cfg['source']['ts_source']['host'],
                                    token=self.source_cfg['source']['ts_source']['token'],
                                    org=self.source_cfg['source']['ts_source'].get('org', 'scampml'),
                                    timeout=60_000,)
            query = self.ede_cfg['operators']['cycle_detect']['pattern']
            feature = self.ede_cfg['source']['ts_source'].get("feature", "_value")
            self.__job_stat('Fetching pattern from influxdb')
            df = client.query_api().query_data_frame(query)
            df['_time'] = pd.to_datetime(df['_time'])
            df.set_index('_time', inplace=True)
            df.to_csv(default_pattern_file, index=True)
            return df
        except Exception as inst:
            self.__job_stat(f'Error fetching pattern from influxdb with {type(inst)} and {inst.args}')
            raise Exception(f'Error fetching pattern from influxdb with {type(inst)} and {inst.args}')

    def __load_pattern(self):
        # Load the patterns
        if not isinstance(self.pattern, list):
            self.__job_stat('Using previously defined cycle pattern')
            return self.pattern
        pattern = self.ede_cfg['operators']['cycle_detect'].get('pattern', None)
        if pattern:
            if 'csv' in pattern:
                self.__job_stat('Loading cycle pattern')
                # self.job.meta['status'] = 'Loading cycle pattern'
                df_pattern = pd.read_csv(os.path.join(self.etc_dir, pattern), index_col=0)

            else:
                df_pattern = self.__fetch_pattern_from_influxdb()
            if df_pattern.shape[1] > df_pattern.shape[0]:  # Check if more rows than columns, if true transpose
                df_pattern = df_pattern.T
            self.pattern = df_pattern
            return df_pattern
        else:
            self.__job_stat('No cycle pattern defined')
            # self.job.meta['status'] = 'No cycle pattern defined'
            raise Exception('No pattern defined')

    def __heuristic_overlap(self,
                            matches,
                            pattern):
        delta_bias = self.ede_cfg['operators']['cycle_detect'].get('delta_bias', None)
        if delta_bias:
            self.__job_stat('Computing heuristic overlap')
            # self.job.meta['status'] = 'Computing heuristic overlap'
            # Fileter based on heuristic and delta bias
            df_match = pd.DataFrame(matches, columns=["distance", "pd_id"])
            df_match['remove'] = 0  # marked for deletion
            # df_match['diff'] = 0 # difference
            length_range = len(pattern) - delta_bias  # compute length range between detected patterns, eliminate short patterns
            df_match = df_match.sort_values(by=['pd_id'])
            df_match['diff'] = df_match.pd_id.diff()
            df_match.loc[df_match['diff'] < length_range, 'remove'] = 1
            df_match = df_match[df_match['remove'] == 0]
            df_match.drop(['remove', 'diff'], axis=1, inplace=True)
            matches = df_match.to_numpy()
            return matches
        else:
            return matches

    def __heuristic_overlap_v2(self,
                               df,
                               pattern
                               ):
        delta_bias = self.ede_cfg['operators']['cycle_detect'].get('delta_bias', None)
        if delta_bias:
            # df.insert(0, 'id', range(0, len(df)))
            df.reset_index(inplace=True)
            length_range = len(pattern)-delta_bias
            remove_index = []
            last = None
            for row in df.itertuples():
                if row.dtw_detect == 1:
                    if last is None:
                        last = row.Index
                    else:
                        if row.Index - last < length_range:
                            remove_index.append(row.Index)
                        else:
                            last = row.Index
            for ri in remove_index:
                df.loc[ri, "dtw_detect"] = 0
            return df
        else:
            return df

    def __iterate_windown_dataframe(self, df,
                                    window_size,
                                    stride=1):
        tseries = []
        for i in range(0, len(df), stride):
            tseries.append(df.iloc[i:i + window_size])
        return tseries

    def parallel_type_fix(self, tseries, threads=-1):
        def chunks(df, chunks=threads):
            if chunks == -1:
                return np.array_split(df, os.cpu_count())
            else:
                return np.array_split(df, chunks)

        def fix_type(tseries):
            df_tseries = []
            for ts in tseries:
                df_st = pd.Series(ts).astype('float64')
                # df_st.columns = ['_value', '_time']
                # drop _time column, use default index
                # df_st.drop('_time', axis=1, inplace=True)
                # set _time as index
                # df_ts.set_index('_time', inplace=True)
                df_tseries.append(df_st)
            return df_tseries

        tseries_chuncks = Parallel(n_jobs=threads)(delayed(fix_type)(chunk) for chunk in chunks(tseries, threads))
        tseries_merge = sum(tseries_chuncks, [])
        return tseries_merge

    def iterate_window_dataframe_v2(self,
                                    df,
                                    window_size,
                                    threads=0
                                    ):
        # data_s = df['_value'].to_frame()
        # data_s['_time'] = df.index
        # np_tseries = np.squeeze(sliding_window_view(df.values, window_size, 2))
        np_tseries = sliding_window_view(df.values, window_size)

        print(f"Fixing type of np_tseries")
        if threads:
            self.__job_stat(f'Fixing type of np_tseries using {threads} threads')
            tseries = self.parallel_type_fix(np_tseries, threads=threads)
        else:
            tseries = []
            for ts in np_tseries:
                df_st = pd.Series(ts).astype('float64')
                # df_st = pd.DataFrame(ts).astype('float64')
                # df_st.columns = ['_value', '_time']
                # drop _time column, use default index
                # df_st.drop('_time', axis=1, inplace=True)

                # set _time as index
                # df_ts.set_index('_time', inplace=True)

                tseries.append(df_st)
        return tseries

    def __parallel_iterate_window(self,
                                  df,
                                  window_size,
                                  stride=1,
                                  threads=-1):
        def chunks(df, chunks=threads):
            if chunks == -1:
                return np.array_split(df, os.cpu_count())
            else:
                return np.array_split(df, chunks)

        def missing_elements(L):
            start, end = L[0], L[-1]
            return sorted(set(range(start, end + 1)).difference(L))

        # resetindex, drop old index
        old_index = df.index
        df.reset_index(drop=True, inplace=True)

        # calculate dtw for each chunk
        tseries_chuncks = Parallel(n_jobs=threads)(
            delayed(self.__iterate_windown_dataframe)(chunk, window_size, stride) for chunk in chunks(df, threads))

        # merge chunks
        tseries_merged = sum(tseries_chuncks, [])

        # check for missing values in indexes and return them (assuming they are sequential)
        missing_elem = missing_elements([d.index[0] for d in tseries_merged])

        # generate missing series for missing elements identified
        missing_series = []
        for i in range(0, len(missing_elem)):
            # print(missing_elem[i], missing_elem[i]+stride)
            try:
                missing_series.append(df.iloc[missing_elem[i]:missing_elem[i] + stride])
            except IndexError:
                self.__job_stat(f'Error parallel iterate window for {i} with {missing_elem[i]} and {len(df)}')
                # print(i, missing_elem[i], len(df))

        prev = 0
        test_merge = []
        for dt in tseries_merged:
            # print(dt.index[0])
            # print(prev, dt.index[0])
            if prev != dt.index[0]:
                # print('missing', prev, dt.index[0])
                count_missing = prev
                for ms_dt in missing_series:
                    # print(ms_dt.index[0], count_missing, prev+stride)
                    if ms_dt.index[0] == count_missing:
                        # print(ms_dt.index[0], count_missing, prev+stride)
                        test_merge.append(ms_dt)
                        if count_missing == prev + stride:
                            # print('+++++>stopping')
                            break
                        else:
                            count_missing += 1
                test_merge.append(dt)
                prev += (stride + 1)
            else:
                test_merge.append(dt)
                prev += 1
        return test_merge, old_index

    def __dtw_cyle_detect(self,
                          df,
                          pattern,
                          max_distance=30,
                          window=100,
                          proc=4,
                          parallel=False
                          ):
        enhanced_functioning = os.getenv('EDE_ENH', 0)
        self.__job_stat('Detecting cycles using dtw')
        score = []
        if parallel and not enhanced_functioning:
            print(f"Parallel dtw cycle detection using {proc} threads")
            tseries, old_index = self.__parallel_iterate_window(df, len(pattern), stride=1, threads=proc)
        else:
            if enhanced_functioning:
                if parallel:
                    print(f"Enhanced dtw cycle detection using {proc} threads")
                    tseries = self.iterate_window_dataframe_v2(df, len(pattern), threads=proc)
                else:
                    print(f"Enhanced dtw cycle detection")
                    tseries = self.iterate_window_dataframe_v2(df, len(pattern))
            else:
                print(f"Default dtw cycle detection")
                tseries = self.__iterate_windown_dataframe(df, len(pattern))
            old_index = None
        for t in tseries:
            score.append(dtw.distance_fast(np.asarray(t), np.asarray(pattern), window=window, use_pruning=True))
        score_median = median(score)
        if np.isnan(score_median):
            clean_score = [x for x in score if not np.isnan(x)]
            score_median = median(clean_score)
        if max_distance > 90:
            self.__job_stat(f'Max distance is to low for dtw cycle detection, mean score is {score_median}')
            import sys
            sys.exit(1)
        # fix for incomplete series for v2
        if len(score) != len(df):
            max_score = 9999.0
            score = score + [max_score] * 25
        df = df.to_frame() # Convert to dataframe from series
        df['dtw_score'] = score
        df['dtw_detect'] = 0
        # df.loc[df.dtw_score < max_distance].dtw_detect = 1
        treashold = score_median - percentage(max_distance, score_median)

        # add back old index
        if old_index is not None:
            df.index = old_index

        self.__job_stat(f"Treashold: {treashold}")
        print(f"Treashold: {treashold}")
        df.loc[df["dtw_score"] < treashold, "dtw_detect"] = 1
        df_detect = df[df['dtw_detect'] == 1]
        return df, df_detect

    def __allowed_file(self, filename):
        return '.' in filename and filename.rsplit('.', 1)[1].lower() in self.allowed_extensions

    def __check_local_file(self, file):
        if os.path.isfile(file):
            return True
        else:
            return False

    def check_model(self, model_name):
        try:
            # Connect to Minio
            client = Minio(
                self.source_cfg['source']['minio_source']['host'],
                access_key=self.source_cfg['source']['minio_source']['access_key'],
                secret_key=self.source_cfg['source']['minio_source']['secret_key'],
                secure=False) # TODO fetch secure flag from config, string to boo
            bucket = self.minio_bucket

            # Check model
            objects = client.list_objects(bucket, recursive=True)
            self.__job_stat('Fetching minio models')
            # self.job.meta['status'] = 'Fetching minio models'
            # print(model_name)
            for obj in objects:
                print(obj.object_name)
                if self.__allowed_file(obj.object_name):
                    if obj.object_name == model_name:
                        return True
                    else:
                        return False
                else:
                    return False

        except Exception as inst:
            self.__job_stat(f"Error while connecting to minion with {type(inst)} and {inst.args}")
            print(f"Error while connecting to minion with {type(inst)} and {inst.args}")
            return False

    def __save_model(self, model, model_name):
        try:
            # Connect to Minio
            client = Minio(
                self.source_cfg['source']['minio_source']['host'],
                access_key=self.source_cfg['source']['minio_source']['access_key'],
                secret_key=self.source_cfg['source']['minio_source']['secret_key'],
                secure=False) # TODO fetch secure flag from config, string to boo
            bucket = self.minio_bucket
            # Check if bucket exists if not create source['source']['bucket_in']
            found = client.bucket_exists(bucket)
            if not found:
                client.make_bucket(bucket)
                print(f"Bucket {bucket} created")
            # Save model
            print(f"Saving model {model_name} ...")

            try:
                dump(model, os.path.join(model_dir, f'{model_name}'))
            except Exception as inst:
                print(f"Exception {type(inst)} while saving model {model_name} with {inst.args}")
                return 0

            try:
                model_file = os.path.join(model_dir, model_name)
                with open(model_file, 'rb') as file_data:
                    file_stat = os.stat(model_file)
                    client.put_object(bucket,
                                      model_name,
                                      file_data,
                                      file_stat.st_size,
                                      )
                # self.job.meta['status'] = f'Model {model_name} saved to minio'
                self.__job_stat(f'Model {model_name} saved to minio')
                return True
            except InvalidResponseError as err:
                print(err)
                return False

        except Exception as inst:
            print(f"Error while connecting to minion with {type(inst)} and {inst.args}")
            # self.job.meta['status'] = f'Error while connecting to minion with {type(inst)} and {inst.args}'
            self.__job_stat(f'Error while connecting to minion with {type(inst)} and {inst.args}')
            return False

    def __load_model(self, model_name):
        try:
            # Connect to Minio
            client = Minio(
                self.source_cfg['source']['minio_source']['host'],
                access_key=self.source_cfg['source']['minio_source']['access_key'],
                secret_key=self.source_cfg['source']['minio_source']['secret_key'],
                secure=False) # TODO fetch secure flag from config, string to boo
            bucket = self.minio_bucket
            try:
                model = client.get_object(bucket, model_name)
                self.__job_stat(f'Model {model_name} loaded from minio')
                # self.job.meta['status'] = f'Model {model_name} loaded from minio'
                with open(os.path.join(model_dir, model_name), 'wb') as file_data:
                    for d in model.stream(32 * 1024):
                        file_data.write(d)
            except InvalidResponseError as err:
                self.__job_stat(f'Error while loading model {model_name} from minio with {err}')
                # self.job.meta['status'] = f'Error while loading model {model_name} from minio'
            return load(os.path.join(model_dir, model_name))
        except Exception as inst:
            self.__job_stat(f"Error while connecting to minion with {type(inst)} and {inst.args}")
            # self.job.meta['status'] = f'Error while connecting to minion with {type(inst)} and {inst.args}'
            print(f"Error while connecting to minion with {type(inst)} and {inst.args}")
            return False

    def cycle_detection(self,
                        # feature='RawData.mean_value',
                        feature='_value'
                        ):
        # Load Data
        # set resampling and parallel execution
        resample = self.ede_cfg['operators']['cycle_detect'].get('resample', None)
        parallel = os.getenv('EDE_PARALLEL', 0)

        df_data = self.load_data(resample=resample)
        # Load Pattern
        df_pattern = self.__load_pattern()
        if df_data.empty:
            print("-------> No data from query, DataFrame is empty")
            return 0, 0, 0
        # Cycle Detection
        self.__job_stat('Started Cycle Detection')
        # self.job.meta['status'] = 'Started Cycle Detection'
        max_distance = self.ede_cfg['operators']['cycle_detect'].get('max_distance', None)
        dtw = self.ede_cfg['operators']['cycle_detect'].get('dtw', None)
        if max_distance is None:
            print(f"------> {df_pattern.shape}, {df_data.shape}")
            if dtw is None:
                matches = stumpy.match(df_pattern[feature], df_data[feature])
                len_matches = len(matches)
            else:
                if parallel:
                    matches, detect = self.__dtw_cyle_detect(df_data[feature], df_pattern[feature], parallel=True, proc=int(parallel))
                else:
                    matches, detect = self.__dtw_cyle_detect(df_data[feature], df_pattern[feature])
                len_matches = detect.shape[0]

        else:
            print(f"------> {df_pattern.shape}, {df_data.shape}")
            if dtw is None:
                matches = stumpy.match(df_pattern[feature], df_data[feature], max_distance=max_distance)
                len_matches = len(matches)
            else:
                matches, detect = self.__dtw_cyle_detect(df_data[feature], df_pattern[feature], max_distance=max_distance)
                len_matches = detect.shape[0]

        print(f"Total cycles: {len_matches}")

        if len_matches < 1:
            return 0, 0, 0

        if dtw is None:
            matches = self.__heuristic_overlap(matches, df_pattern[feature])
            print(f"Total cycles after heuristic: {len(matches)}")
            size_of_pattern = len(df_pattern[feature])
            # Store matched cycles
            tseries = []  # store matched cycles
            for match_distance, match_idx in matches:
                match_z_norm = stumpy.core.z_norm(
                    df_data[feature].values[match_idx:match_idx + size_of_pattern])
                tseries.append(match_z_norm)
            # Manually add feature

            self.cdata['detected'] = 0

            pattern_list = []
            tseries = []
            for match_distance, id in matches:
                # # Get the detected cycle
                # pattern_list.append({'start': self.cdata[id:id + size_of_pattern].iloc[0].name,
                #                      'end': self.cdata[id:id + size_of_pattern].iloc[-1].name,
                #                      'cycle': True})

                # Change value based on iloc value
                self.cdata.iloc[id, self.cdata.columns.get_loc('detected')] = 1.0

                # Save detected cycles as numpy array
                tseries.append(stumpy.core.z_norm(self.cdata[feature].values[
                                                  id:id + size_of_pattern]))

        else:
            matches = self.__heuristic_overlap_v2(matches, df_pattern[feature])
            print(f"Total cycles after heuristic v2: {matches[matches['dtw_detect'] == 1].shape[0]}")
            size_of_pattern = len(df_pattern[feature])
            # Store matched cycles
            df_matches = matches[matches['dtw_detect'] == 1]
            tseries = []  # store matched cycles
            for index in df_matches.index:
                tseries.append(matches.iloc[index:index + size_of_pattern]['_value'].values)
            self.cdata['detected'] = matches['dtw_detect'].values
            # matches.set_index('_time', inplace=True)

        #  Save data for bootstrapping
        if self.ede_cfg['operators']['cycle_detect'].get('checkpoint', False):
            df_cycles = pd.DataFrame(np.array(tseries))
            df_cycles.to_csv(os.path.join(self.data_dir, 'cycles.csv'))
            if dtw is None:
                df_matches = pd.DataFrame(matches, columns=['Match_distance', 'id'])
            df_matches.to_csv(os.path.join(self.data_dir, 'matches.csv'))
            # self.cdata.drop(['detected'], axis=1).to_csv(os.path.join(self.data_dir, 'data.csv'))
            self.cdata.to_csv(os.path.join(self.data_dir, 'data.csv'))
        return tseries, matches, size_of_pattern

    def cycle_cluster_trainer(self, save=False):
        '''
        Train clusterer model on cycle data.

        TODO: Add support for other clusterers currently only hdbscan is supported
        '''

        # If bootstrap enabled then try to load cycles and match index
        if self.ede_cfg['operators']['cluster'].get('bootstrap', False):
            if self.__check_local_file(os.path.join(self.data_dir, 'cycles.csv')) and self.__check_local_file(
                    os.path.join(self.data_dir, 'matches.csv')) and \
                    self.__check_local_file(os.path.join(self.data_dir, 'data.csv')):
                tseries = pd.read_csv(os.path.join(self.data_dir, 'cycles.csv'), index_col=0).to_numpy()
                matches = pd.read_csv(os.path.join(self.data_dir, 'matches.csv'), index_col=0).to_numpy()
                self.cdata = pd.read_csv(os.path.join(self.data_dir, 'data.csv'), index_col=0)
                self.cdata.index = pd.to_datetime(self.cdata.index)
                # Todo fix this, bootstrap not woking properly
            else:
                tseries = []
                matches = []
                self.cdata = pd.DataFrame()
        else:
            tseries, matches, size_of_pattern = self.cycle_detection(feature=self.ede_cfg['source']['ts_source'].get("feature", "_value"))
        # self.job.meta['status'] = 'Computing DTW'
        self.__job_stat('Computing DTW')
        print("Computing DTW")
        # Compute distance matrix with dtw
        distance_matrix = dtw.distance_matrix_fast(tseries)

        # Train clustering model
        print("Started training clusterer")
        # self.job.meta['status'] = 'Creating HDDBSCAN model'
        self.__job_stat('Creating HDDBSCAN model')
        clusterer = hdbscan.HDBSCAN(min_cluster_size=self.ede_cfg['operators']['cluster']['HDSCAN'].get('min_cluster_size', 30),
                                    metric='precomputed',
                                    # prediction_data=True, # TODO Not working for precomputed metric
                                    # min_samples=self.ede_cfg['operators']['cluster']['HDSCAN'].get('min_samples', 5),
                                    # cluster_selection_epsilon=0.0,
                                    # leaf_size=self.ede_cfg['operators']['cluster']['HDSCAN'].get('leaf_size', 30),
                                    ).fit(distance_matrix)
        labels = clusterer.labels_
        print(f"Unique clusters: {np.unique(labels, return_counts=True)}")

        dtw_method = self.ede_cfg['operators']['cycle_detect'].get('dtw', None)
        if dtw_method is None:
            # Add clustered data
            df_matches = pd.DataFrame(matches, columns=['Match_distance', 'id'])
            df_matches['labels'] = labels

            self.cdata['labels'] = 0

            for match_distance, id in matches:
                self.cdata.iloc[id, self.cdata.columns.get_loc('labels')] = df_matches.loc[
                    df_matches['id'] == id, 'labels']
        else:
            series_labels = dict(zip(matches[matches['dtw_detect'] == 1].index, labels))
            for k, v in series_labels.items():
                matches.loc[k, 'labels'] = v

        if save:
            # Save clusterer to minio bucket scamp-models
            self.__save_model(clusterer, self.ede_cfg['operators']['cluster']['model'])
        return tseries, matches, size_of_pattern

    def detect(self):
        # Output detected cycles
        tseries = []
        matches = []
        size_of_pattern = 0
        # self.job.meta['status'] = 'Started Detection'
        self.__job_stat('Started Detection')
        self.__job_config(self.ede_cfg)
        dtw = self.ede_cfg['operators']['cycle_detect'].get('dtw', None)
        if self.ede_cfg['operators'].get('cluster', {}):
            tseries, matches, size_of_pattern = self.cycle_cluster_trainer()
        else:
            if 'ts_source' in self.ede_cfg['source'].keys():
                tseries, matches, size_of_pattern = self.cycle_detection(feature=self.ede_cfg['source']['ts_source'].get("feature", "_value"))
            else:
                self.__job_stat(f'TS source not set with, using default feature _value')
                tseries, matches, size_of_pattern = self.cycle_detection(feature='_value')
            if not tseries: # if no pattern was found
                return {'cycles': []}
        if self.ede_cfg['operators'].get('anomaly', {}):
            tseries, matches, size_of_pattern = self.cycle_anomaly_inference(tseries, matches, size_of_pattern)

        pattern_list = []
        # test = 0
        # self.job.meta['status'] = 'Generating output of detection'
        self.__job_stat('Generating output of detection')
        if dtw is None:
            for match_distance, id in matches:
                # print(self.cdata[id:id + size_of_pattern].iloc[0].labels)
                # Get the detected cycle
                pattern = {'start': self.cdata[id:id + size_of_pattern].iloc[0].name,
                           'end': self.cdata[id:id + size_of_pattern].iloc[-1].name,
                           'cycle': True,
                           # 'cluster': self.cdata[id:id + size_of_pattern].iloc[0].labels
                           }
                try:
                    pattern['cluster'] = self.cdata[id:id + size_of_pattern].iloc[0].labels
                except Exception as e:
                    # print(f"No clusterer predictions")
                    pattern['cluster'] = None

                try:
                    pattern['anomaly'] = self.cdata[id:id + size_of_pattern].iloc[0].anomaly
                except Exception as e:
                    # print(f"No anomaly predictions")
                    pattern['anomaly'] = None
                pattern_list.append(pattern)
        else:
            for index in matches[matches['dtw_detect'] == 1].index:
                pattern = {'start': self.cdata[index:index + size_of_pattern].iloc[0].name,
                           'end': self.cdata[index:index + size_of_pattern].iloc[-1].name,
                           'cycle_type': True,
                           # 'cluster': self.cdata[id:id + size_of_pattern].iloc[0].labels
                           }
                try:
                    pattern['cluster'] = matches[index:index + size_of_pattern].iloc[0].labels
                except Exception as e:
                    # print(f"No clusterer predictions")
                    pattern['cluster'] = None

                try:
                    pattern['anomaly'] = matches[index:index + size_of_pattern].iloc[0].anomaly
                except Exception as e:
                    # print(f"No anomaly predictions")
                    pattern['anomaly'] = None
                pattern_list.append(pattern)
        detected_cycles = {'cycles': pattern_list}
        # print(detected_cycles)
        # send data to output
        self.__output(detected_cycles)
        return detected_cycles

    def cycle_anomaly_trainer(self, save=False):
        tseries, matches, size_of_pattern = self.cycle_detection(feature=self.ede_cfg['source']['ts_source'].get("feature", "_value"))
        model = list(self.ede_cfg['operators'].get('anomaly', {}).keys())[0]
        if model: # if no scaler defined then return org data
            module = model.split('.')[0]
            name = model.split('.')[1]
            full_module = self.anom_name + '.' + module
            # print(list(self.ede_cfg['operators'].get('anomaly', {}).values())[0])
            an_model = getattr(importlib.import_module(full_module), name)(**list(self.ede_cfg['operators'].get('anomaly', {}).values())[0])
            an_model.fit(np.array(tseries))

            ano_label = an_model.predict(np.array(tseries))
            print(f"Detected anomalies: {np.unique(ano_label, return_counts=True)}")
            dtw_method = self.ede_cfg['operators']['cycle_detect'].get('dtw', None)
            if dtw_method is None:
                df_matches = pd.DataFrame(matches, columns=['Match_distance', 'id'])
                df_matches["anomaly"] = ano_label
                self.cdata["anomaly"] = 0
                for match_distance, id in matches:
                    self.cdata.iloc[id, self.cdata.columns.get_loc("anomaly")] = df_matches.loc[
                        df_matches['id'] == id, "anomaly"]
            else:
                series_labels = dict(zip(matches[matches['dtw_detect'] == 1].index, ano_label))
                for k, v in series_labels.items():
                    matches.loc[k, 'anomaly'] = v

            if save:
                # Save clusterer to minio bucket scamp-models
                self.__save_model(an_model, self.ede_cfg['operators']['anomaly']['model'])
        return tseries, matches, size_of_pattern

    def cycle_anomaly_inference(self,
                                tseries=[],
                                matches=[],
                                size_of_pattern=0):

        model_name = self.ede_cfg['operators']['anomaly']['model']
        ano_model = self.__load_model(model_name)
        if ano_model:
            if not tseries:
                tseries, matches, size_of_pattern = self.cycle_detection(feature=self.ede_cfg['source']['ts_source'].get("feature", "_value"))
            ano_label = ano_model.predict(np.array(tseries))
            print(f"Detected anomalies: {np.unique(ano_label, return_counts=True)}")
            df_matches = pd.DataFrame(matches, columns=['Match_distance', 'id'])
            df_matches["anomaly"] = ano_label
            self.cdata["anomaly"] = 0
            for match_distance, id in matches:
                self.cdata.iloc[id, self.cdata.columns.get_loc("anomaly")] = df_matches.loc[
                    df_matches['id'] == id, "anomaly"]
        return tseries, matches, size_of_pattern

if __name__ == '__main__':
    data_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'data')
    etc_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'etc')
    model_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'models')

    ede_cfg_file = os.path.join(etc_dir, 'ede_engine.json')
    source_cfg_file = os.path.join(etc_dir, 'source_cfg.yaml')

    with open(ede_cfg_file) as f:
        ede_cfg = json.load(f)
    with open(source_cfg_file) as f:
        source_cfg = yaml.safe_load(f)

    def ede_engine(ede_cfg):
        # Load the data
        ede = EDEScampEngine(ede_cfg, source_cfg, data_dir, etc_dir, model_dir)
        # df = ede.load_data()
        # df = ede.scale_data(df)
        # ede.output('data')
        # ede.cycle_detection()
        # ede.cycle_cluster_trainer()
        ede.detect()
    ede_engine(ede_cfg)


