import os
# limit GPU allocation
os.environ["CUDA_DEVICE_ORDER"]="PCI_BUS_ID" #issue #152
os.environ['CUDA_VISIBLE_DEVICES'] = '1'
import numpy as np
np.random.seed(42)
import importlib
import yaml
import matplotlib.pyplot as plt
import seaborn as sns
from sklearn.decomposition import SparsePCA, PCA
from sklearn.base import clone
from collections import Counter
import pandas as pd
import json
import stumpy
from minio import Minio
from minio.error import InvalidResponseError
from kafka import KafkaProducer
from dtaidistance import dtw
import hdbscan
import tqdm
import glob
import fnmatch
import gzip
import re
import sys
import random
import itertools
# Import all models
from joblib import dump, load
from sklearn.ensemble import IsolationForest
from sklearn.preprocessing import StandardScaler, RobustScaler, MinMaxScaler

from logging import getLogger
log = getLogger(__name__)


# Influx Connection
# from influxdb import InfluxDBClient, DataFrameClient

from subprocess import check_output


class EDEScampEngine():
    def __init__(self,
                 ede_cfg,
                 source_cfg,
                 data_dir,
                 etc_dir,
                 models_dir,
                 minio_bucket='scamp-models'
                 # pattern_file
                 ):
        self.ede_cfg = ede_cfg
        self.source_cfg = source_cfg
        self.data_dir = data_dir
        self.etc_dir = etc_dir
        self.models_dir = models_dir
        # self.pattern_file = pattern_file
        self.mod_name = 'sklearn.preprocessing'
        self.anom_name = 'pyod.models'
        self.minio_bucket = minio_bucket
        self.allowed_extensions = {'joblib', 'pickle'}
        self.fillna = True
        self.cdata = 0

    def load_data(self):
        '''
        Load the data from different sources as stated in ede engine config
        and defiend in source_cfg
        '''
        # Load the data
        if 'local' in self.ede_cfg['source'].keys():
            df = self.__local_data()
        elif 'ts_source' in self.ede_cfg['source'].keys():
            pass # TODO: Implement this
        elif 'minio_source' in self.ede_cfg['source'].keys():
            df = self.__minio_data()
        elif 'kafka_source'in self.ede_cfg['source'].keys():
            pass # TODO: Implement this
        else:
            # print(ede_cfg['source'].keys())
            raise Exception('Unknown source type')
        self.cdata = df.copy(deep=True)
        return df

    def __local_data(self):
        '''
        Load local data
        '''
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

            # Get the data
            local_data = os.path.join(data_dir, data_object)
            bdata = client.get_object(bucket, data_object)
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

    def __kafka_out(self, body):
        # Output the results to kafka
        try:
            producer = KafkaProducer(value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                                          bootstrap_servers=["{}".format(self.ede_cfg['out']['kafka']['broker'])],
                                          retries=5)
            producer.send(self.ede_cfg['out']['kafka']['topic'], body)
        except Exception as inst:
            print('Error outputing to kafka with {} and {}'.format(type(inst), inst.args))

    def __output(self, data):
        # Output the results
        if 'grafana' in self.ede_cfg['out'].keys():
            print('grafana')
        if 'kafka' in self.ede_cfg['out'].keys():
            self.__kafka_out(data)

    def __scale_data(self, df):
        # scale the data
        if self.fillna:
            df.fillna(self.fillna, inplace=True)
        scaler = self.ede_cfg['operators'].get('scaler', None)
        if scaler: # if no scaler defined then return org data
            for k, v in scaler.items(): # TODO only one scaler is supported
                scaler = getattr(importlib.import_module(self.mod_name), k)(**v)
                resp_scaled = scaler.fit_transform(df)
            df_resp_scaled = pd.DataFrame(resp_scaled, index=df.index, columns=df.columns)
        else:
            return df
        return df_resp_scaled

    def __load_pattern(self):
        # Load the patterns
        pattern = self.ede_cfg['operators']['cycle_detect'].get('pattern', None)
        if pattern:
            df_pattern = pd.read_csv(os.path.join(self.etc_dir, pattern), index_col=0)
            if df_pattern.shape[1] > df_pattern.shape[0]: # Check if more rows than columns, if true transpose
                df_pattern = df_pattern.T
            return df_pattern
        else:
            raise Exception('No pattern defined')

    def __heuristic_overlap(self,
                            matches,
                            pattern):
        delta_bias = self.ede_cfg['operators']['cycle_detect'].get('delta_bias', None)
        if delta_bias:
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
                return True
            except InvalidResponseError as err:
                print(err)
                return False

        except Exception as inst:
            print(f"Error while connecting to minion with {type(inst)} and {inst.args}")
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

                with open(os.path.join(model_dir, model_name), 'wb') as file_data:
                    for d in model.stream(32 * 1024):
                        file_data.write(d)
            except InvalidResponseError as err:
                print(err)
            print(load(os.path.join(model_dir, model_name)))
            return load(os.path.join(model_dir, model_name))
        except Exception as inst:
            print(f"Error while connecting to minion with {type(inst)} and {inst.args}")
            return False

    def cycle_detection(self,
                        feature='RawData.mean_value'
                        ):
        # Load Data
        df_data = self.load_data()
        # Load Pattern
        df_pattern = self.__load_pattern()

        # Cycle Detection
        max_distance = self.ede_cfg['operators']['cycle_detect'].get('max_distance', None)
        if max_distance is None:
            matches = stumpy.match(df_pattern[feature], df_data[feature])
        else:
            matches = stumpy.match(df_pattern[feature], df_data[feature], max_distance=max_distance)
        print(f"Total cycles: {len(matches)}")
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
            tseries.append(stumpy.core.z_norm(self.cdata['RawData.mean_value'].values[
                               id:id + size_of_pattern]))


        #  Save data for bootstrapping
        if self.ede_cfg['operators']['cycle_detect'].get('checkpoint', False):
            df_cycles = pd.DataFrame(np.array(tseries))
            df_cycles.to_csv(os.path.join(self.data_dir, 'cycles.csv'))
            df_matches = pd.DataFrame(matches, columns=['Match_distance', 'id'])
            df_matches.to_csv(os.path.join(self.data_dir, 'matches.csv'))
            self.cdata.drop(['detected'], axis=1).to_csv(os.path.join(self.data_dir, 'data.csv'))
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
            tseries, matches, size_of_pattern = self.cycle_detection()
        print("Computing DTW")
        # Compute distance matrix with dtw
        distance_matrix = dtw.distance_matrix_fast(tseries)

        # Train clustering model
        print("Started training clusterer")
        clusterer = hdbscan.HDBSCAN(min_cluster_size=self.ede_cfg['operators']['cluster']['HDSCAN'].get('min_cluster_size', 30),
                                    metric='precomputed',
                                    # prediction_data=True, # TODO Not working for precomputed metric
                                    # min_samples=self.ede_cfg['operators']['cluster']['HDSCAN'].get('min_samples', 5),
                                    # cluster_selection_epsilon=0.0,
                                    # leaf_size=self.ede_cfg['operators']['cluster']['HDSCAN'].get('leaf_size', 30),
                                    ).fit(distance_matrix)
        labels = clusterer.labels_
        print(f"Unique clusters: {np.unique(labels, return_counts=True)}")

        # Add clustered data
        df_matches = pd.DataFrame(matches, columns=['Match_distance', 'id'])
        df_matches['labels'] = labels

        self.cdata['labels'] = 0

        for match_distance, id in matches:
            self.cdata.iloc[id, self.cdata.columns.get_loc('labels')] = df_matches.loc[
                df_matches['id'] == id, 'labels']

        if save:
            # Save clusterer to minio bucket scamp-models
            self.__save_model(clusterer, self.ede_cfg['operators']['cluster']['model'])
        return tseries, matches, size_of_pattern

    def detect(self):
        # Output detected cycles
        tseries = []
        matches = []
        size_of_pattern = 0
        if self.ede_cfg['operators'].get('cluster', {}):
            tseries, matches, size_of_pattern = self.cycle_cluster_trainer()
        else:
            tseries, matches, size_of_pattern = self.cycle_detection()
        if self.ede_cfg['operators'].get('anomaly', {}):
            tseries, matches, size_of_pattern = self.cycle_anomaly_inference(tseries, matches, size_of_pattern)

        pattern_list = []
        # test = 0
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
                print(f"No clusterer predictions")
                pattern['cluster'] = None

            try:
                pattern['anomaly'] = self.cdata[id:id + size_of_pattern].iloc[0].anomaly
            except Exception as e:
                print(f"No anomaly predictions")
                pattern['anomaly'] = None
            pattern_list.append(pattern)

            # test += 1
            # if test > 10:
            #     break
        detected_cycles = {'cycles': pattern_list}
        # print(detected_cycles)
        # send data to output
        self.__output(detected_cycles)
        return detected_cycles

    def cycle_anomaly_trainer(self, save=False):
        tseries, matches, size_of_pattern = self.cycle_detection()
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
            df_matches = pd.DataFrame(matches, columns=['Match_distance', 'id'])
            df_matches["anomaly"] = ano_label
            self.cdata["anomaly"] = 0
            for match_distance, id in matches:
                self.cdata.iloc[id, self.cdata.columns.get_loc("anomaly")] = df_matches.loc[
                    df_matches['id'] == id, "anomaly"]
            if save:
                # Save clusterer to minio bucket scamp-models
                self.__save_model(an_model, self.ede_cfg['operators']['anomaly']['model'])
        return tseries, matches, size_of_pattern



    def cycle_anomaly_inference(self, tseries=[],
                                matches=[],
                                size_of_pattern=0):

        model_name = self.ede_cfg['operators']['anomaly']['model']
        ano_model = self.__load_model(model_name)
        if ano_model:
            if not tseries:
                tseries, matches, size_of_pattern = self.cycle_detection()
            ano_label = ano_model.predict(np.array(tseries))
            print(f"Detected anomalies: {np.unique(ano_label, return_counts=True)}")
            df_matches = pd.DataFrame(matches, columns=['Match_distance', 'id'])
            df_matches["anomaly"] = ano_label
            self.cdata["anomaly"] = 0
            for match_distance, id in matches:
                self.cdata.iloc[id, self.cdata.columns.get_loc("anomaly")] = df_matches.loc[
                    df_matches['id'] == id, "anomaly"]
        return tseries, matches, size_of_pattern



data_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'data')
etc_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'etc')
model_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'models')

# ede_cfg_file = "/Users/Gabriel/Documents/workspaces/scamp-ede-service/etc/ede_engine.json"
ede_cfg_file = "/Users/Gabriel/Documents/workspaces/scamp-ede-service/etc/ede_engine_local.json"
source_cfg_file = "/Users/Gabriel/Documents/workspaces/scamp-ede-service/etc/source_cfg.yaml"

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
    # ede.cycle_anomaly_trainer(save=True)
    # ede.cycle_anomaly_inference()






# engine = EDEScampEngine(data_dir, etc_dir)
# df = engine.load_data('test.csv')
# print(df)

ede_engine(ede_cfg)


