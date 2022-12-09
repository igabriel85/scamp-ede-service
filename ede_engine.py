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
from joblib import dump, load
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





# Influx Connection
# from influxdb import InfluxDBClient, DataFrameClient

from subprocess import check_output


class EDEScampEngine():
    def __init__(self,
                 ede_cfg,
                 source_cfg,
                 data_dir,
                 etc_dir,
                 # pattern_file
                 ):
        self.ede_cfg = ede_cfg
        self.source_cfg = source_cfg
        self.data_dir = data_dir
        self.etc_dir = etc_dir
        # self.pattern_file = pattern_file
        self.mod_name = 'sklearn.preprocessing'
        self.fillna = True
        self.cdata = 0

    def load_data(self):
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
            print(ede_cfg['source'].keys())
            raise Exception('Unknown source type')
        self.cdata = df.copy(deep=True)
        return df

    def __local_data(self):
        df = pd.read_csv(os.path.join(self.data_dir, self.ede_cfg['source']['local']), index_col=0)
        df.index = pd.to_datetime(df.index)
        df = self.__scale_data(df)
        # print(df)
        return df

    def __minio_data(self):
        try:
            # Connect to Minio
            client = Minio(
                self.source_cfg['source']['minio_source']['host'],
                access_key=self.source_cfg['source']['minio_source']['access_key'],
                secret_key=self.source_cfg['source']['minio_source']['secret_key'],
                secure=False) # TlDO fetch secure flag from config, string to boo
            bucket = self.ede_cfg['source']['minio_source']['bucket']
            data_object = self.ede_cfg['source']['minio_source']['data']

            # Get the data
            local_data = os.path.join(data_dir, data_object)
            bdata = client.get_object(bucket, data_object)
            with open(local_data, 'wb') as file_data:
                for d in bdata.stream(32 * 1024):
                    file_data.write(d)
        except Exception as inst:
            print(type(inst), inst.args)
            raise Exception('Error loading data from minio')
        df = pd.read_csv(local_data, index_col=0)
        df.index = pd.to_datetime(df.index)
        df = self.__scale_data(df)
        return df

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

    def cycle_detection(self,
                        feature='RawData.mean_value'):
        # Load Data
        df_data = self.load_data()
        # Load Pattern
        df_pattern = self.__load_pattern()

        # Cycle Detection
        max_distance = self.ede_cfg['operators']['cycle_detect'].get('max_distance', None)
        matches = stumpy.match(df_pattern[feature], df_data[feature], max_distance=max_distance)
        print(f"Total cycles: {len(matches)}")
        matches = self.__heuristic_overlap(matches, df_pattern[feature])
        print(f"Total cycles after heuristic: {len(matches)}")
        # Store matched cycles
        tseries = []  # store matched cycles
        for match_distance, match_idx in matches:
            match_z_norm = stumpy.core.z_norm(
                df_data[feature].values[match_idx:match_idx + len(df_pattern[feature])])
            tseries.append(match_z_norm)
        # Manually add feature

        self.cdata['detected'] = 0
        size_of_pattern = len(df_pattern[feature])
        for match_distance, id in matches:
            # Change value based on iloc value
            self.cdata.iloc[id, self.cdata.columns.get_loc('detected')] = 1.0
            # df_n_test_cp.iloc[id+size_of_pattern, 'detected'] = 1



data_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'data')
etc_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'etc')

ede_cfg_file = "/Users/Gabriel/Documents/workspaces/scamp-ede-service/etc/ede_engine.json"
source_cfg_file = "/Users/Gabriel/Documents/workspaces/scamp-ede-service/etc/source_cfg.yaml"
with open(ede_cfg_file) as f:
    ede_cfg = json.load(f)
with open(source_cfg_file) as f:
    source_cfg = yaml.safe_load(f)
def ede_engine(ede_cfg):
    # Load the data
    ede = EDEScampEngine(ede_cfg, source_cfg, data_dir, etc_dir)
    # df = ede.load_data()
    # df = ede.scale_data(df)
    ede.cycle_detection()





# engine = EDEScampEngine(data_dir, etc_dir)
# df = engine.load_data('test.csv')
# print(df)

ede_engine(ede_cfg)


