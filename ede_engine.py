import os
# limit GPU allocation
os.environ["CUDA_DEVICE_ORDER"]="PCI_BUS_ID" #issue #152
os.environ['CUDA_VISIBLE_DEVICES'] = '1'
import numpy as np
np.random.seed(42)
import importlib
import matplotlib.pyplot as plt
import seaborn as sns
from sklearn.decomposition import SparsePCA, PCA
from sklearn.base import clone
from collections import Counter
import pandas as pd
import json
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
    def __init__(self, data_dir, etc_dir):
        self.data_dir = data_dir
        self.etc_dir = etc_dir
        self.mod_name = 'sklearn.preprocessing'
        self.fillna = True

    def load_data(self, data):
        # Load the data
        df = pd.read_csv(os.path.join(self.data_dir, data), index_col=0)
        df.index = pd.to_datetime(df.index)

        return df

    def __scale_data(self, df):
        # Preprocess the data
        if self.fillna:
            df = df.fillna(self.fillna)

        scaler = {"MinMaxScaler": {
            "copy": False,
            "clip": False
        }}

        for k, v in scaler.items():
            scaler = getattr(importlib.import_module(self.mod_name), k)(**v)
            print(scaler)
            df = pd.DataFrame(scaler.fit_transform(df), columns=df.columns, index=df.index)
        return df

data_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'data')
etc_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'etc')

engine = EDEScampEngine(data_dir, etc_dir)
df = engine.load_data('test.csv')
print(df)



