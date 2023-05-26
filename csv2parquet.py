#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Apr  7 11:34:47 2023

@author: jacobelder
"""

import pandas as pd

import dask as dd

import dask.dataframe as dd

import pyarrow as pa

from pyarrow import csv, parquet

import os

import pyarrow.parquet as pq

import chardet

def file_to_data_frame_to_parquet(local_file: str, parquet_file: str) -> None:
    table = csv.read_csv(local_file)
    parquet.write_table(table, parquet_file)

# assign directory
directory = '/Volumes/Research Project/Jake/Healthy Minds/2007-2022 datasets/'
 
# iterate over files in
# that directory
for filename in os.listdir(directory):
    f = os.path.join(directory, filename)
    # checking if it is a file
    if os.path.isfile(f):
        with open(f, 'rb') as f1:
            enc = chardet.detect(f1.read())  # or readline if the file is large
        if f[-len(".csv"):len(f)] == ".csv":
            #temp = pd.read_csv(f, encoding = enc['encoding'])
            temp = dd.read_csv(f)
            new_filename = f[:-len(".csv")] + ('.parquet')
        elif f[-len(".xslx"):len(f)] == ".xlsx":
            #temp = pd.read_xlsx(f, encoding = enc['encoding'])
            temp = dd.read_xlsx(f)
            new_filename = f[:-len(".xslx")] + ('.parquet')
        #temp = pd.read_csv(f, encoding = enc['encoding'])
        #table = pa.Table.from_pandas(temp)
        new_filename = f[:-4] + ('.parquet')
        #pq.write_table(table, new_filename)
        
        temp.repartition(npartitions=1).to_parquet(new_filename)
        #temp.to_parquet(new_filename)