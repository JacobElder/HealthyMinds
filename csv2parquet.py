#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Apr  7 11:34:47 2023

@author: jacobelder
"""

import pandas as pd

import pyarrow as pa

import os

import pyarrow.parquet as pq

import chardet

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
        temp = pd.read_csv(f, encoding = enc['encoding'])
        table = pa.Table.from_pandas(temp)
        new_filename = f[:-4] + ('.parquet')
        pq.write_table(table, new_filename)