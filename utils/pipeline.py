import numpy as np
import pandas as pd

import os
import json

import multiprocessing
from multiprocessing import Pool
from functools import partial
import os
import sys

from lazy import lazy

def fpath_to_json(json_folder_path,file):
    json_path = os.path.join(json_folder_path, file)
    with open(json_path) as json_file:
        json_data = json.load(json_file)
    json_data_df = pd.io.json.json_normalize(json_data)
    return json_data_df
class Pipeline(object):
    __BASE_PATH__ = './CORD-19-research-challenge/'
    use_mp = False
    def get_file_ext_sets(self):
        for dirname, _, filenames in os.walk(self.__BASE_PATH__):
            for filename in filenames:
                count += 1
                file_ext = filename.split(".")[-1]
                file_exts.append(file_ext)
        file_ext_set = set(file_exts)
        return list(file_ext_set)

    def get_folders(self):
        return list(filter(lambda x:len(x.split('/'))>3,
                list(map(lambda x:(x[0].replace(os.path.sep,'/')),os.walk(self.__BASE_PATH__)))))
    @lazy
    def _map(self):
        if self.use_mp:
            pool = Pool(processes=multiprocessing.cpu_count())
            return pool.map
        else:
            return map

    def get_files_for_folder(self,json_folder_path,limit=-1):
        list_of_files = list(os.listdir(json_folder_path))#[0:500]
        data_function = partial(fpath_to_json,json_folder_path)
        data = pd.concat(self._map(data_function,list_of_files[:limit]))
        return data