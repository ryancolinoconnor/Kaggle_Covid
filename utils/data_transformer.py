import pandas as pd
import numpy as np


class DataTransformer(object):
    __NaNs__ = [np.NaN,'',None,'[]']
    def __init__(self,data):
        self._underlying_data = data
        
    def collapse_as_dict(self,types=[]):
        data = self._underlying_data.copy()
        for type_ in types:
            data = DataTransformer.collapse_df(data,type_,self.__NaNs__)
        return data
    @staticmethod
    def remove_NaNs(series,nans):
        series = series[~series.isna()]
        series[series.apply(lambda x:(len(str(x))>0) & (str(x) not in nans))]
        return series#[~series.isin(nans)]
    
    @staticmethod
    def collapse_df(data,type_,NaNs):
        cols = list(filter(lambda x:type_ in x,data.columns))
        ret_cols = list(filter(lambda x:type_ not in x,data.columns))
        tmp = data.copy()
        col_name = type_+'_collapsed'
        tmp[col_name] = tmp[cols].apply(lambda x:json.dumps(
                DataTransformer.remove_NaNs(x,NaNs).to_dict()),axis=1)
        return tmp[ret_cols + [col_name]]