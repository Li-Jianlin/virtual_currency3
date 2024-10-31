import numpy as np
import pandas as pd
from pandas import DataFrame
from abc import ABC, abstractmethod
from dataio.csv_reader_writer import CSVReader
from msg_log.mylog import get_logger

class DataProcess:
    """对数据进行各种处理，例如缺失值处理、重复值删除、跌涨幅、最高/最低价计算等操作"""
    def __init__(self, data: DataFrame, **kwargs):
        self.data = data
        self.csv_reader = CSVReader()
        self.logger = get_logger(__name__)

    def fill_na(self, method='ffill'):
        """缺失值填充"""



