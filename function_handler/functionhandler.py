from typing import Literal
from datetime import datetime, timedelta
from dataio.csv_handler import CSVReader, CSVWriter
import pandas as pd
import os
from msg_log.mylog import get_logger

PROJECT_ROOT_PATH = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

logger = get_logger(__name__, filename=os.path.join(PROJECT_ROOT_PATH, 'log', 'function_handler.log'))


class FunctionHandler:
    def __init__(self, **kwargs):
        self.range_data_day = None
        self.range_data = None
        self.functions = []
        self.results = []
        self.datetime = kwargs.get('datetime', datetime.now().replace(second=0))
        self.data = kwargs.get('data', None)
        self.csv_reader = kwargs.get('reader', CSVReader("China"))
        self.csv_writer = kwargs.get('writer', CSVWriter("China"))

    def get_range_data_hours(self, start_datetime: datetime, end_datetime: datetime,
                             inclusive: Literal['both', 'neither', 'left', 'right'] = 'both'):
        self.range_data = self.csv_reader.get_data_between_hours(start_datetime, end_datetime, inclusive)

    def get_range_data_days(self, start_datetime: datetime, end_datetime: datetime,
                            inclusive: Literal['both', 'neither', 'left', 'right'] = 'both'):
        self.range_data_day = self.csv_reader.get_data_between_days(start_datetime, end_datetime, inclusive)

    @staticmethod
    def filter_by_amplitude(data: pd.DataFrame, comparison: Literal['gt', 'lt', 'ge', 'le', 'eq', 'neq'],
                            threshold: int):
        """筛选 振幅 符合条件的数据,gt:> lt:< ge:>= le:<="""
        if comparison == 'gt':
            data = data[data['amplitude'] > threshold]
        elif comparison == 'lt':
            data = data[data['amplitude'] < threshold]
        elif comparison == 'ge':
            data = data[data['amplitude'] >= threshold]
        elif comparison == 'le':
            data = data[data['amplitude'] <= threshold]
        elif comparison == 'eq':
            data = data[data['amplitude'] == threshold]
        elif comparison == 'neq':
            data = data[data['amplitude'] != threshold]
        else:
            logger.warning(f'不支持的比较运算符:{comparison}')
            raise ValueError(f'不支持的比较运算符:{comparison}')
        return data

    @staticmethod
    def filter_by_change_rate(data: pd.DataFrame, comparison: Literal['gt', 'lt', 'ge', 'le', 'eq', 'neq'],
                              threshold: int):
        """筛选 跌涨幅 符合条件的数据,gt:> lt:< ge:>= le:<="""
        if comparison == 'gt':
            data = data[data['change'] > threshold]
        elif comparison == 'lt':
            data = data[data['change'] < threshold]
        elif comparison == 'ge':
            data = data[data['change'] >= threshold]
        elif comparison == 'le':
            data = data[data['change'] <= threshold]
        elif comparison == 'eq':
            data = data[data['change'] == threshold]
        elif comparison == 'neq':
            data = data[data['change'] != threshold]
        else:
            logger.warning(f'不支持的比较运算符:{comparison}')
            raise ValueError(f'不支持的比较运算符:{comparison}')
        return data

    @staticmethod
    def filter_by_virtual_drop(data: pd.DataFrame, comparison: Literal['gt', 'lt', 'ge', 'le', 'eq', 'neq'],
                               threshold: int):
        """筛选 虚拟跌 符合条件的数据,gt:> lt:< ge:>= le:<="""
        if comparison == 'gt':
            data = data[data['virtual_drop'] > threshold]
        elif comparison == 'lt':
            data = data[data['virtual_drop'] < threshold]
        elif comparison == 'ge':
            data = data[data['virtual_drop'] >= threshold]
        elif comparison == 'le':
            data = data[data['virtual_drop'] <= threshold]
        elif comparison == 'eq':
            data = data[data['virtual_drop'] == threshold]
        elif comparison == 'neq':
            data = data[data['virtual_drop'] != threshold]
        else:
            logger.warning(f'不支持的比较运算符:{comparison}')
            raise ValueError(f'不支持的比较运算符:{comparison}')
        return data

    def add_function(self, funcs: list):
        # 添加以小时为单位的函数
        self.functions = funcs

    def execute_all(self):
        # 执行所有函数
        self.results = []
        for func in self.functions:
            if not self.range_data.empty:
                try:
                    res = func()
                    if res:
                        self.results.append(res)
                except Exception as e:
                    logger.exception(e)