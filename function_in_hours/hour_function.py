import os
import pandas as pd
from  msg_log.mylog import get_logger
from typing import Literal
from datetime import datetime, timedelta
from dataio.csv_handler import CSVReader, CSVWriter
PROJECT_ROOT_PATH = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

logger = get_logger(__name__, filename=os.path.join(PROJECT_ROOT_PATH, 'log', 'hourly_function_handler.log'))

class FunctionHandler:
    def __init__(self, **kwargs):
        self.datetime = datetime.now().replace(minute=0, second=0)
        self.time = kwargs.get('time', self.datetime.strftime('%Y-%m-%d %H:%M:%S'))
        self.data = None
        self.range_data = None
        self.functions = []
        self.results = []
        self.csv_reader = kwargs.get('reader', CSVReader("China"))
        self.csv_writer = kwargs.get('writer', CSVWriter("China"))

    @staticmethod
    def filter_by_amplitude(data: pd.DataFrame, comparison: Literal['gt', 'lt', 'ge', 'le'], threshold: int):
        """筛选 振幅 符合条件的数据,gt:> lt:< ge:>= le:<="""
        if comparison == 'gt':
            data = data[data['amplitude'] > threshold]
        elif comparison == 'lt':
            data = data[data['amplitude'] < threshold]
        elif comparison == 'ge':
            data = data[data['amplitude'] >= threshold]
        elif comparison == 'le':
            data = data[data['amplitude'] <= threshold]
        else:
            logger.warning(f'不支持的比较运算符:{comparison}')
            raise ValueError(f'不支持的比较运算符:{comparison}')

    @staticmethod
    def filter_by_change_rate(data: pd.DataFrame, comparison: Literal['gt', 'lt', 'ge', 'le'], threshold: int):
        """筛选 跌涨幅 符合条件的数据,gt:> lt:< ge:>= le:<="""
        if comparison == 'gt':
            data = data[data['change'] > threshold]
        elif comparison == 'lt':
            data = data[data['change'] < threshold]
        elif comparison == 'ge':
            data = data[data['change'] >= threshold]
        elif comparison == 'le':
            data = data[data['change'] <= threshold]
        else:
            logger.warning(f'不支持的比较运算符:{comparison}')
            raise ValueError(f'不支持的比较运算符:{comparison}')

    @staticmethod
    def filter_by_virtual_drop(data: pd.DataFrame, comparison: Literal['gt', 'lt', 'ge', 'le'], threshold: int):
        """筛选 虚拟跌 符合条件的数据,gt:> lt:< ge:>= le:<="""
        if comparison == 'gt':
            data = data[data['virtual_drop'] > threshold]
        elif comparison == 'lt':
            data = data[data['virtual_drop'] < threshold]
        elif comparison == 'ge':
            data = data[data['virtual_drop'] >= threshold]
        elif comparison == 'le':
            data = data[data['virtual_drop'] <= threshold]
        else:
            logger.warning(f'不支持的比较运算符:{comparison}')
            raise ValueError(f'不支持的比较运算符:{comparison}')

    def add_function(self, func):
        # 添加以小时为单位的函数
        self.functions.append(func)

    def execute_all(self):
        # 执行所有函数
        for func in self.functions:
            res = func()
            if res:
                self.results.append(res)


class HourlyFunctionHandler(FunctionHandler):
    """处理以小时为单位的各种函数"""

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def get_range_data(self, start_time: str, end_time: str, inclusive: Literal['both', 'neither', 'left', 'right']='both'):
        self.range_data = self.csv_reader.get_data_between_hours(start_time, end_time, inclusive)

    def change_and_virtual_drop_and_price_func_1(self):
        """
        有ABC三个时刻。其中C为当前时刻
	    C时刻在跌
	    A时刻与C时刻不超过24小时。
	    A时刻在B时刻之前，A和B时刻均要满足虚降>=0.9%。并且如果在增还要求增幅<=1%,如果在跌则不作跌涨幅的判断。
	    C时刻收盘价同时小于A和B的最低价。
	    A时刻收盘价大于B时刻收盘价。

	    **新增条件**
        C收盘价小于等于A最低价和B最低价中最小值的98%（此条件单独记录）
        :return:
        """

        data_at_C = self.data.copy()
        change_data_at_C = self.filter_by_change_rate(data_at_C, 'lt', 0)

        data_A_to_B = self.range_data

        datetime_A = self.datetime - timedelta(hours=24)
        timestr_A = datetime_A.strftime('%Y-%m-%d %H:%M:%S')
        data_A_to_B = data_A_to_B[data_A_to_B['time'].between(timestr_A, self.time, inclusive='left')]
        data_A_to_B = data_A_to_B[data_A_to_B['coin_name'].isin(change_data_at_C['coin_name'])]

        change_data_A_to_B = self.filter_by_change_rate(data_A_to_B, 'lt', 0)
        virtual_drop_and_change_data_A_to_B = self.filter_by_virtual_drop(change_data_A_to_B, 'ge', 0.9).copy()

        merged_ABC_data = virtual_drop_and_change_data_A_to_B.merge(change_data_at_C[['coin_name', 'spider_web', 'close']],
                                                                    on=['coin_name', 'spider_web'], how='left', suffixes=('', '_C'))



