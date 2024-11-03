import numpy as np
import pandas as pd
from typing import Literal
from decimal import Decimal
from numpy.ma.core import filled
from pandas import DataFrame
from abc import ABC, abstractmethod
from datetime import datetime, timedelta

from dataio.csv_handler import CSVReader, CSVWriter
from msg_log.mylog import get_logger
from error_exception.customerror import DataNotExistError
logger = get_logger(__name__)


class DataProcess:
    """对数据进行各种处理，例如缺失值处理、重复值删除、跌涨幅、最高/最低价计算等操作"""

    def __init__(self, data: DataFrame, data_region: Literal['China', 'Foreign'], unit_time: Literal['hour', 'day'], **kwargs):
        self.data = data

        self.csv_reader = kwargs.get('reader', CSVReader(data_region))
        self.csv_writer = kwargs.get('writer', CSVWriter(data_region))

        self.time = kwargs.get('time', datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
        self.unit_time = unit_time
        self.method = kwargs.get('method', 'ffill')


    def get_needed_data(self):
        """获取后续处理需要的数据"""
        self.previous_all_data = self.csv_reader.get_previous_all_data(self.time, self.unit_time)
        if self.previous_all_data.empty:
            logger.warning(f'{self.time}之前没有数据')
        else:
            self.previous_all_data = self.previous_all_data[
                slef.previous_all_data['coin_name'].isin(self.data['coin_name'])]

        self.detail_data = self.csv_reader.get_detail_data(self.time)
        if self.detail_data.empty:
            logger.warning(f'{self.time}没有详细数据，无法计算')
            self.detail_data = self.data

        self.combined_data = pd.concat([self.previous_all_data, self.data], ignore_index=False)
        self.statistics_table = self.csv_reader.get_statistical_table(self.unit_time)
        return self

    def fill_na(self):
        """缺失值填充"""
        self.data = self.data[self.data['price'] != 0]
        self.data = self.data['price'].astype(Decimal)
        filled_data = self.combined_data.groupby(['coin_name', 'spider_web']).apply(
            lambda group: group.fillna(method=self.method)).reset_index(drop=True)
        lasted_data = filled_data.sort_values('time').groupby(['coin_name', 'spider_web']).tail(1)
        self.data = lasted_data
        return self

    def get_price_columns(self):
        """获取最高、最低、开盘、收盘价"""
        if self.unit_time == 'hour':
            data_for_calculate_columns = self.detail_data
            data_for_calculate_columns = pd.concat([data_for_calculate_columns, self.data], ignore_index=False)
        else:
            data_for_calculate_columns = self.combined_data
        self.data['higt'] = self.data_for_calculate_columns.groupby(['coin_name', 'spider_web'])['price'].transform('max')
        self.data['low'] = self.data_for_calculate_columns.groupby(['coin_name', 'spider_web'])['price'].transform('min')
        self.data['open'] = self.data_for_calculate_columns.groupby(['coin_name', 'spider_web'])['price'].transform('first')
        self.data['close'] = self.data['price']
        return self

    def calculate_change_rate(self):
        """计算跌涨幅,(收盘价-开盘价)/开盘价"""
        self.data['change'] = (self.data['close'] - self.data['open']) / self.data['open'] * 100
        return self

    def calculate_amplitude(self):
        """计算振幅"""
        self.data['amplitude'] = (self.data['higt'] - self.data['low']) / self.data['open'] * 100
        return self

    def calculate_virtual_drop(self):
        """计算虚降"""
        self.data['virtual_drop'] = self.data.apply(lambda x: (x['open'] - x['low']) / x['open'] * 100 \
            if x['change'] >= 0 else (x['close'] - x['low']) / x['open'] * 100)
        return self

    def update_statistics_table(self):
        """更新统计表信息"""
        to_be_merged_data = self.data[['high','low', 'coin_name', 'spider_web']]
        to_be_merged_data = to_be_merged_data.rename({'high': f"{self.time}_high", 'low': f"{self.time}_low"})
        if not self.statistics_table.empty:
            # 删除第一列
            self.statistics_table = self.statistics_table.drop(self.statistics_table.columns[2:4], axis=1)
            self.statistics_table = self.statistics_table.merge(to_be_merged_data, how='outer', on=['coin_name', 'spider_web'])
        else:
            self.statistics_table = to_be_merged_data
        writer.write_statistical_table(self.statistics_table, self.unit_time)
        return self

    # 计算
    def calculate_all(self):
        self.fill_na()
        self.get_price_columns()
        self.calculate_change_rate()
        self.calculate_amplitude()
        self.calculate_virtual_drop()
        self.update_statistics_table()




if __name__ == '__main__':
    da = DataProcess(pd.DataFrame(), 'China')
    da.fill_na()
