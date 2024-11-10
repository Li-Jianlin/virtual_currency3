import numpy as np
import pandas as pd
from typing import Literal
from decimal import Decimal
from pandas import DataFrame
from abc import ABC, abstractmethod
from datetime import datetime, timedelta
import os

from dataio.csv_handler import CSVReader, CSVWriter
from msg_log.mylog import get_logger
from error_exception.customerror import DataNotExistError

PROJECT_ROOT_PATH = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
logger = get_logger(__name__, filename=os.path.join(PROJECT_ROOT_PATH, 'log', 'data_process.log'))


class DataProcess:
    """对数据进行各种处理，例如缺失值处理、重复值删除、跌涨幅、最高/最低价计算等操作"""

    def __init__(self, data: DataFrame, data_region: Literal['China', 'Foreign'], unit_time: Literal['hour', 'day'],
                 **kwargs):
        self.previous_all_data = None
        self.detail_data = None
        self.combined_data = None
        self.statistics_table = None
        self.data = data
        default_base_file_path = os.path.join(PROJECT_ROOT_PATH, 'data', data_region)
        self.csv_reader = kwargs.get('reader', CSVReader(data_region, base_file_path=kwargs.get('base_file_path',
                                                                                                default_base_file_path)))
        self.csv_writer = kwargs.get('writer', CSVWriter(data_region, base_file_path=kwargs.get('base_file_path',
                                                                                                default_base_file_path)))
        self.datetime = kwargs.get('datetime' ,datetime.now().replace(second=0))

        self.time = kwargs.get('time', self.datetime.strftime('%Y-%m-%d %H:%M:%S'))
        self.unit_time = unit_time

    def get_needed_data(self):
        """获取后续处理需要的数据"""
        self.data['coin_price'] = self.data['coin_price'].apply(Decimal)
        self.data = self.data[self.data['coin_price'] != 0]
        self.detail_data = self.csv_reader.get_detail_data()
        self.statistics_table = self.csv_reader.get_statistical_table(self.unit_time)

        if self.detail_data.empty:
            # self.detail_data = self.detail_data[self.detail_data['coin_name'].isin(self.data['coin_name'])]
            logger.warning(f'{self.time}没有详细数据，无法计算')

        if self.unit_time == 'hour':
            self.combined_data = pd.concat([self.data, self.detail_data], ignore_index=True)
        else:
            self.previous_all_data = self.csv_reader.get_data_between_hours(self.datetime - timedelta(days=1) - timedelta(hours=1),
                                                                            self.datetime, 'both')
            if self.previous_all_data.empty:
                logger.warning(f'{self.time}之前没有数据')
                # 没有小时数据
                self.previous_all_data = pd.DataFrame(columns=['coin_name','spider_web','coin_price','time','high','low',
                                                               'open','close','change','amplitude','virtual_drop'])
            self.combined_data = self.previous_all_data

        self.combined_data.drop_duplicates(inplace=True)
        self.combined_data = self.combined_data.merge(self.data[['coin_name', 'spider_web']], on=['coin_name', 'spider_web'], how='inner')
        return self

    def fill_na(self):
        """缺失值填充"""

        filled_data = self.combined_data.groupby(['coin_name', 'spider_web'])[['coin_price', 'time']].apply(
            lambda group: group.ffill()).reset_index(drop=False).set_index('level_2')
        lasted_data = filled_data.sort_values('time').groupby(['coin_name', 'spider_web']).tail(1)
        self.data = lasted_data
        return self

    def get_price_columns(self):
        """获取最高、最低、开盘、收盘价"""
        data_for_calculate_columns = self.combined_data.copy().sort_values('time', ascending=True)
        # 设置 data 的索引
        data = self.data.copy().set_index(['coin_name', 'spider_web'])

        data['high'] = data_for_calculate_columns.groupby(['coin_name', 'spider_web'])['coin_price'].apply('max')
        data['low'] = data_for_calculate_columns.groupby(['coin_name', 'spider_web'])['coin_price'].apply('min')
        data['open'] = data_for_calculate_columns.groupby(['coin_name', 'spider_web'])['coin_price'].apply('first')
        data['close'] = data_for_calculate_columns.groupby(['coin_name', 'spider_web'])['coin_price'].apply('last')

        self.data = data.reset_index()
        del data
        return self

    def calculate_change_rate(self):
        """计算跌涨幅,(收盘价-开盘价)/开盘价"""
        self.data['change'] = (self.data['close'] - self.data['open']) / self.data['open'] * 100
        return self

    def calculate_amplitude(self):
        """计算振幅"""
        self.data['amplitude'] = (self.data['high'] - self.data['low']) / self.data['open'] * 100
        return self

    def calculate_virtual_drop(self):
        """计算虚降"""
        self.data['virtual_drop'] = self.data.apply(lambda x: (x['open'] - x['low']) / x['open'] * 100 \
            if x['change'] >= 0 else (x['close'] - x['low']) / x['open'] * 100, axis=1)
        return self

    def update_statistics_table(self):
        """更新统计表信息"""
        pre_time = None
        if self.unit_time == 'hour':
            pre_time = (self.datetime - timedelta(hours=1)).strftime('%Y-%m-%d %H:%M:%S')
        elif self.unit_time == 'day':
            pre_time = (self.datetime - timedelta(days=1)).strftime('%Y-%m-%d %H:%M:%S')
        to_be_merged_data = self.data[['high', 'low', 'coin_name', 'spider_web']]
        to_be_merged_data = to_be_merged_data.rename(columns={'high': f"{pre_time}_high", 'low': f"{pre_time}_low"})
        if not self.statistics_table.empty:
            # 排序
            self.statistics_table = self.statistics_table.reindex(columns=['coin_name', 'spider_web'] + sorted(
                self.statistics_table.columns.difference(['coin_name', 'spider_web'])))

            # self.statistics_table = self.statistics_table.drop(self.statistics_table.columns[2:4], axis=1)
            self.statistics_table = self.statistics_table.merge(to_be_merged_data, how='outer',
                                                                on=['coin_name', 'spider_web'])
        else:
            self.statistics_table = to_be_merged_data
            self.statistics_table = self.statistics_table.reindex(columns=['coin_name', 'spider_web'] + sorted(
                self.statistics_table.columns.difference(['coin_name', 'spider_web'])))
        self.csv_writer.write_statistical_table(self.statistics_table, self.unit_time)
        return self

    # 计算
    def calculate_all(self):
        if self.unit_time == 'hour':
            self.fill_na()
        try:
            self.get_price_columns().calculate_change_rate().calculate_amplitude().calculate_virtual_drop().update_statistics_table()
        except Exception as e:
            logger.warning(e)


if __name__ == '__main__':
    try:
        # data = pd.DataFrame({
        #     'coin_name': ['BTC', 'ETH', 'USDT', 'BTC', 'ETH', 'USDT'],
        #     'coin_price': [str(68218.00), str(2443.51), str(34.20), str(68218.00), str(2443.51), str(34.20)],
        #     'spider_web': ['binance', 'binance', 'binance', 'coin-stats', 'coin-stats', 'coin-stats'],
        #     'time': ['2024-11-04 01:00:00', '2024-11-04 01:00:00', '2024-11-04 01:00:00', '2024-11-04 01:00:00',
        #              '2024-11-04 01:00:00', '2024-11-04 01:00:00']
        # })
        data = pd.read_csv('../data_00.csv', encoding='utf-8', dtype={'coin_price': str})
        data['coin_price'] = data['coin_price'].apply(Decimal)
        da = DataProcess(data, 'Foreign', unit_time='day', time='2024-11-05 00:00:00'
                         )
        da.get_needed_data()
        da.calculate_all()

        pass
    except KeyboardInterrupt:
        print('程序被用户终止')
