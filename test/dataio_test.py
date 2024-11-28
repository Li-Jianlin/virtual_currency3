import os
import unittest
from decimal import Decimal
from HtmlTestRunner import HTMLTestRunner
import pandas as pd
from datetime import datetime, timedelta

from scipy.stats import invgamma

from dataio.csv_handler import CSVReader, CSVWriter


class DataIOTest(unittest.TestCase):

    def setUp(self):
        data = {
            'coin_name': ['BTC', 'ETH', 'LTC'],
            'coin_price': ['1000', '2000', '3000'],
            'time': ['2020-01-01 00:00:00', '2020-01-01 00:01:00', '2020-01-01 00:02:00'],
            'spider_web': ['binance', 'binance', 'binance'],
            'high': ['1100', '2100', '3100'],
            'low': ['900', '1900', '2900'],
            'open': ['1000', '2000', '3000'],
            'close': ['1100', '2100', '3100'],
            'change': ['10', '20', '30.2'],
            'amplitude': ['1.2', '2.2', '3.2'],
            'virtual_drop': ['1', '2', '3']
        }
        self.data = pd.DataFrame(data)
        self.reader = CSVReader(data_region='China')
        self.writer = CSVWriter(data_region='China')

    def test_change_column_type_to_Decimal_only_coin_price(self):
        data = self.reader.change_column_type_to_Decimal(self.data.copy(), True)
        self.assertIsInstance(data['coin_price'][0], Decimal)

    def test_change_column_type_to_Decimal_other_columns(self):
        data = self.reader.change_column_type_to_Decimal(self.data.copy(), False)
        columns = ['coin_price', 'high', 'low', 'open', 'close', 'amplitude', 'virtual_drop', 'change']
        result = []
        for column in columns:
            result.append(isinstance(data[column][0], Decimal))
        self.assertTrue(all(result))
    # @unittest.skip
    def test_get_time_range_data_in_24_hours(self):
        # 读取指定时间的数据
        start_time = datetime(2024, 11, 24,18,0,0)
        end_time = datetime(2024, 11, 25,16,0,0)
        data = self.reader.get_data_between_hours(start_time, end_time, inclusive='both')

        data.sort_values('time', ascending=True, inplace=True)
        # 判断起始时间相同
        self.assertTrue(data['time'].iloc[0] == start_time and data['time'].iloc[-1] == end_time)

    def test_get_time_range_data_over_24_hours(self):
        start_time = datetime(2024, 11, 23, 18, 0, 0)
        end_time = datetime(2024, 11, 25, 16, 0, 0)
        data = self.reader.get_data_between_hours(start_time, end_time, inclusive='both')

        data.sort_values('time', ascending=True, inplace=True)
        # 判断起始时间相同
        self.assertTrue(data['time'].iloc[0] == start_time and data['time'].iloc[-1] == end_time)

    def test_get_time_range_data_days(self):
        start_time = datetime(2024, 11, 18, 0, 0, 0)
        end_time = datetime(2024, 11, 25, 0, 0, 0)
        data = self.reader.get_data_between_days(start_time, end_time, inclusive='both')
        data.sort_values('time', ascending=True, inplace=True)
        self.assertTrue(data['time'].iloc[0] == start_time and data['time'].iloc[-1] == end_time)

    # 数据写入
    def test_data_write(self):
        self.writer.write_data(self.data, 'hour')
        time = datetime(2020, 1,1,0,0,0)
        end_time =datetime(2020, 1,1,0,3,0)
        data = self.reader.get_data_between_hours(time, end_time, inclusive='both')
        self.data['time'] = pd.to_datetime(self.data['time'])
        self.data = self.reader.change_column_type_to_Decimal(self.data.copy(), False)

        # 判断两个数据是否完全相等
        self.assertTrue(data.equals(self.data))

if __name__ == '__main__':
    output_dir = 'report'
    os.makedirs(output_dir, exist_ok=True)
    htmltestrunner = HTMLTestRunner(
        output=output_dir,
        report_name='test_result.html',
        report_title='test_result',
        combine_reports=True
    )
    suite = unittest.TestLoader().loadTestsFromTestCase(DataIOTest)
    htmltestrunner.run(suite)
