import unittest
import random
import pandas as pd
import numpy as np

from decimal import ROUND_DOWN
from data_process.data_process import DataProcess
from dataio.csv_handler import CSVReader
from decimal import Decimal


class DataProcessTest(unittest.TestCase):
    def setUp(self):
        data = {
            'coin_name': ['BTC', 'ETH', 'USDT', 'BTC', 'ETH', 'USDT'],
            'coin_price': [str(68218.00), str(2443.51), str(34.20), str(68218.00), str(2443.51), str(34.20)],
            'spider_web': ['binance', 'binance', 'binance', 'coin-stats', 'coin-stats', 'coin-stats'],
            'time': ['2021-05-05 09:58:00', '2021-05-05 09:58:00', '2021-05-05 09:58:00', '2021-05-05 09:58:00',
                     '2021-05-05 09:58:00', '2021-05-05 09:58:00']
        }
        data_df = pd.DataFrame(data)
        data_df['time'] = pd.to_datetime(data_df['time'])
        data_df = CSVReader.change_column_type_to_Decimal(data_df, True)
        # 批量生产数据
        previous_data = pd.concat([data_df] * 10, ignore_index=True)
        # 修改时间
        # 使用 for 循环调整时间，逐条减少每 10 条数据 1 小时
        time_decrease = pd.to_timedelta(0, unit='min')  # 初始时间差为0
        for idx in range(0, len(previous_data), 6):  # 每 6 条数据为一组
            previous_data.loc[idx:idx + 5, 'time'] -= time_decrease
            previous_data.loc[idx:idx + 5, 'coin_price'] += Decimal(idx)
            time_decrease += pd.to_timedelta(3, unit='min')  # 每组时间递减1小时

        filled_data = {
            'coin_name': ['BTC', 'ETH', 'USDT', 'BTC', 'ETH', 'USDT'],
            'coin_price': [np.nan, str(2443.51), str(34.20), str(68218.00), np.nan, str(34.20)],
            'spider_web': ['binance', 'binance', 'binance', 'coin-stats', 'coin-stats', 'coin-stats'],
            'time': ['2021-05-05 10:00:00', '2021-05-05 10:00:00', '2021-05-05 10:00:00', '2021-05-05 10:00:00',
                     '2021-05-05 10:00:00', '2021-05-05 10:00:00']
        }
        filled_data = pd.DataFrame(filled_data)
        filled_data['time'] = pd.to_datetime(filled_data['time'])
        filled_data = CSVReader.change_column_type_to_Decimal(filled_data, True)
        self.combined_data = pd.concat([previous_data, filled_data], ignore_index=True)
        self.dataprocess = DataProcess(filled_data, data_region='China', unit_time='hour')
        self.dataprocess.combined_data = self.combined_data

    def test_fill_na(self):
        self.dataprocess.fill_na()
        data = self.dataprocess.data
        # 检查是否存在缺失值
        res = data.isna().sum()

        self.assertEqual(res.sum(), 0)

    def test_get_columns(self):
        """生成最高、最低、开盘、收盘价"""

        self.dataprocess.get_price_columns()
        data = self.dataprocess.data
        total_columns = data.columns.tolist()
        columns_judgement = np.isin(total_columns,
                                    ['coin_name', 'coin_price', 'spider_web', 'time', 'high', 'low', 'open',
                                     'close']).all()
        # 判断binance中ETH的价格
        ETH_data = data.loc[(data['coin_name'] == 'ETH') & (data['spider_web'] == 'binance')]
        # 强制设置精度
        open = Decimal(2497.51).quantize(Decimal('0.01'), rounding=ROUND_DOWN)
        close = Decimal(2443.51).quantize(Decimal('0.01'), rounding=ROUND_DOWN)
        high = Decimal(2497.51).quantize(Decimal('0.01'), rounding=ROUND_DOWN)
        low = Decimal(2443.51).quantize(Decimal('0.01'), rounding=ROUND_DOWN)

        # 将 Decimal 对象转换为字符串进行比较
        ETH_open = str(ETH_data['open'].iloc[0])
        ETH_close = str(ETH_data['close'].iloc[0])
        ETH_high = str(ETH_data['high'].iloc[0])
        ETH_low = str(ETH_data['low'].iloc[0])

        open_str = str(open)
        close_str = str(close)
        high_str = str(high)
        low_str = str(low)

        # 比较字符串
        result = [
            (ETH_open == open_str),
            (ETH_close == close_str),
            (ETH_high == high_str),
            (ETH_low == low_str),
            columns_judgement
        ]

        print(result)
        self.assertTrue(all(result))


if __name__ == '__main__':
    unittest.main()
