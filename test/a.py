import os
from datetime import datetime, timedelta
from pprint import pprint
from typing import Literal
import pandas as pd
import numpy as np
import random
from decimal import Decimal
from xml.etree import ElementTree as ET
from collections import defaultdict
import csv_handler
from csv_handler import CSVReader
from functionhandler import FunctionHandler

PROJECT_ROOT_PATH = os.path.dirname(os.path.abspath(__file__))
from controller import ProgramCotroller


def filter_A_and_B_price_column(group: pd.DataFrame, filter_column: str,
                                comparison: Literal['gt', 'lt', 'ge', 'le', 'eq', 'neq']):
    """对两个时刻的某一个价格字段进行大小比较的筛选"""

    # 升序排列，A时刻在前，B时刻在后
    group.sort_values(by='time', ascending=True, inplace=True)
    total_price_list = group[filter_column].tolist()
    print(group)
    length = len(group)
    merged_results = []
    for i in range(length):
        for j in range(i + 1, length):
            A_price = total_price_list[i]
            B_price = total_price_list[j]
            A_row = group.iloc[i]
            B_row = group.iloc[j]

            if comparison == 'gt' and A_price > B_price:
                merged_results.append((A_row, B_row))
            elif comparison == 'lt' and A_price < B_price:
                merged_results.append((A_row, B_row))
            elif comparison == 'ge' and A_price >= B_price:
                merged_results.append((A_row, B_row))
            elif comparison == 'le' and A_price <= B_price:
                merged_results.append((A_row, B_row))
            elif comparison == 'eq' and A_price == B_price:
                merged_results.append((A_row, B_row))
            elif comparison == 'neq' and A_price != B_price:
                merged_results.append((A_row, B_row))

    # # 生成数据
    # for A_row, B_row in merged_results:
    #     A_df = pd.DataFrame([A_row])
    #     B_df = pd.DataFrame([B_row])
    #     pass


def find_A_close_gt_B_close_included_C_close(group):
    """寻找AB时刻符合A收盘价大于B收盘价关系的数据"""
    coin_name, spider_web = group.name
    group.sort_values(by='time', ascending=True, inplace=True)
    close_price = group['close'].tolist()
    result = []
    for i in range(len(close_price) - 1):
        for j in range(i + 1, len(close_price)):
            if close_price[i] > close_price[j]:
                result.append({
                    'coin_name': coin_name,
                    'spider_web': spider_web,
                    'time_A': group.iloc[i]['time'],
                    'time_B': group.iloc[j]['time'],
                    'close_A': group.iloc[i]['close'],
                    'close_B': group.iloc[j]['close'],
                    'virtual_drop_A': group.iloc[i]['virtual_drop'],
                    'virtual_drop_B': group.iloc[i]['virtual_drop'],
                    'min_low_in_AB': min(group.iloc[i]['low'], group.iloc[j]['low'])
                })
                res_data = pd.DataFrame(result)
                return res_data
    res_data = pd.DataFrame(
        columns=['coin_name', 'spider_web', 'time_A', 'time_B', 'close_A', 'close_B', 'virtual_drop_A',
                 'virtual_drop_B', 'min_low_in_AB'])
    return res_data
if __name__ == "__main__":
    # data = pd.read_csv(r'D:\PythonCode\virtual_currency-3.0\data\China\2024-11\8.csv', encoding='utf-8')
    # data = csv_handler.CSVReader.change_data_type(data, only_price=False)
    # group_by = data.groupby(['coin_name', 'spider_web'])
    # start_time = datetime.now()
    # # res_data = group_by.apply(lambda group:filter_A_and_B_price_column(group, 'close', 'gt'), include_groups=False)
    # res_data = group_by.apply(find_A_close_gt_B_close_included_C_close)
    # # reset_index_data = res_data.reset_index(drop=False)
    # end_time = datetime.now()
    # print(end_time - start_time)
    # pass
    # comparison_matrix = np.tril(np.ones((4, 4), dtype=bool), k=-1)
    # print(comparison_matrix)
    # data = {
    #     'coin_name': ['BTC', 'ETH', 'USDT'],
    #     'coin_price': ['0', '678.90', '12.34'],
    #     'spider_web': ['binance', 'binance', 'binance'],
    #     'time': ['2024-11-04 01:00:00', '2024-11-04 01:00:00', '2024-11-04 01:00:00']
    # }
    # data = pd.DataFrame(data)
    # data = pd.DataFrame(columns=data.columns)
    # print(data)
    # # data = CSVReader.change_column_type_to_Decimal(data, True)
    # # eq_0 = data[data['coin_price'] != 0]
    # # 计数
    # time_count = data['time'].drop_duplicates().count()
    # print(time_count)
    # # print(eq_0)
    import time
    a = "欢迎高育良莅临宜宾指导工作"
    while True:
        a = a[1:] + a[0]
        print(a)
        # 清空控制台
        os.system('cls')
        time.sleep(0.1)

