import os
from decimal import Decimal
from wsgiref.handlers import format_date_time

import pandas as pd
from datetime import timedelta, datetime
from collections import Counter
from function_handler.functionhandler import FunctionHandler
from msg_log.mylog import get_logger
from dataio.csv_handler import CSVReader

PROJECT_ROOT_PATH = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

logger = get_logger(__name__, filename=os.path.join(PROJECT_ROOT_PATH, 'log', 'function_handler.log'))


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
                    'close_C': group.iloc[i]['close_C'],
                    'virtual_drop_A': group.iloc[i]['virtual_drop'],
                    'virtual_drop_B': group.iloc[i]['virtual_drop'],
                    'min_low_in_AB': min(group.iloc[i]['low'], group.iloc[j]['low'])
                })
                res_data = pd.DataFrame(result)
                return res_data
    res_data = pd.DataFrame(
        columns=['coin_name', 'spider_web', 'time_A', 'time_B', 'close_A', 'close_B', 'close_C', 'virtual_drop_A',
                 'virtual_drop_B', 'min_low_in_AB'])
    return res_data


class HourlyFunctionHandler(FunctionHandler):
    """处理以小时为单位的各种函数"""

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def update_func_1_cnt(self, original_data: pd.DataFrame, additional_data: pd.DataFrame):
        """更新函数1的记录数据并返回,每隔24小时清空"""
        count_file_path = os.path.join(PROJECT_ROOT_PATH, 'function_handler', 'record_data',
                                       'hour_func_1_coin_count.csv')
        # 统计出现次数
        try:
            count_data = pd.read_csv(count_file_path, encoding='utf-8')
            count_data['time'] = pd.to_datetime(count_data['time'])
        except FileNotFoundError:
            count_data = pd.DataFrame(columns=['coin_name', 'cnt', 'time'])
        # 当前统计
        total_coin_names = original_data['coin_name'].tolist() + additional_data['coin_name'].tolist()
        new_counts = Counter(total_coin_names)
        new_count_data = pd.DataFrame(new_counts.items(), columns=['coin_name', 'cnt'])
        new_count_data['time'] = self.datetime

        # 将新统计数据与已有统计数据合并，如果币种已经存在，累加出现次数
        merged_data = count_data.merge(new_count_data, on='coin_name', how='outer', suffixes=('', '_new'))

        merged_data['time'] = merged_data['time'].fillna(self.datetime)
        merged_data['time_new'] = merged_data['time_new'].fillna(merged_data['time'])
        merged_data['cnt'] = merged_data['cnt'].fillna(0).astype(int)
        merged_data['cnt_new'] = merged_data['cnt_new'].fillna(0).astype(int)

        # 超过24小时全部cnt更新为0
        merged_data  = merged_data.apply(
            lambda x: x['cnt_new'] if ((self.datetime - x['time']).total_seconds() / 3600) > 24
            else x['cnt'] + x['cnt_new'],
            axis=1
        )
        merged_data = merged_data.apply(
            lambda x: self.datetime if ((self.datetime - x['time']).total_seconds() / 3600) > 24 else x['time'],
            axis=1
        )

        merged_data.to_csv(count_file_path, encoding='utf-8', index=False, date_format='%Y-%m-%d %H:%M:%S')

        original_data = original_data.merge(merged_data[['coin_name', 'cnt']], on='coin_name', how='left')
        # 如果cnt为0则更新为1
        original_data['cnt'] = original_data['cnt'].apply(lambda x: 1 if x == 0 else x)
        additional_data = additional_data.merge(merged_data[['coin_name', 'cnt']], on='coin_name', how='left')
        additional_data['cnt'] = additional_data['cnt'].apply(lambda x: 1 if x == 0 else x)

        return original_data[['coin_name', 'virtual_drop_A', 'virtual_drop_B', 'cnt']], additional_data[
            ['coin_name', 'virtual_drop_A', 'virtual_drop_B', 'cnt']]

    def change_and_virtual_drop_and_price_func_1(self):
        """
        有ABC三个时刻。其中C为当前时刻
	    C时刻在跌 C_CHANGE < 0
	    A时刻与C时刻不超过MAX_TIME_INTERVAL小时。
	    A时刻在B时刻之前，A和B时刻均要满足虚降>=AB_VIRTUAL_DROP%,且跌涨幅<=AB_CHANGE%
	    C时刻收盘价同时小于A和B的最低价。
	    A时刻收盘价大于B时刻收盘价。

	    **新增条件**
        C收盘价小于等于A最低价和B最低价中最小值的(ADDITIONAL_PRICE_PERCENT)%（此条件单独记录）
        :return:
        """
        logger.info('开始执行函数：change_and_virtual_drop_and_price_func_1')
        MAX_TIME_INTERVAL = 24
        C_CHANGE = 0
        AB_CHANGE = 1
        AB_VIRTUAL_DROP = 0.9
        ADDITIONAL_PRICE_PERCENT = '0.96'

        res_original_str = None
        res_additional_str = None
        cur_func_result = []

        data_at_C = self.data.copy()
        change_data_at_C = self.filter_by_change_rate(data_at_C, 'lt', C_CHANGE)

        data_A_to_B = self.range_data
        datetime_A = self.datetime - timedelta(hours=MAX_TIME_INTERVAL)

        data_A_to_B = data_A_to_B[data_A_to_B['time'].between(datetime_A, self.datetime, inclusive='left')]
        data_A_to_B = data_A_to_B[data_A_to_B['coin_name'].isin(change_data_at_C['coin_name'])]
        data_A_to_B_sort_by_time_asc = data_A_to_B.sort_values('time', ascending=True)

        change_data_A_to_B = self.filter_by_change_rate(data_A_to_B_sort_by_time_asc, 'le', AB_CHANGE)
        virtual_drop_and_change_data_A_to_B = self.filter_by_virtual_drop(change_data_A_to_B, 'ge',
                                                                          AB_VIRTUAL_DROP).copy()

        merged_ABC_data = virtual_drop_and_change_data_A_to_B.merge(
            change_data_at_C[['coin_name', 'spider_web', 'close']],
            on=['coin_name', 'spider_web'], how='right', suffixes=('', '_C')).dropna()

        condition_C_close_less_AB_min = (merged_ABC_data['close_C'] <= merged_ABC_data['low'])
        conform_C_close_AB_min = merged_ABC_data[condition_C_close_less_AB_min]

        group_by_coin_and_spiderweb = conform_C_close_AB_min.groupby(['coin_name', 'spider_web'])

        A_close_gt_B_close_data = group_by_coin_and_spiderweb.apply(
            find_A_close_gt_B_close_included_C_close).dropna(axis=1, how='all').reset_index(drop=True)
        if A_close_gt_B_close_data.empty:
            logger.warning('无符合条件的数据')
            A_close_gt_B_close_data = pd.DataFrame(
                columns=['coin_name', 'spider_web', 'time_A', 'time_B', 'close_A', 'close_B', 'close_C',
                         'virtual_drop_A',
                         'virtual_drop_B', 'min_low_in_AB'])
        A_close_gt_B_close_data = A_close_gt_B_close_data.sort_values(['spider_web', 'coin_name', 'time_A'],
                                                                      ascending=[True, True, True])

        # 新增条件  C收盘价小于等于A最低价和B最低价中最小值的98%（此条件单独记录）
        C_close_le_minlow_in_AB_condition = A_close_gt_B_close_data['close_C'] <= (
                A_close_gt_B_close_data['min_low_in_AB'] * Decimal(ADDITIONAL_PRICE_PERCENT))

        C_close_le_minlow_in_AB_data = A_close_gt_B_close_data[C_close_le_minlow_in_AB_condition]

        res_original_condition = A_close_gt_B_close_data[
            ['coin_name', 'spider_web', 'virtual_drop_A', 'virtual_drop_B']].drop_duplicates('coin_name', keep='first')

        res_additional_condition = C_close_le_minlow_in_AB_data[
            ['coin_name', 'spider_web', 'virtual_drop_A', 'virtual_drop_B']].drop_duplicates('coin_name', keep='first')

        update_cnt_original_data, update_additional_data = self.update_func_1_cnt(original_data=res_original_condition,
                                                                                  additional_data=res_additional_condition)

        if not update_cnt_original_data.empty:
            logger.info('\n' + A_close_gt_B_close_data.to_string(index=False) + '\n')

            res_original_str = update_cnt_original_data.to_string(index=False)
            cur_func_result.append(res_original_str)

        if not update_additional_data.empty:
            logger.info('--C收盘价<=A最低价和B最低价中最小值的96%--')
            logger.info(C_close_le_minlow_in_AB_data.to_string(index=False))

            res_additional_str = update_additional_data.to_string(index=False)
            cur_func_result.append("--C收盘价<=A最低价和B最低价中最小值的96%--")
            cur_func_result.append(res_additional_str)

        func_desc = (
            f"[函数1]{len(update_cnt_original_data['coin_name']) + len(update_additional_data['coin_name'])}只股票异常有ABC三个时刻。其中C为当前时刻,C时刻在跌。"
            f"A时刻与C时刻不超过{MAX_TIME_INTERVAL}小时。A时刻在B时刻之前，A和B时刻均要满足虚降>={AB_VIRTUAL_DROP}%且跌涨幅<={AB_CHANGE}%，A时刻收盘价大于B时刻收盘价。"
            "C时刻收盘价同时小于A和B的最低价。")

        if cur_func_result:
            cur_func_result.insert(0, func_desc)
            cur_func_result_str = '\n'.join(cur_func_result)
            # self.results.append(cur_func_result_str)
            return cur_func_result_str

        logger.info(f'函数1执行完毕')


if __name__ == "__main__":
    csv_reader = CSVReader(data_region='China')

    data = pd.read_csv(r"D:\PythonCode\virtual_currency-3.0\aaa.csv")
    data = csv_reader.change_data_type(data, only_price=False)

    hourly_function_hander = HourlyFunctionHandler(reader=csv_reader, datetime=datetime(2024,11,9,0,0,0), data=data)

    hourly_function_hander.get_range_data_hours(datetime(2024,11,8,10,0,0), datetime(2024,11,9,0,0,0), inclusive='left')
    hourly_function_hander.change_and_virtual_drop_and_price_func_1()
