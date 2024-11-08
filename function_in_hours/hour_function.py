import os
from decimal import Decimal
import pandas as pd
from numpy.core.records import record
from numpy.f2py.cfuncs import includes
from pandas.core.apply import include_axis

from msg_log.mylog import get_logger
from typing import Literal
from datetime import datetime, timedelta
from dataio.csv_handler import CSVReader, CSVWriter

PROJECT_ROOT_PATH = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

logger = get_logger(__name__, filename=os.path.join(PROJECT_ROOT_PATH, 'log', 'function_handler.log'))


def find_A_close_gt_B_close_included_C_close(group):
    """寻找AB时刻符合A收盘价大于B收盘价关系的数据"""
    coin_name, spider_web = group.name
    close_price = group['close'].tolist()
    result = []
    for i in range(len(close_price) - 1):
        for j in range(i + 1, len(close_price)):
            if close_price[i] > close_price[j]:
                result.append({
                    'coin_name': coin_name,
                    'spider_web': spider_web,
                    'time_A': group.iloc[i]['time'][8:],
                    'time_B': group.iloc[j]['time'][8:],
                    'close_A': group.iloc[i]['close'],
                    'close_B': group.iloc[j]['close'],
                    'close_C': group.iloc[i]['close_C'],
                    'min_low_in_AB': min(group.iloc[i]['low'], group.iloc[j]['low'])
                })
                res_data = pd.DataFrame(result)
                return res_data
    res_data = pd.DataFrame(
        columns=['coin_name', 'spider_web', 'time_A', 'time_B', 'close_A', 'close_B', 'close_C', 'min_low_in_AB'])
    return res_data


def find_A_close_gt_B_close_Exclusive_of_C_close(group):
    """寻找AB时刻符合A收盘价大于B收盘价关系的数据"""
    coin_name, spider_web = group.name
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
                    'coin_price': group.iloc[j]['coin_price_C']
                })
                res_data = pd.DataFrame(result)
                return res_data
    res_data = pd.DataFrame(
        columns=['coin_name', 'spider_web', 'time_A', 'time_B'])
    return res_data


class FunctionHandler:
    def __init__(self, **kwargs):
        self.range_data_day = None
        self.range_data = None
        self.functions = []
        self.results = []
        self.datetime = datetime.now().replace(minute=0, second=0)
        self.time = kwargs.get('time', self.datetime.strftime('%Y-%m-%d %H:%M:%S'))
        self.data = kwargs.get('data', None)
        self.csv_reader = kwargs.get('reader', CSVReader("China"))
        self.csv_writer = kwargs.get('writer', CSVWriter("China"))

    def get_range_data_hours(self, start_time: str, end_time: str,
                             inclusive: Literal['both', 'neither', 'left', 'right'] = 'both'):
        self.range_data = self.csv_reader.get_data_between_hours(start_time, end_time, inclusive)

    def get_range_data_days(self, start_time: str, end_time: str,
                            inclusive: Literal['both', 'neither', 'left', 'right'] = 'both'):
        self.range_data_day = self.csv_reader.get_data_between_days(start_time, end_time, inclusive)

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

    def change_and_virtual_drop_and_price_func_1(self):
        """
        有ABC三个时刻。其中C为当前时刻
	    C时刻在跌 C_CHANGE < 0
	    A时刻与C时刻不超过MAX_TIME_INTERVAL小时。
	    A时刻在B时刻之前，A和B时刻均要满足虚降>=AB_VIRTUAL_DROP%,且跌涨幅<=AB_CHANGE%
	    C时刻收盘价同时小于A和B的最低价。
	    A时刻收盘价大于B时刻收盘价。

	    **新增条件**
        C收盘价小于等于A最低价和B最低价中最小值的98%(ADDITIONAL_PRICE_PERCENT)（此条件单独记录）
        :return:
        """
        logger.info('开始执行函数：change_and_virtual_drop_and_price_func_1')

        MAX_TIME_INTERVAL = 24
        C_CHANGE = 0
        AB_CHANGE = 1
        AB_VIRTUAL_DROP = 0.9
        ADDITIONAL_PRICE_PERCENT = '0.98'

        res_original_str = None
        res_additional_str = None
        cur_func_result = []

        data_at_C = self.data.copy()
        change_data_at_C = self.filter_by_change_rate(data_at_C, 'lt', C_CHANGE)

        data_A_to_B = self.range_data
        # self.datetime = datetime(2024,11,6, 10, 0, 0)

        datetime_A = self.datetime - timedelta(hours=MAX_TIME_INTERVAL)
        timestr_A = datetime_A.strftime('%Y-%m-%d %H:%M:%S')
        data_A_to_B = data_A_to_B[data_A_to_B['time'].between(timestr_A, self.time, inclusive='left')]
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

        A_close_gt_B_close_data = group_by_coin_and_spiderweb.apply(find_A_close_gt_B_close_included_C_close,
                                                                    include_groups=False).reset_index(drop=True)
        if A_close_gt_B_close_data.empty:
            logger.warning('无符合条件的数据')
            A_close_gt_B_close_data = pd.DataFrame(
                columns=['coin_name', 'spider_web', 'time_A', 'time_B', 'close_A', 'close_B', 'close_C',
                         'min_low_in_AB'])
        A_close_gt_B_close_data = A_close_gt_B_close_data.sort_values(['spider_web', 'coin_name', 'time_A'],
                                                                      ascending=[True, True, True])
        # 新增条件  C收盘价小于等于A最低价和B最低价中最小值的98%（此条件单独记录）
        C_close_le_minlow_in_AB_condition = A_close_gt_B_close_data['close_C'] <= (
                A_close_gt_B_close_data['min_low_in_AB'] * Decimal(ADDITIONAL_PRICE_PERCENT))

        C_close_le_minlow_in_AB_data = A_close_gt_B_close_data[C_close_le_minlow_in_AB_condition]

        res_original_condition = A_close_gt_B_close_data[['coin_name', 'spider_web', 'time_A', 'time_B']]

        res_additional_condition = C_close_le_minlow_in_AB_data[['coin_name', 'spider_web', 'time_A', 'time_B']]

        if not res_original_condition.empty:
            res_original_str = res_original_condition.to_string(index=False)
            cur_func_result.append(res_original_str)
        if not res_additional_condition.empty:
            res_additional_str = res_additional_condition.to_string(index=False)
            cur_func_result.append("--C收盘价<=A最低价和B最低价中最小值的98%--")
            cur_func_result.append(res_additional_str)

        func_desc = (
            f"[函数1]{len(A_close_gt_B_close_data['coin_name']) + len(C_close_le_minlow_in_AB_data['coin_name'])}只股票异常有ABC三个时刻。其中C为当前时刻,C时刻在跌。"
            f"A时刻与C时刻不超过{MAX_TIME_INTERVAL}小时。A时刻在B时刻之前，A和B时刻均要满足虚降>={AB_VIRTUAL_DROP}%且跌涨幅<={AB_CHANGE}%，A时刻收盘价大于B时刻收盘价。"
            "C时刻收盘价同时小于A和B的最低价。")

        if cur_func_result:
            cur_func_result.insert(0, func_desc)
            logger.info(f'函数1执行完毕，结果如下：\n')
            cur_func_result_str = '\n'.join(cur_func_result)
            logger.info(f'{cur_func_result_str}\n')
            # self.results.append(cur_func_result_str)
            return cur_func_result_str
        else:
            logger.info(f'函数1执行完毕，无异常')


class MinuteFunctionHandler(FunctionHandler):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.record_data_floder_path = os.path.join(PROJECT_ROOT_PATH, 'function_in_hours', 'record_data')
        os.makedirs(self.record_data_floder_path, exist_ok=True)

    def filter_and_update_data(self, to_be_filter_data: pd.DataFrame, record_data: pd.DataFrame, MAX_TIME_INTERVAL):
        """将发送次数限制在3次以内"""
        RECORD_COUNT = 3
        # 两个数据的列的差异
        to_be_filter_data_columns = to_be_filter_data.columns
        record_data_columns = record_data.columns
        diff_columns = list(set(record_data_columns) - set(to_be_filter_data_columns))

        # 统计出包含“time”字眼的列
        time_columns = [col for col in record_data_columns if 'time' in col]
        # merge时作为主键的列
        primary_key_columns = ['coin_name', 'spider_web'] + time_columns

        merged_data = record_data.merge(to_be_filter_data, on=primary_key_columns, how='outer',
                                        suffixes=('', '_filter'))

        if 'cnt' in diff_columns:
            merged_data['cnt'] = merged_data['cnt'].fillna(0).infer_objects().astype(int)
            has_new_data = merged_data['coin_price'].notna()
            merged_data.loc[has_new_data, 'cnt'] += 1

        if 'first_price' in diff_columns:
            merged_data['first_price'] = merged_data['first_price'].fillna(merged_data['coin_price'])

        # 对时间列的名称进行升序排序，找出第一个时间列的名称
        first_time_col = sorted(time_columns)[0]

        start_time = (self.datetime - timedelta(hours=MAX_TIME_INTERVAL)).strftime("%Y-%m-%d %H:%M:%S")

        update_data = merged_data[merged_data[first_time_col].between(start_time, self.time, 'both')]

        # 根据coin_name和spider_web筛选出to_be_filter_data数据
        coin_spider_data = to_be_filter_data[primary_key_columns]
        filter_data = update_data.merge(coin_spider_data, on=primary_key_columns, how='inner')
        filter_condition = ((filter_data['cnt'] == 1) | (
                filter_data['cnt'] <= 3 & (filter_data['coin_price'] < filter_data['first_price'])))
        filter_data = filter_data[filter_condition]
        filter_data = filter_data[primary_key_columns]
        # 去除带 _filter 后缀的临时列
        filter_data = filter_data.loc[:, ~filter_data.columns.str.endswith('_filter')]
        update_data = update_data.loc[:, ~update_data.columns.str.endswith('_filter')]
        update_data.drop(columns='coin_price', inplace=True)
        return filter_data, update_data

    def change_and_virtual_drop_and_price_func_1_minute(self):
        """
        有ABC三个时刻。其中C为当前时刻（当前分钟）
	    A时刻与C时刻不超过MAX_TIME_INTERVAL小时。
	    A时刻在B时刻之前，A和B时刻均要满足虚降>=AB_VIRTUAL_DROP%,且跌涨幅<=AB_CHANGE%
	    C时刻收盘价同时小于A和B的最低价。
	    A时刻收盘价大于B时刻收盘价。
	    后面两次的价格必须小于第一次异常的价格才记录
	    对于同一时刻的异常股票最多只能提示三次（发送邮件）

        :return:
        """
        logger.info('开始执行每分钟函数1：change_and_virtual_drop_and_price_func_1_minute')
        RECORD_DATA_PATH = os.path.join(self.record_data_floder_path, 'func_1.csv')
        MAX_TIME_INTERVAL = 24
        AB_CHANGE = 1
        AB_VIRTUAL_DROP = 1

        C_data = self.data.copy()

        range_data = self.range_data
        # self.datetime = datetime(2024,11,6, 10, 0, 0)
        datetime_A = self.datetime - timedelta(hours=MAX_TIME_INTERVAL)
        timestr_A = datetime_A.strftime('%Y-%m-%d %H:%M:%S')
        data_A_to_B = range_data[range_data['time'].between(timestr_A, self.time, inclusive='both')]

        # AB跌涨幅筛选
        change_A_to_B_data = self.filter_by_change_rate(data_A_to_B, 'le', AB_CHANGE)
        # AB虚降筛选
        change_and_virtual_drop_data_A_to_B = self.filter_by_virtual_drop(change_A_to_B_data, 'ge',
                                                                          AB_VIRTUAL_DROP).copy()


        # AB收盘价与C价格对比
        merged_ABC_data = change_and_virtual_drop_data_A_to_B.merge(C_data[['coin_name', 'spider_web', 'coin_price']],
                                                                    on=['coin_name', 'spider_web'], how='right',
                                                                    suffixes=('', '_C')).dropna()

        AB_low_gt_C_price_condition = (merged_ABC_data['low'] > merged_ABC_data['coin_price_C'])

        AB_low_gt_C_price_data = merged_ABC_data[AB_low_gt_C_price_condition]

        # A收盘价大于B收盘价
        AB_low_gt_C_price_data_sort_by_time = AB_low_gt_C_price_data.sort_values(['spider_web', 'coin_name', 'time'],
                                                                                 ascending=[True, True, True])

        group_by_coin_and_spiderweb = AB_low_gt_C_price_data_sort_by_time.groupby(['coin_name', 'spider_web'])

        A_close_gt_B_close_data = group_by_coin_and_spiderweb.apply(find_A_close_gt_B_close_Exclusive_of_C_close,
                                                                    include_groups=False).reset_index(drop=True)
        if A_close_gt_B_close_data.empty:
            logger.warning('无符合条件的数据')
            A_close_gt_B_close_data = pd.DataFrame(
                columns=['coin_name', 'spider_web', 'time_A', 'time_B', 'coin_price'])

        # 更新记录数据
        try:
            record_data = pd.read_csv(RECORD_DATA_PATH, encoding='utf-8')
        except FileNotFoundError:
            record_data = pd.DataFrame(columns=['coin_name', 'spider_web', 'time_A', 'time_B', 'first_price', 'cnt'])

        filter_data, record_data = self.filter_and_update_data(A_close_gt_B_close_data, record_data, MAX_TIME_INTERVAL)

        if not record_data.empty:
            record_data.to_csv(RECORD_DATA_PATH, index=False, encoding='utf-8')
        cur_res_list = []
        if not filter_data.empty:
            filter_data.sort_values(['coin_name', 'spider_web'], ascending=[True, True], inplace=True)
            func_desc = f"[分钟函数1]{len(filter_data['coin_name'])}只股票异常有ABC三个时刻。其中C为当前时刻。"
            f"A时刻与C时刻不超过24小时。A时刻在B时刻之前，A和B时刻均要满足虚降>={AB_VIRTUAL_DROP}%且跌涨幅<={AB_CHANGE}%，A时刻收盘价大于B时刻收盘价。"
            "C时刻价格同时小于A和B的最低价，后面两次异常价格低于第一次异常时价格。"
            cur_res_list.append(func_desc)
            cur_res_list.append(filter_data.to_string(index=False))
            cur_res_str = '\n'.join(cur_res_list)
            logger.info('每分钟函数1执行完毕，结果如下' + '\n' + cur_res_str + '\n')
            return cur_res_str
        else:
            logger.info('每分钟函数1执行完毕，无异常')


class DayFunctionHandler(FunctionHandler):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def continous_change_drop_func_1(self):
        """
        当前状态在跌
        当前虚降大于等于5%
        （1）前三天均为跌，前三天的跌幅之和加上当前跌幅小于等于20%
        （2）前四天均为跌，前四天跌幅之和加上当前跌幅小于等于20%
        (1)和(2)只需要满足其中一个即可
        :return:
        """
        CUR_CHANGE = 0
        VIRTUAL_DROP = 5
        CHANGE_SUM = 20

        logger.info('开始执行日函数1：continous_change_drop_func_1')

        data_A = self.data[['coin_name', 'spider_web', 'change', 'virtual_drop', 'coin_price', 'time']].copy()

        # A在跌
        decline_data_A = self.filter_by_change_rate(data_A, 'lt', CUR_CHANGE)
        # A虚降大于等于5%
        decline_and_virtual_drop_data_A = self.filter_by_virtual_drop(decline_data_A, 'ge', VIRTUAL_DROP)

        fourth_day_ahead_datetime = self.datetime - timedelta(days=4)
        fourth_day_ahead_timestr = fourth_day_ahead_datetime.strftime('%Y-%m-%d %H:%M:%S')

        third_day_ahead_datetime = self.datetime - timedelta(days=3)
        third_day_ahead_timestr = third_day_ahead_datetime.strftime('%Y-%m-%d %H:%M:%S')
        # 前四天数据
        fourth_day_ahead_data = self.range_data_day[
            ['coin_name', 'spider_web', 'change', 'time']]
        fourth_day_ahead_data = fourth_day_ahead_data[
            fourth_day_ahead_data['time'].between(fourth_day_ahead_timestr, self.time, inclusive='left')].copy()

        decline_fourth_day_ahead_data = self.filter_by_change_rate(fourth_day_ahead_data, 'lt', CUR_CHANGE).copy()

        # 前三天数据
        third_day_ahead_data = fourth_day_ahead_data[
            fourth_day_ahead_data['time'].between(third_day_ahead_timestr, self.time, inclusive='both')]
        decline_third_day_ahead_data = self.filter_by_change_rate(third_day_ahead_data, 'lt', CUR_CHANGE).copy()

        # 合并
        fourth_day_and_A_data = pd.concat([decline_fourth_day_ahead_data, decline_and_virtual_drop_data_A[['coin_name', 'spider_web', 'time', 'change']]], axis=0, ignore_index=True)
        third_day_and_A_data = pd.concat([decline_and_virtual_drop_data_A[['coin_name', 'spider_web', 'time', 'change']], decline_third_day_ahead_data], axis=0, ignore_index=True)

        # 筛选出只存在于A时刻的币种网站组合
        fourth_day_and_A_data = fourth_day_ahead_data[fourth_day_ahead_data[['coin_name', 'spider_web']].isin(
            decline_and_virtual_drop_data_A[['coin_name', 'spider_web']].drop_duplicates())]

        third_day_and_A_data = third_day_and_A_data[third_day_and_A_data[['coin_name', 'spider_web']].isin(
            decline_and_virtual_drop_data_A[['coin_name', 'spider_web']].drop_duplicates())]

        # 分组
        fourth_group_by_coin_and_spiderweb = fourth_day_and_A_data.groupby(['coin_name', 'spider_web'])
        third_group_by_coin_and_spiderweb = third_day_and_A_data.groupby(['coin_name', 'spider_web'])

        # 计算跌幅之和
        fourth_day_change_sum = fourth_group_by_coin_and_spiderweb['change'].sum().reset_index()
        third_day_change_sum = third_group_by_coin_and_spiderweb['change'].sum().reset_index()

        # 跌幅大于等于20%
        decline_fourth_day = fourth_day_change_sum[fourth_day_change_sum['change'] <= -CHANGE_SUM]
        decline_third_day = third_day_change_sum[third_day_change_sum['change'] <= -CHANGE_SUM]

        # 找出在前四天和前三天均满足跌幅大于等于 20% 的币种
        combined_data = pd.concat([decline_fourth_day, decline_third_day], axis=0, ignore_index=True)
        combined_data.drop_duplicates(subset=['coin_name', 'spider_web'], inplace=True)

        if not combined_data.empty:
            func_desc = (f"[天]函数1{len(combined_data['coin_name'])}"
                         f"当前状态在跌,当前虚降大于等于{VIRTUAL_DROP}%."
                         f"（1）前三天均为跌，前三天的跌幅之和加上当前跌幅小于等于{CHANGE_SUM}%;"
                         f"（2）前四天均为跌，前四天跌幅之和加上当前跌幅小于等于{CHANGE_SUM}%。"
                         f"(1)和(2)只需要满足其中一个即可")
            res_str = combined_data[['coin_name', 'spider_web']].to_string(index=True)
            logger.info(func_desc + '\n' + res_str + '\n')
            return res_str
        else:
            logger.info('日函数1无异常')

if __name__ == "__main__":
    csv_reader = CSVReader(data_region='China')

    data = pd.read_csv(r"D:\PythonCode\virtual_currency-3.0\test.csv")
    data = csv_reader.change_data_type(data, only_price=False)

    # hourly_function_hander = HourlyFunctionHandler(reader=csv_reader, time='2024-11-06 10:00:00', data=data)

    # hourly_function_hander.get_range_data_hours("2024-11-05 10:00:00", "2024-11-06 10:00:00", inclusive='left')
    # hourly_function_hander.change_and_virtual_drop_and_price_func_1()

    # minute_function_handler = MinuteFunctionHandler(reader=csv_reader, time='2024-11-06 10:00:00', data=data)
    # minute_function_handler.get_range_data_hours("2024-11-05 10:00:00", "2024-11-06 10:00:00", inclusive='left')
    # minute_function_handler.change_and_virtual_drop_and_price_func_1_minute()

    dayfunctionhandler = DayFunctionHandler(reader=csv_reader, time='2024-11-06 10:00:00', data=data)
    dayfunctionhandler.get_range_data_days("2024-11-05 00:00:00", "2024-11-06 00:00:00", inclusive='left')
    dayfunctionhandler.continous_change_drop_func_1()
