import collections


from msg_log.msg_send import send_email

import numpy as np
import os
import pandas as pd
from datetime import datetime, timedelta
from decimal import Decimal
from typing import Literal
from collections import defaultdict


from dataio.csv_handler import CSVReader, CSVWriter
from function_handler.functionhandler import FunctionHandler
from msg_log.mylog import get_logger

PROJECT_ROOT_PATH = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

logger = get_logger(__name__, filename=os.path.join(PROJECT_ROOT_PATH, 'log', 'new_minute_function_handler.log'))


class NewMinuteFunctionHandler(FunctionHandler):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    @staticmethod
    def synchronous_data(template_data, *args):
        """
        以列表中的第一个数据为模板，使得列表中其他数据的coin_name和spider_web均和模板数据同步
        :param template_data:
        :return:
        """
        results = []
        for data in args:
            results.append(
                (data.merge(template_data[['coin_name', 'spider_web']].drop_duplicates(),
                            on=['coin_name', 'spider_web'], how='inner')))
        # 解包列表
        if len(results) == 1:
            results = results[0]
        return results

    @staticmethod
    def filter_and_update_func_1_data(abnormal_data: pd.DataFrame, record_data: pd.DataFrame):
        """
        后面价格低于第一次价格
        record_data:columns:coin_name, spider_web, first_price
        """

        if abnormal_data.empty:
            return abnormal_data, record_data

        merged_data = abnormal_data.merge(record_data, on=['coin_name', 'spider_web'], how='outer')

        # 文件与数据中均存在, first_price不为空， coin_price不为空
        both_exist_abnormal_data = merged_data[
            (merged_data['coin_price_C'].notna()) & (merged_data['first_price'].notna())].copy()

        # 数据中存在，文件中不存在 first_price为空， coin_price不为空
        only_exist_abnormal_data = merged_data[merged_data['first_price'].isna()].copy()
        only_exist_abnormal_data['first_price'] = merged_data['coin_price_C']

        # 数据中不存在，文件中存在，first_price不为空，coin_price为空
        only_exist_file_data = merged_data[merged_data['coin_price_C'].isna()].copy()

        total_abnormal_data = pd.concat([both_exist_abnormal_data, only_exist_abnormal_data], ignore_index=True)
        condition = total_abnormal_data['coin_price_C'] <= total_abnormal_data['first_price']
        total_abnormal_data = total_abnormal_data[condition].copy()
        total_abnormal_data.drop(columns=['first_price'], inplace=True)

        to_be_write_data = pd.concat([both_exist_abnormal_data, only_exist_abnormal_data, only_exist_file_data],
                                     ignore_index=True)
        to_be_write_data = to_be_write_data[['coin_name', 'spider_web', 'first_price']].copy()
        to_be_write_data.drop_duplicates(inplace=True)
        return total_abnormal_data, to_be_write_data

    @staticmethod
    def filter_cnt_lt_3_on_condition_1(abnormal_data: pd.DataFrame, record_data: pd.DataFrame):
        """
        基础函数1的附加条件1进行筛选，将异常币种的出现次数限制在3次以内
        columns=['coin_name' 'spider_web', 'cnt', 'lasted_price']
        """
        record_cnt = 3
        # 拼接
        merged_data = abnormal_data.merge(record_data, on=['coin_name', 'spider_web'], how='outer')
        merged_data.drop_duplicates(subset=['coin_name', 'spider_web'], inplace=True)
        # 数据中有，文件中没有——cnt为空,coin_price不为空
        only_exist_abnormal_data = merged_data[merged_data['cnt'].isna()].copy()
        only_exist_abnormal_data['cnt'] = 1
        only_exist_abnormal_data['lasted_price'] = only_exist_abnormal_data['coin_price_C']

        # 文件中有，数据中也有——cnt不为空, coin_price也不为空
        both_exist_abnormal_and_file_data = merged_data[
            (merged_data['cnt'].notna()) & (merged_data['coin_price_C'].notna())].copy()
        # 再次分出两部分，一部分是满足条件的数据，另一部分是未满足条件的数据
        both_exist_abnormal_and_file_data['condition_met'] = (
                both_exist_abnormal_and_file_data['lasted_price'] * Decimal(0.99) >
                both_exist_abnormal_and_file_data['coin_price_C'])

        both_exist_abnormal_and_file_data['lasted_price'] = both_exist_abnormal_and_file_data.apply(
            lambda x: x['coin_price_C'] if x['condition_met'] == True else x['lasted_price'], axis=1)
        both_exist_abnormal_and_file_data['cnt'] = both_exist_abnormal_and_file_data.apply(
            lambda x: x['cnt'] + 1 if x['condition_met'] == True else x['cnt'], axis=1)

        abnormal_in_both_exist_abnormal_and_file_data = both_exist_abnormal_and_file_data[
            both_exist_abnormal_and_file_data['condition_met'] == True].copy()
        abnormal_in_both_exist_abnormal_and_file_data.drop(columns='condition_met', inplace=True)
        both_exist_abnormal_and_file_data.drop(columns='condition_met', inplace=True)
        # 文件中有，数据中没有——cnt不为空，coin_price为空
        only_exist_file_data = merged_data[(merged_data['coin_price_C'].isna()) &
                                           (merged_data['cnt'].notna())].copy()

        # 符合条件的异常数据
        cur_abnormal_data = pd.concat([only_exist_abnormal_data, abnormal_in_both_exist_abnormal_and_file_data],
                                      ignore_index=True)

        # 写回文件的数据
        to_be_write_data = pd.concat(
            [only_exist_abnormal_data, both_exist_abnormal_and_file_data, only_exist_file_data], ignore_index=True)
        to_be_write_data = to_be_write_data[['coin_name', 'spider_web', 'cnt', 'lasted_price']].copy()
        # 次数大于3的币种
        total_coins = to_be_write_data.copy()
        # 统计每个币种的cnt，将同一币种的cnt加起来
        group_data = total_coins[['coin_name', 'cnt']].groupby('coin_name').sum().reset_index(drop=False)

        cnt_gt_3_coins = group_data[group_data['cnt'] > record_cnt]['coin_name'].values

        cur_abnormal_data = cur_abnormal_data[
            (cur_abnormal_data['cnt'] <= record_cnt) & (~cur_abnormal_data['coin_name'].isin(cnt_gt_3_coins))].copy()
        cur_abnormal_data.drop(columns=['cnt', 'lasted_price'], inplace=True)

        return cur_abnormal_data, to_be_write_data

    @staticmethod
    def filter_by_figure_columns(data: pd.DataFrame, conditions: dict):
        """

        :param data:
        :param conditions:
        conditions = {
            'change': ('le', 1.2, 1),
            'virtual_drop' : ('gt', 0.8, 0.99)
        }
        ('le', 1.2, 1)中'le'表示目标字段要小于等于，而1.2则表示被比较的数字，最后的1则表示目标字段数据要乘以的倍数
        :return:
        """

        for column, condition in conditions.items():
            comparator = condition[0]
            threshold = Decimal(condition[1])
            magnification = Decimal(condition[2])
            if comparator == 'gt':
                data = data[data[column] * magnification > threshold]
            elif comparator == 'lt':
                data = data[data[column] * magnification < threshold]
            elif comparator == 'ge':
                data = data[data[column] * magnification >= threshold]
            elif comparator == 'le':
                data = data[data[column] * magnification <= threshold]
            elif comparator == 'eq':
                data = data[data[column] * magnification == threshold]
            elif comparator == 'neq':
                data = data[data[column] * magnification != threshold]
        return data

    @staticmethod
    def filter_AB_by_colse_price(group, magnification=Decimal(0.99)):
        """
        通过收盘价比较初步筛选出A和B时刻数据
        A时刻收盘价的magnification倍大于B时刻收盘价
        :return:
        """
        if not isinstance(magnification, Decimal):
            magnification = Decimal(magnification)
        group.sort_values(by='time', ascending=True, inplace=True)
        close_price = group['close'].values
        group_values = group.values
        result = []
        for i in range(len(close_price) - 1):
            for j in range(i + 1, len(close_price)):
                if close_price[i] * magnification >= close_price[j]:
                    # 将两个结果横向拼接起来加入result
                    combiend_row = np.concatenate((group_values[i], group_values[j]), axis=None)
                    result.append(combiend_row)
        columns = [f'{col}_A' for col in group.columns] + [f'{col}_B' for col in group.columns]
        result_data = pd.DataFrame(result, columns=columns)
        return result_data

    @staticmethod
    def filter_by_after_B_price(A2B_data: pd.DataFrame, total_data: pd.DataFrame,
                                magnification=Decimal(0.005)) -> pd.DataFrame:
        """
        某一个B时刻后面存在一个开盘价低于B时刻`最低价*(虚降 * 0.005 + 1)`的时刻，
        然后再有开盘价高于此B时刻`最低价*(虚降 * 0.005 + 1)`的时刻，则该B废弃不用。
        :return:
        """
        # 找出每一个B时刻后面对应的数据

        if not isinstance(magnification, Decimal):
            magnification = Decimal(magnification)
        after_B_data = defaultdict(pd.DataFrame)
        total_data = total_data[['coin_name', 'spider_web', 'coin_price', 'time']]
        indexs = A2B_data[['coin_name', 'spider_web', 'time_B']].drop_duplicates().values
        for coin_name, spider_web, time_B in indexs:
            after_B_data[(coin_name, spider_web, time_B)] = total_data[(total_data['coin_name'] == coin_name) &
                                                                       (total_data['spider_web'] == spider_web) &
                                                                       (total_data['time'] > time_B)]

        # 按照coin_name,spider_web,time_B进行分组
        group_data = A2B_data[['coin_name', 'spider_web', 'low_B', 'virtual_drop_B', 'time_B']].drop_duplicates(
            subset=['coin_name', 'spider_web', 'time_B'])
        group_data['compare_price'] = group_data['low_B'] * (group_data['virtual_drop_B'] * magnification + Decimal(1))

        def filte(row):
            coin_name = row['coin_name']
            spider_web = row['spider_web']
            time_B = row['time_B']
            after_data = after_B_data[(coin_name, spider_web, time_B)]
            if after_data.empty:
                return True
            after_data.sort_values(by='time', ascending=True, inplace=True)
            # 找出价格低于compare_price的数据
            low_price_data = after_data[after_data['coin_price'] < row['compare_price']]
            if low_price_data.empty:
                return True
            # 获取第一个索引
            first_index = low_price_data.index[0]

            # 再找出价格高于compare_price的数据
            high_price_data = after_data[
                (after_data['coin_price'] > row['compare_price']) & (after_data.index > first_index)]
            if high_price_data.empty:
                return True
            return False

        group_data['condition_met'] = group_data.apply(lambda row: filte(row), axis=1)
        group_data = group_data[group_data['condition_met'] == True].copy()
        group_data.dropna().drop(columns='compare_price', inplace=True)
        result_data = A2B_data.merge(group_data[['coin_name', 'spider_web', 'time_B']],
                                     on=['coin_name', 'spider_web', 'time_B'], how='inner')
        return result_data

    def filter_by_AB_before_6_days(self, A2B_data: pd.DataFrame, total_data: pd.DataFrame,
                                   spider_web: str) -> pd.DataFrame:
        """
        binance数据：

        1. 虚降 >= 跌幅的2.5倍， 跌幅<=-0.7%
        2. 在A之前6小时内存在或者在B之前6小时内存在跌幅<=-2.5%,且对应的前6小时开盘价大于等于A或者B时刻`max(收盘价, 开盘价)

        其他网数据：
        1. 虚降 >= 跌幅的3.5倍， 跌幅<=-0.7%
        2. 在A之前6小时内存在或者在B之前6小时内存在跌幅<=-4%,且对应的前6小时开盘价大于等于A或者B时刻`max(收盘价, 开盘价)`

        以上1、2条件满足其中一个即可
        :param A2B_data:
        :param total_data:
        :param spider_web
        :return:
        """
        CHANGE = -0.7
        if spider_web == 'binance':
            MAGNIFICATION = 1.7
            BEFORE_CHANGE = -2
        elif spider_web == 'other':
            MAGNIFICATION = 3
            BEFORE_CHANGE = -3.5

        # 条件1：
        condition_1 = ((A2B_data['change_A'] <= CHANGE) & (
                A2B_data['virtual_drop_A'] >= A2B_data['change_A'].abs() * Decimal(MAGNIFICATION))) | (
                              (A2B_data['change_B'] <= CHANGE) & (
                              A2B_data['virtual_drop_B'] >= A2B_data['change_B'].abs() * Decimal(MAGNIFICATION)))

        conform_condition_1_data = A2B_data[condition_1].copy()

        conform_condition_1_data['condition'] = f'A或者B虚降大于{MAGNIFICATION}倍跌幅，且跌幅 <= {CHANGE}({spider_web})%'

        # 条件2：A或者B前6天存在跌幅 < -2.5/-4% 的时刻，且该时刻的开盘价大于等于A或者B时刻`max(收盘价, 开盘价)`

        # 计算时间窗口
        A2B_data['time_start_A'] = A2B_data['time_A'] - timedelta(hours=6)
        A2B_data['time_start_B'] = A2B_data['time_B'] - timedelta(hours=6)

        # 每个coin_name,spider_web的数据
        indexs = A2B_data[['coin_name', 'spider_web']].drop_duplicates().values
        group_data = defaultdict(pd.DataFrame)
        total_data.sort_values(by='time', ascending=True, inplace=True)

        for coin_name, cur_spider_web in indexs:
            group_data[(coin_name, cur_spider_web)] = total_data[(total_data['coin_name'] == coin_name) &
                                                                 (total_data['spider_web'] == cur_spider_web)]

        def filte(row):
            coin_name = row['coin_name']
            cur_spider_web = row['spider_web']
            time_start_A = row['time_start_A']
            time_start_B = row['time_start_B']
            cur_data = group_data[(coin_name, cur_spider_web)]
            if cur_data.empty:
                return False
            # A时刻之前
            # A max(收盘价，开盘价)
            max_A = max(row['open_A'], row['close_A'])
            before_A_data = cur_data[cur_data['time'].between(time_start_A, row['time_A'], inclusive='left')]
            condition_2_A_data = before_A_data[
                (before_A_data['change'] < BEFORE_CHANGE) & (before_A_data['coin_price'] >= max_A)]

            result_A = not condition_2_A_data.empty

            # B时刻之前
            max_B = max(row['open_B'], row['close_B'])
            before_B_data = cur_data[cur_data['time'].between(time_start_B, row['time_B'], inclusive='left')]
            condition_2_B_data = before_B_data[
                (before_B_data['change'] < BEFORE_CHANGE) & (before_B_data['coin_price'] >= max_B)]
            result_B = not condition_2_B_data.empty

            if result_A or result_B:
                return True
            return False

        A2B_data['condition_met'] = A2B_data.apply(lambda row: filte(row), axis=1)
        conform_condition_2_data = A2B_data[A2B_data['condition_met'] == True].copy()
        conform_condition_2_data.drop(columns=['time_start_A', 'time_start_B', 'condition_met'], inplace=True)
        conform_condition_2_data[
            'condition'] = f'A/B前6小时跌幅<{BEFORE_CHANGE}%,该时刻的开盘价>=A/B时刻`max(开盘, 收盘)`{spider_web}'

        combined_data = pd.concat([conform_condition_1_data, conform_condition_2_data], ignore_index=True)
        return combined_data

    def filter_by_international_change(self, spider_web: str, base_data: pd.DataFrame, total_data: pd.DataFrame,
                                       **kwargs):
        """
        binance数据：当前时刻相较于国际时间（国内8点）的跌涨幅 <= 4%

    	其他网站数据：当前时刻相较于国际时间的跌涨幅 <=  -5%
        :param **kwargs:
        :param spider_web:
        :param base_data:
        :param total_data:
        :return:
        """
        if spider_web == 'binance':
            CHANGE_ON_INTERNATIONAL_TIME = 4
        else:
            CHANGE_ON_INTERNATIONAL_TIME = -5
        cur_datetime = self.datetime

        # 大于当天8点则使用当天的8点
        if cur_datetime >= cur_datetime.replace(hour=8, minute=0):
            international_time = cur_datetime.replace(hour=8, minute=0)
        else:
            # 小于当天8点则使用前一天8点
            international_time = cur_datetime.replace(hour=8, minute=0) - timedelta(days=1)

        # 获取8八点数据
        international_data = total_data[total_data['time'] == international_time].copy()
        international_data = international_data[international_data['coin_price'] != 0]
        # 计算跌涨幅
        merged_data = base_data[['coin_name', 'spider_web', 'coin_price']].drop_duplicates().merge(
            international_data[['coin_name', 'spider_web', 'coin_price']],
            on=['coin_name', 'spider_web'], how='left', suffixes=('_close', '_open'))
        merged_data['change'] = (merged_data['coin_price_close'] - merged_data['coin_price_open']) / merged_data[
            'coin_price_open'] * Decimal(100)

        conform_condition_data = merged_data[merged_data['change'] <= CHANGE_ON_INTERNATIONAL_TIME].copy()
        return conform_condition_data

    def func_1(self):
        """
        1.C为当前时刻，A与C时刻不超过24天
        2.A时刻在B时刻之前
        3.C时刻开盘价同时小于最近两天收盘价的最小值
        4.C时刻开盘价小于A的最低价和B的最低价
        5.AB均为跌且虚降大于等于5%
        6.（1）A或者B虚降大于1.5倍跌幅，且跌幅 <= -5%
           （2）A或者B前6天存在跌幅 < -10% 的时刻，且该时刻的开盘价大于等于A或者B时刻`max(收盘价, 开盘价)`
        条件6满足其中一个即可
        7.A时刻收盘价的0.99大于B时刻收盘价
        8.某一个B时刻后面存在一个低于B时刻`最低价*(虚降 * 0.005 + 1)`的时刻，然后再有高于此B时刻`最低价*(虚降 * 0.005 + 1)`的时刻，则���B废弃不用。
        :return:
        """
        MAX_TIME_INTERVAL = 24
        AB_CHANGE = 1
        AB_VIRTUAL_DROP = 1.1
        C_PRE_TIME_INTERVAL = 2
        AFTER_B_VIRTUAL_DROP_MAGNIFICATION = 0.005

        C_data = self.data.copy()

        if C_data.empty:
            logger.info("C数据为空,结束当前函数")
            return
        cur_datetime = self.datetime.replace(minute=0)
        A_datetime = cur_datetime - timedelta(hours=MAX_TIME_INTERVAL)
        B_datetime = cur_datetime - timedelta(hours=1)
        start_datetime = A_datetime - timedelta(hours=6)  # A之前6天
        # 获取 24 + 6 天的数据
        total_data = self.get_range_data_hours(start_datetime=start_datetime, end_datetime=B_datetime)
        # 筛选
        total_data = self.synchronous_data(C_data, total_data)
        # A到B的数据
        A2B_data = total_data[total_data['time'].between(A_datetime, B_datetime, inclusive='both')]

        # C开盘价小于最近两天收盘价的最小值
        # 近两天数据
        last_two_days_datetime = (
            cur_datetime - timedelta(hours=C_PRE_TIME_INTERVAL), cur_datetime - timedelta(hours=1))
        last_two_days_data = A2B_data[
            A2B_data['time'].between(last_two_days_datetime[0], last_two_days_datetime[1], inclusive='both')]
        groupby_coin_web = last_two_days_data.groupby(['coin_name', 'spider_web'])
        min_low = groupby_coin_web[['open', 'close']].apply(lambda group: group.min().min())
        min_low.rename('min_low', inplace=True)
        C_data = C_data.merge(min_low, on=['coin_name', 'spider_web'], how='inner')
        C_data = C_data[C_data['coin_price'] < C_data['min_low']].copy()

        if C_data.empty:
            logger.info('当前时刻没有满足条件：C开盘价小于近两小时最低价的数据')
            return
        C_data.drop(columns=['min_low'], inplace=True)

        A2B_data, total_data = self.synchronous_data(C_data, A2B_data, total_data)

        # 对AB进行筛选——AB均为跌且虚降大于等于5%
        A2B_change_and_virtual_drop_condition = {
            'change': ('lt', AB_CHANGE, 1),
            'virtual_drop': ('ge', AB_VIRTUAL_DROP, 1)
        }
        A2B_data = self.filter_by_figure_columns(A2B_data, A2B_change_and_virtual_drop_condition)

        # C开盘价小于A的最低价和B的最低价
        C_open_lt_AB_low_data = A2B_data.merge(C_data[['coin_name', 'spider_web', 'coin_price']],
                                               on=['coin_name', 'spider_web'],
                                               suffixes=['', '_C'], how='inner')
        C_open_lt_AB_low_data = C_open_lt_AB_low_data[
            C_open_lt_AB_low_data['coin_price_C'] <= C_open_lt_AB_low_data['low']].copy()
        A2B_data = C_open_lt_AB_low_data.drop(columns=['coin_price_C'])

        if A2B_data.empty:
            logger.info('当前时刻没有满足条件：C开盘价小于A的最低价和B的最低价的数据')
            return

        # 初步选择出AB时刻：A时刻收盘价的0.99大于B时刻收盘价
        A2B_data_groupby_coin_web = A2B_data.groupby(['coin_name', 'spider_web'])
        A2B_filter_by_close_data = A2B_data_groupby_coin_web.apply(lambda group: self.filter_AB_by_colse_price(group),
                                                                   include_groups=False).reset_index(drop=False).drop(
            columns=['level_2'])
        A2B_data = A2B_filter_by_close_data

        total_data = self.synchronous_data(A2B_data.drop_duplicates(subset=['coin_name', 'spider_web']).copy(),
                                           total_data)

        # 某一个B时刻后面存在一个低于B时刻`最低价*(虚降 * 0.005 + 1)`的时刻，
        # 然后再有高于此B时刻`最低价*(虚降 * 0.005 + 1)`的时刻，则该B废弃不用。
        A2B_data = self.filter_by_after_B_price(A2B_data, total_data, magnification=AFTER_B_VIRTUAL_DROP_MAGNIFICATION)

        if A2B_data.empty:
            logger.info(
                '当前时刻没有满足条件：某一个B时刻后面存在一个开盘价低于B时刻`最低价*(虚降 * 0.005 + 1)`的时刻，且之后再存在开盘价高于此数据的时刻')
            return

        record_data_file_path = os.path.join(PROJECT_ROOT_PATH, 'function_handler', 'record_data',
                                             'new_minute_record_data.csv')

        # 筛选出后面价格低于第一次价格的数据
        try:
            record_data = pd.read_csv(record_data_file_path, encoding='utf-8', low_memory=False)
            last_modified_datetime = datetime.fromtimestamp(os.path.getmtime(record_data_file_path))
        except FileNotFoundError:
            record_data = pd.DataFrame(columns=['coin_name', 'spider_web', 'first_price'])
            last_modified_datetime = self.datetime

        if self.datetime.minute == 0 or last_modified_datetime.hour != datetime.now().hour:
            try:
                os.remove(record_data_file_path)
            except FileNotFoundError:
                pass
            record_data = pd.DataFrame(columns=['coin_name', 'spider_web', 'first_price'])
        record_data['first_price'] = record_data['first_price'].apply(Decimal)

        A2B_data = A2B_data.merge(C_data[['coin_name', 'spider_web', 'coin_price']],
                                  on=['coin_name', 'spider_web'], how='left', suffixes=['', '_C']).rename(columns={
            'coin_price': 'coin_price_C'})

        filtered_data, record_data = self.filter_and_update_func_1_data(A2B_data.copy(),
                                                                        record_data)
        if not record_data.empty:
            record_data.dropna(inplace=True)
            record_data.to_csv(record_data_file_path, mode='w', index=False, encoding='utf-8')

        if filtered_data.empty:
            logger.info('当前时刻没有满足条件：筛选出后面价格低于第一次价格的数据')
            return

        # （1）A或者B虚降大于1.5倍跌幅，且跌幅 <= -5%
        # （2）A或者B前6天存在跌幅 < -10% 的时刻，且该时刻的开盘价大于等于A或者B时刻`max(收盘价, 开盘价)`
        # 满足其中一个即可
        with open(os.path.join(PROJECT_ROOT_PATH, 'data', 'binance_coins_USDT.csv'), 'r') as file:
            binance_coins = file.read().splitlines()

        binance_data = A2B_data[A2B_data['coin_name'].isin(binance_coins)]
        other_data = A2B_data[~A2B_data['coin_name'].isin(binance_coins)]

        result_binance_data = self.filter_by_AB_before_6_days(binance_data.copy(), total_data.copy(), 'binance')
        result_other_data = self.filter_by_AB_before_6_days(other_data.copy(), total_data.copy(), 'other')

        C_data = C_data.merge(A2B_data[['coin_name', 'spider_web']].copy().drop_duplicates(),
                              on=['coin_name', 'spider_web'], how='inner')
        C_binance_data = C_data[C_data['coin_name'].isin(binance_coins)]
        C_other_data = C_data[~C_data['coin_name'].isin(binance_coins)]

        international_change_binance_data = self.filter_by_international_change('binance', C_binance_data.copy(),
                                                                                total_data.copy())
        international_change_other_data = self.filter_by_international_change('other', C_other_data.copy(),
                                                                              total_data.copy())
        combined_international_change_data = pd.concat(
            [international_change_binance_data, international_change_other_data], ignore_index=True)

        combined_data = pd.concat([result_binance_data, result_other_data], ignore_index=True)
        combined_data = combined_data.merge(
            combined_international_change_data[['coin_name', 'spider_web']].drop_duplicates(),
            on=['coin_name', 'spider_web'], how='inner')
        if combined_data.empty:
            logger.info('当前没有满足所有条件的数据')
            return

        # 筛选出前小时异常次数小于等于3次的数据
        record_data_file_path = os.path.join(PROJECT_ROOT_PATH, 'function_handler', 'record_data',
                                             'new_minute_record_data_cnt.csv')
        try:
            record_data = pd.read_csv(record_data_file_path, encoding='utf-8')
            last_modified_datetime = datetime.fromtimestamp(os.path.getmtime(record_data_file_path))
        except FileNotFoundError:
            record_data = pd.DataFrame(columns=['coin_name', 'spider_web', 'cnt', 'lasted_price'])
            last_modified_datetime = self.datetime

        if self.datetime.minute == 0 or last_modified_datetime.hour != datetime.now().hour:
            try:
                os.remove(record_data_file_path)
            except FileNotFoundError:
                pass
            record_data = pd.DataFrame(columns=['coin_name', 'spider_web', 'cnt', 'lasted_price'])
        record_data['lasted_price'] = record_data['lasted_price'].apply(Decimal)

        filter_data, record_data = self.filter_cnt_lt_3_on_condition_1(combined_data.copy(),
                                                                       record_data.copy())

        if not record_data.empty:
            record_data.dropna(inplace=True, how='any')
            record_data.to_csv(record_data_file_path, index=False, mode='w', header=True)
        if filtered_data.empty:
            logger.info('当前没有满足所有条件的数据')
            return
        self.price_comparison_results[f'{self.func_1.__name__}'] = filter_data.copy()
        result = self.round_and_simple_data(filter_data, decimals=2)
        self.send_messages[f'apply_condition_1_to_func_1_base'] = result

    def add_filte_in_minute_and_hour(self):
        """
        1.以分钟为单位的函数：每一个时刻的C价格 <= 上一个C时刻价格的99%
        2.以小时为单位的函数：每一个时刻的C价格 <= 上一个C时刻价格的99%

        表中同一个AB时刻的同一币种出现次数最大为5次。
        对于一个币种，记录的时间跨度最多为23小时。超过23小时则删除该条件记录。
        columns = coin_name spider_web time_A time_B lasted_price_C_minute lasted_price_C_hour first_record_time  cnt
        :return:
        """
        minute_and_hour_cnt_file_path = os.path.join(PROJECT_ROOT_PATH, 'function_handler', 'record_data',
                                                     'new_minute_and_hour_cnt_le5.csv')
        logger.info('开始执行每分钟函数3：add_filte_in_minute_and_hour')
        data = self.price_comparison_results[f'{self.func_1.__name__}'].copy()

        if data.empty:
            logger.info("数据为空,结束函数")
            return

        result_data = self.filter_by_hour_and_minute(data, 'minute', minute_and_hour_cnt_file_path)

        if result_data is None or result_data.empty:
            logger.info("无结果")
            return
        self.price_comparison_results[f'{self.add_filte_in_minute_and_hour.__name__}'] = result_data.copy()
        send_email("[分钟]小时-分钟记录——V2", result_data.to_string(index=False), False)


if __name__ == '__main__':
    data = pd.read_csv(r'D:\PythonCode\virtual_currency-3.0\data\China\detail_data.csv')
    data = CSVReader.change_column_type_to_Decimal(data, only_price=True)
    data['time'] = pd.to_datetime(data['time'], format='%Y-%m-%d %H:%M:%S')
    cur_datetime = datetime(2024, 12, 12, 21, 25, 0)
    cur_data = data[data['time'] == cur_datetime]
    handler = NewMinuteFunctionHandler(data=cur_data, datetime=cur_datetime)
    handler.func_1()

    pass
