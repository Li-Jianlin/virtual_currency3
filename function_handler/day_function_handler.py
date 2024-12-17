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
from config import ConfigHandler

PROJECT_ROOT_PATH = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

logger = get_logger(__name__, filename=os.path.join(PROJECT_ROOT_PATH, 'log', 'day_function_handler.log'))


class DayFunctionHandler(FunctionHandler):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def execute_all(self):
        # 执行所有函数
        self.results = []
        logger.info('执行每日函数')
        for func in self.functions:
            logger.info('执行函数', func.__name__)
            try:
                res = func()
                if res:
                    self.results.append(res)
            except Exception as e:
                logger.exception(e)

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
                if close_price[i] * magnification > close_price[j]:
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
        group_data = group_data[group_data['condition_met'] == True].copy().dropna().drop(columns='compare_price')
        result_data = A2B_data.merge(group_data[['coin_name', 'spider_web', 'time_B']],
                                     on=['coin_name', 'spider_web', 'time_B'], how='inner')
        return result_data

    def filter_by_AB_before_6_days(self, A2B_data: pd.DataFrame, total_data: pd.DataFrame, config: dict) -> pd.DataFrame:
        """
        （1）A或者B虚降大于1.5倍跌幅，且跌幅 <= -5%
        （2）A或者B前6天存在跌幅 < -10% 的时刻，且该时刻的开盘价大于等于A或者B时刻`max(收盘价, 开盘价)`
        条件6满足其中一个即可
        :param A2B_data:
        :param total_data:
        :param config:
        :return:
        """
        MAGNIFICATION = config.get('MAGNIFICATION')
        # try:
        #     AB_CHANGE = (config.get('AB_CHANGE'))
        # except KeyError:
        #     AB_CHANGE = 0
        # else:
        #     AB_CHANGE = Decimal(AB_CHANGE)
        if "AB_CHANGE" in config:
            AB_CHANGE = Decimal(config.get('AB_CHANGE'))
        else:
            AB_CHANGE = Decimal(0)
        BEFORE_CHANGE = int(config.get('BEFORE_CHANGE'))
        # 条件1：
        condition_1 = ((A2B_data['change_A'] <= AB_CHANGE) & (
                A2B_data['virtual_drop_A'] > A2B_data['change_A'].abs() * Decimal(MAGNIFICATION))) | (
                              (A2B_data['change_B'] <= AB_CHANGE) & (
                              A2B_data['virtual_drop_B'] > A2B_data['change_B'].abs() * Decimal(MAGNIFICATION)))

        conform_condition_1_data = A2B_data[condition_1].copy()

        conform_condition_1_data['condition'] = f'A或者B虚降大于{MAGNIFICATION}倍跌幅，且跌幅 <= {AB_CHANGE}%'

        # 条件2：A或者B前6天存在跌幅 < -10% 的时刻，且该时刻的开盘价大于等于A或者B时刻`max(收盘价, 开盘价)`

        # 计算时间窗口
        A2B_data['time_start_A'] = A2B_data['time_A'] - timedelta(days=6)
        A2B_data['time_start_B'] = A2B_data['time_B'] - timedelta(days=6)

        # 每个coin_name,spider_web的数据
        indexs = A2B_data[['coin_name', 'spider_web']].drop_duplicates().values
        group_data = defaultdict(pd.DataFrame)
        total_data.sort_values(by='time', ascending=True, inplace=True)

        for coin_name, spider_web in indexs:
            group_data[(coin_name, spider_web)] = total_data[(total_data['coin_name'] == coin_name) &
                                                             (total_data['spider_web'] == spider_web)]

        def filte(row):
            coin_name = row['coin_name']
            spider_web = row['spider_web']
            time_start_A = row['time_start_A']
            time_start_B = row['time_start_B']
            cur_data = group_data[(coin_name, spider_web)]
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

        A2B_data['comdition_met'] = A2B_data.apply(lambda row: filte(row), axis=1)

        conform_condition_2_data = A2B_data[A2B_data['comdition_met'] == True].drop(columns=['time_start_A', 'time_start_B'])
        conform_condition_2_data.drop(columns=['comdition_met'], inplace=True)
        conform_condition_2_data[
            'condition'] = f'A/B前6天跌幅<{BEFORE_CHANGE}%,该时刻的开盘价>=A/B时刻`max(开盘, 收盘)`'

        combined_data = pd.concat([conform_condition_1_data, conform_condition_2_data], ignore_index=True)
        return combined_data

    def func_1(self):
        """
        1.C为当前时刻，A与C时刻不超过24天
        2.A时刻在B时刻之前
        3.C时刻开盘价同时小于最近两天的最低价
        4.C时刻开盘价小于A的最低价和B的最低价
        5.AB均为跌且虚降大于等于5%
        6.（1）A或者B虚降大于1.5倍跌幅，且跌幅 <= -5%
           （2）A或者B前6天存在跌幅 < -10% 的时刻，且该时刻的开盘价大于等于A或者B时刻`max(收盘价, 开盘价)`
        条件6满足其中一个即可
        7.A时刻收盘价的0.99大于B时刻收盘价
        8.某一个B时刻后面存在一个低于B时刻`最低价*(虚降 * 0.005 + 1)`的时刻，然后再有高于此B时刻`最低价*(虚降 * 0.005 + 1)`的时刻，则该B废弃不用。
        :return:
        """
        logger.info('开始每日函数1')
        config = self.config.get('func_1')
        MAX_TIME_INTERVAL = int(config.get('MAX_TIME_INTERVAL'))
        AB_CHANGE = config.get('AB_CHANGE')
        AB_VIRTUAL_DROP = config.get('AB_VIRTUAL_DROP')
        C_PRE_TIME_INTERVAL = int(config.get('C_PRE_TIME_INTERVAL'))
        AFTER_B_VIRTUAL_DROP_MAGNIFICATION = config.get('AFTER_B_VIRTUAL_DROP_MAGNIFICATION')
        C_data = self.data.copy()
        if C_data.empty:
            logger.info("C数据为空,结束当前函数")
            return
        cur_datetime = self.datetime
        A_datetime = cur_datetime - timedelta(days=MAX_TIME_INTERVAL)
        B_datetime = cur_datetime - timedelta(days=1)
        start_datetime = A_datetime - timedelta(days=6)  # A之前6天
        # 获取 24 + 6 天的数据
        total_data = self.get_range_data_days(start_datetime=start_datetime, end_datetime=B_datetime)
        if total_data.empty:
            logger.warning(f'获取数据失败，开始时间：{start_datetime}, 结束时间：{B_datetime}')
            return
        # 筛选
        total_data = self.synchronous_data(C_data, total_data)
        # A到B的数据
        A2B_data = total_data[total_data['time'].between(A_datetime, B_datetime, inclusive='both')]

        # C开盘价小于最近两天的最低价
        # 近两天数据
        last_two_days_datetime = (cur_datetime - timedelta(days=C_PRE_TIME_INTERVAL), cur_datetime - timedelta(days=1))
        last_two_days_data = A2B_data[
            A2B_data['time'].between(last_two_days_datetime[0], last_two_days_datetime[1], inclusive='both')]
        groupby_coin_web = last_two_days_data.groupby(['coin_name', 'spider_web'])
        min_low = groupby_coin_web[['open', 'close']].apply(lambda group: group.min().min())
        min_low.rename('min_close', inplace=True)
        C_data = C_data.merge(min_low, on=['coin_name', 'spider_web'], how='inner')
        C_data = C_data[C_data['open'] <= C_data['min_close']].copy()

        if C_data.empty:
            logger.info('当前时刻没有满足条件：C开盘价小于近两天最低价的数据')
            return
        C_data.drop(columns=['min_close'], inplace=True)

        A2B_data, total_data = self.synchronous_data(C_data, A2B_data, total_data)

        # 对AB进行筛选——AB均为跌且虚降大于等于5%
        A2B_change_and_virtual_drop_condition = {
            'change': ('lt', AB_CHANGE, 1),
            'virtual_drop': ('ge', AB_VIRTUAL_DROP, 1)
        }
        A2B_data = self.filter_by_figure_columns(A2B_data, A2B_change_and_virtual_drop_condition)

        # C开盘价小于A的最低价和B的最低价
        C_open_lt_AB_low_data = A2B_data.merge(C_data[['coin_name', 'spider_web', 'open']],
                                               on=['coin_name', 'spider_web'],
                                               suffixes=['', '_C'], how='inner')
        C_open_lt_AB_low_data = C_open_lt_AB_low_data[
            C_open_lt_AB_low_data['open_C'] < C_open_lt_AB_low_data['low']].copy()
        C_open_lt_AB_low_data = C_open_lt_AB_low_data.drop(columns=['open_C'])
        A2B_data = C_open_lt_AB_low_data

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

        # （1）A或者B虚降大于1.5倍跌幅，且跌幅 <= -5%
        # （2）A或者B前6天存在跌幅 < -10% 的时刻，且该时刻的开盘价大于等于A或者B时刻`max(收盘价, 开盘价)`
        # 满足其中一个即可
        result_data = self.filter_by_AB_before_6_days(A2B_data, total_data, config.get("filter_by_AB_before_6_days"))
        if result_data.empty:
            logger.info('当前没有满足所有条件的数据')
            return

        self.price_comparison_results[f'{DayFunctionHandler.func_1.__name__}'] = result_data.copy()
        result_data = self.round_and_simple_data(result_data, decimals=2)
        self.send_messages[f'{DayFunctionHandler.func_1.__name__}'] = result_data

    def func_2(self):
        """
        1.C为当前时刻，A与C时刻不超过45天
        2.A时刻在B时刻之前
        3.C时刻开盘价同时小于最近两天收盘价的最小值
        4.C时刻开盘价小于A的最低价和B的最低价
        5.AB均为跌且虚降大于等于5%，跌幅<=-4%;
        6.（1）A或者B虚降大于1.5倍跌幅，
         （2）A或者B前6天存在跌幅 < -10% 的时刻，且该时刻的开盘价大于等于A或者B时刻max(收盘价, 开盘价)
        条件6满足其中一个即可
        7.A时刻收盘价的0.95大于B时刻收盘价
        8.B时刻的失效问题，暂时不管；

        针对非必安的其他股票：
        （1）A或者B虚降大于1.6倍跌幅，且跌幅 <= -5%，
        （2）A或者B前6天存在跌幅 < -10% 的时刻，且该时刻的开盘价大于等于A或者B时刻max(收盘价, 开盘价)
        :return: 
        """
        logger.info("开始执行func_2")
        config = self.config.get('func_2')
        cur_datetime = self.datetime
        C_data = self.data.copy()

        # A与B时间
        A_datetime = cur_datetime - timedelta(days=45)
        A_before_6_days_datetime = cur_datetime - timedelta(days=6)
        B_datetime = cur_datetime - timedelta(days=1)
        total_data = self.get_range_data_days(start_datetime=A_before_6_days_datetime, end_datetime=B_datetime,
                                              inclusive='both')

        total_data = self.synchronous_data(C_data, total_data)
        A2B_data = total_data[total_data['time'].between(A_datetime, B_datetime, inclusive='both')]

        # C时刻开盘价小于近两天收盘价的最小值
        B_before_datetime = B_datetime - timedelta(days=1)
        pre_two_days_data = A2B_data[A2B_data['time'].between(B_before_datetime, B_datetime, inclusive='both')]
        groupby_coin_web = pre_two_days_data.groupby(['coin_name', 'spider_web'])
        min_close_price = groupby_coin_web[['open', 'close']].apply(lambda group: group.min().min())
        min_close_price.rename('min_close', inplace=True)
        C_data = C_data.merge(min_close_price, on=['coin_name', 'spider_web'], how='inner')
        C_data = C_data[C_data['open'] <= C_data['min_close']].copy()
        C_data = C_data.drop(columns=['min_close'])

        if C_data.empty:
            logger.info('当前时刻没有满足条件：C时刻收盘价小于近两天收盘价的最小值的数据')
            return

        A2B_data, total_data = self.synchronous_data(C_data, A2B_data, total_data)

        # AB均为跌且虚降 >= 5%， 跌幅 <= -4%
        AB_change_and_virtual_drop_condition = {
            'change': ('le', -4, 1),
            'virtual_drop': ('ge', 5, 1)
        }
        A2B_data = self.filter_by_figure_columns(A2B_data, AB_change_and_virtual_drop_condition)

        C_data, total_data = self.synchronous_data(A2B_data.drop_duplicates(subset=['coin_name', 'spider_web']), C_data,
                                                   total_data)

        # A时刻收盘价的0.95大于B时刻收盘价
        A2B_data_groupby_coin_web = A2B_data.groupby(['coin_name', 'spider_web'])
        A2B_filter_by_close_data = A2B_data_groupby_coin_web.apply(
            lambda group: self.filter_AB_by_colse_price(group, 0.95),
            include_groups=False).reset_index(drop=False).drop(
            columns=['level_2'])
        A2B_data = A2B_filter_by_close_data
        if A2B_data.empty:
            logger.info('当前时刻没有满足条件：A时刻收盘价的0.95大于B时刻收盘价')
            return

        # C开盘价小于A的最低价和B的最低价
        A2B_C_merged_data = A2B_data.merge(C_data[['coin_name', 'spider_web', 'open']], on=['coin_name', 'spider_web'],
                                           how='inner').rename(columns={'open': 'open_C'})
        C_open_lt_AB_low_condition = (A2B_C_merged_data['open_C'] < A2B_C_merged_data['low_A']) & (
                    A2B_C_merged_data['open_C'] < A2B_C_merged_data['low_B'])
        C_open_lt_AB_low = A2B_C_merged_data[C_open_lt_AB_low_condition].copy().drop(columns=['open_C'])

        A2B_data = C_open_lt_AB_low

        if A2B_data.empty:
            logger.info('当前时刻没有满足条件：C开盘价小于A的最低价和B的最低价')
            return
        
        # 针对binnace
        # （1）A或者B虚降大于1.5倍跌幅，
        # （2）A或者B前6天存在跌幅 < -10% 的时刻，且该时刻的开盘价大于等于A或者B时刻max(收盘价, 开盘价)
        # 满足其中一个即可
        with open(os.path.join(PROJECT_ROOT_PATH, 'data', 'binance_coins_USDT.csv'), 'r') as file:
            binance_coins = file.read().splitlines()
        binance_data = A2B_data[A2B_data['coin_name'].isin(binance_coins)].copy()
        other_data = A2B_data[~A2B_data['coin_name'].isin(binance_coins)].copy()
        binance_conform_condition_6_data = self.filter_by_AB_before_6_days(binance_data, total_data,config.get('binance_filter_by_AB_before_6_days'))
        other_conform_condition_6_data = self.filter_by_AB_before_6_days(other_data, total_data,config.get('other_filter_by_AB_before_6_days'))

        combined_data = pd.concat([binance_conform_condition_6_data, other_conform_condition_6_data], ignore_index=True)
        if combined_data.empty:
            logger.info('当前时刻没有满足条件：针对binance和other')
            return

        combined_data = self.round_and_simple_data(combined_data, 2)
        self.send_messages[f'{DayFunctionHandler.func2.__name__}'] = combined_data.copy()




if __name__ == '__main__':
    csv_reader = CSVReader("China")
    csv_writer = CSVWriter("China")
    cur_datetime = datetime(2024, 11, 29, 0, 0, 0)
    data = pd.read_csv(r'D:\PythonCode\virtual_currency-3.0\data\China\2024-11\all_midnight.csv')
    data = csv_reader.change_column_type_to_Decimal(data, only_price=False)
    data['time'] = pd.to_datetime(data['time'])
    config = ConfigHandler(file_path=r'D:\PythonCode\virtual_currency-3.0\config.xml')
    config.load_config()
    data = data[data['time'] == cur_datetime]
    dayhandler = DayFunctionHandler(data=data, config=config.config.get('day_function'), datetime=cur_datetime,
                                    reader=csv_reader, writer=csv_writer)
    dayhandler.get_range_data_days(start_datetime=cur_datetime - timedelta(days=24), end_datetime=cur_datetime,
                                   inclusive='left')
    t1 = datetime.now()
    dayhandler.func_1()
    dayhandler.func_2()
    print(datetime.now() - t1)
    pass
