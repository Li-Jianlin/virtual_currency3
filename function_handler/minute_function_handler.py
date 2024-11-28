import os
from collections import defaultdict

import pandas as pd
from datetime import timedelta, datetime
from decimal import Decimal

from config import ConfigHandler
from dataio.csv_handler import CSVReader
from function_handler.functionhandler import FunctionHandler
from msg_log.mylog import get_logger

PROJECT_ROOT_PATH = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

logger = get_logger(__name__, filename=os.path.join(PROJECT_ROOT_PATH, 'log', 'minute_function.log'))


class MinuteFunctionHandler(FunctionHandler):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.record_data_floder_path = os.path.join(PROJECT_ROOT_PATH, 'function_handler', 'record_data')
        os.makedirs(self.record_data_floder_path, exist_ok=True)

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
        columns=['coin_name' 'spider_web', 'cnt']
        """
        record_cnt = 3
        # 拼接
        merged_data = abnormal_data.merge(record_data, on=['coin_name', 'spider_web'], how='outer')

        # 数据中有，文件中没有——cnt为空,coin_price不为空
        only_exist_abnormal_data = merged_data[merged_data['cnt'].isna()].copy()
        only_exist_abnormal_data['cnt'] = 1
        # 文件中有，数据中也有——cnt不为空, coin_price也不为空
        both_exist_abnormal_and_file_data = merged_data[
            (merged_data['cnt'].notna()) & (merged_data['coin_price_C'].notna())].copy()
        both_exist_abnormal_and_file_data['cnt'] += 1
        # 文件中有，数据中没有——cnt不为空，coin_price为空
        only_exist_file_data = merged_data[merged_data['coin_price_C'].isna()].copy()

        # 符合条件的异常数据
        cur_abnormal_data = pd.concat([only_exist_abnormal_data, both_exist_abnormal_and_file_data], ignore_index=True)
        cur_abnormal_data = cur_abnormal_data[cur_abnormal_data['cnt'] <= record_cnt].copy()
        cur_abnormal_data.drop(columns=['cnt'], inplace=True)

        # 写回文件的数据
        to_be_write_data = pd.concat(
            [only_exist_abnormal_data, both_exist_abnormal_and_file_data, only_exist_file_data], ignore_index=True)
        to_be_write_data = to_be_write_data[['coin_name', 'spider_web', 'cnt']].copy()

        return cur_abnormal_data, to_be_write_data

    @staticmethod
    def filter_from_45_day_max_price_data(to_be_filter: pd.DataFrame, record_data: pd.DataFrame):
        RECORD_COUNT = 3
        to_be_filter['cnt'] = 1
        merged_data = record_data.merge(to_be_filter, on=['coin_name', 'spider_web'], how='outer',
                                        suffixes=('', '_filter'))
        merged_data['cnt_filter'] = merged_data['cnt_filter'].fillna(0).infer_objects().astype(int)
        merged_data['cnt'] = merged_data['cnt'].fillna(0).infer_objects().astype(int)
        merged_data['cnt'] = merged_data['cnt'] + merged_data['cnt_filter']

        filter_data = merged_data.merge(to_be_filter[['coin_name', 'spider_web']], on=['coin_name', 'spider_web'],
                                        how='inner')
        filter_data = filter_data[filter_data['cnt'] <= RECORD_COUNT]
        filter_data = filter_data[['coin_name', 'spider_web']]

        update_data = merged_data[['coin_name', 'spider_web', 'cnt']]
        return filter_data, update_data

    def minute_func_1_base(self):
        """
        有ABC三个时刻。其中C为当前时刻（当前分钟）
        1.A时刻与C时刻不超过24小时。
        2.A时刻在B时刻之前，A和B时刻均要满足虚降>=1.2%,且跌涨幅<=1%
        3.C时刻价格同时小于A和B的最低价。
        4.A时刻收盘价大于B时刻收盘价。
        5.对于同一时刻的异常股票最多只能提示三次（发送邮件）
        6.后面两次的价格低于第一次的价格
        7.某一个B时刻后面存在一个低于B时刻最低价的时刻，然后再有高于此B时刻最低价的时刻，则该B废弃不用。
        8.C时刻小于前C_PRE_TIME_INTERVAL小时最低价里的最小值
        :return:
        """
        config = self.config.get('minute_func_1_base')
        logger.info("开始执行基础版函数1")
        MAX_TIME_INTERVAL = config.get('MAX_TIME_INTERVAL')
        record_data_file_path = os.path.join(self.record_data_floder_path, 'minute_func_1_record_data.csv')
        AB_CHANGE = config.get('AB_CHANGE')
        AB_VIRTUAL_DROP = config.get('AB_VIRTUAL_DROP')
        C_PRE_TIME_INTERVAL = int(config.get('C_PRE_TIME_INTERVAL'))
        # 当前时刻数据
        current_data = self.data.copy()

        # A到B时间范围的数据
        datetime_at_A = (self.datetime - timedelta(hours=int(MAX_TIME_INTERVAL))).replace(minute=0)  # A时间
        datetime_at_B = (self.datetime - timedelta(hours=1)).replace(minute=0)  # B时间
        range_A_to_B_data = self.filter_by_datetime(self.range_data_hours.copy(), start_datetime=datetime_at_A,
                                                    end_datetime=datetime_at_B, inclusive='both')
        range_A_to_B_data.dropna(how='any', inplace=True)
        # 前n小时数据
        pre_datetime = (self.datetime - timedelta(hours=C_PRE_TIME_INTERVAL)).replace(minute=0)
        pre_hours_data = self.filter_by_datetime(range_A_to_B_data.copy(), start_datetime=pre_datetime,
                                                 end_datetime=datetime_at_B, inclusive='both')
        current_data = self.filter_C_by_price_lt_pre_hours_low_price(current_data, pre_hours_data, unit_time='minute')
        # A和B时刻均要满足虚降>=AB_VIRTUAL_DROP%,且跌涨幅<=AB_CHANGE%
        filter_columns_and_thresholds = {
            'virtual_drop': ('ge', AB_VIRTUAL_DROP),
            'change': ('le', AB_CHANGE)
        }
        filter_by_virtual_drop_and_change_data = self.filter_by_multiple_conditions(range_A_to_B_data,
                                                                                    filter_columns_and_thresholds)
        # A时刻收盘价大于B时刻收盘价
        A_close_gt_B_close_data = pd.DataFrame()
        group_by_coin_name_and_spider_web = filter_by_virtual_drop_and_change_data.groupby(
            ['coin_name', 'spider_web'])
        A_close_gt_B_close_data = group_by_coin_name_and_spider_web.apply(lambda group: self.filter_by_price_comparison(
            group, 'close', 'gt'), include_groups=False)
        A_close_gt_B_close_data = A_close_gt_B_close_data.dropna().reset_index(drop=False).drop(columns='level_2')
        filtered_B_data = self.filter_B_data_with_following_conditions(A_close_gt_B_close_data.copy(),
                                                                       range_A_to_B_data.copy())

        if filtered_B_data.empty:
            filtered_B_data = pd.DataFrame(
                columns=['coin_name', 'spider_web', 'coin_price_A', 'time_A', 'high_A', 'low_A',
                         'open_A', 'close_A', 'change_A', 'amplitude_A', 'virtual_drop_A',
                         'coin_price_B', 'time_B', 'high_B', 'low_B', 'open_B', 'close_B',
                         'change_B', 'amplitude_B', 'virtual_drop_B'])

        # 将C时刻数据与AB范围内的数据合并，C时刻数据后缀为'_C'
        data_C = current_data.rename(
            columns={col: f'{col}_C' if col != 'coin_name' and col != 'spider_web' else col for col in
                     current_data.columns})
        merged_ABC_data = filtered_B_data.merge(data_C, on=['coin_name', 'spider_web'],
                                                how='inner', suffixes=('', '_C'))

        # C时刻收盘价同时小于A的最低价和B的最低价。

        C_coin_price_lt_A_low_and_B_low_condition = (merged_ABC_data['coin_price_C'] < merged_ABC_data['low_A']) & (
                merged_ABC_data['coin_price_C'] < merged_ABC_data['low_B'])
        C_coin_price_lt_A_low_and_B_low_data = merged_ABC_data[C_coin_price_lt_A_low_and_B_low_condition]

        if C_coin_price_lt_A_low_and_B_low_data.empty:
            self.price_comparison_results['minute_func_1_base'] = C_coin_price_lt_A_low_and_B_low_data.copy()
            return

        # 筛选出后面价格低于第一次价格的数据
        try:
            record_data = pd.read_csv(record_data_file_path, encoding='utf-8', low_memory=False)
            last_modified_datetime = datetime.fromtimestamp(os.path.getmtime(record_data_file_path))
        except FileNotFoundError:
            record_data = pd.DataFrame(columns=['coin_name', 'spider_web', 'first_price'])
            last_modified_datetime = self.datetime

        if self.datetime.minute == 0 or last_modified_datetime.hour != self.datetime.hour:
            os.remove(record_data_file_path)
            record_data = pd.DataFrame(columns=['coin_name', 'spider_web', 'first_price'])
        record_data['first_price'] = record_data['first_price'].apply(Decimal)

        filtered_data, record_data = self.filter_and_update_func_1_data(C_coin_price_lt_A_low_and_B_low_data,
                                                                        record_data)
        if not record_data.empty:
            record_data.dropna(inplace=True)
            record_data.to_csv(record_data_file_path, mode='w', index=False, encoding='utf-8')

        self.price_comparison_results['minute_func_1_base'] = filtered_data.copy()

        # self.send_messages['minute_func_1_base'] = filtered_data

    def apply_condition_1_to_func_1_base(self):
        """
       binance数据：

        1. 虚降 >= 跌幅的2.5倍， 跌幅<=-0.7%
        2. 在A之前6小时内存在或者在B之前6小时内存在跌幅<=-2.5%,且对应的前6小时开盘价大于等于A或者B时刻`max(收盘价, 开盘价)

        其他网站数据：
        1. 虚降 >= 跌幅的3.5倍， 跌幅<=-0.7%
        2. 在A之前6小时内存在或者在B之前6小时内存在跌幅<=-4%,且对应的前6小时开盘价大于等于A或者B时刻`max(收盘价, 开盘价)`

        以上1、2条件满足其中一个即可
        :return:
        """
        config = self.config.get(f'{self.apply_condition_1_to_func_1_base.__name__}')
        record_data_file_path = os.path.join(self.record_data_floder_path, 'minute_apply_condiiton_1_on_base_1.csv')

        logger.info("开始执行函数1条件1")

        base_data = self.price_comparison_results['minute_func_1_base'].copy()

        if base_data.empty:
            return

        A_to_now_data = self.get_range_data_hours((self.datetime - timedelta(hours=6)).replace(minute=0), self.datetime,
                                                  'left')
        A_to_now_data = self.csv_reader.change_column_type_to_Decimal(A_to_now_data, False)
        # biance网站的数据
        binance_coins = base_data[base_data['spider_web'] == 'binance']['coin_name'].unique()
        binance_data = base_data[base_data['coin_name'].isin(binance_coins)]
        conform_condition_binance_data = self.filter_change_virtual_drop_or_change_price_by_spider_web('binance',
                                                                                                       binance_data.copy(),
                                                                                                       A_to_now_data.copy(),
                                                                                                       config)

        # 其他网站数据
        other_data = base_data[~base_data['coin_name'].isin(binance_coins)]
        conform_condition_other_data = self.filter_change_virtual_drop_or_change_price_by_spider_web('other',
                                                                                                     other_data.copy(),
                                                                                                     A_to_now_data.copy(),
                                                                                                     config)
        conform_condition_1_and_2_data = pd.concat([conform_condition_binance_data, conform_condition_other_data], ignore_index=True)

        conform_condition_1_and_2_data.drop_duplicates(subset=['coin_name', 'spider_web'], inplace=True, keep='first')

        if conform_condition_1_and_2_data.empty:
            return

        result_data = conform_condition_1_and_2_data.drop(columns='condition')
        # 存入函数运行结果
        self.price_comparison_results['apply_condition_1_to_func_1_base'] = result_data.copy()

        # 筛选出当前小时异常次数小于等于3次的数据
        try:
            record_data = pd.read_csv(record_data_file_path, encoding='utf-8')
            last_modified_datetime = datetime.fromtimestamp(os.path.getmtime(record_data_file_path))
        except FileNotFoundError:
            record_data = pd.DataFrame(columns=['coin_name', 'spider_web', 'cnt'])
            last_modified_datetime = self.datetime

        if self.datetime.minute == 0 or last_modified_datetime.hour != self.datetime.hour:
            os.remove(record_data_file_path)
            record_data = pd.DataFrame(columns=['coin_name', 'spider_web', 'cnt'])

        filter_data, record_data = self.filter_cnt_lt_3_on_condition_1(conform_condition_1_and_2_data.copy(),
                                                                       record_data.copy())

        if not record_data.empty:
            record_data.dropna(inplace=True, how='any')
            record_data.to_csv(record_data_file_path, index=False, mode='w', header=True)

        filter_data = self.round_and_simple_data(filter_data)
        # 存入邮件内容
        self.send_messages['apply_condition_1_to_func_1_base'] = filter_data
        logger.info("函数1条件1执行完毕" + '\n' + filter_data.to_string(index=False))

    def current_price_compare_with_45_day_max_price(self):
        """
        当前值小于等于近45天内最高价的50%。
        发送三次邮件
        :return:
        """
        logger.info('开始执行每分钟函数2：current_price_compare_with_45_day_max_price')
        data = self.data.copy()
        max_price_data_file_path = os.path.join(PROJECT_ROOT_PATH, 'function_handler', 'record_data',
                                                '45_day_max_price.csv')
        max_price_data = pd.read_csv(max_price_data_file_path, encoding='utf-8', low_memory=False,
                                     index_col='coin_name')
        max_price_data['max_price_in_45_day'] = max_price_data['max_price_in_45_day'].apply(Decimal)

        # 合并两张表
        merged_data = data.merge(max_price_data, on='coin_name', how='inner')

        # 条件
        condition = (merged_data['coin_price'] <= merged_data['max_price_in_45_day'] * Decimal('0.5'))
        # 筛选
        conform_condition_data = merged_data[condition].copy()

        # 发送次数筛选
        record_data_file_path = os.path.join(PROJECT_ROOT_PATH, 'function_handler', 'record_data',
                                             'current_price_compare_with_45_day_max_price.csv')
        try:
            record_data = pd.read_csv(record_data_file_path, encoding='utf-8', low_memory=False)
        except FileNotFoundError:
            record_data = pd.DataFrame(columns=['coin_name', 'spider_web', 'cnt'])

        filter_data, update_data = self.filter_from_45_day_max_price_data(conform_condition_data, record_data)

        if self.datetime.hour == 23 and self.datetime.minute == 59:
            os.remove(record_data_file_path)
        else:
            update_data.to_csv(record_data_file_path, index=False, encoding='utf-8')

        res_list = []
        if not filter_data.empty:
            func_desc = f"[分钟函数2]{len(filter_data['coin_name'])}当前值小于等于近45天内最高价的50%。"
            logger.info(func_desc)
            res_list.append(func_desc)
            filter_data_str = filter_data.to_string(index=False)
            logger.info('\n' + filter_data_str)
            res_list.append(filter_data_str)
            logger.info('每分钟函数2执行完毕')
            return '\n'.join(res_list)
        logger.info('每分钟函数2执行完毕，无异常')


if __name__ == '__main__':
    csvreader = CSVReader('China')
    config = ConfigHandler(file_path=r'D:\PythonCode\virtual_currency-3.0\config.xml')
    config.load_config()
    data = pd.read_csv(r'D:\PythonCode\virtual_currency-3.0\data\China\detail_data.csv')
    data['coin_price'] = data['coin_price'].apply(Decimal)
    data['time'] = pd.to_datetime(data['time'])
    cur_time = datetime(2024, 11, 28, 12, 29,0)
    pre_day_datetime = cur_time - timedelta(hours=24)
    data = data[data['time'] == cur_time].copy()
    minute_handler = MinuteFunctionHandler(data=data, datetime=cur_time,
                                           reader=csvreader, config=config.config.get('minute_function'))
    minute_handler.get_range_data_hours(start_datetime=pre_day_datetime,
                                        end_datetime=cur_time, inclusive='left')
    # res = minute_handler.current_price_compare_with_45_day_max_price()
    minute_handler.minute_func_1_base()
    minute_handler.apply_condition_1_to_func_1_base()
    # minute_handler.apply_condition_2_to_func_1_base()
    pass
