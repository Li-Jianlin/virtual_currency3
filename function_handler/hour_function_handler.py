import os
from decimal import Decimal, ROUND_HALF_UP
import pandas as pd
from datetime import timedelta, datetime
from collections import Counter

from config import ConfigHandler
from function_handler.functionhandler import FunctionHandler
from msg_log.mylog import get_logger
from dataio.csv_handler import CSVReader
import warnings
from collections import defaultdict

warnings.filterwarnings("ignore", category=FutureWarning)

PROJECT_ROOT_PATH = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

logger = get_logger(__name__, filename=os.path.join(PROJECT_ROOT_PATH, 'log', 'function_handler.log'))


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
        merged_data['cnt'] = merged_data['cnt'].fillna(0).infer_objects().astype(int)
        merged_data['cnt_new'] = merged_data['cnt_new'].fillna(0).infer_objects().astype(int)

        # 超过24小时全部cnt更新为0
        merged_data['cnt'] = merged_data.apply(
            lambda x: x['cnt_new'] if ((self.datetime - x['time']).total_seconds() / 3600) > 24
            else x['cnt'] + x['cnt_new'], axis=1
        )
        merged_data['time'] = merged_data.apply(
            lambda x: self.datetime if ((self.datetime - x['time']).total_seconds() / 3600) > 24 else x['time'], axis=1
        )

        merged_data.to_csv(count_file_path, encoding='utf-8', index=False, columns=['coin_name', 'cnt', 'time'],
                           date_format='%Y-%m-%d %H:%M:%S')

        original_data = original_data.merge(merged_data[['coin_name', 'cnt']], on='coin_name', how='left')
        # 如果cnt为0则更新为1
        original_data['cnt'] = original_data['cnt'].apply(lambda x: 1 if x == 0 else x)
        additional_data = additional_data.merge(merged_data[['coin_name', 'cnt']], on='coin_name', how='left')
        additional_data['cnt'] = additional_data['cnt'].apply(lambda x: 1 if x == 0 else x)

        return original_data[['coin_name', 'virtual_drop_A', 'virtual_drop_B', 'cnt']], additional_data[
            ['coin_name', 'virtual_drop_A', 'virtual_drop_B', 'cnt']]

    @staticmethod
    def record_coin_frequency_and_virtual_drop(record_file_path, recorded_data: pd.DataFrame):
        """
        记录24小时内每一只股票出现的频率和两次时间的虚降
        :return:
        """
        if recorded_data.empty:
            return pd.DataFrame(columns=['coin_name', 'spider_web', 'frequency', 'virtual_drop_A', 'virtual_drop_B'])

        recorded_data = recorded_data[['coin_name', 'spider_web', 'time_C', 'virtual_drop_A', 'virtual_drop_B']]


        try:
            record_data = pd.read_csv(record_file_path, encoding='utf-8', low_memory=False)
        except FileNotFoundError:
            record_data = pd.DataFrame(columns=['coin_name', 'spider_web', 'frequency', 'time'])
        record_data['time'] = pd.to_datetime(record_data['time'])

        # 拼接
        merged_data = record_data.merge(recorded_data, on=['coin_name', 'spider_web'], how='outer',
                                        suffixes=('', '_now'))


        # 文件与当前数据均存在对应数据, 此时没有缺失值;
        no_missing_data = merged_data.dropna(subset=['frequency', 'virtual_drop_A'], how='any')
        # 通过条件更新 frequency 列
        no_missing_data.loc[
            (no_missing_data['time_C'] - no_missing_data['time']).dt.total_seconds() / 3600 < 24, 'frequency'] += 1
        no_missing_data.loc[
            (no_missing_data['time_C'] - no_missing_data['time']).dt.total_seconds() / 3600 >= 24, 'frequency'] = 1

        # 文件中不存在数据，数据中存在则此时frequency和time为空；
        missing_frequency_data = merged_data[merged_data['frequency'].isna()].copy()
        missing_frequency_data['frequency'] = 1
        missing_frequency_data['time'] = missing_frequency_data['time_C']

        # 文件中存在而数据中不存在则time_C、virtual_drop_A、virtual_drop_B为空
        missing_virtual_drop_data = merged_data[merged_data['virtual_drop_A'].isna()]

        # 当前时刻异常的数据
        current_data = pd.concat([no_missing_data, missing_frequency_data], ignore_index=True)

        # 写回文件的数据
        total_data = pd.concat([current_data, missing_virtual_drop_data], ignore_index=True).dropna(how='any').drop_duplicates()

        total_data.to_csv(record_file_path, encoding='utf-8', index=False,
                          columns=['coin_name', 'spider_web', 'frequency', 'time'], date_format='%Y-%m-%d %H:%M:%S')


        current_data = current_data[['coin_name', 'spider_web', 'frequency', 'virtual_drop_A', 'virtual_drop_B']].drop_duplicates()
        return current_data

    def hour_func_1_base(self):
        """
        有ABC三个时刻。其中C为当前时刻
        C时刻在跌 C_CHANGE < 0
        A时刻与C时刻不超过MAX_TIME_INTERVAL小时。
        A时刻在B时刻之前，A和B时刻均要满足虚降>=AB_VIRTUAL_DROP%,且跌涨幅<=AB_CHANGE%
        A时刻收盘价大于B时刻收盘价。
        C时刻收盘价同时小于A的最低价和B的最低价。
        C时刻小于前C_PRE_TIME_INTERVAL小时最低价里的最小值
        :return:
        """
        logger.info("开始执行基础版函数1")
        config = self.config.get(f'{self.hour_func_1_base.__name__}')
        MAX_TIME_INTERVAL = config.get('MAX_TIME_INTERVAL')
        C_CHANGE = config.get('C_CHANGE')
        AB_CHANGE = config.get('AB_CHANGE')
        AB_VIRTUAL_DROP = config.get('AB_VIRTUAL_DROP')
        C_PRE_TIME_INTERVAL = int(config.get('C_PRE_TIME_INTERVAL'))
        # 当前时刻数据
        currrent_data = self.data.copy()

        # 对当前时刻进行跌幅筛选，要求跌涨幅小于0
        change_lt_0_data = self.filter_by_column(currrent_data, 'change', 'lt', C_CHANGE)

        # A到B时间范围的数据
        datetime_at_A = self.datetime - timedelta(hours=int(MAX_TIME_INTERVAL))  # A时间
        datetime_at_B = self.datetime - timedelta(hours=1)  # B时间
        range_A_to_B_data = self.filter_by_datetime(self.range_data_hours.copy(), start_datetime=datetime_at_A,
                                                    end_datetime=datetime_at_B, inclusive='both')
        range_A_to_B_data.dropna(how='any', inplace=True)

        # 前n小时数据
        pre_datetime = self.datetime - timedelta(hours=C_PRE_TIME_INTERVAL)
        pre_hours_data = self.filter_by_datetime(range_A_to_B_data.copy(), start_datetime=pre_datetime,
                                                 end_datetime=datetime_at_B, inclusive='both')
        change_lt_0_data = self.filter_C_by_price_lt_pre_hours_low_price(change_lt_0_data, pre_hours_data, 'hour')

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

        filtered_B_data = self.filter_B_data_with_following_conditions(A_close_gt_B_close_data.copy(), range_A_to_B_data.copy())

        if filtered_B_data.empty:
            filtered_B_data = pd.DataFrame(
                columns=['coin_name', 'spider_web', 'coin_price_A', 'time_A', 'high_A', 'low_A',
                         'open_A', 'close_A', 'change_A', 'amplitude_A', 'virtual_drop_A',
                         'coin_price_B', 'time_B', 'high_B', 'low_B', 'open_B', 'close_B',
                         'change_B', 'amplitude_B', 'virtual_drop_B'])

        # 将C时刻数据与AB范围内的数据合并，C时刻数据后缀为'_C'
        change_lt_0_data = change_lt_0_data.rename(columns={col : f'{col}_C' if col != 'coin_name' and col != 'spider_web' else col for col in change_lt_0_data.columns})
        merged_ABC_data = filtered_B_data.merge(change_lt_0_data, on=['coin_name', 'spider_web'],
                                                        how='inner', suffixes=('', '_C'))

        # C时刻收盘价同时小于A的最低价和B的最低价。

        C_close_lt_A_low_and_B_low_condition = (merged_ABC_data['close_C'] < merged_ABC_data['low_A']) & (
                merged_ABC_data['close_C'] < merged_ABC_data['low_B'])
        C_close_lt_A_low_and_B_low_data = merged_ABC_data[C_close_lt_A_low_and_B_low_condition]

        self.price_comparison_results['hour_func_1_base'] = C_close_lt_A_low_and_B_low_data.copy()

    def apply_condition_1_to_func_1_base(self):
        """
         C收盘价小于等于A最低价和B最低价中最小值的96%
        :return:
        """
        config = self.config.get(f'{self.apply_condition_1_to_func_1_base.__name__}')
        # CLOSE_PRICE_THRESHOLD = '0.96'
        CLOSE_PRICE_THRESHOLD = config.get('CLOSE_PRICE_THRESHOLD')
        record_file_path = os.path.join(PROJECT_ROOT_PATH, 'function_handler', 'record_data',
                                        'hour_func_1_coin_frequency_in_24_hours.csv')
        func_1_base_data = self.price_comparison_results['hour_func_1_base']
        if func_1_base_data.empty:
            return
        # 过滤掉不需要的字段
        func_1_base_data = func_1_base_data[['coin_name', 'spider_web', 'low_A', 'low_B', 'close_C', 'time_C',
                                             'virtual_drop_A', 'virtual_drop_B']].copy()
        # 找出A最低价和B最低价中的最小值
        min_low_in_AB = func_1_base_data[['low_A', 'low_B']].min(axis=1)
        func_1_base_data['min_low_in_AB'] = min_low_in_AB

        # 进行条件筛选
        condition_1 = (
                    func_1_base_data['close_C'] <= func_1_base_data['min_low_in_AB'] * Decimal(CLOSE_PRICE_THRESHOLD))
        condition_1_data = func_1_base_data[condition_1]


        send_data = self.record_coin_frequency_and_virtual_drop(record_file_path, condition_1_data.copy())

        if send_data.empty:
            return

        self.price_comparison_results['hour_func_1_base_after_condition_1'] = send_data.copy()
        # 四舍五入
        send_data['virtual_drop_A'] = send_data['virtual_drop_A'].apply(HourlyFunctionHandler.round_decimal, decimals=3)
        send_data['virtual_drop_B'] = send_data['virtual_drop_B'].apply(HourlyFunctionHandler.round_decimal, decimals=3)
        send_data = send_data[['coin_name', 'spider_web', 'virtual_drop_A', 'virtual_drop_B', 'frequency']].copy()
        send_data.drop_duplicates(subset='coin_name', inplace=True, keep='first')
        # self.send_messages['apply_condition_1_to_func_1_base'] = send_data

        logger.info("执行函数1完成" + '\n' + send_data.to_string(index=False))

    def apply_condition_2_to_func_1_base(self):
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
        logger.info("开始执行函数1条件2")
        config = self.config.get(f'{self.apply_condition_2_to_func_1_base.__name__}')

        base_data = self.price_comparison_results['hour_func_1_base'].copy()

        if base_data.empty:
            return

        A_to_now_data = self.get_range_data_hours((self.datetime - timedelta(hours=6)).replace(minute=0), self.datetime,
                                                  'left')
        A_to_now_data = self.csv_reader.change_column_type_to_Decimal(A_to_now_data, False)
        # biance网站的数据
        binance_data = base_data[base_data['spider_web'] == 'binance']
        binance_coins = binance_data['coin_name'].unique()
        conform_condition_binance_data = self.filter_change_virtual_drop_or_change_price_by_spider_web('binance',
                                                                                                       binance_data.copy(),
                                                                                                       A_to_now_data.copy(),
                                                                                                       config)

        # 其他网站数据
        other_data = base_data[(base_data['spider_web'] != 'binance') & (~base_data['coin_name'].isin(binance_coins))]
        conform_condition_other_data = self.filter_change_virtual_drop_or_change_price_by_spider_web('other',
                                                                                                     other_data.copy(),
                                                                                                     A_to_now_data.copy(),
                                                                                                     config)
        conform_condition_1_and_2_data = pd.concat([conform_condition_binance_data, conform_condition_other_data],
                                                   ignore_index=True)

        conform_condition_1_and_2_data.drop_duplicates(subset=['coin_name', 'spider_web'], inplace=True, keep='first')

        if conform_condition_1_and_2_data.empty:
            return

        result_data = conform_condition_1_and_2_data.drop(columns='condition')

        conform_condition_1_and_2_data.drop_duplicates(subset=['coin_name', 'spider_web'], inplace=True, keep='first')

        self.price_comparison_results['apply_condition_2_to_func_1_base'] = conform_condition_1_and_2_data.copy()

        conform_condition_1_and_2_data = self.round_and_simple_data(conform_condition_1_and_2_data)

        self.send_messages['apply_condition_2_to_func_1_base'] = conform_condition_1_and_2_data
        logger.info("函数1条件2执行完毕" + '\n' + conform_condition_1_and_2_data.to_string(index=False))


if __name__ == "__main__":
    csv_reader = CSVReader(data_region='China')

    data = pd.read_csv(r"D:\PythonCode\virtual_currency-3.0\test\test.csv")
    data = csv_reader.change_column_type_to_Decimal(data, only_price=False)
    data['time'] = pd.to_datetime(data['time'])
    configHandler = ConfigHandler(file_path=rf'{os.path.join(PROJECT_ROOT_PATH, "config.xml")}')
    configHandler.load_config()
    hourly_function_hander = HourlyFunctionHandler(reader=csv_reader, datetime=datetime(2024, 11, 7, 0, 0, 0),
                                                   data=data, config=configHandler.config.get('hour_function'))

    hourly_function_hander.get_range_data_hours(datetime(2024, 11, 6, 0, 0, 0), datetime(2024, 11, 7, 0, 0, 0),
                                                inclusive='left')
    hourly_function_hander.hour_func_1_base()
    hourly_function_hander.apply_condition_1_to_func_1_base()
    hourly_function_hander.apply_condition_2_to_func_1_base()
