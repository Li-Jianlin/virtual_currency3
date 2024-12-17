from typing import Literal
from datetime import datetime, timedelta
from collections import defaultdict
from dataio.csv_handler import CSVReader, CSVWriter
import pandas as pd
import numpy as np
import os
from msg_log.mylog import get_logger
from decimal import Decimal, ROUND_HALF_UP
import warnings

PROJECT_ROOT_PATH = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

logger = get_logger(__name__, filename=os.path.join(PROJECT_ROOT_PATH, 'log', 'function_handler.log'))
warnings.filterwarnings("ignore", category=FutureWarning)


class FunctionHandler:
    def __init__(self, **kwargs):
        self.range_data_days = None
        self.range_data_hours = None
        self.functions = []
        self.results = []
        self.datetime = kwargs.get('datetime', datetime.now().replace(second=0))
        self.data = kwargs.get('data', None)
        self.config = kwargs.get('config', None)
        self.csv_reader = kwargs.get('reader', CSVReader("China"))
        self.csv_writer = kwargs.get('writer', CSVWriter("China"))
        self.price_comparison_results = defaultdict(pd.DataFrame)
        self.send_messages = defaultdict(str)

    @staticmethod
    def round_decimal(val, decimals=2):
        """
        对Decimal数据进行四舍五入
        :return:
        """
        return val.quantize(Decimal('1.' + '0' * decimals), rounding=ROUND_HALF_UP)

    def get_range_data_hours(self, start_datetime: datetime, end_datetime: datetime,
                             inclusive: Literal['both', 'neither', 'left', 'right'] = 'both'):
        self.range_data_hours = self.csv_reader.get_data_between_hours(start_datetime, end_datetime, inclusive)
        return self.range_data_hours

    def get_range_data_days(self, start_datetime: datetime, end_datetime: datetime,
                            inclusive: Literal['both', 'neither', 'left', 'right'] = 'both'):
        self.range_data_days = self.csv_reader.get_data_between_days(start_datetime, end_datetime, inclusive)
        return self.range_data_days

    @staticmethod
    def filter_by_column(data: pd.DataFrame, column: str, comparison: Literal['gt', 'lt', 'ge', 'le', 'eq', 'neq'],
                         threshold) -> pd.DataFrame:
        """通用的筛选函数，筛选列符合条件的数据"""
        # 类型判断
        if isinstance(threshold, (str, int)):
            threshold = Decimal(threshold)

        threshold = Decimal(threshold)  # 转换阈值为 Decimal 类型
        values = data[column].values

        comparison_map = {
            'gt': values > threshold,
            'lt': values < threshold,
            'ge': values >= threshold,
            'le': values <= threshold,
            'eq': values == threshold,
            'neq': values != threshold
        }

        # 检查比较运算符是否合法
        if comparison not in comparison_map:
            logger.warning(f'不支持的比较运算符: {comparison}')
            raise ValueError(f'不支持的比较运算符: {comparison}')

        return data[comparison_map[comparison]]

    @staticmethod
    def filter_by_multiple_conditions(data: pd.DataFrame, conditions: dict) -> pd.DataFrame:
        """
        使用动态条件字典来筛选数据。
        :param data: 传入的待筛选数据
        :param conditions: 传入多个筛选条件，例如：
            {
                'change': ('gt', 0.5),
                'amplitude': ('lt', 1.0),
            }
        :return: 筛选后的 DataFrame
        """

        for column, (comparison, threshold) in conditions.items():
            data = FunctionHandler.filter_by_column(data, column, comparison, threshold)
        return data

    @staticmethod
    def filter_by_datetime(data: pd.DataFrame, start_datetime: datetime, end_datetime: datetime,
                           inclusive: Literal['both', 'neither', 'left', 'right'] = 'both'):
        """通过时间筛选数据"""
        filtered_data = data[data['time'].between(start_datetime, end_datetime, inclusive=inclusive)]

        return filtered_data

    @staticmethod
    def filter_by_price_comparison(group: pd.DataFrame, filter_column: str,
                                   comparison: Literal['gt', 'lt', 'ge', 'le', 'eq', 'neq']):
        """
        筛选符合特定价格比较条件的数据行

        该函数根据给定的比较运算符（如 'gt', 'lt' 等），对指定列（`filter_column`）中每对数据行进行比较。
        比较只考虑时间顺序上，`A` 时刻的价格与 `B` 时刻的价格的关系。最终返回符合比较条件的所有数据对。

        参数：
        - group (pd.DataFrame): 输入的 DataFrame，其中包含多个时刻的价格数据。
        - filter_column (str): 用于比较的价格列名。
        - comparison (str): 比较运算符，可以是 'gt'（大于）、'lt'（小于）、'ge'（大于等于）、'le'（小于等于）、
                            'eq'（等于）或 'neq'（不等于）。

        返回：
        - pd.DataFrame: 返回一个 DataFrame，包含符合条件的所有 A 时刻和 B 时刻的配对数据。
        """
        MAGNIFICATION = '0.99'
        # 升序排列，A时刻在前，B时刻在后
        group.sort_values(by='time', ascending=True, inplace=True)

        # 获取价格列，转换为 numpy 数组
        total_price_list = group[filter_column].values
        n = len(group)

        # 获取所有列的 NumPy 数组
        group_values = group.values  # 直接获取整个数据框的 NumPy 数组

        # 创建一个比较矩阵，仅考虑上三角部分 (i < j)
        comparison_matrix = np.triu(np.ones((n, n), dtype=bool), k=1)

        # 根据不同的比较条件生成一个逻辑矩阵
        comparison_map = {
            'gt': total_price_list[:, None] * Decimal(MAGNIFICATION) > total_price_list,
            'lt': total_price_list[:, None] < total_price_list,
            'ge': total_price_list[:, None] >= total_price_list,
            'le': total_price_list[:, None] <= total_price_list,
            'eq': total_price_list[:, None] == total_price_list,
            'neq': total_price_list[:, None] != total_price_list
        }

        valid_combinations = comparison_map[comparison]

        # 将逻辑矩阵和比较矩阵按位与（&），保留满足条件的组合
        valid_combinations = np.logical_and(valid_combinations, comparison_matrix)

        # 获取符合条件的索引
        valid_indices = np.where(valid_combinations)

        # 存储最终结果
        merged_results = []

        # 创建一个空的结果数组，预分配内存
        merged_results_array = np.empty((len(valid_indices[0]), 2 * len(group.columns)), dtype=object)

        # 批量处理符合条件的组合，减少每次拼接的开销
        for idx, (i, j) in enumerate(zip(*valid_indices)):
            # 提取 A 和 B 行
            A_row = group_values[i]
            B_row = group_values[j]

            # 将 A 和 B 的行数据直接赋值到结果数组中
            merged_results_array[idx, :len(A_row)] = A_row
            merged_results_array[idx, len(A_row):] = B_row

        # 创建列名列表，给每列加上 _A 和 _B 后缀
        column_names = [f"{col}_A" for col in group.columns if col not in ['coin_name', 'spider_web']] + [f"{col}_B" for
                                                                                                          col in
                                                                                                          group.columns
                                                                                                          if
                                                                                                          col not in [
                                                                                                              'coin_name',
                                                                                                              'spider_web']]

        # 将结果数组转换为 DataFrame
        result_df = pd.DataFrame(merged_results_array, columns=column_names)

        return result_df

    @staticmethod
    def filter_B_data_with_following_conditions(filter_data, range_data):
        """B时刻后面有价格低于B，且再有价格高于B，则该B时刻废弃"""
        # 添加索引以加速条件筛选
        range_data.set_index(['coin_name', 'spider_web', 'time'], inplace=True)
        range_data.sort_index(ascending=True, inplace=True)
        # 存储符合条件的 B 时刻数据
        filter_B_data = []

        # 矢量化操作：按 coin_name 和 spider_web 分组处理
        grouped_data = filter_data.groupby(['coin_name', 'spider_web'])
        for (coin_name, spider_web), group in grouped_data:
            # 获取对应范围数据
            full_data = range_data.loc[(coin_name, spider_web)]
            if full_data.empty:
                continue
            # 遍历 B 时刻的每一行
            for _, row in group.iterrows():
                MAGANIFICATION = row['virtual_drop_B'] * Decimal('0.005') + Decimal('1')
                # MAGANIFICATION = 1
                time_B, min_B = row['time_B'], row['low_B'] * MAGANIFICATION

                # 筛选 B 时刻之后的数据
                following_data = full_data.loc[full_data.index > time_B]
                if following_data.empty:
                    filter_B_data.append(row)
                    continue

                # 找到第一个低于或等于 min_B 的时刻
                below_min_B_mask = following_data['coin_price'] <= min_B
                if not below_min_B_mask.any():
                    filter_B_data.append(row)
                    continue

                first_time = following_data[below_min_B_mask].index[0]

                # 检查该时刻后是否存在价格高于 min_B 的数据
                after_first_time = following_data.loc[following_data.index > first_time]
                if not (after_first_time['coin_price'] > min_B).any():
                    filter_B_data.append(row)

        filter_data = pd.DataFrame(filter_B_data)

        return filter_data

    def round_and_simple_data(self, data: pd.DataFrame, decimals=3) -> pd.DataFrame:
        """处理结果"""
        # 只保留一个币种（去重）
        data.drop_duplicates(subset=['coin_name'], inplace=True, keep='first')
        # 四舍五入
        data['virtual_drop_A'] = data['virtual_drop_A'].apply(
            FunctionHandler.round_decimal, decimals=decimals)
        data['virtual_drop_B'] = data['virtual_drop_B'].apply(
            FunctionHandler.round_decimal, decimals=decimals)
        data = data[
            ['coin_name', 'spider_web', 'virtual_drop_A', 'virtual_drop_B', 'time_A', 'time_B', 'condition']].copy()
        # 简化时间
        data['time_A'] = data['time_A'].apply(
            lambda x: x.strftime('%m-%d %H:%M'))
        data['time_B'] = data['time_B'].apply(
            lambda x: x.strftime('%m-%d %H:%M'))
        return data

    @staticmethod
    def filter_C_by_price_lt_pre_hours_low_price(cur_data, pre_three_hours_data,
                                                 unit_time: Literal['hour', 'day', 'minute']):
        """C小于前n小时内最低价的最小值"""

        # 对前n小时数据进行分组
        pre_three_hours_group_data = pre_three_hours_data.groupby(['coin_name', 'spider_web'])

        # 找出每组收盘价和开盘价中的最小值
        min_low = pre_three_hours_group_data[['close', 'open']].apply(lambda group: group.min().min())

        # 更改字段名
        min_low = min_low.rename('min_low')

        # 拼接
        combined_data = cur_data.merge(min_low, left_on=['coin_name', 'spider_web'], right_index=True,
                                       how='inner').reset_index(drop=True)

        condition = None
        if unit_time == 'minute':
            condition = combined_data['coin_price'] <= combined_data['min_low']
        elif unit_time == 'hour':
            condition = combined_data['open'] <= combined_data['min_low']

        filter_data = combined_data[condition]
        filter_data = filter_data.drop(columns=['min_low'])
        return filter_data.copy()

    @staticmethod
    def filter_change_virtual_drop_or_change_price_by_spider_web(spider_web: Literal['binance', 'other'],
                                                                 base_data: pd.DataFrame, total_data: pd.DataFrame,
                                                                 config: dict):
        """
        1.虚降 >= 跌幅的MAGNIFICATION倍，跌幅<=CHANGE%
        2. 在A之前6小时内存在或者在B之前6小时内存在跌幅<=A_OR_B_CHANGE%,且对应的前6小时开盘价大于等于A或者B时刻`max(收盘价, 开盘价)
        根据网站来决定
        :param config:
        :param spider_web:
        :param base_data:
        :param total_data:
        :return:
        """
        if spider_web == 'binance':
            MAGNIFICATION_KEY = 'MAGNIFICATION_BINANCE'
            A_OR_B_CHANGE_KEY = 'A_OR_B_CHANGE_BINANCE'
        else:
            MAGNIFICATION_KEY = 'MAGNIFICATION_OTHER'
            A_OR_B_CHANGE_KEY = 'A_OR_B_CHANGE_OTHER'
        CHANGE_KEY = 'CHANGE'
        MAGNIFICATION = Decimal(config[MAGNIFICATION_KEY])
        A_OR_B_CHANGE = Decimal(config[A_OR_B_CHANGE_KEY])
        CHANGE = Decimal(config[CHANGE_KEY])

        # condition_1
        condition_1 = ((base_data['change_A'] <= CHANGE) & (
                base_data['virtual_drop_A'] > base_data['change_A'].abs() * MAGNIFICATION)) | (
                              (base_data['change_B'] <= CHANGE) & (
                              base_data['virtual_drop_B'] > base_data['change_B'].abs() * MAGNIFICATION))

        # condition_2
        change_le_A_OR_B_CHANGE_data = FunctionHandler.filter_by_column(total_data, 'change', 'le',
                                                                        A_OR_B_CHANGE).copy()

        change_le_A_OR_B_CHANGE_data.sort_values('time', ascending=True, inplace=True)

        # 计算A和B前6小时的范围
        base_data['time_window_A_start'] = base_data['time_A'] - timedelta(hours=6)
        base_data['time_window_B_start'] = base_data['time_B'] - timedelta(hours=6)

        unique_coin_spider = base_data[['coin_name', 'spider_web']].drop_duplicates()

        # 为了提高效率，我们提前为每个 coin_name 和 spider_web 提取符合条件的变化数据
        valid_changes = defaultdict(pd.DataFrame)
        for _, row in unique_coin_spider.iterrows():
            coin_name = row['coin_name']
            cur_spider_web = row['spider_web']

            # 提取符合条件的前6小时数据
            valid_changes[(coin_name, cur_spider_web)] = change_le_A_OR_B_CHANGE_data[
                (change_le_A_OR_B_CHANGE_data['coin_name'] == coin_name) &
                (change_le_A_OR_B_CHANGE_data['spider_web'] == cur_spider_web)
                ]

        if not valid_changes:
            return

        # 判断 A 和 B 时刻前6小时内是否存在符合条件的数据
        def check_change_in_window(row):
            coin_name = row['coin_name']
            spider_web = row['spider_web']
            time_window_A_start = row['time_window_A_start']
            time_window_B_start = row['time_window_B_start']

            # 从预先筛选好的数据中提取 A 和 B 时间窗口内的符合条件数据
            condition_A = valid_changes.get((coin_name, spider_web), pd.DataFrame())
            condition_A = condition_A[
                (condition_A['time'] >= time_window_A_start) & (condition_A['time'] < row['time_A'])]

            # 找出A中开盘价的最大值
            if not condition_A.empty:
                max_open_price_A = condition_A['open'].max()
                conform_A = max_open_price_A >= max(row['close_A'], row['open_A'])
            else:
                conform_A = False

            condition_B = valid_changes.get((coin_name, spider_web), pd.DataFrame())
            condition_B = condition_B[
                (condition_B['time'] >= time_window_B_start) & (condition_B['time'] < row['time_B'])]

            if not condition_B.empty:
                # 找出B中开盘价的最大值
                max_open_price_B = condition_B['open'].max()
                conform_B = max_open_price_B >= max(row['close_B'], row['open_B'])
            else:
                conform_B = False

            res = conform_A or conform_B
            return res

        # 向量化操作，使用 pandas 内置的 `apply` 处理每一行
        base_data['condition_met'] = base_data.apply(check_change_in_window, axis=1)

        # 满足条件1
        conform_condition_1_data = base_data[condition_1].copy()
        conform_condition_1_data['condition'] = f'change <={CHANGE}({spider_web})'
        # 满足条件2
        conform_condition_2_data = base_data[base_data['condition_met']].copy()
        conform_condition_2_data['condition'] = f'A或者B前6小时存在跌幅 <= {A_OR_B_CHANGE}%({spider_web})'

        conform_condition_1_and_2_data = pd.concat([conform_condition_1_data, conform_condition_2_data],
                                                   ignore_index=True)

        conform_condition_1_and_2_data.drop(columns=['condition_met', 'time_window_A_start', 'time_window_B_start'],
                                            inplace=True)
        return conform_condition_1_and_2_data

    def filter_by_international_change(self, spider_web: str, base_data: pd.DataFrame, total_data: pd.DataFrame,
                                       config: dict):
        """
        binance数据：当前时刻相较于国际时间（国内8点）的跌涨幅 <= 4%

	    其他网站数据：当前时刻相较于国际时间的跌涨幅 <=  -5%
        :param spider_web:
        :param base_data:
        :param total_data:
        :param config:
        :return:
        """
        if spider_web == 'binance':
            CHANGE_ON_INTERNATIONAL_TIME_KEY = 'CHANGE_ON_INTERNATIONAL_TIME_BINANCE'
        else:
            CHANGE_ON_INTERNATIONAL_TIME_KEY = 'CHANGE_ON_INTERNATIONAL_TIME_OTHER'
        CHANGE_ON_INTERNATIONAL_TIME = Decimal(config[CHANGE_ON_INTERNATIONAL_TIME_KEY])
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
        merged_data = base_data[['coin_name', 'spider_web', 'coin_price_C']].drop_duplicates().merge(
            international_data[['coin_name', 'spider_web', 'coin_price']],
            on=['coin_name', 'spider_web'], how='left', suffixes=('_close', '_open'))
        merged_data['change'] = (merged_data['coin_price_C'] - merged_data['coin_price']) / merged_data[
            'coin_price'] * Decimal(100)

        conform_condition_data = merged_data[merged_data['change'] <= CHANGE_ON_INTERNATIONAL_TIME].copy()
        return conform_condition_data

    def filter_by_hour_and_minute(self, cur_data: pd.DataFrame, unit_time: str, file_path: str):
        """
        1.以分钟为单位的函数：每一个时刻的C价格 <= 上一个C时刻价格的99%
        2.以小时为单位的函数：每一个时刻的C价格 <= 上一个C时刻价格的99%

        表中同一个AB时刻的同一币种出现次数最大为5次。
        对于一个币种，记录的时间跨度最多为23小时。超过23小时则删除该条件记录。
        columns = coin_name spider_web time_A time_B lasted_price_C_minute lasted_price_C_hour first_record_time  cnt
        :return:
        """


        if unit_time == 'hour':
            price_column = 'lasted_price_C_hour'
        else:
            price_column = 'lasted_price_C_minute'

        # 读取文件
        try:
            record_data = pd.read_csv(file_path, encoding='utf-8', low_memory=False)
        except FileNotFoundError:
            record_data = pd.DataFrame(
                columns=['coin_name', 'spider_web', 'time_A', 'time_B', 'lasted_price_C_minute', 'lasted_price_C_hour',
                         'first_record_time', 'cnt'])

        record_data['first_record_time'] = pd.to_datetime(record_data['first_record_time'])
        record_data['time_A'] = pd.to_datetime(record_data['time_A'])
        record_data['time_B'] = pd.to_datetime(record_data['time_B'])
        record_data['lasted_price_C_minute'] = record_data['lasted_price_C_minute'].apply(Decimal)
        record_data['lasted_price_C_hour'] = record_data['lasted_price_C_hour'].apply(Decimal)
        # 过滤掉时间超出23小时的数据
        pre_datetime = self.datetime - timedelta(hours=23)
        record_data = record_data[record_data['first_record_time'] >= pre_datetime].copy()

        # 分批处理
        merged_data = record_data.merge(cur_data, on=['coin_name', 'spider_web', 'time_A', 'time_B'],
                                        how='outer')
        merged_data[price_column] = merged_data[price_column].fillna(merged_data['coin_price_C'])

        # 只存在于数据:cnt为空
        only_exist_data = merged_data[merged_data['cnt'].isna()].copy()
        only_exist_data['cnt'] = only_exist_data['cnt'].fillna(1)
        only_exist_data['first_record_time'] = self.datetime
        # 只存在于文件: coin_price_C为空
        only_exist_file = merged_data[merged_data['coin_price_C'].isna()].copy()  # 不做处理

        # 共同存在于数据和文件: cnt和 coin_price_C均不为空
        both_exits_file_and_data = merged_data[~merged_data['cnt'].isna() & ~merged_data['coin_price_C'].isna()].copy()
        both_exits_file_and_data['condition_met'] = both_exits_file_and_data['coin_price_C'] <= \
                                                    both_exits_file_and_data[price_column] * Decimal('0.995')
        both_exits_file_and_data['cnt'] = both_exits_file_and_data.apply(
            lambda x: x['cnt'] + 1 if x['condition_met'] else x['cnt'], axis=1)
        both_exits_file_and_data[price_column] = both_exits_file_and_data.apply(
            lambda x: x['coin_price_C'] if x['condition_met'] else x[price_column], axis=1)

        # 合并当前时刻异常的数据
        both_exits_abnormal_data = both_exits_file_and_data[both_exits_file_and_data['condition_met'] == True].copy()
        both_exits_file_and_data.drop(columns=['condition_met'], inplace=True)
        both_exits_abnormal_data.drop(columns=['condition_met'], inplace=True)
        cur_abnormal_data = pd.concat([only_exist_data, both_exits_abnormal_data], ignore_index=True)
        # 找出cnt小于等于5的数据
        group_by_coin_data = cur_abnormal_data[['coin_name', 'cnt']].groupby('coin_name').sum().reset_index(drop=False)
        coin_names = group_by_coin_data[group_by_coin_data['cnt'] <= 5]['coin_name'].values

        abnormal_data = cur_abnormal_data[cur_abnormal_data['coin_name'].isin(coin_names)].copy()
        # 写回文件的数据
        write_to_file = pd.concat([only_exist_file, both_exits_file_and_data, only_exist_data], ignore_index=True)
        write_to_file = write_to_file[
            ['coin_name', 'spider_web', 'time_A', 'time_B', 'lasted_price_C_minute', 'lasted_price_C_hour',
             'first_record_time', 'cnt']].copy()
        write_to_file.to_csv(file_path, mode='w', index=False, encoding='utf-8', header=True)

        abnormal_data = self.round_and_simple_data(abnormal_data, 2)
        return abnormal_data.copy()

    def add_function(self, funcs: list):
        # 添加以小时为单位的函数
        self.functions = funcs

    def execute_all(self):
        # 执行所有函数
        self.results = []
        for func in self.functions:
            if not self.range_data_hours.empty:
                try:
                    res = func()
                    if res:
                        self.results.append(res)
                except Exception as e:
                    logger.exception(e)


if __name__ == "__main__":
    data = pd.read_csv(r'test_filter_C_by_price_lt_pre_hours_low_price_range_data.csv', encoding='utf-8')
    data['time'] = pd.to_datetime(data['time'])
    range_data = pd.read_csv(r'test_filter_by_price_comparison_data.csv')

    data = CSVReader.change_column_type_to_Decimal(data, only_price=False)
    range_data = CSVReader.change_column_type_to_Decimal(range_data, only_price=True)
    range_data['time'] = pd.to_datetime(range_data['time'])
    start_time = datetime(2024, 11, 18, 15, 0, 0)
    end_time = start_time - timedelta(hours=3)
    range_data = FunctionHandler.filter_by_datetime(range_data, end_time, start_time, inclusive='left')
    # range_data = range_data.rename(columns={'time': 'time_B', 'low':'low_B'})

    res = FunctionHandler.filter_C_by_price_lt_pre_hours_low_price(data, range_data, 'hour')
    pass
