import os
import pandas as pd
from datetime import timedelta, datetime
from decimal import Decimal
from dataio.csv_handler import CSVReader
from function_handler.functionhandler import FunctionHandler
from msg_log.mylog import get_logger

PROJECT_ROOT_PATH = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

logger = get_logger(__name__, filename=os.path.join(PROJECT_ROOT_PATH, 'log', 'minute_function.log'))


def find_A_close_gt_B_close_Exclusive_of_C_close(group):
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
                    'coin_price': group.iloc[j]['coin_price_C'],
                    'virtual_drop_A': group.iloc[i]['virtual_drop'],
                    'virtual_drop_B': group.iloc[j]['virtual_drop'],
                    'change_A': group.iloc[i]['change'],
                    'change_B': group.iloc[j]['change']
                })
                res_data = pd.DataFrame(result)
                return res_data
    res_data = pd.DataFrame(
        columns=['coin_name', 'spider_web', 'time_A', 'time_B', 'coin_price', 'virtual_drop_A', 'virtual_drop_B',
                 'change_A',
                 'change_B'])
    return res_data


class MinuteFunctionHandler(FunctionHandler):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.record_data_floder_path = os.path.join(PROJECT_ROOT_PATH, 'function_handler', 'record_data')
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

        to_be_filter_data['cnt'] = 1
        merged_data = record_data.merge(to_be_filter_data, on=primary_key_columns, how='outer',
                                        suffixes=('', '_filter'))

        merged_data['cnt_filter'] = merged_data['cnt_filter'].fillna(0).infer_objects().astype(int)
        merged_data['cnt'] = merged_data['cnt'].fillna(0).infer_objects().astype(int)
        merged_data['cnt'] = merged_data['cnt'] + merged_data['cnt_filter']

        if 'first_price' in diff_columns:
            merged_data['first_price'] = merged_data['first_price'].fillna(merged_data['coin_price'])
            # 用无穷大填充
            merged_data['coin_price'] = merged_data['coin_price'].fillna(Decimal('Infinity'))
        # 对时间列的名称进行升序排序，找出第一个时间列的名称

        first_time_col = sorted(time_columns)[0]

        start_datetime = (self.datetime - timedelta(hours=MAX_TIME_INTERVAL))

        update_data = merged_data[merged_data[first_time_col].between(start_datetime, self.datetime, 'both')]

        # 根据coin_name和spider_web筛选出to_be_filter_data数据
        coin_spider_data = to_be_filter_data[primary_key_columns]
        filter_data = update_data.merge(coin_spider_data, on=primary_key_columns, how='inner')
        filter_condition = ((filter_data['cnt'] == 1) | (
                filter_data['cnt'] <= 3 & (filter_data['coin_price'] < filter_data['first_price'])))
        filter_data = filter_data[filter_condition]

        # 去除带 _filter 后缀的临时列
        filter_data = filter_data.loc[:, ~filter_data.columns.str.endswith('_filter')]
        filter_data = filter_data[primary_key_columns + ['virtual_drop_A', 'virtual_drop_B']]
        update_data = update_data.loc[:, ~update_data.columns.str.endswith('_filter')]
        update_data.drop(columns=['coin_price', 'virtual_drop_A', 'virtual_drop_B'], inplace=True)
        return filter_data, update_data

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

    def change_and_virtual_drop_and_price_func_1_minute(self):
        """
        有ABC三个时刻。其中C为当前时刻（当前分钟）
	    A时刻与C时刻不超过MAX_TIME_INTERVAL小时。
	    A时刻在B时刻之前，A和B时刻均要满足虚降>=AB_VIRTUAL_DROP%,且跌涨幅<=AB_CHANGE%
	    C时刻收盘价同时小于A和B的最低价。
	    A时刻收盘价大于B时刻收盘价。
	    后面两次的价格必须小于第一次异常的价格才记录
	    对于同一时刻的异常股票最多只能提示三次（发送邮件）

	    新增条件：
	    虚降 >= 跌幅的ADDITIONAL_VIRTUAL_DROP_AND_CHANGE_MAGNIFICATION倍，跌幅 <= ADDITIONAL_CHANGE（此条件两次异常中只需要满足一次即可）

        :return:
        """
        logger.info('开始执行每分钟函数1：change_and_virtual_drop_and_price_func_1_minute')
        RECORD_DATA_PATH = os.path.join(self.record_data_floder_path, 'func_1.csv')
        MAX_TIME_INTERVAL = 24
        AB_CHANGE = 1
        AB_VIRTUAL_DROP = 1
        ADDITIONAL_VIRTUAL_DROP_AND_CHANGE_MAGNIFICATION = Decimal('1.2')
        ADDITIONAL_CHANGE = Decimal('-0.6')

        C_data = self.data.copy()

        range_data = self.range_data
        # self.datetime = datetime(2024,11,6, 10, 0, 0)
        datetime_A = self.datetime - timedelta(hours=MAX_TIME_INTERVAL)
        data_A_to_B = range_data[range_data['time'].between(datetime_A, self.datetime, inclusive='both')]

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

        A_close_gt_B_close_data = group_by_coin_and_spiderweb.apply(
            find_A_close_gt_B_close_Exclusive_of_C_close).dropna(axis=1, how='all').reset_index(drop=True)
        if A_close_gt_B_close_data.empty:
            logger.warning('无符合条件的数据')
            A_close_gt_B_close_data = pd.DataFrame(
                columns=['coin_name', 'spider_web', 'time_A', 'time_B', 'coin_price', 'virtual_drop_A',
                         'virtual_drop_B', 'change_A',
                         'change_B'])

        new_condition = ((A_close_gt_B_close_data['change_A'] <= ADDITIONAL_CHANGE) & (
                A_close_gt_B_close_data['virtual_drop_A'] >= A_close_gt_B_close_data[
            'change_A'].abs() * ADDITIONAL_VIRTUAL_DROP_AND_CHANGE_MAGNIFICATION)) | (
                                (A_close_gt_B_close_data['change_B'] <= ADDITIONAL_CHANGE) & (
                                A_close_gt_B_close_data['virtual_drop_B'] >= A_close_gt_B_close_data[
                            'change_B'].abs() * ADDITIONAL_VIRTUAL_DROP_AND_CHANGE_MAGNIFICATION))

        conform_new_condition_data = A_close_gt_B_close_data[new_condition]

        # 更新记录数据
        try:
            record_data = pd.read_csv(RECORD_DATA_PATH, encoding='utf-8')
            for column in record_data.columns:
                if 'time' in column:  # 检查列名是否包含"时间"
                    record_data[column] = pd.to_datetime(record_data[column])
        except FileNotFoundError:
            record_data = pd.DataFrame(columns=['coin_name', 'spider_web', 'time_A', 'time_B', 'first_price', 'cnt'])

        filter_data, record_data = self.filter_and_update_data(
            conform_new_condition_data[['coin_name', 'spider_web', 'time_A', 'time_B', 'coin_price', 'virtual_drop_A', 'virtual_drop_B']].copy(), record_data,
            MAX_TIME_INTERVAL)


        if not record_data.empty:
            record_data.to_csv(RECORD_DATA_PATH, index=False, encoding='utf-8', date_format='%Y-%m-%d %H:%M:%S')

        cur_res_list = []
        if not filter_data.empty:
            logger.info(filter_data.to_string(index=False))
            filter_data = filter_data.drop_duplicates('coin_name', keep='first')
            filter_data.sort_values(by='coin_name', ascending=True, inplace=True)
            func_desc = (f"[分钟函数1]{len(filter_data['coin_name'])}只股票异常有ABC三个时刻。其中C为当前时刻。"
                         f"A时刻与C时刻不超过24小时。A时刻在B时刻之前，A和B时刻均要满足虚降>={AB_VIRTUAL_DROP}%且跌涨幅<={AB_CHANGE}%，A时刻收盘价大于B时刻收盘价。"
                         "C时刻价格同时小于A和B的最低价，后面两次异常价格低于第一次异常时价格。"
                         f"虚降>=跌幅的{str(ADDITIONAL_VIRTUAL_DROP_AND_CHANGE_MAGNIFICATION)}倍, 跌幅<={str(ADDITIONAL_CHANGE)}")
            cur_res_list.append(func_desc)
            cur_res_list.append(filter_data[['coin_name', 'virtual_drop_A', 'virtual_drop_B']].to_string(index=False, header=True))

            cur_res_str = '\n'.join(cur_res_list)
            return cur_res_str
        else:
            logger.info('每分钟函数1执行完毕，无异常')

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
    data = pd.read_csv(r'D:\PythonCode\virtual_currency-3.0\data_00.csv')
    data['coin_price'] = data['coin_price'].apply(Decimal)
    data['time'] = pd.to_datetime(data['time'])
    minute_handler = MinuteFunctionHandler(data=data, datetime=datetime(2024, 11, 8, 22, 19, 0),
                                           reader=csvreader)
    minute_handler.get_range_data_hours(start_datetime=datetime(2024, 11, 7, 22, 19, 0),
                                        end_datetime=datetime(2024, 11, 8, 22, 19, 0), inclusive='left')
    # res = minute_handler.current_price_compare_with_45_day_max_price()
    res = minute_handler.change_and_virtual_drop_and_price_func_1_minute()
    pass
