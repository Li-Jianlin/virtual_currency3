import pandas as pd
import os

from dateutil.relativedelta import relativedelta

from msg_log.mylog import get_logger
import math
from datetime import datetime, timedelta
from typing import Literal
from decimal import Decimal

PROJECT_ROOT_PATH = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
logger = get_logger(__name__, filename=os.path.join(PROJECT_ROOT_PATH, 'log', f'csv_handler.log' ))


def make_sure_path_exists(path):
    """
    判断路径是否存在，不存在则创建
    :param path: 文件路径
    :return:
    """
    os.makedirs(path, exist_ok=True)


class CSVReader:
    """从csv文件中读取想要的数据"""

    def __init__(self, data_region: str = 'China', **kwargs):
        """

        :param data_region:表示地区，China表示国内数据，Foreign表示国际数据
        :param spider_web: 爬取数据的网站
        :param kwargs:
        """
        self.data_region = data_region
        self.base_file_path = kwargs.get('base_file_path', os.path.join(PROJECT_ROOT_PATH, 'data', self.data_region))
    @staticmethod
    def change_data_type(data: pd.DataFrame, only_price: bool = True):
        """将数据中数据列的数据类型转换为Decimal"""
        if only_price:
            data['coin_price'] = data['coin_price'].apply(Decimal)
        else:
            data['coin_price'] = data['coin_price'].apply(Decimal)
            data['high'] = data['high'].apply(Decimal)
            data['low'] = data['low'].apply(Decimal)
            data['open'] = data['open'].apply(Decimal)
            data['close'] = data['close'].apply(Decimal)
            data['change'] = data['change'].apply(Decimal)
            data['amplitude'] = data['amplitude'].apply(Decimal)
            data['virtual_drop'] = data['virtual_drop'].apply(Decimal)

        return data

    def get_fillna_data(self, filled_date: datetime):
        """读取用于填充指定网站缺失值的数据"""
        filled_date = filled_date - timedelta(hours=1)
        filled_data_path = os.path.join(self.base_file_path, f"{filled_date.year}-{filled_date.month}",
                                        f"{filled_date.day}.csv")
        try:
            target_web_data = pd.read_csv(filled_data_path, low_memory=False, encoding='utf-8', dtype='str')
        except FileNotFoundError:
            logger.warning(f'{filled_data_path}文件不存在')
            target_web_data = pd.DataFrame()
        return target_web_data

    def get_previous_all_data(self, cur_datetime: datetime, unit_time: Literal['hour', 'day']):
        """读取出前一个单位时间数据所在的文件中所有数据"""
        if unit_time == 'hour':
            previous_datetime = cur_datetime - timedelta(hours=1)
            target_file_name = f'{previous_datetime.day}.csv'
        elif unit_time == 'day':
            previous_datetime = cur_datetime - timedelta(days=1)
            target_file_name = f'all_midnight.csv'
        else:
            raise KeyError('unit_time参数只能是hour或day')
        target_file_path = os.path.join(self.base_file_path, f'{previous_datetime.year}-{previous_datetime.month}',
                                        target_file_name)
        try:
            target_data = pd.read_csv(target_file_path, low_memory=False, encoding='utf-8', dtype='str')
            target_data = self.change_data_type(target_data, only_price=False)
            target_data['time'] = pd.to_datetime(target_data['time'])
        except FileNotFoundError:
            logger.warning(f'{target_file_path}文件不存在')
            target_data = pd.DataFrame()
        return target_data

    def get_detail_data(self):
        """获取前一个小时的所有详细数据"""
        detail_data_path = os.path.join(self.base_file_path, 'detail_data.csv')
        try:
            detail_data = pd.read_csv(detail_data_path, low_memory=False, encoding='utf-8', dtype='str')
            detail_data = self.change_data_type(detail_data, only_price=True)
            detail_data['time'] = pd.to_datetime(detail_data['time'])
        except FileNotFoundError:
            logger.warning(f'{detail_data_path}文件不存在')
            detail_data = pd.DataFrame()
        return detail_data

    def get_data_between_hours(self, start_datetime: datetime, end_datetime: datetime, inclusive: Literal["both", "neither", "left", "right"] = "both" ):
        """根据起始和终止时间读取数据
        :param end_datetime:
        :param start_datetime:
        :param inclusive: 决定选择文件的边界。both:[], left:[), right: (], neither: ()
        """
        delta_time = math.ceil((end_datetime - start_datetime).total_seconds() / 3600)
        file_count = math.floor(delta_time / 24)
        datetime_list = [end_datetime - timedelta(days=i) for i in range(file_count + 1)]
        file_path_list = [
            os.path.join(self.base_file_path, f'{cur_datetime.year}-{cur_datetime.month}', f'{cur_datetime.day}.csv')
            for cur_datetime in datetime_list]
        combined_data = pd.DataFrame()
        for file_path in file_path_list:
            try:
                target_data = pd.read_csv(file_path, low_memory=False, encoding='utf-8', dtype='str')
                target_data = self.change_data_type(target_data, only_price=False)

            except FileNotFoundError:
                logger.warning(f'{file_path}文件不存在')
                target_data = pd.DataFrame()
            else:
                combined_data = pd.concat([combined_data, target_data], axis=0, ignore_index=True)
        if not combined_data.empty:
            combined_data['time'] = pd.to_datetime(combined_data['time'], format='%Y-%m-%d %H:%M:%S')
            combined_data = combined_data[combined_data['time'].between(start_datetime, end_datetime, inclusive=inclusive)]
        return combined_data


    def get_data_between_days(self, start_datetime: datetime, end_datetime: datetime, inclusive: Literal["both", "neither", "left", "right"] = "both" ):
        """根据起始和终止时间读取数据
        :param start_datetime:
        :param end_datetime:
        :param inclusive: both:[], left:[), right: (], neither: ()
        """
        months_diff = (end_datetime.year - start_datetime.year) * 12 + end_datetime.month - start_datetime.month

        if end_datetime.day - start_datetime.day > 0:
            months_diff += 1

        file_count = months_diff

        datetime_list = [end_datetime - relativedelta(months=i) for i in range(file_count)]

        file_path_list = [os.path.join(self.base_file_path, f'{cur_datetime.year}-{cur_datetime.month}', 'all_midnight.csv') for cur_datetime in datetime_list]
        combined_data = pd.DataFrame()
        for file_path in file_path_list:
            try:
                target_data = pd.read_csv(file_path, low_memory=False, encoding='utf-8', dtype='str')
            except FileNotFoundError:
                continue
            else:
                combined_data = pd.concat([combined_data, target_data], ignore_index=True)
        if not combined_data.empty:
            combined_data['time'] = pd.to_datetime(combined_data['time'])
            combined_data = combined_data[combined_data['time'].between(start_datetime, end_datetime, inclusive=inclusive)]
            combined_data = self.change_data_type(combined_data, only_price=False)
        return combined_data

    def get_statistical_table(self, unit_time: Literal['hour', 'day']):
        """获取n小时或n天的统计数据。"""
        if unit_time == 'hour':
            target_file_name = 'statistical_table_hour.csv'
        else:
            target_file_name = 'statistical_table_day.csv'
        target_file_path = os.path.join(self.base_file_path, target_file_name)
        try:
            statistical_table = pd.read_csv(target_file_path, low_memory=False, encoding='utf-8', dtype='str')
        except FileNotFoundError:
            logger.warning(f'{target_file_path}文件不存在')
            statistical_table = pd.DataFrame()
        return statistical_table


class CSVWriter:
    """向csv中写入数据"""

    def __init__(self, data_region: str = 'China', **kwargs):
        self.data_region = data_region
        self.base_file_path = kwargs.get('base_file_path', os.path.join(PROJECT_ROOT_PATH, 'data', self.data_region))
        self.is_check = False

    def write_data(self, data: pd.DataFrame, unit_time: Literal['hour', 'day']):
        """写入数据"""
        data['time'] = pd.to_datetime(data['time'])
        cur_timedate = data.iloc[0]['time']
        target_file_folder = os.path.join(self.base_file_path, f'{cur_timedate.year}-{cur_timedate.month}')
        make_sure_path_exists(target_file_folder)
        if unit_time == 'hour':
            target_file_name = f'{cur_timedate.day}.csv'
        elif unit_time == 'day':
            target_file_name = f'all_midnight.csv'
        else:
            raise KeyError('unit_time参数只能是hour或day')
        target_file_path = os.path.join(target_file_folder, target_file_name)
        # data['time'] = data['time'].dt.strftime("%Y-%m-%d %H:%M:%S")
        if os.path.exists(target_file_path):
            data.to_csv(target_file_path, mode='a', header=False, index=False, encoding='utf-8', date_format='%Y-%m-%d %H:%M:%S')
        else:
            data.to_csv(target_file_path, index=False, encoding='utf-8', date_format='%Y-%m-%d %H:%M:%S')

    def write_detail_data(self, data: pd.DataFrame):
        """将当前爬取的详细数据写入到detail_data文件中"""
        data['time'] = pd.to_datetime(data['time'])
        cur_datetime = data.iloc[0]['time']
        target_file_folder = self.base_file_path
        make_sure_path_exists(target_file_folder)
        target_file_name = f'detail_data.csv'
        target_file_path = os.path.join(target_file_folder, target_file_name)

        # 中途程序断开后，重新启动时需要判断文件中的数据是否为当前时刻的详细数据
        if not self.is_check:
            if os.path.exists(target_file_path):
                checked_data = pd.read_csv(target_file_path, low_memory=False, encoding='utf-8')
                checked_data['time'] = pd.to_datetime(checked_data['time'])
                # 不是当前时刻的数据则删除
                file_datetime = checked_data.iloc[0]['time']
                if file_datetime.hour != cur_datetime.hour:
                    logger.info(f'{target_file_path}文件中存在非当前时刻数据，删除文件重新写入')
                    os.remove(target_file_path)
            self.is_check = True

        if cur_datetime.minute == 0 or not os.path.exists(target_file_path):
            data.to_csv(target_file_path, index=False, encoding='utf-8', date_format='%Y-%m-%d %H:%M:%S')
        else:
            data.to_csv(target_file_path, mode='a', header=False, index=False, encoding='utf-8', date_format='%Y-%m-%d %H:%M:%S')
        logger.info(f'写入详细数据成功-{self.data_region}')

    def write_statistical_table(self, statictical_table, unit_time: Literal['hour', 'day']):
        """将表写回文件"""
        if unit_time == 'hour':
            target_file_name = 'statistical_table_hour.csv'
        else:
            target_file_name = 'statistical_table_day.csv'
        target_file_path = os.path.join(self.base_file_path, target_file_name)
        statictical_table.to_csv(target_file_path, index=False, encoding='utf-8')

if __name__ == '__main__':
    file_path = '../data/China/2020-05'
    # make_sure_path_exists(file_path)
    df = pd.DataFrame({'coin': ['a', 'b'], 'time': ['2020-05-01 01:01:01', '2020-05-01 01:01:01'],
                       'spider_web': ['binance', 'binance']})
    writer = CSVWriter('China')
    print(writer.base_file_path)
    writer.data_region = 'Foreign'
    print(writer.base_file_path)
    writer.write_data(df, 'hour')
    writer.write_detail_data(df)
    pass

