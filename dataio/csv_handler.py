import pandas as pd
import os
from msg_log.mylog import get_logger
import pandas as pd
from datetime import datetime, timedelta
from typing import Literal
from decimal import Decimal
logger = get_logger(__name__)


def make_sure_path_exists(path):
    """
    判断路径是否存在，不存在则创建
    :param path: 文件路径
    :return:
    """
    os.makedirs(path, exist_ok=True)


class CSVReader:
    """从csv文件中读取想要的数据"""

    def __init__(self, data_region: str, **kwargs):
        """

        :param data_region:表示地区，China表示国内数据，Foreign表示国际数据
        :param spider_web: 爬取数据的网站
        :param kwargs:
        """
        self.data_region = data_region
        self.base_file_path = kwargs.get(base_file_path, os.path.join('data', self.data_region))

    def get_fillna_data(self, filled_date: str):
        """读取用于填充指定网站缺失值的数据"""
        filled_date = (datetime.strptime(filled_date, '%Y-%m-%d %H:%M:%S')) - timedelta(hours=1)
        filled_data_path = os.path.join(self.base_file_path, f'{filled_date.year}-{filled_date.month}',
                                        f'{filled_date.day}.csv')
        try:
            target_web_data = pd.read_csv(filled_data_path, low_memory=False, encoding='utf-8', dtype='str')
        except FileNotFoundError:
            logger.warning(f'{filled_data_path}文件不存在')
            target_web_data = pd.DataFrame()
        return target_web_data

    def get_previous_all_data(self, cur_date: str, unit_time: Literal['hour', 'day']):
        """读取出前一个单位时间数据所在的文件中所有数据"""
        if unit_time == 'hour':
            previous_datetime = (datetime.strptime(cur_date, '%Y-%m-%d %H:%M:%S')) - timedelta(hours=1)
            target_file_name = f'{previous_datetime.day}.csv'
        elif unit_time == 'day':
            previous_datetime = (datetime.strptime(cur_date, '%Y-%m-%d %H:%M:%S')) - timedelta(days=1)
            target_file_name = f'all_midnight.csv'
        else:
            raise KeyError('unit_time参数只能是hour或day')
        target_file_path = os.path.join(self.base_file_path, f'{previous_datetime.year}-{previous_datetime.month}',
                                        target_file_name)
        try:
            target_data = pd.read_csv(target_file_path, low_memory=False, encoding='utf-8', dtype='str')
            target_data = target_data['price'].astype(Decimal)
        except FileNotFoundError:
            logger.warning(f'{target_file_path}文件不存在')
            make_sure_path_exists(target_file_path)
            target_data = pd.DataFrame()
        return target_data

    def get_detail_data(self, cur_date: str):
        """获取前一个小时的所有详细数据"""
        cur_datetime = datetime.strptime(cur_date, '%Y-%m-%d %H:%M:%S')
        detail_data_path = os.path.join(self.base_file_path, f'{cur_datetime.year}-{cur_datetime.month}',
                                        'detail_data.csv')
        try:
            detail_data = pd.read_csv(detail_data_path, low_memory=False, encoding='utf-8', dtype='str')
            detail_data = detail_data['price'].astype(Decimal)
        except FileNotFoundError:
            logger.warning(f'{detail_data_path}文件不存在')
            detail_data = pd.DataFrame()
        return detail_data

    def get_data_between_hours(self, start_time: str, end_time: str):
        """根据起始和终止时间读取数据"""
        start_datetime = datetime.strptime(start_time, '%Y-%m-%d %H:%M:%S')
        end_datetime = datetime.strptime(end_time, '%Y-%m-%d %H:%M:%S')
        delta_time = math.ceil((end_datetime - start_datetime).total_seconds() / 3600)
        file_count = math.floor(delta_time / 24)
        datetime_list = [start_datetime - timedelta(days=i) for i in range(file_count)]
        file_path_list = [
            os.path.join(self.base_file_path, f'{cur_datetime.year}-{cur_datetime.month}', f'{cur_datetime.day}.csv')
            for cur_datetime in datetime_list]
        combined_data = pd.DataFrame()
        for file_path in file_path_list:
            try:
                target_data = pd.read_csv(file_path, low_memory=False, encoding='utf-8', dtype='str')
                target_data = target_data['price'].astype(Decimal)
            except FileNotFoundError:
                logger.warning(f'{file_path}文件不存在')
                target_data = pd.DataFrame()
            else:
                combined_data = pd.concat([combined_data, target_data], axis=0, ignore_index=True)
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

    def __init__(self, data_region: str):
        self.data_region = data_region
        self.base_file_path = os.path.join('data', self.data_region)
        self.is_check = False

    def write_data(self, data: pd.DataFrame, unit_time: Literal['hour', 'day']):
        """写入数据"""
        cur_time = data.iloc[0]['time']
        cur_timedate = datetime.strptime(cur_time, '%Y-%m-%d %H:%M:%S')
        target_file_folder = os.path.join(self.base_file_path, f'{cur_timedate.year}-{cur_timedate.month}')
        make_sure_path_exists(target_file_folder)
        if unit_time == 'hour':
            target_file_name = f'{cur_timedate.day}.csv'
        elif unit_time == 'day':
            target_file_name = f'all_midnight.csv'
        else:
            raise KeyError('unit_time参数只能是hour或day')
        target_file_path = os.path.join(target_file_folder, target_file_name)
        if os.path.exists(target_file_path):
            data.to_csv(target_file_path, mode='a', header=False, index=False, encoding='utf-8')
        else:
            data.to_csv(target_file_path, index=False, encoding='utf-8')

    def write_detail_data(self, data: pd.DataFrame):
        """将当前爬取的详细数据写入到detail_data文件中"""
        cur_time = data.iloc[0]['time']
        cur_datetime = datetime.strptime(cur_time, '%Y-%m-%d %H:%M:%S')
        target_file_folder = os.path.join(self.base_file_path, f'{cur_datetime.year}-{cur_datetime.month}')
        make_sure_path_exists(target_file_folder)
        target_file_name = f'detail_data.csv'
        target_file_path = os.path.join(target_file_folder, target_file_name)

        # 中途程序断开后，重新启动时需要判断文件中的数据是否为当前时刻的详细数据
        if not self.is_check:
            if os.path.exists(target_file_path):
                checked_data = pd.read_csv(target_file_path, low_memory=False, encoding='utf-8')
                # 不是当前时刻的数据则删除
                file_time = checked_data.iloc[-1]['time']
                file_datetime = datetime.strptime(file_time, '%Y-%m-%d %H:%M:%S')
                if file_datetime.hour != cur_datetime.hour and file_datetime.day != cur_datetime.day:
                    logger.info(f'{target_file_path}文件中存在非当前时刻数据，删除文件重新写入')
                    os.remove(target_file_path)
            self.is_check = True

        if cur_datetime.minute == 0 or not os.path.exists(target_file_path):
            data.to_csv(target_file_path, index=False, encoding='utf-8')
        else:
            data.to_csv(target_file_path, mode='a', header=False, index=False, encoding='utf-8')
        logger.info(f'写入{data.iloc[0]["spider_web"]}网站数据成功')


if __name__ == '__main__':
    file_path = '../data/China/2020-05'
    # make_sure_path_exists(file_path)
    df = pd.DataFrame({'coin': ['a', 'b'], 'time': ['2020-05-01 01:01:01', '2020-05-01 01:01:01'],
                       'spider_web': ['binance', 'binance']})
    writer = CSVWriter('China')
    writer.write_data(df, 'hour')
    writer.write_detail_data(df)
    pass
