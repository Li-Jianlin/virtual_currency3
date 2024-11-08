import os
from typing import Literal
import pandas as pd
from threading import Thread, Lock
from datetime import datetime, timedelta

base_folder = os.path.join(os.path.dirname(__file__), 'log')
if not os.path.exists(base_folder):
    os.makedirs(base_folder, exist_ok=True)

from get_data_by_spider.get_data import DataGetter
from dataio.csv_handler import CSVReader, CSVWriter
from data_process.data_process import DataProcess
from config import SpiderWeb
from msg_log.mylog import get_logger
from function_in_hours.hour_function import HourlyFunctionHandler, DayFunctionHandler, MinuteFunctionHandler
from msg_log.msg_send import send_email
PROJECT_ROOT_PATH = os.path.dirname(os.path.abspath(__file__))
logger = get_logger(__name__, filename=os.path.join(PROJECT_ROOT_PATH, 'log', 'controll.log'))


class ProgramCotroller:
    """整个程序的控制类"""
    base_file_path = {
        'China': os.path.join(PROJECT_ROOT_PATH, 'data', 'China'),
        'Foreign': os.path.join(PROJECT_ROOT_PATH, 'data', 'Foreign')
    }

    def __init__(self, data_region: Literal['China', 'Foreign'], **kwargs):
        logger.info('初始化读写器')
        self.reader = CSVReader(data_region=data_region, base_file_path=self.base_file_path.get(data_region))
        self.writer = CSVWriter(data_region=data_region, base_file_path=self.base_file_path.get(data_region))
        self.cur_datetime = datetime.now().replace(second=0)
        self.cur_time = self.cur_datetime.strftime("%Y-%m-%d %H:%M:%S")
        self.pre_hour_datetime = self.cur_datetime - timedelta(hours=1)
        self.pre_hour_strtime = self.pre_hour_datetime.strftime("%Y-%m-%d %H:%M:%S")
        # 爬虫
        logger.info('初始化数据爬取器')
        self.binance_data_getter = DataGetter(SpiderWeb.BINANCE)
        self.inversting_data_getter = DataGetter(SpiderWeb.INVERSTING)
        self.lock = Lock()
        logger.info('初始化数据处理器')
        self.data_processer = DataProcess(data=pd.DataFrame(), data_region=data_region, unit_time='hour',
                                          writer=self.writer,
                                          reader=self.reader, time=self.cur_time)
        logger.info('初始化成功')

    def change_data_region(self, data_region: Literal['China', 'Foreign']):
        """改变读写器路径"""
        self.reader.base_file_path = self.base_file_path.get(data_region)
        self.writer.base_file_path = self.base_file_path.get(data_region)

    def change_data_processer(self, data: pd.DataFrame, time: str, cur_datetime: datetime, unit_time='hour',
                              data_region='China'):
        """修改数据计算对象的相关属性"""

        self.data_processer.data = data
        self.data_processer.time = time
        self.data_processer.unit_time = unit_time
        self.data_processer.data_region = data_region
        self.data_processer.datetime = cur_datetime

    def add_time_column(self, cur_time: str, data: pd.DataFrame):
        """为当前数据加上时间列"""
        if not data.empty:
            data['time'] = cur_time
        return data

    def get_data(self, data_getter: DataGetter, **kwargs):
        """通过爬虫获取数据"""
        try:
            data_getter.get_data().filter_data()
        except ConnectionError:
            logger.warning('连接失败')
        data = data_getter.data
        with self.lock:
            self.res.append(data)

    def get_data_by_multithreading(self):
        self.threads = []
        self.res = []
        name = ['inversting', 'binance']
        for cur_getter in (self.inversting_data_getter, self.binance_data_getter):
            thread = Thread(target=self.get_data, args=(cur_getter,), daemon=True)
            self.threads.append(thread)
        for cur_thread in self.threads:
            cur_thread.start()
            logger.info(f'线程{cur_thread.name}已启动')
        for cur_thread in self.threads:
            cur_thread.join()
            logger.info(f'线程{cur_thread.name}已结束')

    def calculate_data(self):
        """对数据进行计算"""
        self.data_processer.data.drop_duplicates(subset=['coin_name', 'spider_web'], inplace=True)
        self.data_processer.get_needed_data()

        self.data_processer.calculate_all()
        calculated_data = self.data_processer.data
        return calculated_data

    def get_time(self, cur_datetime: datetime):
        """得到当前时间"""
        self.cur_datetime = cur_datetime
        self.cur_minute = cur_datetime.minute
        self.cur_time = cur_datetime.strftime("%Y-%m-%d %H:%M:%S")
        self.foreign_datetime = cur_datetime - timedelta(hours=8)
        self.foreign_time = self.foreign_datetime.strftime("%Y-%m-%d %H:%M:%S")
        self.pre_hour_datetime = cur_datetime - timedelta(hours=1)
        self.pre_hour_strtime = self.pre_hour_datetime.strftime("%Y-%m-%d %H:%M:%S")

    def hours_data_process(self, combined_data):
        logger.info('计算国内整点数据')
        self.change_data_region('China')
        self.change_data_processer(data=combined_data, time=self.cur_time,
                                         cur_datetime=self.cur_datetime,
                                         unit_time='hour')
        logger.info('开始计算')
        calculated_data = self.calculate_data()
        pre_time = (self.cur_datetime - timedelta(hours=1)).strftime("%Y-%m-%d %H:%M:%S")
        calculated_data['time'] = pre_time
        logger.info('计算完成，写入数据')
        self.writer.write_data(calculated_data, unit_time='hour')
        logger.info('写入完成，生成国际数据')
        foreign_calculated_data = calculated_data.copy()
        foreign_calculated_data['time'] = (self.foreign_datetime - timedelta(hours=1)).strftime(
            "%Y-%m-%d %H:%M:%S")
        self.change_data_region('Foreign')
        self.writer.write_data(foreign_calculated_data, unit_time='hour')
        logger.info('国际数据写入完成')
        return calculated_data

    def days_data_process(self, combined_data, data_region='China'):
        if data_region == 'China':
            logger.info('计算国内0点数据')
            time = self.cur_time
            cur_datetime = self.cur_datetime
        else:
            logger.info('计算国际0点数据')
            time = self.foreign_time
            cur_datetime = self.foreign_datetime
        self.change_data_region(data_region=data_region)
        self.change_data_processer(data=combined_data, time=time,
                                         cur_datetime=cur_datetime,
                                         unit_time='day')
        logger.info('开始计算')
        calculated_data_day = self.calculate_data()
        calculated_data_day['time'] = (cur_datetime - timedelta(days=1)).strftime("%Y-%m-%d %H:%M:%S")
        logger.info('计算完成，写入数据')
        self.writer.write_data(calculated_data_day, unit_time='day')
        logger.info('写入完成')
        return calculated_data_day


logger.info('启动程序')

controller = ProgramCotroller('China')
hourfunctionhandler = HourlyFunctionHandler(data=None, reader=controller.reader, writer=controller.writer,
                                            time=controller.cur_time)
dayfunctionhandler = DayFunctionHandler(data=None, reader=controller.reader, writer=controller.writer,
                                        time=controller.cur_time)
minutefunctionhandler = MinuteFunctionHandler(data=None, reader=controller.reader, writer=controller.writer,
                                              time=controller.cur_time)

hourfunctionhandler.add_function(hourfunctionhandler.change_and_virtual_drop_and_price_func_1)
minutefunctionhandler.add_function(minutefunctionhandler.change_and_virtual_drop_and_price_func_1_minute)
while True:
    cur_datetime = datetime.now()
    if cur_datetime.second == 0:
        # 更新时间
        controller.get_time(cur_datetime)
        logger.info('爬取数据')

        controller.get_data_by_multithreading()

        if not controller.res:
            logger.warning(f"爬取失败,终止后续操作")
            continue
        combined_data = pd.concat(controller.res, ignore_index=True)
        if combined_data.empty:
            logger.warning(f"当前数据为空,终止后续操作")
            continue

        logger.info('为数据增加time列')
        combined_data = controller.add_time_column(controller.cur_time, combined_data)
        combined_data = controller.reader.change_data_type(combined_data, only_price=True)
        foreign_data = combined_data.copy()
        foreign_data['time'] = controller.foreign_time

        # 国内整点(每小时)
        if controller.cur_minute == 0:
            calculated_data = controller.hours_data_process(combined_data=combined_data.copy())

            # 小时函数
            hourfunctionhandler.data = calculated_data
            pre_24_hours_strtime = (controller.pre_hour_datetime - timedelta(days=1)).strftime("%Y-%m-%d %H:%M:%S")
            hourfunctionhandler.get_range_data_hours(pre_24_hours_strtime, controller.cur_time, inclusive='left')
            hourfunctionhandler.execute_all()
            res_hour = '\n'.join(hourfunctionhandler.results)
            if res_hour:
                send_email(subject='每小时函数结果', content=res_hour, test=True)

        # 国内0点
        if cur_datetime.hour == 0 and controller.cur_minute == 0:
            controller.days_data_process(combined_data.copy(), data_region='China')

        # 国际0点（国内8点）
        if cur_datetime.hour == 8 and controller.cur_minute == 0:
            calculated_data_day = controller.days_data_process(foreign_data.copy(), data_region='Foreign')

        # 不是整点
        logger.info('写入详情数据')
        controller.change_data_region('China')
        controller.writer.write_detail_data(combined_data)
        controller.change_data_region('Foreign')
        controller.writer.is_check = False
        controller.writer.write_detail_data(foreign_data)
        logger.info('详情数据写入完成,执行每分钟函数')
        pre_24_hours_strtime = (controller.pre_hour_datetime - timedelta(days=1)).strftime("%Y-%m-%d %H:%M:%S")
        minutefunctionhandler.get_range_data_hours(pre_24_hours_strtime, controller.cur_time, inclusive='left')
        minutefunctionhandler.data = combined_data

        minutefunctionhandler.execute_all()
        res_minute = '\n'.join(minutefunctionhandler.results)
        if res_minute:
            send_email(subject='分钟函数结果', content=res_minute, test=True)
        logger.info('执行完毕')
