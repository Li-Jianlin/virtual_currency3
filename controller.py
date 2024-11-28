import os
import logging
import time
from typing import Literal
import pandas as pd
from threading import Thread, Lock
from datetime import datetime, timedelta
from decimal import Decimal
from xml.etree import ElementTree
import collections

base_folder = os.path.join(os.path.dirname(__file__), "log")
if not os.path.exists(base_folder):
    os.makedirs(base_folder, exist_ok=True)

from get_data_by_spider.get_data import DataGetter
from dataio.csv_handler import CSVReader, CSVWriter, make_sure_path_exists
from data_process.data_process import DataProcess
from config import SpiderWeb, hour_function_description, minute_function_description, ConfigHandler
from msg_log.mylog import get_logger
from function_handler.minute_function_handler import MinuteFunctionHandler
from function_handler.hour_function_handler import HourlyFunctionHandler
from function_handler.day_function_handler import DayFunctionHandler
from msg_log.msg_send import send_email

PROJECT_ROOT_PATH = os.path.dirname(os.path.abspath(__file__))
logger = get_logger(
    __name__, filename=os.path.join(PROJECT_ROOT_PATH, "log", "controll.log")
)
RESULT_FOLDER_PATH = os.path.join(PROJECT_ROOT_PATH, "result")

class ProgramCotroller:
    """整个程序的控制类"""

    base_file_path = {
        "China": os.path.join(PROJECT_ROOT_PATH, "data", "China"),
        "Foreign": os.path.join(PROJECT_ROOT_PATH, "data", "Foreign"),
    }

    def __init__(self, data_region: Literal["China", "Foreign"], **kwargs):
        self.foreign_datetime = None
        self.threads = None
        self.foreign_time = None
        self.cur_minute = None
        logger.info("初始化读写器")
        self.reader = CSVReader(
            data_region=data_region, base_file_path=self.base_file_path.get(data_region)
        )
        self.writer = CSVWriter(
            data_region=data_region, base_file_path=self.base_file_path.get(data_region)
        )

        self.config_handler = ConfigHandler(kwargs.get('config_file'))
        self.cur_datetime = datetime.now().replace(second=0)
        self.cur_time = self.cur_datetime.strftime("%Y-%m-%d %H:%M:%S")
        self.pre_hour_datetime = self.cur_datetime - timedelta(hours=1)
        self.pre_hour_strtime = self.pre_hour_datetime.strftime("%Y-%m-%d %H:%M:%S")
        # 爬虫
        logger.info("初始化数据爬取器")
        self.binance_data_getter = DataGetter(SpiderWeb.BINANCE)
        # self.inversting_data_getter = DataGetter(SpiderWeb.INVERSTING)
        self.coin_stats_data_getter = DataGetter(SpiderWeb.COIN_STATS)
        self.lock = Lock()
        logger.info("初始化数据处理器")
        self.data_processer = DataProcess(
            data=pd.DataFrame(),
            data_region=data_region,
            unit_time="hour",
            writer=self.writer,
            reader=self.reader,
            time=self.cur_time,
            datetime=self.cur_datetime,
        )
        logger.info("初始化成功")

    def change_data_region(self, data_region: Literal["China", "Foreign"]):
        """改变读写器路径"""
        self.reader.base_file_path = self.base_file_path.get(data_region)
        self.writer.base_file_path = self.base_file_path.get(data_region)

    def change_data_processer(
            self,
            data: pd.DataFrame,
            time: str,
            cur_datetime: datetime,
            unit_time="hour",
            data_region="China",
    ):
        """修改数据计算对象的相关属性"""

        self.data_processer.data = data
        self.data_processer.time = time
        self.data_processer.unit_time = unit_time
        self.data_processer.data_region = data_region
        self.data_processer.datetime = cur_datetime

    def add_time_column(self, data: pd.DataFrame):
        """为当前数据加上时间列"""
        if not data.empty:
            data["time"] = self.cur_datetime
        return data

    def get_data(self, data_getter: DataGetter, **kwargs):
        """通过爬虫获取数据"""
        try:
            data_getter.get_data().filter_data()
        except ConnectionError:
            logger.warning("连接失败")
        data = data_getter.data
        with self.lock:
            if not data.empty:
                self.res.append(data)

    def get_data_by_multithreading(self):
        self.threads = []
        self.res = []

        for cur_getter in (
                # self.inversting_data_getter,
                self.binance_data_getter,
                self.coin_stats_data_getter
        ):
            thread = Thread(target=self.get_data, args=(cur_getter,), daemon=True)
            self.threads.append(thread)
        for cur_thread in self.threads:
            cur_thread.start()
            logger.info(f"线程{cur_thread.name}已启动")
        for cur_thread in self.threads:
            cur_thread.join()
            logger.info(f"线程{cur_thread.name}已结束")

    def calculate_data(self):
        """对数据进行计算"""
        self.data_processer.data.drop_duplicates(
            subset=["coin_name", "spider_web"], inplace=True
        )
        self.data_processer.get_needed_data()

        self.data_processer.calculate_all()
        calculated_data = self.data_processer.data
        return calculated_data

    def update_time(self, cur_datetime: datetime):
        """得到当前时间"""
        self.cur_datetime = cur_datetime
        self.cur_minute = cur_datetime.minute
        self.cur_time = cur_datetime.strftime("%Y-%m-%d %H:%M:%S")
        self.foreign_datetime = cur_datetime - timedelta(hours=8)
        self.foreign_time = self.foreign_datetime.strftime("%Y-%m-%d %H:%M:%S")
        self.pre_hour_datetime = cur_datetime - timedelta(hours=1)
        self.pre_day_datetime = cur_datetime - timedelta(days=1)

    @staticmethod
    def update_45_day_max_price(cur_day_data: pd.DataFrame):
        file_path = os.path.join(
            PROJECT_ROOT_PATH, "function_handler", "record_data", "45_day_max_price.csv"
        )
        max_price_45_day_data = pd.read_csv(
            file_path, encoding="utf-8", low_memory=False
        )
        max_price_45_day_data["max_price_in_45_day"] = max_price_45_day_data[
            "max_price_in_45_day"
        ].apply(Decimal)
        cur_day_data = cur_day_data[cur_day_data["spider_web"] == "binance"].copy()
        cur_data_high = (
            cur_day_data.groupby("coin_name")["high"]
            .apply("max")
            .reset_index(drop=False)
        )
        # 如果当前最高价高于45天最高价，则更新45天最高价
        merged_data = max_price_45_day_data.merge(
            cur_data_high, on="coin_name", how="outer"
        )
        merged_data = merged_data.fillna(Decimal("0"))
        merged_data["max_price_in_45_day"] = merged_data.apply(
            lambda x: (
                x["high"]
                if x["high"] > x["max_price_in_45_day"]
                else x["max_price_in_45_day"]
            ),
            axis=1,
        )
        merged_data.drop(columns=["high"], inplace=True)
        merged_data.to_csv(file_path, index=False, encoding="utf-8")

    def hours_data_process(self, combined_data):
        logger.info("计算国内整点数据")
        self.change_data_region("China")
        self.change_data_processer(
            data=combined_data,
            time=self.cur_time,
            cur_datetime=self.cur_datetime,
            unit_time="hour",
        )
        logger.info("开始计算")
        calculated_data = self.calculate_data()
        pre_time = self.cur_datetime - timedelta(hours=1)
        calculated_data["time"] = pre_time
        logger.info("计算完成，写入数据")
        self.writer.write_data(calculated_data, unit_time="hour")
        logger.info("写入完成，生成国际数据")
        foreign_calculated_data = calculated_data.copy()
        foreign_calculated_data["time"] = self.foreign_datetime - timedelta(hours=1)
        self.change_data_region("Foreign")
        self.writer.write_data(foreign_calculated_data, unit_time="hour")
        logger.info("国际数据写入完成")
        return calculated_data

    def days_data_process(self, combined_data, data_region="China"):
        if data_region == "China":
            logger.info("计算国内0点数据")
            time = self.cur_time
            cur_datetime = self.cur_datetime
        else:
            logger.info("计算国际0点数据")
            time = self.foreign_time
            cur_datetime = self.foreign_datetime
        self.change_data_region(data_region=data_region)
        self.change_data_processer(
            data=combined_data, time=time, cur_datetime=cur_datetime, unit_time="day"
        )
        logger.info("开始计算")
        calculated_data_day = self.calculate_data()
        calculated_data_day["time"] = (
                cur_datetime - timedelta(days=1) - timedelta(hours=1)
        )
        logger.info("计算完成，写入数据")
        self.writer.write_data(calculated_data_day, unit_time="day")
        logger.info("写入完成")
        return calculated_data_day

    def create_function_handler(self):
        """创建函数处理器"""
        self.hourfunctionhandler = HourlyFunctionHandler(
            data=None,
            reader=self.reader,
            writer=self.writer,
            datetime=self.cur_datetime,
            config=self.config_handler.config.get('hour_function')
        )
        self.dayfunctionhandler = DayFunctionHandler(
            data=None,
            reader=self.reader,
            writer=self.writer,
            datetime=self.cur_datetime
        )
        self.minutefunctionhandler = MinuteFunctionHandler(
            data=None,
            reader=self.reader,
            writer=self.writer,
            datetime=self.cur_datetime,
            config=self.config_handler.config.get('minute_function')
        )

    def add_funtion_to_handler(self, unit_time: Literal["hour", "day", "minute"]):
        if unit_time == "hour":
            self.hourfunctionhandler.add_function(
                [self.hourfunctionhandler.hour_func_1_base,
                 self.hourfunctionhandler.apply_condition_1_to_func_1_base,
                 self.hourfunctionhandler.apply_condition_2_to_func_1_base]
            )

        if unit_time == "day":
            self.dayfunctionhandler.add_function(
                [self.dayfunctionhandler.continous_change_drop_func_1]
            )

        if unit_time == "minute":
            self.minutefunctionhandler.add_function(
                [
                    self.minutefunctionhandler.minute_func_1_base,
                    self.minutefunctionhandler.apply_condition_1_to_func_1_base
                ]
            )

    # 执行小时函数
    def execute_hour_function(self, cur_data):
        # 更新数据以及相关信息
        self.hourfunctionhandler.data = cur_data
        self.change_data_region("China")
        self.hourfunctionhandler.price_comparison_results.clear()
        self.hourfunctionhandler.send_messages.clear()
        # 时间
        self.hourfunctionhandler.datetime = self.pre_hour_datetime
        # 数据
        pre_24_hours_datetime = self.pre_hour_datetime - timedelta(hours=24)
        self.hourfunctionhandler.get_range_data_hours(
            pre_24_hours_datetime, self.cur_datetime, inclusive="left"
        )
        self.hourfunctionhandler.execute_all()

    # 执行日函数
    def execute_day_function(self, cur_data):
        self.dayfunctionhandler.data = cur_data
        self.dayfunctionhandler.datetime = self.pre_day_datetime

        pre_four_days_datetime = self.pre_day_datetime - timedelta(days=4)
        self.dayfunctionhandler.get_range_data_days(
            pre_four_days_datetime, self.pre_day_datetime, inclusive="left"
        )
        self.dayfunctionhandler.execute_all()
        if self.dayfunctionhandler.results:
            res_str = "\n".join(self.dayfunctionhandler.results)
            return res_str

    def execute_minute_function(self, cur_data):
        self.minutefunctionhandler.data = cur_data
        self.minutefunctionhandler.datetime = self.cur_datetime
        self.minutefunctionhandler.price_comparison_results.clear()
        self.minutefunctionhandler.send_messages.clear()
        pre_24_hours_datetime = (self.cur_datetime - timedelta(hours=24)).replace(minute=0)
        self.minutefunctionhandler.get_range_data_hours(
            pre_24_hours_datetime, self.cur_datetime, inclusive="left"
        )
        self.minutefunctionhandler.execute_all()

    def result_record(self, unit_time: Literal["hour", "day", "minute"]):
        make_sure_path_exists(RESULT_FOLDER_PATH)

        result_list = list()
        send_messages = collections.defaultdict(pd.DataFrame)
        func_description = collections.defaultdict(str)
        function_result_file = None

        if unit_time == 'hour':
            function_result_file = os.path.join(RESULT_FOLDER_PATH, 'hour_function_result.csv')
            send_messages = self.hourfunctionhandler.send_messages
            func_description = hour_function_description

        elif unit_time == 'minute':
            function_result_file = os.path.join(RESULT_FOLDER_PATH, 'minute_function_result.csv')
            send_messages = self.minutefunctionhandler.send_messages
            func_description = minute_function_description

        for key, value in send_messages.items():
            if not value.empty:
                cur_func_desc = func_description.get(key)
                result_list.append(f"\n{cur_func_desc}\n{value.to_string(index=False)}\n")
                value['func'] = key
                self.writer.write_result_data(value, function_result_file)

        result_str = "\n".join(result_list)
        return result_str

    def clear(self):
        del self.hourfunctionhandler
        del self.minutefunctionhandler
        del self.dayfunctionhandler
        del self.writer
        del self.reader
        del self.data_processer


if __name__ == "__main__":

    logger.info("启动程序")
    is_Test = False
    controller = ProgramCotroller("China", config_file=rf'{os.path.join(PROJECT_ROOT_PATH, 'config.xml')}')
    controller.config_handler.load_config()
    # 开启线程监测文件改动
    controller.config_handler.start_monitoring(5)
    controller.create_function_handler()
    controller.add_funtion_to_handler("minute")
    controller.add_funtion_to_handler("hour")
    # controller.add_funtion_to_handler('day')
    try:
        while True:
            cur_datetime = datetime.now().replace(microsecond=0)
            if cur_datetime.second == 0:
                # 更新时间
                controller.update_time(cur_datetime)
                logger.info("爬取数据")

                controller.get_data_by_multithreading()

                if not controller.res:
                    logger.warning(f"爬取失败,终止后续操作")
                    continue
                combined_data = pd.concat(controller.res, ignore_index=True)
                print(combined_data)
                if combined_data.empty:
                    logger.warning(f"当前数据为空,终止后续操作")
                    continue

                logger.info("为数据增加time列")
                combined_data = controller.add_time_column(combined_data)
                print(combined_data)
                # print(combined_data)
                # 改变coin_price字段数据类型
                combined_data = controller.reader.change_column_type_to_Decimal(combined_data, only_price=True)
                # 生成国际数据
                foreign_data = combined_data.copy()
                foreign_data["time"] = controller.foreign_datetime
                print(foreign_data)
                # 国内整点(每小时)
                if controller.cur_minute == 0:
                    # 计算
                    calculated_data = controller.hours_data_process(
                        combined_data=combined_data.copy()
                    )

                    controller.execute_hour_function(calculated_data)
                    res_hour = controller.result_record("hour")
                    if res_hour:
                        send_email(subject="每小时函数结果-v1", content=res_hour, test=is_Test)

                # 国内0点
                if cur_datetime.hour == 1 and controller.cur_minute == 0:
                    calculated_data_day = controller.days_data_process(
                        combined_data.copy(), data_region="China"
                    )
                    controller.update_45_day_max_price(calculated_data_day.copy())
                    res_day = controller.execute_day_function(calculated_data_day)
                    if res_day:
                        send_email(subject="每天函数结果-v1", content=res_day, test=is_Test)
                # 国际0点（国内8点）
                if cur_datetime.hour == 9 and controller.cur_minute == 0:
                    calculated_data_day_foreign = controller.days_data_process(
                        foreign_data.copy(), data_region="Foreign"
                    )
                    res_day_foreign = controller.execute_day_function(
                        calculated_data_day_foreign
                    )
                    if res_day_foreign:
                        send_email(
                            subject="国际每天函数结果-v1",
                            content=res_day_foreign,
                            test=is_Test,
                        )

                # 不是整点
                logger.info("写入详情数据")
                controller.change_data_region("China")
                print(combined_data)
                controller.writer.write_detail_data(combined_data)
                controller.change_data_region("Foreign")
                controller.writer.is_check = False
                controller.writer.write_detail_data(foreign_data)
                logger.info("详情数据写入完成,执行每分钟函数")
                controller.change_data_region("China")
                controller.execute_minute_function(combined_data)
                res_minute = controller.result_record("minute")
                if res_minute:
                    send_email(subject="分钟函数结果-v1", content=res_minute, test=is_Test)
                logger.info("执行完毕")
                logging.shutdown()
    except KeyboardInterrupt:
        logging.shutdown()
    finally:
        controller.clear()
