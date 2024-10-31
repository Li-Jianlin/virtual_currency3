import pandas as pd
import os
from msg_log.mylog import get_logger

logger = get_logger(__name__)

class CSVReader:
    """从csv文件中读取想要的数据"""

    def __init__(self, data_region: str, spider_web: str, **kwargs):
        """

        :param data_region:表示地区，China表示国内数据，Foreign表示国际数据
        :param spider_web: 爬取数据的网站
        :param kwargs:
        """
        self.spider_web = spider_web
        self.data_region = data_region



    def read_fillna_data(self):
        """读取用于填充缺失值的数据"""
