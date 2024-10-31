import logging
from datetime import datetime

from socks import method

from get_data_by_spider.spider_base import Spider
from requests_spider import SpiderByRequests
from selenium_spider import SpiderBySelenium
from spider_base import SpiderWeb
from msg_log.mylog import get_logger
from error_exception.keyerror import KeyNotFound, SpiderFailedError
from pandas import DataFrame

from config import BLACKLIST_FILEPATH

# 设置日志
logger = get_logger(__name__)



def read_blacklist(file_path):
    """获取黑名单"""
    try:
        with open(file_path, 'r', encoding='utf-8') as file:
            blacklist = [line.strip() for line in file if line.strip()]
        return blacklist
    except Exception as e:
        logger.exception(f"黑名单读取失败: {e}")
        return []


class DataGetter:
    """创建爬虫对象并获取数据"""
    spider_method = {
        SpiderWeb.BINANCE: 'requests',
        SpiderWeb.COIN_STATS: 'requests',
        SpiderWeb.INVERSTING: 'selenium',
        SpiderWeb.COIN_GLASS: 'selenium',
        SpiderWeb._528_btc: 'selenium'
    }
    def __init__(self, spider_web: SpiderWeb, **kwargs):
        self.method = self.spider_method.get(spider_web)
        logger.info(f"创建爬取{spider_web.value}网站的对象")
        if self.method == 'requests':
            self.spider = SpiderByRequests(spider_web)
        elif self.method == 'selenium':
            self.spider = SpiderBySelenium(spider_web)
            logger.info("创建selenium驱动对象")
            self.spider.get_headless_driver()
            logger.info("加载页面")
            # 提前加载出页面
            self.load_page()
            logger.info("加载页面完成")
        self.blacklist = read_blacklist(BLACKLIST_FILEPATH)
        self.data = DataFrame()

    def get_data(self)-> DataFrame:
        """获取数据"""
        logger.info("爬取数据")
        if self.method == 'selenium':
            self.get_data_by_selenium()
        elif self.method == 'requests':
            self.get_data_by_requests()
        logger.info("数据爬取完成,开始生成DataFrame数据")
        self.spider.transform_dataframe()
        logger.info("数据生成完成,开始过滤黑名单")
        self.filter_data()
        logger.info("黑名单过滤完成")


    def get_data_by_selenium(self):
        """通过selenium获取数据"""
        self.spider.crawl_data()

    def load_page(self):
        """加载页面"""
        self.spider.load_page()

    def get_data_by_requests(self):
        """通过requests获取数据"""
        self.spider.get_content()
        self.spider.parse()

    def filter_data(self):
        """过滤数据"""
        if not self.spider.coin_data.empty:
            data = self.spider.coin_data
            data = data[~data['coin_name'].isin(self.blacklist)].copy()
            self.data = data







if __name__ == '__main__':

    data_getter = DataGetter(SpiderWeb.INVERSTING)
    while True:
        cur_time = datetime.now()
        if cur_time.second == 0:
            data_getter.get_data()
            print(data_getter.data)
    logger.info('爬取完成')
