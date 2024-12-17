import logging
import math
from datetime import datetime

import pandas as pd
from pandas import DataFrame
from selenium.common import InvalidArgumentException, TimeoutException
from socks import method
import os
from get_data_by_spider.requests_spider import SpiderByRequests
from get_data_by_spider.selenium_spider import SpiderBySelenium
from msg_log.mylog import get_logger
from error_exception.customerror import KeyNotFound, SpiderFailedError
from config import BLACKLIST_FILEPATH, CONFIG_JSON, SpiderWeb
PROJECT_ROOT_PATH = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
# 设置日志
logger = get_logger(__name__, filename=os.path.join(PROJECT_ROOT_PATH, 'log', f'get_data.log'))


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
        SpiderWeb._528_btc: 'selenium',
        SpiderWeb.GATE: 'requests'
    }
    def __init__(self, spider_web: SpiderWeb, **kwargs):
        self.method = self.spider_method.get(spider_web)
        self.spider_web_name = spider_web.value
        logger.info(f"创建爬取{self.spider_web_name}网站的对象")
        if self.method == 'requests':
            self.spider = SpiderByRequests(spider_web)
        elif self.method == 'selenium':
            self.spider = SpiderBySelenium(spider_web)
            logger.info("创建selenium驱动对象")
            self.spider.get_driver()
            logger.info("加载页面")
            # 提前加载出页面
            try:
                self.load_page()
            except TimeoutException:
                logger.warning("selenium加载页面超时,重新加载.....")
                self.load_page()
            except InvalidArgumentException as e:
                logger.exception(f'无效参数异常,重新加载\n{e}')
                self.load_page()
            logger.info("加载页面完成")
        self.blacklist = read_blacklist(BLACKLIST_FILEPATH)
        self.data = DataFrame()

    def get_data(self):
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
        logger.info('\n' + self.data.head(2).to_string() +'\n')
        return self


    def get_data_by_selenium(self):
        """通过selenium获取数据"""
        self.spider.crawl_data()

    def load_page(self):
        """加载页面"""
        self.spider.load_page()

    def get_data_by_requests(self):
        """通过requests获取数据"""
        cnt = 1
        if self.spider_web_name == 'gate':
            data = CONFIG_JSON.get('gate').get('data')
            total_nums = CONFIG_JSON.get('gate').get('total_nums')
            pagesize = CONFIG_JSON.get('gate').get('pageSize')
            cnt = math.ceil(total_nums / pagesize)
        for page in range(cnt):
            try:
                data.update({'page': page})
            except Exception as e:
                data = dict()
            self.spider.get_content(data=data)
            self.spider.parse()

    def filter_data(self):
        """过滤数据"""
        self.blacklist = read_blacklist(BLACKLIST_FILEPATH)
        if not self.spider.coin_data.empty:
            data = self.spider.coin_data
            data = data[~data['coin_name'].isin(self.blacklist)]
            # 去除所有以$符号开头的币种
            data = data[~data['coin_name'].str.startswith('$')].copy()
            self.data = data
        else:
            logger.warning("数据为空")
            self.data = DataFrame(columns=['coin_name', 'coin_price', 'spider_web'])
        return self



if __name__ == '__main__':

    # data_getter = DataGetter(SpiderWeb.INVERSTING)
    # while True:
    #     cur_time = datetime.now()
    #     if cur_time.second == 0:
    #         data_getter.get_data()
    #         print(data_getter.data)
    data = pd.DataFrame({
        'coin_name': ['ETH', 'BTC', 'USDT', 'PIN', 'CULT'],
        'coin_price': [100, 200, 300, 400, 500]
    })
    blacklist = read_blacklist(BLACKLIST_FILEPATH)
    print(blacklist)
    print(len(blacklist))
    filter_data = data[~data['coin_name'].isin(blacklist)]
    print(filter_data)