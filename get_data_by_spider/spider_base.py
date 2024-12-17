import os
import random
import pandas as pd
from config import USER_AGENTS
from msg_log.mylog import get_logger

PROJECT_ROOT_PATH = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
logger = get_logger(__name__, filename=os.path.join(PROJECT_ROOT_PATH, 'log', f'spider_base.log'))

# 动态加载User-Agent列表
def get_random_user_agents():
    return random.choice(USER_AGENTS)

DROP_DUPLICATES_WEB = {
    'spider_web': ['binance', 'coin-stats'],
    'keep': 'last'
}

class Spider:
    """
    爬虫的基类
    url: 当前要爬取的网址
    headers: 请求头。模拟浏览器避免被识别为爬虫
    datetime: 获取数据时的时间
    coins: 存储获取的币种
    prices: 存储获取的价格
    """
    def __init__(self, spider_web: str):
        self.spider_web = spider_web
        self.headers = {'User-Agent': get_random_user_agents()}
        self.coins: list = []
        self.prices: list = []

    def get_content(self):
        """获取网页内容,将内容存储在self.res_json中"""
        # 这里应该实现具体的网络请求逻辑
        pass

    def parse(self):
        """解析网页内容"""
        # 这里应该实现具体的解析逻辑
        pass

    def transform_dataframe(self):
        """生成DataFrame数据"""
        if not self.coins or not self.prices:
            self.coin_data = pd.DataFrame(columns=['coin_name', 'coin_price', 'spider_web'])
            return self.coin_data
        try:
            coin_data = pd.DataFrame({'coin_name': self.coins, 'coin_price': self.prices, 'spider_web': self.spider_web})
            # 若数据来源属于特定网站，则需要去重
            if self.spider_web in DROP_DUPLICATES_WEB.get('spider_web'):
                coin_data = coin_data.drop_duplicates(subset='coin_name', keep=DROP_DUPLICATES_WEB.get('keep'))

            self.coin_data = coin_data
        except Exception as e:
            logger.error(f"Error creating DataFrame: {e}")
            self.coin_data = pd.DataFrame(columns=['coin_name', 'coin_price', 'spider_web'])
        return self.coin_data


if __name__ == '__main__':
    spider = Spider('binance')
    spider.coins = ['BTC', 'ETH', 'USDT', 'BTC']
    spider.prices = None
    data = spider.transform_dataframe()
    pass