import random
import pandas as pd
from config import SpiderWeb

# 动态加载User-Agent列表
def load_user_agents():
    user_agents = [
        "Mozilla/5.0 (Windows NT 10.0; WOW64)",
        'Mozilla/5.0 (Windows NT 6.3; WOW64)',
        'Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.11 (KHTML, like Gecko) Chrome/23.0.1271.64 Safari/537.11',
        'Mozilla/5.0 (Windows NT 6.3; WOW64; Trident/7.0; rv:11.0) like Gecko',
        'Mozilla/5.0 (Windows NT 5.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/28.0.1500.95 Safari/537.36',
        'Mozilla/5.0 (Windows NT 6.1; WOW64; Trident/7.0; SLCC2; .NET CLR 2.0.50727; .NET CLR 3.5.30729; .NET CLR 3.0.30729; Media Center PC 6.0; .NET4.0C; rv:11.0) like Gecko)',
        'Mozilla/5.0 (Windows; U; Windows NT 5.2) Gecko/2008070208 Firefox/3.0.1',
        'Mozilla/5.0 (Windows; U; Windows NT 5.1) Gecko/20070309 Firefox/2.0.0.3',
        'Mozilla/5.0 (Windows; U; Windows NT 5.1) Gecko/20070803 Firefox/1.5.0.12',
        'Opera/9.27 (Windows NT 5.2; U; zh-cn)',
        'Mozilla/5.0 (Macintosh; PPC Mac OS X; U; en) Opera 8.0',
        'Opera/8.0 (Macintosh; PPC Mac OS X; U; en)',
        'Mozilla/5.0 (Windows; U; Windows NT 5.1; en-US; rv:1.8.1.12) Gecko/20080219 Firefox/2.0.0.12 Navigator/9.0.0.6',
        'Mozilla/4.0 (compatible; MSIE 8.0; Windows NT 6.1; Win64; x64; Trident/4.0)',
        'Mozilla/4.0 (compatible; MSIE 8.0; Windows NT 6.1; Trident/4.0)',
        'Mozilla/5.0 (compatible; MSIE 10.0; Windows NT 6.1; WOW64; Trident/6.0; SLCC2; .NET CLR 2.0.50727; .NET CLR 3.5.30729; .NET CLR 3.0.30729; Media Center PC 6.0; InfoPath.2; .NET4.0C; .NET4.0E)',
        'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.1 (KHTML, like Gecko) Maxthon/4.0.6.2000 Chrome/26.0.1410.43 Safari/537.1 ',
        'Mozilla/5.0 (compatible; MSIE 10.0; Windows NT 6.1; WOW64; Trident/6.0; SLCC2; .NET CLR 2.0.50727; .NET CLR 3.5.30729; .NET CLR 3.0.30729; Media Center PC 6.0; InfoPath.2; .NET4.0C; .NET4.0E; QQBrowser/7.3.9825.400)',
        'Mozilla/5.0 (Windows NT 6.1; WOW64; rv:21.0) Gecko/20100101 Firefox/21.0 ',
        'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.1 (KHTML, like Gecko) Chrome/21.0.1180.92 Safari/537.1 LBBROWSER',
        'Mozilla/5.0 (compatible; MSIE 10.0; Windows NT 6.1; WOW64; Trident/6.0; BIDUBrowser 2.x)',
        'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/536.11 (KHTML, like Gecko) Chrome/20.0.1132.11 TaoBrowser/3.0 Safari/536.11'
    ]
    return user_agents


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
        self.headers = {'User-Agent': random.choice(load_user_agents())}
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
        if not self.coins:
            self.coin_data = pd.DataFrame()
            return self.coin_data
        try:
            coin_data = pd.DataFrame({'coin_name': self.coins, 'coin_price': self.prices, 'spider_web': self.spider_web})
            if self.spider_web == 'binance':
                coin_data = coin_data.drop_duplicates(subset='coin_name', keep='last')
            self.coin_data = coin_data
        except Exception as e:
            logging.error(f"Error creating DataFrame: {e}")
            self.coin_data = pd.DataFrame()
        return self.coin_data
