import requests
import pandas as pd

from config import CONFIG_JSON, SpiderWeb
from get_data_by_spider.spider_base import Spider
from msg_log.mylog import get_logger
from error_exception.customerror import KeyNotFound, SpiderFailedError



logger = get_logger(__name__)



class SpiderByRequests(Spider):
    """
    通过requests得到数据
    res_json: 存储解析返回信息得到的json格式数据
    records: 包含name和price的键值对形式数据
    """

    def __init__(self, spider_web: SpiderWeb, **kwargs):
        super().__init__(spider_web.value)
        self.res_json: dict = None
        self.records:list = None
        self.__dict__.update(kwargs)
        self.web_info = CONFIG_JSON.get(self.spider_web)
        self.url = self.web_info.get('url')
        self.coins_key = self.web_info.get('coins_key')
        self.price_key = self.web_info.get('price_key')
        self.name_key = self.web_info.get('name_key')


    def get_content(self, request_mode='get', **kwargs):
        """获取网页内容，将内容解析为json格式存储在变量self.res_json中"""
        try:
            if request_mode == 'get':
                responses = requests.get(self.url, headers=self.headers, timeout=10)
            elif request_mode == 'post':
                data = kwargs.get('data')
                responses = requests.post(self.url, headers=self.headers, data=data, timeout=10)
            else:
                raise ValueError(f'不支持的请求模式 {request_mode}: 必须为 "get" 或 "post"')

            # 检查响应状态码
            responses.raise_for_status()  # 如果响应代码不是200-299, 抛出HTTPError异常

            self.res_json = responses.json()

        except requests.exceptions.RequestException as e:
            logger.exception(f'请求失败: {e}')
        except ValueError as value_err:
            logger.exception(f'{self.url} 返回的内容不是 JSON 格式: {value_err}')
        except Exception as e:
            logger.exception(f'未知错误: {e}')

    def parse(self):
        """从通过处理的json数据中拿到想要的数据，将币种和价格分别存在self.coins和self.prices中"""
        # 检验能否正常取出数据
        try:
            self.records = self.res_json.get(self.coins_key)
        except KeyError:
            raise KeyNotFound(f'未找到期望的key: {self.coins_key}')
        except AttributeError as e:
            logger.exception(e)


        try:
            price = self.records[0].get(self.price_key)
            coin = self.records[0].get(self.name_key)
            if price is None and coin is None:
                raise KeyNotFound(f'未找到期望的key: {self.price_key} 或 {self.name_key}')
        except IndexError:
            raise KeyNotFound(f'未找到期望的key: {self.name_key} 或 {self.price_key}')
        except Exception as e:
            logger.exception(e)
            return
        # 遍历记录，提取并处理币种和价格数据
        for record in self.records:
            self.process_record(record)

    def process_record(self, record):
        coin = record.get(self.name_key).upper()
        price = record.get(self.price_key)

        if isinstance(price, str):
            price = price.replace('$', '').replace(',', '')


        if coin and price:
            self.coins.append(coin)
            self.prices.append(price)
        else:
            raise SpiderFailedError('爬取失败')

if __name__ == '__main__':
    spider_web = SpiderWeb.COIN_STATS
    spider = SpiderByRequests(spider_web)
    spider.get_content()
    # print(spider.res_json)
    spider.parse()
    df = spider.transform_dataframe()
    print(df)
    del spider
