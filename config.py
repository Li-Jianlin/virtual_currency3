from enum import Enum
import sys
import os
from threading import Thread
import time
from xml.etree import ElementTree
from msg_log.mylog import get_logger
from pprint import pprint

HOST = 'localhost'
PORT = 3306
USERNAME = 'root'
PASSWORD = '123123'
DB = 'td1'
DB_URI = f'mysql+pymysql://{USERNAME}:{PASSWORD}@{HOST}:{PORT}/{DB}'

PROJECT_ROOT_PATH = os.path.dirname(os.path.abspath(__file__))
logger = get_logger(
    __name__, filename=os.path.join(PROJECT_ROOT_PATH, "log", "config.log")
)


class ConfigHandler:
    """配置文件读取类"""

    def __init__(self, file_path):
        self.file_path = file_path
        self.last_modified_time = None
        self.config = None

    def load_config(self):
        tree = ElementTree.parse(self.file_path)
        root = tree.getroot()
        self.config = self._parse_element(root)

    def _parse_element(self, element):
        if list(element):
            return {child.tag: self._parse_element(child) for child in element}
        return element.text.strip() if element.text else None

    def check_and_reload(self):
        try:
            modified_time = os.path.getmtime(self.file_path)
            if self.last_modified_time is None or modified_time > self.last_modified_time:
                print('检测到配置文件变动，重新加载配置信息')
                self.last_modified_time = modified_time
                self.load_config()
        except FileNotFoundError:
            logger.warning(f"配置文件{self.file_path}不存在")

    def start_monitoring(self, interval=2):
        """启动线程监控文件"""

        def monitor():
            while True:
                self.check_and_reload()
                time.sleep(interval)

        Thread(target=monitor, daemon=True).start()


class SpiderWeb(Enum):
    COIN_GLASS = 'coin_glass'
    _528_btc = '528_btc'
    INVERSTING = 'inversting'
    BINANCE = 'binance'
    COIN_STATS = 'coin-stats'


configHandler = ConfigHandler(file_path=rf'{os.path.join(PROJECT_ROOT_PATH, 'config.xml')}')
configHandler.load_config()
configHandler.start_monitoring(5)
minute_function_config = configHandler.config.get('minute_function')
hour_function_config = configHandler.config.get('hour_function')



CONFIG_JSON = {
    'coin-stats': {
        'url': 'https://api.coin-stats.com/v4/coins?skip=0&limit=900',
        'coins_key': 'coins',
        'price_key': 'pu',
        'name_key': 's'
    },
    'binance': {
        'url': 'https://www.binance.th/bapi/asset/v2/public/asset-service/product/get-products?includeEtf=true',
        'coins_key': 'data',
        'price_key': 'c',
        'name_key': 'b'
    }
}

CONFIG_JSON_SELENIUM = {
    '528_btc': {
        'url': 'https://www.528btc.com/coin/',
        'method': 'slide',
        'num_of_slide': 10,
        'coin_name_css': '.table_box > .title',
        'coin_price_xpath': '//tbody/tr/td[4]'
    },
    'coin_lass': {
        'url': 'https://www.coinglass.com/zh/exchanges/Binance',
        'method': 'click',
        'more_msg_button_css': 'button.MuiButton-root.MuiButton-variantSoft.MuiButton-colorNeutral.MuiButton-sizeMd.cg-style-sbekbk',
        'coin_name_xpath': '//*[@id="__next"]/div[2]/div[2]/div[2]/div/div[4]/div/div/div/div/div[2]/table/tbody/tr/td[2]/a/div/div[1]/div',
        'coin_price_xpath': '//*[@id="__next"]/div[2]/div[2]/div[2]/div/div[4]/div/div/div/div/div[2]/table/tbody/tr/td[3]/div/div'
    },
    'inversting': {
        'url': 'https://cn.investing.com/crypto/currencies',
        'method': 'click',
        'num_of_click': 8,
        'more_msg_button_css': "div[class='flex h-full cursor-pointer items-center justify-center text-inv-blue-500']  span[class='mr-2'",
        'coin_name_xpath': '//tbody/tr/td[4]/div[2]/span[2]',
        'coin_price_xpath': '//tbody/tr/td[5]'
    }
}

USER_AGENTS = [
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

BLACKLIST_FILEPATH = os.path.join(PROJECT_ROOT_PATH, 'data', 'blacklist.csv')

hour_function_description = {
    'hour_func_1_base': ('有ABC三个时刻。其中C为当前时刻C时刻在跌;'
                         f'A时刻与C时刻不超过{hour_function_config.get("hour_func_1_base").get("MAX_TIME_INTERVAL")}小时。'
                         f'A时刻在B时刻之前，A和B时刻均要满足虚降>={hour_function_config.get("hour_func_1_base").get("AB_VIRTUAL_DROP")}%,'
                         f'且跌涨幅<={hour_function_config.get("hour_func_1_base").get("AB_CHANGE")}%;'
                         'A时刻收盘价的0.99倍大于B时刻收盘价。;C时刻收盘价同时小于A的最低价和B的最低价。'
                         f'C价格小于前{hour_function_config.get("hour_func_1_base").get("C_PRE_TIME_INTERVAL")}小时最低价的最小值'),
    'apply_condition_1_to_func_1_base': ('有ABC三个时刻。其中C为当前时刻C时刻在跌;'
                                         f'A时刻与C时刻不超过{hour_function_config.get("hour_func_1_base").get("MAX_TIME_INTERVAL")}小时。'
                                         f'A时刻在B时刻之前，A和B时刻均要满足虚降>={hour_function_config.get("hour_func_1_base").get("AB_VIRTUAL_DROP")}%,'
                                         f'且跌涨幅<={hour_function_config.get("hour_func_1_base").get("AB_CHANGE")}%;'
                                         f'A时刻收盘价的0.99倍大于B时刻收盘价。C时刻收盘价同时小于A的最低价和B的最低价.'
                                         f'C价格小于前{hour_function_config.get("hour_func_1_base").get("C_PRE_TIME_INTERVAL")}小时最低价的最小值;'
                                         f'C收盘价小于等于A最低价和B最低价中最小值的'
                                         f'{hour_function_config.get("apply_condition_1_to_func_1_base").get("CLOSE_PRICE_THRESHOLD")}倍'),
    'apply_condition_2_to_func_1_base': ('有ABC三个时刻。其中C为当前时刻;'
                                         f'A时刻与C时刻不超过{hour_function_config.get("hour_func_1_base").get("MAX_TIME_INTERVAL")}小时。'
                                         f'A时刻在B时刻之前，A和B时刻均要满足虚降>={hour_function_config.get("hour_func_1_base").get("AB_VIRTUAL_DROP")}%,'
                                         f'且跌涨幅<={hour_function_config.get("hour_func_1_base").get("AB_CHANGE")}%;'
                                         f'A时刻收盘价的0.99倍大于B时刻收盘价。C时刻价格同时小于A的最低价和B的最低价.'
                                         f'C价格小于前{hour_function_config.get("hour_func_1_base").get("C_PRE_TIME_INTERVAL")}小时最低价的最小值;'
                                         f'A或者B其中一个至少满足：虚降>=跌幅的{hour_function_config.get("apply_condition_2_to_func_1_base").get("MAGNIFICATION_BINANCE")}(binance)/'
                                         f'{hour_function_config.get("apply_condition_2_to_func_1_base").get("MAGNIFICATION_OTHER")}(other)倍，'
                                         f'并且跌幅小于等于{hour_function_config.get("apply_condition_2_to_func_1_base").get("CHANGE")}.'
                                         f'或者满足条件：A或者B前6小时存在跌幅<={hour_function_config.get("apply_condition_2_to_func_1_base").get("A_OR_B_CHANGE_BINANCE")}(binance)/'
                                         f'{hour_function_config.get("apply_condition_2_to_func_1_base").get("A_OR_B_CHANGE_OTHER")}(other),'
                                         f'且该时刻的开盘价大于等于A或者B的max(开盘价, 收盘价)')
}

minute_function_description = {
    'minute_func_1_base': (f'有ABC三个时刻。其中C为当前时刻;'
                           f'A时刻与C时刻不超过{minute_function_config.get("minute_func_1_base").get("MAX_TIME_INTERVAL")}小时。'
                           f'A时刻在B时刻之前，A和B时刻均要满足虚降>={minute_function_config.get("minute_func_1_base").get("AB_VIRTUAL_DROP")}%,'
                           f'且跌涨幅<={minute_function_config.get("minute_func_1_base").get("AB_CHANGE")}%;'
                           f'A时刻收盘价的0.99倍大于B时刻收盘价。C时刻价格同时小于A的最低价和B的最低价.'
                           f'C价格小于前{minute_function_config.get("minute_func_1_base").get("C_PRE_TIME_INTERVAL")}小时最低价的最小值;'
                           f'后两次价格低于第一次价格'),
    'apply_condition_1_to_func_1_base': ('有ABC三个时刻。其中C为当前时刻;'
                                         f'A时刻与C时刻不超过{minute_function_config.get("minute_func_1_base").get("MAX_TIME_INTERVAL")}小时。'
                                         f'A时刻在B时刻之前，A和B时刻均要满足虚降>={minute_function_config.get("minute_func_1_base").get("AB_VIRTUAL_DROP")}%,'
                                         f'且跌涨幅<={minute_function_config.get("minute_func_1_base").get("AB_CHANGE")}%;'
                                         f'A时刻收盘价的0.99倍大于B时刻收盘价。C时刻价格同时小于A的最低价和B的最低价.'
                                         f'C价格小于前{minute_function_config.get("minute_func_1_base").get("C_PRE_TIME_INTERVAL")}小时最低价的最小值;'
                                         f'后两次价格低于第一次价格\n'
                                         f'A或者B其中一个至少满足：虚降>=跌幅的{minute_function_config.get("apply_condition_1_to_func_1_base").get("MAGNIFICATION_BINANCE")}(binance)/'
                                         f'{minute_function_config.get("apply_condition_1_to_func_1_base").get("MAGNIFICATION_OTHER")}(other)倍，'
                                         f'并且跌幅小于等于{minute_function_config.get("apply_condition_1_to_func_1_base").get("CHANGE")}.'
                                         f'或者满足条件：A或者B前6小时存在跌幅<={minute_function_config.get("apply_condition_1_to_func_1_base").get("A_OR_B_CHANGE_BINANCE")}(binance)/'
                                         f'{minute_function_config.get("apply_condition_1_to_func_1_base").get("A_OR_B_CHANGE_OTHER")}(other),'
                                         f'且该时刻的开盘价大于等于A或者B的max(开盘价, 收盘价)')
}
