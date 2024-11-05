from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.action_chains import ActionChains

import pandas as pd
import time
import random
import os
from msg_log.mylog import get_logger
from config import CONFIG_JSON_SELENIUM, SpiderWeb
from get_data_by_spider.spider_base import Spider
from decimal import Decimal

PROJECT_ROOT_PATH = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
logging = get_logger(__name__, filename=os.path.join(PROJECT_ROOT_PATH, 'log', f'selenium_spider.log'))


class SpiderBySelenium(Spider):
    """通过selenium获取数据"""
    def __init__(self, spider_web: SpiderWeb):
        super().__init__(spider_web.value)
        self.coins = []
        self.prices = []
        self.web_info = CONFIG_JSON_SELENIUM.get(self.spider_web)
        self.url = self.web_info.get('url')
        self.method = self.web_info.get('method')

    def get_headless_driver(self):
        """使用无头浏览器"""
        self.options = Options()
        self.options.add_argument('--headless')
        self.options.add_argument("--disable-gpu")  # 禁用GPU加速
        self.options.page_load_strategy = 'none' # 不等待页面加载完成
        # 关闭浏览器上部提示语：Chrome正在受到自动软件的控制
        self.options.add_experimental_option('excludeSwitches', ['enable-automation'])
        self.options.add_experimental_option('useAutomationExtension', False)
        self.options.add_argument("--window-size=1920,1080")  # 设置浏览器分辨率（窗口大小）
        self.options.add_argument("blink-settings=imagesEnabled=false")  # 不加载图片, 提升速度
        self.options.add_argument('--no-sandbox')  # 解决DevToolsActivePort文件不存在的报错
        self.options.add_argument('--hide-scrollbars')  # 隐藏滚动条, 应对一些特殊页面
        self.options.add_argument(
            f'user-agent={self.headers["User-Agent"]}')
        self.driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=self.options)
        self.wait = WebDriverWait(self.driver, 30)
        self.driver.get(self.url)
        time.sleep(3)

    def get_driver(self):
        """使用有头浏览器"""
        self.driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()))
        self.wait = WebDriverWait(self.driver, 10)
        self.driver.get(self.url)
        time.sleep(3)

    def load_page(self):
        """根据配置文件中的加载方法将要爬取的页面全部加载出来。slide为滑动加载页面，click为点击按钮加载页面"""

        if self.method == 'slide':
            slide_num = self.web_info.get('num_of_slide')
            for _ in range(slide_num):
                self.driver.execute_script(
                    "window.scrollTo({ top: document.body.scrollHeight, behavior: 'smooth' });")
                time.sleep(random.randint(8, 12))
        if self.method == 'click':
            num_of_click = self.web_info.get('num_of_click', 1)
            for _ in range(num_of_click):
                self.driver.execute_script(
                    "window.scrollTo({ top: document.body.scrollHeight, behavior: 'smooth' });")
                time.sleep(random.randint(2, 3))
                try:
                    more_msg_button = self.wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, self.web_info.get('more_msg_button_css'))))
                except Exception as e:
                    more_msg_button = self.wait.until(EC.element_to_be_clickable((By.XPATH, self.web_info.get('more_msg_button_xpath'))))
                else:
                    self.driver.execute_script("arguments[0].click();", more_msg_button)
                    time.sleep(random.uniform(7, 12))
            self.driver.execute_script(
                "window.scrollTo({ top: document.body.scrollHeight, behavior: 'smooth' });")
    def crawl_data(self):
        """从网页爬取数据"""
        try:
            coin_names = self.wait.until(EC.presence_of_all_elements_located((By.XPATH, self.web_info.get('coin_name_xpath'))))
        except Exception as e:
            coin_names = self.wait.until(EC.presence_of_all_elements_located((By.CSS_SELECTOR, self.web_info.get('coin_name_css'))))

        try:
            coin_prices = self.wait.until(EC.presence_of_all_elements_located((By.XPATH, self.web_info.get('coin_price_xpath'))))
        except Exception as e:
            coin_prices = self.wait.until(EC.presence_of_all_elements_located((By.CSS_SELECTOR, self.web_info.get('coin_price_css'))))

        self.coins = [coin.text for coin in coin_names]
        self.prices = [price.text.replace('$', '').replace(',', '') for price in coin_prices]

    def transform_dataframe(self):
        """生成DataFrame数据"""
        if not self.coins or not self.prices:
            self.coin_data = pd.DataFrame()
            return self.coin_data
        try:
            self.coin_data = pd.DataFrame({'coin_name': self.coins, 'coin_price': self.prices, 'spider_web': self.spider_web})
            if self.spider_web == SpiderWeb.COIN_GLASS.value:
                self.coin_data = self.coin_data[self.coin_data['coin_name'].str.endswith('/USDT')]
                self.coin_data['coin_name'] = self.coin_data['coin_name'].replace('/USDT', '', regex=True)
        except Exception as e:
            logging.error(f"Error creating DataFrame: {e}")
            self.coin_data = pd.DataFrame()
        return self.coin_data


if __name__ == '__main__':
    spider = SpiderBySelenium(SpiderWeb.COIN_GLASS)
    spider.get_headless_driver()
    spider.load_page()
    spider.crawl_data()
    df = spider.transform_dataframe()
    spider.driver.close()
    print(df)