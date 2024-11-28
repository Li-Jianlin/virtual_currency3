import unittest
import pandas as pd
import numpy as np
from get_data_by_spider.requests_spider import SpiderByRequests, SpiderWeb
from get_data_by_spider.selenium_spider import SpiderBySelenium
from get_data_by_spider.get_data import DataGetter


class GetDataTest(unittest.TestCase):
    def setUp(self):
        self.data = {
            'coin_name': ['BTC', 'ETH', '$ABC', '$BTC', 'AA', 'ABT', 'BTC', 'ETH', '$ABC', '$BTC', 'AA', 'ABT'],
            'price': [100, 200, 300, 400, 500, 600, 700, 800, 900, 1000, 1100, 1200],
            'spider_web': ['binance', 'binance', 'binance', 'binance', 'binance', 'binance', 'inversting', 'inversting',
                           'inversting', 'inversting', 'inversting', 'inversting']
        }
        self.data_df = pd.DataFrame(self.data)
        self.coins =['BTC', 'ETH', '$ABC', '$BTC', 'AA', 'ABT', 'BTC', 'ETH', '$ABC', '$BTC', 'AA', 'ABT']
        self.prices =[100, 200, 300, 400, 500, 600, 700, 800, 900, 1000, 1100, 1200]

    def test_transform_dataframe(self):
        # 去重
        spider = SpiderByRequests(SpiderWeb.BINANCE)
        spider.coins = self.coins
        spider.prices = self.prices
        length = len(self.coins)
        spider.transform_dataframe()
        self.assertEqual(length/2, len(spider.coin_data['coin_name']))

    def test_requests_spider(self):
        spider = SpiderByRequests(SpiderWeb.BINANCE)
        spider.get_content()
        spider.parse()
        spider.transform_dataframe()

        data = spider.coin_data
        res = not data.empty
        self.assertTrue(res)

    def test_selenium_spider(self):
        spider = SpiderBySelenium(SpiderWeb.INVERSTING)
        spider.get_driver()
        spider.load_page()
        spider.crawl_data()
        spider.transform_dataframe()
        data = spider.coin_data
        spider.driver.close()
        res = not data.empty
        self.assertTrue(res)

    def test_filter_blick(self):
        get_data = DataGetter(SpiderWeb.BINANCE)
        get_data.spider.coin_data = self.data_df
        get_data.filter_data()
        coins = get_data.data['coin_name'].values
        print(coins)
        res_1 = not np.isin(['$ABC', '$BTC', 'AA', 'ABT'], coins).all()
        res_2 = np.isin(['BTC', 'ETH'], coins).all()
        self.assertTrue(res_1 and res_2)


if __name__ == '__main__':
    unittest.main()
