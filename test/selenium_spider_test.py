import unittest
from get_data_by_spider.selenium_spider import SpiderBySelenium, SpiderWeb
from threading import Thread
from msg_log import mylog

logger = mylog.get_logger(__name__)
class TestSeleniumSpider(unittest.TestCase):

    def setUp(self):

        self.spider_coin_glass = SpiderBySelenium(SpiderWeb.COIN_GLASS)

        self.spider_528_btc = SpiderBySelenium(SpiderWeb._528_btc)

        self.spider_investing = SpiderBySelenium(SpiderWeb.INVERSTING)

    def run_spider_in_thread(self, spider):
        logger.info(f'测试{spider.spider_web}')
        spider.load_page()
        spider.get_headless_driver()
        spider.crawl_data()
        df = spider.transform_dataframe()
        logger.info(f'{spider.spider_web}数据获取完毕')
        print(df.to_string())
        spider.driver.close()
        # assert df.shape[0] > 0

    def test_selenium_spiders_multithreading(self):
        thread = [
            Thread(target=self.run_spider_in_thread, args=(self.spider_coin_glass,)),
            Thread(target=self.run_spider_in_thread, args=(self.spider_528_btc,)),
            Thread(target=self.run_spider_in_thread, args=(self.spider_investing,))
        ]

        for t in thread:
            t.start()

        for t in thread:
            t.join()

if __name__ == '__main__':
    unittest.main()