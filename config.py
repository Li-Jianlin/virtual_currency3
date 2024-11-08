from enum import Enum
import sys
import os
HOST = 'localhost'
PORT = 3306
USERNAME = 'root'
PASSWORD = '123123'
DB = 'td1'
DB_URI = f'mysql+pymysql://{USERNAME}:{PASSWORD}@{HOST}:{PORT}/{DB}'


class SpiderWeb(Enum):
    COIN_GLASS = 'coin_glass'
    _528_btc = '528_btc'
    INVERSTING = 'inversting'
    BINANCE = 'binance'
    COIN_STATS = 'coin-stats'

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

BLACKLIST_FILEPATH = 'data/blacklist.csv'