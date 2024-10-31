from get_data_by_spider import requests_spider, selenium_spider

with open(f'../data/blacklist.csv', 'r', encoding='utf-8') as file:
    blacklist = file.read().split('\n')


