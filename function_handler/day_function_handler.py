from function_handler.functionhandler import FunctionHandler
from msg_log.mylog import get_logger
import os
import pandas as pd
from datetime import datetime, timedelta
from decimal import Decimal
from typing import Literal

PROJECT_ROOT_PATH = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

logger = get_logger(__name__, filename=os.path.join(PROJECT_ROOT_PATH, 'log', 'function_handler.log'))


class DayFunctionHandler(FunctionHandler):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def continous_change_drop_func_1(self):
        """
        当前状态在跌
        当前虚降大于等于5%
        （1）前三天均为跌，前三天的跌幅之和加上当前跌幅小于等于20%
        （2）前四天均为跌，前四天跌幅之和加上当前跌幅小于等于20%
        (1)和(2)只需要满足其中一个即可
        :return:
        """
        CUR_CHANGE = 0
        VIRTUAL_DROP = 5
        CHANGE_SUM = 20

        logger.info('开始执行日函数1：continous_change_drop_func_1')

        data_A = self.data[['coin_name', 'spider_web', 'change', 'virtual_drop', 'coin_price', 'time']].copy()

        # A在跌
        decline_data_A = self.filter_by_change_rate(data_A, 'lt', CUR_CHANGE)
        # A虚降大于等于5%
        decline_and_virtual_drop_data_A = self.filter_by_virtual_drop(decline_data_A, 'ge', VIRTUAL_DROP)

        fourth_day_ahead_datetime = self.datetime - timedelta(days=4)

        third_day_ahead_datetime = self.datetime - timedelta(days=3)
        # 前四天数据
        fourth_day_ahead_data = self.range_data_day[
            ['coin_name', 'spider_web', 'change', 'time']]
        fourth_day_ahead_data = fourth_day_ahead_data[
            fourth_day_ahead_data['time'].between(fourth_day_ahead_datetime, self.datetime, inclusive='left')].copy()

        decline_fourth_day_ahead_data = self.filter_by_change_rate(fourth_day_ahead_data, 'lt', CUR_CHANGE).copy()

        # 前三天数据
        third_day_ahead_data = fourth_day_ahead_data[
            fourth_day_ahead_data['time'].between(third_day_ahead_datetime, self.datetime, inclusive='both')]
        decline_third_day_ahead_data = self.filter_by_change_rate(third_day_ahead_data, 'lt', CUR_CHANGE).copy()

        # 合并
        fourth_day_and_A_data = pd.concat([decline_fourth_day_ahead_data, decline_and_virtual_drop_data_A[
            ['coin_name', 'spider_web', 'time', 'change']]], axis=0, ignore_index=True)
        third_day_and_A_data = pd.concat(
            [decline_and_virtual_drop_data_A[['coin_name', 'spider_web', 'time', 'change']],
             decline_third_day_ahead_data], axis=0, ignore_index=True)

        # 筛选出只存在于A时刻的币种网站组合
        fourth_day_and_A_data = fourth_day_ahead_data.merge(
            decline_and_virtual_drop_data_A[['coin_name', 'spider_web']], on=['coin_name', 'spider_web'], how='inner')

        third_day_and_A_data = third_day_and_A_data.merge(decline_and_virtual_drop_data_A[['coin_name', 'spider_web']],
                                                           on=['coin_name', 'spider_web'], how='inner')
        # 分组
        fourth_group_by_coin_and_spiderweb = fourth_day_and_A_data.groupby(['coin_name', 'spider_web'])
        third_group_by_coin_and_spiderweb = third_day_and_A_data.groupby(['coin_name', 'spider_web'])

        # 计算跌幅之和
        fourth_day_change_sum = fourth_group_by_coin_and_spiderweb['change'].sum().reset_index()
        third_day_change_sum = third_group_by_coin_and_spiderweb['change'].sum().reset_index()

        # 跌幅大于等于20%
        decline_fourth_day = fourth_day_change_sum[fourth_day_change_sum['change'] <= -CHANGE_SUM]
        decline_third_day = third_day_change_sum[third_day_change_sum['change'] <= -CHANGE_SUM]

        # 找出在前四天和前三天均满足跌幅大于等于 20% 的币种
        combined_data = pd.concat([decline_fourth_day, decline_third_day], axis=0, ignore_index=True)
        combined_data.drop_duplicates(subset=['coin_name', 'spider_web'], inplace=True)

        if not combined_data.empty:
            func_desc = (f"[天]函数1{len(combined_data['coin_name'])}"
                         f"当前状态在跌,当前虚降大于等于{VIRTUAL_DROP}%."
                         f"（1）前三天均为跌，前三天的跌幅之和加上当前跌幅小于等于{CHANGE_SUM}%;"
                         f"（2）前四天均为跌，前四天跌幅之和加上当前跌幅小于等于{CHANGE_SUM}%。"
                         f"(1)和(2)只需要满足其中一个即可")
            res_str = combined_data[['coin_name', 'spider_web']].to_string(index=True)
            logger.info(func_desc + '\n' + res_str + '\n')
            return res_str
        else:
            logger.info('日函数1无异常')
