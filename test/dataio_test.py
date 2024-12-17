import unittest
import os
from decimal import Decimal
import pandas as pd

from test.json_process import JsonController
from config import ConfigHandler
from csv_handler import CSVReader, CSVWriter
PROJECT_ROOT_PATH = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

class DataIoTest(unittest.TestCase):
    def __init__(self, methodName='runTest'):  # 接收 methodName 参数
        super().__init__(methodName)  # 将 methodName 传递给父类的 __init__
        self.json_controller = JsonController(os.path.join(PROJECT_ROOT_PATH, 'test', 'test_data.json'))
        self.config_handler = ConfigHandler(os.path.join(PROJECT_ROOT_PATH, 'config.xml'))

        self.config = self.config_handler.load_config()
        self.json_controller.load_json()
        self.func_test_data = self.json_controller.get_data_by_key("func_test")
        self.day_test_data = self.json_controller.get_data_by_key("day_test")
        self.hour_test_data = self.json_controller.get_data_by_key("hour_test")
        self.minute_test_data = self.json_controller.get_data_by_key("minute_test")

    def test_type_change_1(self):
        """只转换价格字段，且其中不包含异常数据"""
        data_dict = self.func_test_data["base_func"]["change_data_type_to_Decimal"]["test_1"]["data"]
        data_df = pd.DataFrame(data_dict)
        data_df = CSVReader.change_column_type_to_Decimal(data_df, True)
        self.assertIsInstance(data_df['coin_price'][0], Decimal)

if __name__ == '__main__':
    unittest.main()