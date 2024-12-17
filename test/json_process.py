import json
import os
import pandas as pd
PROJECT_ROOT_PATH = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

class JsonController:
    def __init__(self, file_path: str):
        self.file_path = file_path
        self.json_data = None
    def load_json(self):
        with open(self.file_path, 'r', encoding='utf-8') as file:
            self.json_data = json.load(file)
        return self.json_data

    def save_json(self):
        if self.json_data is None:
            raise ValueError("JSON 数据为空，无法保存。")
        with open(self.file_path, 'w', encoding='utf-8') as file:
            json.dump(self.json_data, file, ensure_ascii=False, indent=4)

    def get_data_by_key(self, key: str):
        """根据指定key获取对应的测试数据"""
        if self.json_data is None:
            raise ValueError("JSON 数据为空，无法获取数据。")
        if key not in self.json_data:
            raise KeyError(f"指定的键 '{key}' 不存在于 JSON 数据中。")
        return self.json_data[key]

    def add_data_by_key(self, key: str, data: dict):
        """添加测试数据"""
        self.json_data[key] = data

    def trans_df_to_json(self, data_df: pd.DataFrame, key: str):
        """将DataFrame格式的数据转换为JSON格式"""
        data_dict = data_df.to_dict(orient='records')
        self.json_data[key] = data_dict

if __name__ == '__main__':
    json_file_path = os.path.join(PROJECT_ROOT_PATH, 'test', 'test_data.json')
    jsonController = JsonController(json_file_path)
    data = {
  "hour_data": {
    "desc": "存储以小时为单位的数据",
    "func_1": {
      "desc": "1.有ABC三个时刻。其中C为当前时刻\n2.C时刻在跌 C_CHANGE < 0\n3.A时刻与C时刻不超过MAX_TIME_INTERVAL小时。\n4.A时刻在B时刻之前，A和B时刻均要满足虚降>=AB_VIRTUAL_DROP%,且跌涨幅<=AB_CHANGE%\n5.A时刻收盘价大于B时刻收盘价。6.C时刻收盘价同时小于A的最低价和B的最低价。7.C时刻小于前C_PRE_TIME_INTERVAL小时最低价里的最小值",
      "test_1": {
        "desc": ""
      }
    }
  },
  "day_data": {
    "desc": "存储以天为单位的数据",
    "base_data": []
  },
  "minute_data": {
    "desc": "存储以分钟为单位的数据",
    "base_data": []
  },
  "func_test": {
    "desc": "测试单独的功能函数",
    "day_func": {
      "desc": "测试以天为单位的功能函数",
      "func_1": {
        "desc": "功能描述",
        "data_1": {
          "desc": "...",
          "data": [
            {
              "coin": "btc",
              "price": 100
            },
            {
              "coin": "eth",
              "price": 200
            }
          ],
          "result": [
            "btc"
          ]
        }
      }
    },
    "hour_func": {
      "desc": "测试以小时为单位的功能函数"
    },
    "minute_func": {
      "desc": "测试以分钟为单位的功能函数"
    }
  }
}
    jsonController.json_data = data
    jsonController.save_json()
    jsonController.load_json()
    print(jsonController.json_data)

