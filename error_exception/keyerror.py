
class KeyNotFound(Exception):
    """
    在字典或JSON数据中未找到指定的key时引发的异常
    """
    def __init__(self, message="未找到期望的key"):
        super().__init__(message)


class SpiderFailedError(Exception):
    """
    爬虫执行失败时引发的异常
    """
    def __init__(self, message="爬取失败"):
        super().__init__(message)