import logging
import datetime
import logging.handlers
import os
# 定义日志格式
LOG_FORMAT = "%(asctime)s - %(levelname)s - %(message)s - %(pathname)s:%(lineno)d"
# 定义日期格式
DATE_FORMAT = "%m/%d/%Y %H:%M:%S"

def get_logger(name, **kwargs):
    """
    创建并返回一个自定义的日志记录器。

    参数:
    - name: 日志记录器的名称。

    返回:
    - logger: 配置有 stdout 和文件日志输出的日志记录器实例。

    该函数配置了一个日志记录器，包括以下内容：
    - 设置日志记录器的名称为 `name`。
    - 在标准输出和文件中记录日志，级别设置为 WARNING。
    - 使用指定的日志格式和日期格式。
    """
    # 创建日志记录器
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    # 创建一个输出到标准输出的日志处理器
    stdoutHandler = logging.StreamHandler()
    # 创建一个输出到文件的日志处理器，文件路径为 '../mylog.log'
    rf_handler = logging.handlers.TimedRotatingFileHandler(
        filename=kwargs.get('filename', os.path.join('log', f'{name}.log')),  # 日志文件名称
        when='midnight',  # 每天午夜滚动日志文件
        interval=1,  # 每 1 天滚动一次
        backupCount=2,  # 保留最近 7 个备份文件
        atTime=datetime.time(0, 15, 30, 0),  # 每天 00:15:30 滚动
        encoding='utf-8'
    )

    # 设置标准输出和文件输出的日志级别为 WARNING
    stdoutHandler.setLevel(logging.WARNING)
    rf_handler.setLevel(logging.INFO)

    # 创建一个日志格式器，使用指定的格式和日期格式
    fmt = logging.Formatter(LOG_FORMAT, datefmt=DATE_FORMAT)

    # 为标准输出和文件输出的日志处理器设置格式器
    stdoutHandler.setFormatter(fmt)
    rf_handler.setFormatter(fmt)

    # 向日志记录器添加标准输出和文件输出的日志处理器
    if not any(isinstance(h, logging.StreamHandler) for h in logger.handlers):
        logger.addHandler(stdoutHandler)

    if not any(isinstance(h, logging.handlers.TimedRotatingFileHandler) and
               h.baseFilename == rf_handler.baseFilename for h in logger.handlers):
        logger.addHandler(rf_handler)

    # 返回配置好的日志记录器实例
    return logger

# 如果模块被直接运行，则创建一个日志记录器实例并输出一条警告日志
if __name__ == "__main__":
    log = get_logger(__name__)
    log.warning('hello world')
