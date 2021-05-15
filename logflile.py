import logging
import os
import sys


def funLog():
    # 1 配置 日志打印的格式
    file_format = '%(asctime)s - %(name)s - %(levelname)s - 进程ID: %(process)d - %(message)s'

    # 2 获取当前文件所在的路径
    cur_path = os.path.dirname(os.path.realpath(__file__))
    # print(cur_path)
    # # 获取目录下的日志文件夹
    log_catalog_path = os.path.join(cur_path, "log")

    # 获取日志文件夹下的日志文件
    log_file_path = os.path.join(log_catalog_path, "file_log.log")
    # print(log_catalog_path)
    # print(log_file_path)
    # 3. 生成 logger 对象
    logger = logging.getLogger("mylogger")
    # 3.1. 设置统一的日志级别, 全局 日志级别
    logger.setLevel(logging.DEBUG)

    fh = logging.FileHandler(log_file_path, encoding='utf-8')  # 输出到文件
    fh.setLevel(logging.INFO)  # 为单个 handler 设置日志级别
    logger.addHandler(fh)  # 绑定 fh

    # #设置日志文件内容的格式( 可以把格式单独写出来，如下格式。也可以直接配置格式)
    # file_handler.setFormatter(logging.Formatter(fmt="%(asctime) s - %(name)s - %(levelname)s - %(module)s: %(message)s"))
    file_formatter = logging.Formatter(file_format)     # 生成 输出到文件 的模式
    fh.setFormatter(file_formatter)  # 给这个handler选择一个格式

    return logger
#     logger.info('出错信息====')
# funLog()