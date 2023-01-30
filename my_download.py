"""
===============================
@author     : yaoruiqi

@Time       : 2023/1/30 17:32

@version    : V

@introduce  :

@change     : 
===============================
"""
from shutil import copyfileobj
from typing import Union

import requests
from my_retry.retry_model import MyRetry

global_part_download_retry_times = 1
global_download_retry_times = 1


class DownloadEngine:

    def __init__(
            self,
            save_path: str,
            chunk_size: int = 10 * 1024 * 1024,
            threads_num: int = 0,
            timeout: Union[int, tuple] = None,
            part_download_retry_times: int = 3,
            download_retry_times: int = 1,
            header: dict = {},
            cookies: dict = {}
    ):
        """
        :param save_path: 文件存储路径
        :param chunk_size: 分片大小，文件大小小于此数值则不使用分片下载
        :param threads_num: 分片下载时的线程数
        :param timeout: 超时时间
        :param part_download_retry_times: 每个分片下载重试次数
        :param download_retry_times: 整体文件下载重试次数
        :param header: 下载请求头
        :param cookies: 下载请求cookies
        """
        self.file_save_path = save_path
        self.timeout = timeout
        self.threads_num = threads_num
        self.chunk_size = chunk_size
        self.header = header
        self.cookies = cookies
        if download_retry_times:
            global global_download_retry_times
            global_download_retry_times = download_retry_times
        if part_download_retry_times:
            global global_part_download_retry_times
            global_part_download_retry_times = part_download_retry_times

    @MyRetry(times=global_part_download_retry_times, custom_return=(True, False))
    def download_part(self, url: str, part_save_path: str):
        """
        分片下载
        :param url: 下载地址
        :param part_save_path: 分片存储路径
        """
        with requests.get(url, headers=self.header, stream=True, timeout=self.timeout, cookies=self.cookies) as r:
            with open(part_save_path, 'wb') as f:
                copyfileobj(r.raw, f)
        return True
