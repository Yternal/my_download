"""
===============================
@author     : yaoruiqi

@Time       : 2023/1/30 17:32

@version    : V

@introduce  :

@change     : 
===============================
"""
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from shutil import copyfileobj
from typing import Union
from uuid import uuid4
import requests
from my_retry.retry_model import MyRetry
from copy import deepcopy
import warnings


global_part_download_retry_times = 1
global_download_retry_times = 1


class DownloadEngine:

    def __init__(
            self,
            save_path: Union[str, Path],
            cache_dir: str = '',
            chunk_size: int = 10 * 1024 * 1024,
            threads_num: int = 1,
            part_download_retry_times: int = 3,
            download_retry_times: int = 1,
    ):
        """
        :param save_path: 文件存储路径
        :param cache_dir: 缓存文件夹路径
        :param chunk_size: 分片大小，文件大小小于此数值则不使用分片下载
        :param threads_num: 分片下载时的线程数
        :param part_download_retry_times: 每个分片下载重试次数
        :param download_retry_times: 整体文件下载重试次数
        """
        self.chunk_size = chunk_size
        self.threads_num = threads_num
        assert isinstance(chunk_size, int) and chunk_size > 0, 'chunk_size argument error'
        assert isinstance(threads_num, int) and threads_num > 0, 'threads_num argument error'
        if isinstance(save_path, str):
            self.save_path = Path(save_path)
        elif isinstance(save_path, Path):
            self.save_path = save_path
        else:
            raise TypeError('save_path argument must be str or Path')

        if cache_dir:
            if isinstance(cache_dir, str):
                self.cache_dir = Path(cache_dir)
            elif isinstance(cache_dir, Path):
                self.cache_dir = cache_dir
            else:
                raise TypeError('cache_dir argument must be str or Path')
            self.cache_dir.mkdir(parents=True, exist_ok=True)
        else:
            self.cache_dir = self.save_path.parent

        if download_retry_times:
            global global_download_retry_times
            global_download_retry_times = download_retry_times
        if part_download_retry_times:
            global global_part_download_retry_times
            global_part_download_retry_times = part_download_retry_times

    @MyRetry(times=global_part_download_retry_times, custom_return=(True, False))
    def download_part(self, url: str, part_cache_save_path: str, timeout: Union[int, tuple] = None, header: dict = {}, cookies: dict = {}):
        """
        分片下载
        :param url: 下载地址
        :param part_cache_save_path: 存储地址
        :param timeout: 请求超时时间
        :param header: 下载请求头
        :param cookies: 下载请求cookies
        """
        with requests.get(url, headers=header, stream=True, timeout=timeout, cookies=cookies) as r:
            with open(part_cache_save_path, 'wb') as f:
                copyfileobj(r.raw, f)
        return True

    def merge_part(self, part_cache_save_path_list: list):
        """
        合并分片
        :param part_cache_save_path_list: 分片文件存储路径列表
        """
        total_file = open(self.save_path, 'ab')
        for i in part_cache_save_path_list:
            with open(i, 'rb') as r:
                b = r.read()
                total_file.write(b)
        total_file.close()

    def download(self, url: str, timeout: Union[int, tuple] = None, header: dict = {}, cookies: dict = {}):
        """
        :param url: 下载地址
        :param timeout: 请求超时时间
        :param header: 下载请求头
        :param cookies: 下载请求cookies
        """
        for i in range(global_download_retry_times):
            part_cache_save_path_list = []
            try:
                request_headers = deepcopy(header)
                request_headers.update({'Range': f'bytes=0-0'})
                file_header = requests.get(url, headers=request_headers, cookies=cookies, timeout=timeout).headers
                file_size = file_header.get('Content-Range', '')

                if not file_size or (file_size := int(file_size.rsplit('/', 1)[-1])) <= self.chunk_size:
                    download_result = self.download_part(url, self.save_path, timeout, header, cookies)
                    assert download_result, f'文件分片下载失败 - {i}'
                    return True
                else:
                    uid = str(uuid4())
                    start_pos = 0
                    chunk_size_head = []
                    for m in range(0, file_size, self.chunk_size):
                        chunk_size_head.append([start_pos, m + self.chunk_size])
                        start_pos = m + self.chunk_size + 1
                    chunk_size_head[-1][1] = ''
                    with ThreadPoolExecutor(max_workers=length if (length := len(chunk_size_head)) <= self.threads_num else self.threads_num) as t:
                        task_list = []
                        for _id, n in enumerate(chunk_size_head):
                            part_cache_save_path_list.append(part_cache_save_path := self.cache_dir / f'{_id}_{uid}')
                            request_headers = deepcopy(header)
                            request_headers.update({'Range': f'bytes={n[0]}-{n[1]}'})
                            task_list.append(t.submit(self.download_part, url, part_cache_save_path, timeout, request_headers, cookies))
                        for future in as_completed(task_list):
                            result = future.result()
                            assert result, f'文件分片下载失败 - {i}'
                    self.merge_part(part_cache_save_path_list)
                    return True
            except Exception as e:
                if i == global_download_retry_times - 1:
                    raise e
            finally:
                if part_cache_save_path_list:
                    for file in part_cache_save_path_list:
                        try:
                            file.unlink(missing_ok=True)
                        except Exception as e:
                            print(f'Cache file deletion failed, {file}. CAUSE: {e}')


if __name__ == '__main__':
    engine = DownloadEngine(
        save_path=r'C:\Users\Administrator\Desktop\fsdownload\part\13441557_巴尔扎克选集3幻灭下.pdf',
    )
    res = engine.download(url='http://111.161.65.168:9000/yk_e/pdf/13441557_巴尔扎克选集3幻灭下.pdf', timeout=5)
    print(res)
