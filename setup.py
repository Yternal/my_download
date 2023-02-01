try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup

setup(
    name='my_download',
    author='yrq',
    version='0.1.0',
    license='MIT',

    description='file download function，Support split download',
    author_email='1307272972@qq.com',
    url='https://github.com/Yternal/my_download',

    # 包内需要引用的文件夹
    packages=["my_download"],

    # 依赖包
    install_requires=[
        'requests >= 1.18.8',
        'my_retry >= 0.1.0'
    ],

    # python版本
    python_requires='>=3.8',

    zip_safe=True,
)
