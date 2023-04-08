from setuptools import find_packages, setup

setup(
    name="tickmine",
    version="4.6.8",
    author="zhoufan",
    author_email="zhoufan@cdsslh.com",
    description="data layer",

    # 项目主页
    url="http://devpi.cdsslh.com:8090",

    # 项目的依赖库，读取的requirements.txt内容
    install_requires=['pandas>=1.1.5', 'zerorpc==0.6.3', 'pytest>=7.0.1'],

    # 你要安装的包，通过 setuptools.find_packages 找到当前目录下有哪些包
    packages=find_packages())
