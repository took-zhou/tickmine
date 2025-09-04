from setuptools import find_packages, setup

setup(
    name="tickmine",
    version="4.8.5",
    author="zhoufan",
    author_email="zhoufan@tsaodai.com",
    description="data layer",
    package_data={"": ["*.csv"]},

    # 项目主页
    url="https://devpi.tsaodai.com",

    # 项目的依赖库，读取的requirements.txt内容
    install_requires=['pandas>=1.4.3', 'grpcio>=1.60.0', 'pytest>=7.1.2', 'setuptools>=39.0.1'],

    # 你要安装的包，通过 setuptools.find_packages 找到当前目录下有哪些包
    packages=find_packages())
