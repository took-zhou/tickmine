from setuptools import setup, find_packages

setup(
    name="tickmine",
    version="1.0.4",
    author="zhoufan",
    author_email="zhoufan@cdsslh.com",
    description="data layer",

    # 项目主页
    url="http://devpi.cdsslh.com:8090",

    # 项目的依赖库，读取的requirements.txt内容
    install_requires = [
        'numpy>=1.19.5',
        'pandas>=1.1.5',
        'rarfile>=4.0',
        'setuptools>=39.0.1',
        'streamlit>=1.1.0',
        'zerorpc>=0.6.3',
        'plotly>=5.4.0'
    ],

    # 你要安装的包，通过 setuptools.find_packages 找到当前目录下有哪些包
    packages=find_packages()
)
