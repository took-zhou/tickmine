#基于192.168.0.102:8098/tsaodai/ubuntu1804镜像
FROM 192.168.0.102:8098/tsaodai/ubuntu1804:latest

#维护个人信息
MAINTAINER The run Project <zhoufan@cdsslh.com>
ARG process=tickmine

#安装基础工具
RUN apt-get update && apt-get install -y screen && apt-get install -y vim && apt-get install -y net-tools && \
    apt-get install -y iputils-ping && pip install pipreqs && apt-get install -y unrar

RUN pip install --no-deps --index-url http://devpi.cdsslh.com:8090/root/dev tickmine --trusted-host devpi.cdsslh.com && \
    pip install -i https://pypi.tuna.tsinghua.edu.cn/simple tickmine

ENV LC_ALL C.UTF-8
ENV LANG C.UTF-8

#开放端口
expose 11332

COPY tickmine.sh /bin/$process.sh
ENTRYPOINT ["/bin/tickmine.sh"]
