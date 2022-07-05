#!/bin/bash

chown 1000:1000 $HOME

echo 'shell /bin/bash' > $HOME/.screenrc

if [ $run_mode = 'debug' ];
then
pip uninstall -y tickmine;
else
pip install --no-deps --upgrade --index-url http://devpi.cdsslh.com:8090/root/dev tickmine --trusted-host devpi.cdsslh.com;
fi

if [ -e $HOME/.tickmine ]; then echo ".tickmine existed"; else mkdir $HOME/.tickmine; fi

if [ $run_mode = 'release' ];
then
nohup python /usr/local/lib/python3.6/dist-packages/tickmine/content/server.py >> $HOME/.tickmine/output.log 2>&1 &
fi

while true; do sleep 10000; done
