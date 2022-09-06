#!/bin/bash

chown 1000:1000 $HOME

echo 'shell /bin/bash' > $HOME/.screenrc

if [ $run_mode = 'release' ];
then
if [ -e $HOME/.pip/pip.conf ];
then
pip install --no-deps tickmine;
else
pip install --no-deps --index-url http://devpi.cdsslh.com:8090/root/dev tickmine --trusted-host devpi.cdsslh.com;
fi
fi

if [ -e $HOME/.tickmine ]; then echo ".tickmine existed"; else mkdir $HOME/.tickmine; fi

if [ $run_mode = 'release' ];
then
tickmine_path=` python -c "import tickmine;print(tickmine.__path__[0])" `
nohup python $tickmine_path/content/server.py >> $HOME/.tickmine/output.log 2>&1 &
fi

while true; do sleep 10000; done
