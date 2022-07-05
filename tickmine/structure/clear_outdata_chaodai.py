import datetime
import os

from tickmine.global_config import tsaodai_dst_path

old_date = datetime.datetime.today() - datetime.timedelta(60)
old_date = '%04d%02d%02d' % (old_date.year, old_date.month, old_date.day)

for root, dirs, files in os.walk(tsaodai_dst_path):
    for name in files:
        date = name.split('_')[-1].split('.')[0]

        if date < old_date:
            command = 'rm %s' % (os.path.join(root, name))
            os.system(command)
            print(command)

    for name in dirs:
        dir = os.path.join(root, name)
        if len(os.listdir(dir)) == 0:
            command = 'rm -rf %s' % (dir)
            os.system(command)
            print(command)

os.system('sync')
