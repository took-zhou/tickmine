# import gzip
# import os

# import _pickle as cPickle
# import pandas as pd

import datetime
import os
import time

command = "python /share/quantation/straview/tickmine/tickmine/content/raw_tick.py '' option"
msg = '%s ok.' % (command) if os.system(command) == 0 else '%s error.' % (command)
print(msg)

command = "python /share/quantation/straview/tickmine/tickmine/content/k_line.py '' option"
msg = '%s ok.' % (command) if os.system(command) == 0 else '%s error.' % (command)
print(msg)

command = "python /share/quantation/straview/tickmine/tickmine/content/level1.py '' option"
msg = '%s ok.' % (command) if os.system(command) == 0 else '%s error.' % (command)
print(msg)

command = "python /share/quantation/straview/tickmine/tickmine/content/m_line.py '' option"
msg = '%s ok.' % (command) if os.system(command) == 0 else '%s error.' % (command)
print(msg)

# base_dir = '/share/baidunetdisk/citic/naturedata/'

# for curDir, dirs, files in os.walk(base_dir):
#     for file in files:
#         if file.endswith(".csv"):
#             file_path = os.path.join(curDir, file)
#             target_path = file_path.split('.')[0] + '.pkl'

#             if not os.path.exists(target_path) == False and '20220601' <= file.split('.')[0].split('_')[-1]:
#                 print(target_path)
#                 try:
#                     file_data = pd.read_csv(file_path)
#                     file_data.index = pd.to_datetime(file_data['Timeindex'])
#                     file_data = file_data.sort_index()
#                     file_data.pop('Timeindex')

#                     serialized = cPickle.dumps(file_data)
#                     with gzip.open(target_path, 'wb', compresslevel=1) as file_object:
#                         file_object.write(serialized)
#                         file_object.close()
#                 except:
#                     pass

# base_dir = [
#     '/share/baidunetdisk/citic/naturedata/lastprice/d1_kline/CFFEX/', '/share/baidunetdisk/citic/naturedata/lastprice/m1_kline/CFFEX/',
#     '/share/baidunetdisk/citic/naturedata/lastprice/m60_kline/CFFEX/', '/share/baidunetdisk/citic/naturedata/lastprice/ma_line/CFFEX/',
#     '/share/baidunetdisk/citic/naturedata/lastprice/rawtick/CFFEX/', '/share/baidunetdisk/citic/naturedata/lastprice/w1_kline/CFFEX/',
#     '/share/baidunetdisk/citic/naturedata/tradepoint/askbidpair/CFFEX/', '/share/baidunetdisk/citic/naturedata/lastprice/tradepoint/CFFEX/'
# ]

# for aaaa in base_dir:
#     for curDir, dirs, files in os.walk(aaaa):
#         for dir in dirs:
#             dir_path = os.path.join(curDir, dir)
#             dir_files = os.listdir(dir_path)
#             if len(dir_files) == 1 or len(dir_files) == 2:
#                 for item in dir_files:
#                     if '20220615' in item:
#                         print(dir_path)
#                         os.system('rm -rf %s' % (dir_path))

# base_dir = ['/share/baidunetdisk/citic/naturedata', '/share/baidunetdisk/citic/citic_ticks', '/share/baidunetdisk/citic/reconstruct']

# for item in base_dir:
#     ret = os.listdir(item)
#     # print('item', ret)
#     if 'CZCE' in ret and 'DCE' in ret:
#         if not os.path.exists(item.replace('/citic/', '/citic2/')):
#             os.makedirs(item.replace('/citic/', '/citic2/'))
#         command = 'cp -rf %s/CZCE %s/' % (item, item.replace('/citic/', '/citic2/'))
#         print(command)
#         os.system(command)
#         command = 'cp -rf %s/DCE %s/' % (item, item.replace('/citic/', '/citic2/'))
#         print(command)
#         os.system(command)
#         continue
#     else:
#         for sub_item in ret:
#             sub_ret = os.listdir('%s/%s' % (item, sub_item))
#             # print('sub_item', sub_ret)
#             if 'CZCE' in sub_ret and 'DCE' in sub_ret:
#                 if not os.path.exists('%s/%s/' % (item.replace('/citic/', '/citic2/'), sub_item)):
#                     os.makedirs('%s/%s/' % (item.replace('/citic/', '/citic2/'), sub_item))
#                 command = 'cp -rf %s/%s/CZCE %s/%s/' % (item, sub_item, item.replace('/citic/', '/citic2/'), sub_item)
#                 print(command)
#                 os.system(command)
#                 command = 'cp -rf %s/%s/DCE %s/%s/' % (item, sub_item, item.replace('/citic/', '/citic2/'), sub_item)
#                 print(command)
#                 os.system(command)
#                 continue
#             else:
#                 for sub_sub_item in sub_ret:
#                     sub_sub_ret = os.listdir('%s/%s/%s' % (item, sub_item, sub_sub_item))
#                     # print('sub_sub_item', sub_sub_ret)
#                     if 'CZCE' in sub_sub_ret or 'DCE' in sub_sub_ret:
#                         if not os.path.exists('%s/%s/%s/' % (item.replace('/citic/', '/citic2/'), sub_item, sub_sub_item)):
#                             os.makedirs('%s/%s/%s/' % (item.replace('/citic/', '/citic2/'), sub_item, sub_sub_item))

#                         command = 'cp -rf %s/%s/%s/CZCE %s/%s/%s/' % (item, sub_item, sub_sub_item, item.replace(
#                             '/citic/', '/citic2/'), sub_item, sub_sub_item)
#                         print(command)
#                         os.system(command)
#                         command = 'cp -rf %s/%s/%s/DCE %s/%s/%s/' % (item, sub_item, sub_sub_item, item.replace(
#                             '/citic/', '/citic2/'), sub_item, sub_sub_item)
#                         print(command)
#                         os.system(command)
#                         continue

# base_dir = '/share/baidunetdisk/tsaodai/temp'
# import re

# ret = os.listdir(base_dir)
# for item2 in ret:
#     path = '%s/%s' % (base_dir, item2)
#     ret2 = os.listdir(path)
#     for item3 in ret2:
#         path = '%s/%s/%s' % (base_dir, item2, item3)
#         ret3 = os.listdir(path)
#         for item4 in ret3:
#             path = '%s/%s/%s/%s' % (base_dir, item2, item3, item4)
#             ret4 = os.listdir(path)
#             for item5 in ret4:
#                 path = '%s/%s/%s/%s/%s' % (base_dir, item2, item3, item4, item5)
#                 ret5 = os.listdir(path)
#                 for item6 in ret5:
#                     path = '%s/%s/%s/%s/%s/%s' % (base_dir, item2, item3, item4, item5, item6)
#                     name_split = re.split('([0-9]+)', item6)
#                     if len(name_split) >= 3:
#                         _year_ = name_split[1]

#                         if 'CZCE' in path:
#                             if int(_year_[0:1]) > 4:
#                                 yearstr = '201%s' % _year_[0:1]
#                             else:
#                                 yearstr = '202%s' % _year_[0:1]
#                         else:
#                             yearstr = '20%s' % _year_[0:2]

#                         temp_target_path = path.replace('/temp/', '/')
#                         temp_target_path_split = temp_target_path.split('/')
#                         temp_split = temp_target_path_split[0:-3] + [yearstr] + temp_target_path_split[-3:-1]
#                         target_path = '/'.join(temp_split)
#                         if not os.path.exists(target_path):
#                             os.makedirs(target_path)
#                         command = 'mv %s %s' % (path, target_path)
#                         #print(command)
#                         os.system(command)

# for curDir, dirs, files in os.walk(base_dir):
#     for dir in dirs:
#         name_split = re.split('([0-9]+)', dir)
#         if len(name_split) >= 2:
#             _year_ = name_split[1]
#             dir_path = os.path.join(curDir, dir)
#             if 'CZCE' in dir_path:
#                 if int(_year_[1:3]) > 4:
#                     yearstr = '201%s' % _year_[0:1]
#                 else:
#                     yearstr = '202%s' % _year_[0:1]
#             else:
#                 yearstr = '20%s' % _year_[0:2]

#             print('%s %s' % (dir_path, yearstr))

# base_dir = ['/share/baidunetdisk/citic/naturedata', '/share/baidunetdisk/citic/reconstruct']

# import re

# for item in base_dir:
#     ret = os.listdir(item)
#     for item2 in ret:
#         path = '%s/%s' % (item, item2)
#         ret2 = os.listdir(path)
#         for item3 in ret2:
#             if item3 >= '2023':
#                 path = '%s/%s/%s' % (item, item2, item3)
#                 target_path = '%s/%s' % (item.replace('/citic/', '/citic2/'), item2)
#                 if not os.path.exists(target_path):
#                     os.makedirs(target_path)
#                 command = 'mv %s %s' % (path, target_path)
#                 print(command)
#                 os.system(command)
