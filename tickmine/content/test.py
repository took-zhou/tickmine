import gzip
import os

import _pickle as cPickle
import pandas as pd

base_dir = '/share/baidunetdisk/sina/naturedata'

for curDir, dirs, files in os.walk(base_dir):
    for file in files:
        if file.endswith(".csv"):
            file_path = os.path.join(curDir, file)
            target_path = file_path.split('.')[0] + '.pkl'
            print(target_path)

            try:
                file_data = pd.read_csv(file_path)
                file_data.index = pd.to_datetime(file_data['Timeindex'])
                file_data = file_data.sort_index()
                file_data.pop('Timeindex')

                serialized = cPickle.dumps(file_data)
                with gzip.open(target_path, 'wb', compresslevel=1) as file_object:
                    file_object.write(serialized)
                    file_object.close()
            except:
                pass
