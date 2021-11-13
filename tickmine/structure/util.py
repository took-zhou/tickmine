import zipfile
import os
import shutil
import rarfile

def unzip_single(src_file, dest_dir, password):
    ''' 解压单个文件到目标文件夹。
    '''
    if password:
        password = password.encode()
    zf = zipfile.ZipFile(src_file)
    try:
        zf.extractall(path=dest_dir, pwd=password)
    except RuntimeError as e:
        print(e)
    zf.close()

def unrar_single(src_file, dest_dir, password):
    ''' 解压单个文件到目标文件夹。
    '''
    if password:
        password = password.encode()
    zf = rarfile.RarFile(src_file)
    try:
        zf.extractall(dest_dir)
    except RuntimeError as e: 
        print(e)
    zf.close()

def delDir(top):
    for root, dirs, files in os.walk(top, topdown=False):
        for name in files:
            os.remove(os.path.join(root, name))
        for name in dirs:
            os.rmdir(os.path.join(root, name))
    os.removedirs(top)

def delDirTree(top):
    shutil.rmtree(top)

def getDestDirByCompressName(CompressFileName):
    splitedItem = CompressFileName.split(".")
    if len(splitedItem) != 2:
        print("compressFileName split error, length is [{}]".format(len(splitedItem)))
        return False,"",""
    else:
        return True, splitedItem[-2], splitedItem[-1]

def moveFile(fileName, destDir):
    shutil.move(fileName,destDir)

def copyFile(fileName, destFileName):
    shutil.copyfile(fileName,destFileName)

def create_file_linux(filename):
    path = filename[0:filename.rfind("/")]
    if not os.path.isdir(path):  # 无文件夹时创建
        os.makedirs(path)
    if not os.path.isfile(filename):  # 无文件时创建
        fd = open(filename, mode="w", encoding="utf-8")
        fd.close()
    else:
        pass

def create_file_windows(filename):
    path = filename[0:filename.rfind("\\")]
    if not os.path.isdir(path):  # 无文件夹时创建
        os.makedirs(path)
    if not os.path.isfile(filename):  # 无文件时创建
        fd = open(filename, mode="w", encoding="utf-8")
        fd.close()
        print(filename," create ok!")
    else:
        print(filename," has exist!")

def isFileInFileList(file,fileSet:set):
#     tmpFileList = fileList.copy()
    if len(fileSet) == 0:
        return False
    length1 = len(fileSet)
    fileSet.add(file)
    length2 = len(fileSet)
    if length1 != length2:
        return False
    return True
