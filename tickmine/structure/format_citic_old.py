import os
import sys

from tickmine.structure import util
from tickmine.global_config import citic_dst_path
from tickmine.global_config import citic_src_path

instrument_case_match = {
    'CZCE': 'up',
    'DCE': 'low',
    'SHFE': 'low',
    'INE': 'low',
    'CFFEX': 'up'
}

def buildTargetFileName(yearMon:str, dateDir:str, newDir:str, fileName:str,exchangeDir:str,isNight:bool):
    temp_file_path = ''
    timeStr = "".join([yearMon.split('/')[-1][0:4], dateDir])
    if instrument_case_match[exchangeDir] == 'up':
        instrumentId = fileName.split(".")[0].upper()
    else:
        instrumentId = fileName.split(".")[0].lower()
    if isNight == False:
        temp_file_path = newDir + "/" + exchangeDir+"/"+ exchangeDir+"/"+instrumentId +"/"+instrumentId+"_"+timeStr+".csv"
    else:
        temp_file_path = newDir + "/" + exchangeDir + "/" + exchangeDir+"_night" + "/" + instrumentId + "/" + instrumentId + "_" + timeStr + ".csv"

    return temp_file_path

def buildAbsoluteDir(root:str, subDir:dir):
    return root+"/"+subDir

def dealExtractFiles(tmpComFile:str, exchangeDir:str,isNight:bool):
    tmpStorageDir = citic_dst_path + "/" +"tmpStorge"
    if (not os.path.exists(tmpStorageDir)):
        os.makedirs(tmpStorageDir)

    ok,dest_dir,suffix = util.getDestDirByCompressName(tmpComFile)
    if not ok:
        return

    if suffix == "zip" :
        util.unzip_single(tmpComFile, tmpStorageDir, None)
    elif suffix == "rar":
        util.unrar_single(tmpComFile, tmpStorageDir, None)
    else:
        print("compress file [{}] not zip file also not rar file".format(tmpComFile))
        return

    for subDir in os.listdir(tmpStorageDir):
        if isYearMonthDir(subDir) == False:
            tmpSubDir = tmpStorageDir+"/"+subDir
            for yearMonDir in os.listdir(tmpSubDir):
                absYearMonDir = tmpSubDir + "/" + yearMonDir
                extractFiles(absYearMonDir,exchangeDir,isNight)
        else:
            absYearMonDir = tmpStorageDir+"/"+subDir
            extractFiles(absYearMonDir,exchangeDir,isNight)

    util.delDir(tmpStorageDir)

def extractFiles(yearMonDir:str,exchangeDir:str,isNight:bool):
    for letterDir in os.listdir(yearMonDir):
        tmpLetterDir = buildAbsoluteDir(yearMonDir, letterDir)
        if not os.path.isdir(tmpLetterDir):
            continue
        for dateDir in os.listdir(tmpLetterDir):
            tmpDateDir = buildAbsoluteDir(tmpLetterDir, dateDir)
            print("tmpDateDir:",tmpDateDir)
            if os.path.isdir(tmpDateDir):
                for fileName in os.listdir(tmpDateDir):
                    # print("fileName",fileName)
                    instrumentFile = buildAbsoluteDir(tmpDateDir, fileName)
                    # print("instrumentFile:",instrumentFile)
                    targetFileName = buildTargetFileName(yearMonDir,dateDir,citic_dst_path,fileName,exchangeDir,isNight)
                    # print("targetFileName:",targetFileName)
                    destDir = targetFileName[0:targetFileName.rfind("/")]
                    if (not os.path.exists(destDir)):
                        os.makedirs(destDir)
                    fixHeadError(instrumentFile)
                    util.copyFile(instrumentFile, targetFileName)

def isNightDir(subExchangeDir:str):
    strGroup = subExchangeDir.split("_")
    if strGroup[-1] == "night":
        return True
    return False

def isYearMonthDir(tmpDir:str):
    ret = False
    if len(tmpDir) == 6 and tmpDir.isnumeric():
        ret = True

    return ret

def fixHeadError(targetFilePath):
    # 解决文本第一行出现\\r\\n但是没有换行
    #citic_dst_path = "/home/zhoufan/ttt"
    with open(targetFilePath, 'rb') as f:
        lines = []
        row_index = 0
        find_flag = False
        while True:
            line = f.readline()
            if line:
                row_index = row_index + 1
                if row_index == 1:
                    if line[206:210] == b'\\r\\n':
                        print('find %s head length %d is error'%(targetFilePath, len(line)))
                        find_flag = True
                    else:
                        print('file %s head length %d is ok'%(targetFilePath, len(line)))
                        break
                lines.append(line)
            else:
                break
        f.close()

        if find_flag == True:
            temp_a = lines[0][0:206] + b'\r\r\n'
            temp_b = lines[0][210:]
            del lines[0]
            lines.insert(0, temp_b)
            lines.insert(0, temp_a)
            with open(targetFilePath, 'wb') as f:
                f.writelines(lines)
                f.close()

def fixLastContainsCommasAndDoubleContextError(targetFilePath):
    # 解决文本最后一行包含逗号错误 文本重复两次错误
    with open(targetFilePath, 'rb') as f:
        lines = []
        row_index = 0
        find_flag = False
        while True:
            line = f.readline()
            if line:
                row_index = row_index + 1
                if row_index == 1:
                    headline = line

                if line[-4:] == b',\r\r\n':
                    line = line[:-4] + line[-3:]
                    find_flag = True

                if line[-3:] == b',\r\n':
                    line = line[:-3] + line[-2:]
                    find_flag = True

                if line[-1:] == b',':
                    line = line[:-1]
                    find_flag = True

                if row_index > 1 and line == headline:
                    find_flag = True
                    break

                lines.append(line)
            else:
                break
        f.close()

    if find_flag == True:
        print('find %s last contains commas or double context error'%(targetFilePath))
        with open(targetFilePath, 'wb') as f:
            f.writelines(lines)
            f.close()

def main(_time):
    for exchangeDir in os.listdir(citic_src_path):
        tmpExchangePath = citic_src_path + "/" + exchangeDir
        print(tmpExchangePath)
        for subExchangeDir in os.listdir(tmpExchangePath):
            isNight = isNightDir(subExchangeDir)
            tmpSubExchangeDir = tmpExchangePath + "/" + subExchangeDir
            print(tmpSubExchangeDir)
            for dateDir in os.listdir(tmpSubExchangeDir):
                tmpDateDir = tmpSubExchangeDir + "/" + dateDir
                print(tmpDateDir)
                compressFiles = [item for item in os.listdir(tmpDateDir) if item[-3:] == 'zip' or item[-3:] == 'rar']
                if _time == '*':
                    need_compressFiles = compressFiles
                else:
                    need_compressFiles = [item for item in compressFiles if _time in item]
                for comFile in need_compressFiles:
                    tmpComFile = tmpDateDir + "/" + comFile
                    print(f"begin deal with {tmpComFile}")
                    dealExtractFiles(tmpComFile,exchangeDir,isNight)

    os.system("echo 1 > /proc/sys/vm/drop_caches")
    os.system("echo 2 > /proc/sys/vm/drop_caches")
    os.system("echo 3 > /proc/sys/vm/drop_caches")

if __name__=="__main__":
    #pass
    main('2015')
    # main('2016')
    # main('2017')
    # main('2018')
    # main('2019')
    # main('2020')
    # main('202101')
