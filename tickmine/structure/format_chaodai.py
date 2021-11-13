import os
import tarfile
import sys
import json

from tickmine.structure import util

instrument_case_match = {
    'CZCE': 'up',
    'DCE': 'low',
    'SHFE': 'low',
    'INE': 'low',
    'CFFEX': 'up'
}

def untar(fname, dirs):
    """
    解压tar.gz文件
    :param fname: 压缩文件名
    :param dirs: 解压后的存放路径
    :return: bool
    """
    try:
        t = tarfile.open(fname)
        t.extractall(path = dirs)
        return True
    except Exception as e:
        print(e)
        return False

def buildTargetFileName(oldFile:str, newDir:str, fileName:str,exchangeDir:str, isNight:bool):
    groups = oldFile.split("_")
    timeStr = groups[2].split(".")[0]
    splitedTime = timeStr.split("-")
    timeStr = "".join(splitedTime)

    if instrument_case_match[exchangeDir] == 'up':
        instrumentId = fileName.split(".")[0].upper()
    else:
        instrumentId = fileName.split(".")[0].lower()

    if isNight == False:
        return newDir + "/" + exchangeDir+"/"+ exchangeDir+"/"+instrumentId +"/"+instrumentId+"_"+timeStr+".csv"
    return newDir + "/" + exchangeDir + "/" + exchangeDir+"_night" + "/" + instrumentId + "/" + instrumentId + "_" + timeStr + ".csv"

def initRecordInfo(dataRootPath, recordFileName):
    tmpPath = dataRootPath + "/" + recordFileName
    if(not os.path.exists(tmpPath)):
        util.create_file_linux(tmpPath)
    f_record = open(tmpPath, "r")
    f_record.seek(0, 0)
    tmpRecord = {}
    try:
        tmpRecord = json.load(f_record)
    except:
        print(tmpPath+" init, no records")
    f_record.close()
    if "SaveFiles" in tmpRecord.keys():
        return True, tmpRecord["SaveFiles"]
    return False, []

def updateRecordInfo(dataRootPath, recordFileName, comPresFile):
    tmpPath = dataRootPath + "/" + recordFileName

    rawContent = {}
    with open(tmpPath, "r") as fr:
        try:
            rawContent =  json.load(fr)
        except:
            print(tmpPath+" init, no records to read")
    content = []
    if "SaveFiles" in rawContent.keys():
        content = rawContent["SaveFiles"]
    content.append(comPresFile)
    f_record = open(tmpPath, "w")
    f_record.seek(0, 0)
    content = {"SaveFiles":content}
    json.dump(content,f_record, indent=4)
    f_record.close()

def reconstruct(dataRootPath, newDataRootPath, recordFileName, isNight:bool=False):
    if (not os.path.exists(newDataRootPath)):
        os.makedirs(newDataRootPath)
    result,records = initRecordInfo(dataRootPath, recordFileName)
    if not result:
        records = []
    records = set(records)
    for comPresFile in os.listdir(dataRootPath):
        if(comPresFile == recordFileName):
            continue
        if(comPresFile in records):
            print(comPresFile)
            continue
        print(comPresFile)
        compressFile = dataRootPath + "/" + comPresFile
        tmp_uncompress_dir = newDataRootPath+"/"+"tmp"
        if (not os.path.exists(tmp_uncompress_dir)):
            os.makedirs(tmp_uncompress_dir)
        untar(compressFile,tmp_uncompress_dir)
        for exchangeDir in os.listdir(tmp_uncompress_dir):
            tmpExchangeDir = tmp_uncompress_dir + "/" + exchangeDir
            for file in os.listdir(tmpExchangeDir):
                sourceFile = tmpExchangeDir + "/" + file
                targetFileName =buildTargetFileName(comPresFile,newDataRootPath,file,exchangeDir, isNight)
                destDir = targetFileName[0:targetFileName.rfind("/")]
                if (not os.path.exists(destDir)):
                    os.makedirs(destDir)
                util.copyFile(sourceFile, targetFileName)
                #print('copy %s to %s'%(sourceFile, targetFileName))
        util.delDir(tmp_uncompress_dir)
        updateRecordInfo(dataRootPath, recordFileName, comPresFile)

    print("reconstruct ok!")

def main():
    from tickmine.global_config import tsaodai_src_path
    from tickmine.global_config import tsaodai_dst_path

    recordFileName = "record.json"

    reconstruct("%s/day"%(tsaodai_src_path), tsaodai_dst_path, recordFileName, isNight=False)
    reconstruct("%s/night"%(tsaodai_src_path), tsaodai_dst_path, recordFileName, isNight=True)

if __name__=="__main__":
    main()
