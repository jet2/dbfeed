import os
from time import sleep
from settings import tag_typer
import datetime
import csv

def getFilesToDelete(self, newFileName):
    dirName, fName = os.path.split(self.origFileName)
    dName, newFileName = os.path.split(newFileName)

    fileNames = os.listdir(dirName)
    result = []
    prefix = fName + "."
    postfix = self.postfix
    prelen = len(prefix)
    postlen = len(postfix)
    for fileName in fileNames:
        if fileName[:prelen] == prefix and fileName[-postlen:] == postfix and len(
                fileName) - postlen > prelen and fileName != newFileName:
            suffix = fileName[prelen:len(fileName) - postlen]
            if self.extMatch.match(suffix):
                result.append(os.path.join(dirName, fileName))
    result.sort()
    if len(result) < self.backupCount:
        result = []
    else:
        result = result[:len(result) - self.backupCount]
    return result


def make_filegroups():
    dirName, fName = os.path.split(os.path.realpath(__file__))
    dirName = os.path.join(dirName, 'csv')
    fileNames = os.listdir(dirName)
    fileNames.sort()
    goodfiles = dict()
    # все файлы старше 5 минут притягиваются к началу 5-минутного интервала с :Х0 или :Х5 минуты
    for item in fileNames:
        dt = datetime.datetime.strptime(item[:16]+'-00', "%Y-%m-%d-%H-%M-%S")
        dtnow = datetime.datetime.now()
        if dtnow - dt > datetime.timedelta(minutes=5):
            nmstr = '0' # к нему притягиваются [0,1,2,3,4]
            if int(item[15:16]) in [5,6,7,8,9]:
                nmstr = '5'
            dtnode = item[:15] + nmstr
            print(item, nmstr, dtnode)
            if dtnode not in goodfiles:
                goodfiles[dtnode] = list()
            goodfiles[dtnode].append(item)
    return goodfiles



def integrate_filegroups_withmaster_true(files_dict):
    dirName, fName = os.path.split(os.path.realpath(__file__))
    dirName = os.path.join(dirName, 'csv')
    alltagnames = tag_typer["sumbyavg"] + tag_typer["sum"]+tag_typer["avg"]+tag_typer["bool"]
    alltagsTypes = dict()
    for item in tag_typer["sumbyavg"]:
        alltagsTypes[item] = 3
    for item in tag_typer["sum"]:
        alltagsTypes[item] = 2
    for item in tag_typer["avg"]:
        alltagsTypes[item] = 1
    for item in tag_typer["bool"]:
        alltagsTypes[item] = 0

    for minute_node_key, files_arr in files_dict:
        data5min = None
        # для каждой пятиминутки свой набор тэговых массивов
        megadict = dict()
        for tagname in alltagnames:
            megadict[tagname] = list()
        for filename in files_arr:
            try:
                with open(os.path.join(dirName, filename), newline='') as csvfile:
                    data5min = list(csv.reader(csvfile, delimiter=';'))
            except:
                data5min = None
        # если файл загрузился раскладываем строки по тэгам
        if data5min:
            for row in data5min:
                # в последнем поле хранится значение @RM_MASTER
                if row[-1] == "True":
                    megadict[row[1]].append(row)
    for tagname, values_array in megadict:
        result_value = -0.01
        valtype = alltagsTypes.get(tagname,-1)

        if valtype > -1:
            result_value = prepare_value(array_values=values_array, values_type=alltagsTypes[tagname])


def prepare_value(array_values=None, values_type=-1):
    result = -0.01
    if array_values:
        if values_type>-1:
            pass
        elif values_type == 0:
            pass
        elif values_type == 1:
            pass
        elif values_type == 2:
            pass
        elif values_type == 3:
            pass
    return result

# def work_files_loop():
#
#
#     while True:
#         opc_queue_logger.warning( f"constants_loaded: {constants_filled} ", )
#         if not constants_filled:
#             await get_constants()
#         else:
#             return
#         sleep(30)
