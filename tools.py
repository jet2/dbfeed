import os
from time import sleep
from settings import tag_typer
import datetime
import csv

def getFilesToDelete(self, newFileName):
    dirName, fName = os.path.split(self.origFileName)
    dName, newFileName = os.path.split(newFileName)

    fileNames = [f for f in os.listdir(dirName) if os.path.isfile(f)]
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
    fileNames = files = [f for f in os.listdir(dirName) if os.path.isfile(os.path.join(dirName, f))]
    fileNames.sort()
    print(f"filenames={fileNames}")
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
            # print(item, nmstr, dtnode)
            if dtnode not in goodfiles:
                goodfiles[dtnode] = list()
            goodfiles[dtnode].append(item)
    return goodfiles



def integrate_filegroups_withmaster_true(files_dict):
    final_records_array = list()
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

    # для каждой пятиминутки свой набор файлов
    for minute_node_key, files_arr in files_dict.items():
        print(f"NEW BORDER: {minute_node_key}")
        data5min = list()
        # для каждой пятиминутки свой набор тэговых массивов
        megadict = dict()
        for tagname in alltagnames:
            megadict[tagname] = list()
        for filename in files_arr:
            try:
                # загружаем поминутные файлы
                with open(os.path.join(dirName, filename), newline='') as csvfile:
                    data5min1 = list(csv.reader(csvfile, delimiter=';'))
            except:
                data5min1 = None
            if data5min1:
                if len(data5min1)>0:
                    # сваливаем файлы в массив 5 минут
                    data5min.extend(data5min1)
        # если файл загрузился раскладываем строки по тэгам
        if len(data5min)>0:
            for row in data5min:
                # в последнем поле хранится значение @RM_MASTER
                if row[-1] == "True":
                    # mynamefound = alltagsTypes.get()
                    if row[1] in alltagsTypes:
                        megadict[row[1]].append(row)
                    #else:
                    #    print(f"Not Found type for {row[1]}")
        for tagname, values_array in megadict.items():
            valtype = alltagsTypes.get(tagname, -1)
            #print(f"Tagname = {tagname}; valtype = {valtype}")
            if valtype > -1:
                dt_begin = datetime.datetime.strptime(minute_node_key + '-00', "%Y-%m-%d-%H-%M-%S")
                dt_end = dt_begin + datetime.timedelta(minutes=5)
                result_value = prepare_value(array_values=values_array, values_type=valtype, tn = tagname, dt_beginX = dt_begin)

                # final_records_array.append(["dt_begin": dt_begin,"dt_end": dt_end,"tagname": tagname, "tagvalue": result_value})
                final_records_array.append([dt_begin, dt_end, tagname,  result_value])
    return final_records_array

def prepare_value(array_values=None, values_type=-1, tn="", dt_beginX =None):
    result = -0.01

    #print(f"tn: {tn}, vt: {values_type}; len(array_values)= {len(array_values)}")
    # значение готовится на ОДНОЙ ПЯТИМИНУТКЕ
    dt_begin = dt_beginX
    if array_values:
        # if tn == "23_1-1_1/23_1-1_1_RUN.Out#Value":
        #     print(array_values)

        array_values.sort()
        if values_type == 0:
            # дискретные состояния, вычисляем для 15 интервалов

            # узнаем начало интервала
            #"01-01-2020 15:15:15"[:16] = "01-01-2020 15:15"

            preres = ""

            for i in range(15):
                zeroed = False
                for row in array_values:
                    dt_row = datetime.datetime.strptime(row[0][:19], '%d-%m-%Y %H:%M:%S')
                    dt_left = dt_begin + datetime.timedelta(seconds=0+20*i)
                    dt_right = dt_begin + datetime.timedelta(seconds=20+20*i)

                    if dt_left <= dt_row < dt_right:
                        if row[2] == "False":
                            zeroed = True
                    # if tn == "23_1-1_1/23_1-1_1_RUN.Out#Value":
                    #      print(row, dt_left, dt_right, zeroed, i)
                    if zeroed:
                        break
                if zeroed:
                    preres += "2"
                else:
                    preres += "1"
                # if tn == "23_1-1_1/23_1-1_1_RUN.Out#Value":
                #      print(f"i={i}, preres={preres}")
            result = float("0."+preres)
            # if tn == "23_1-1_1/23_1-1_1_RUN.Out#Value":
            #     print(f"tn={tn}, result={result}")
        elif values_type == 1:
            sumx = 0.0
            for item in array_values:
                try:
                    sumx += float(item[2])
                except:
                    print(f"except: {item}")
            result = sumx/len(array_values)
            # среднее, делим сумму значений на количество
            pass
        elif values_type == 2:
            # накопительные - берем последнее
            result = array_values[-1][2]
        elif values_type == 3:
            #накопления вычисляемые из средних
            pass
    return result

def kill_files(files_dict):
    dirName, fName = os.path.split(os.path.realpath(__file__))
    dirName = os.path.join(dirName, 'csv')
    for k, myfiles in files_dict.items():
        for xfile in myfiles:
            os.remove(os.path.join(dirName, xfile))
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

