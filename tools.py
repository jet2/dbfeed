import os
from time import sleep
from settings import tag_typer
import datetime
import csv

from multiprocessing import Process
from orm_pypyodbc import insert_5_minutes, file_processor_logger as fp_logger

file_processor_logger = fp_logger

def millis_interval(start, end):
    """start and end are datetime instances"""
    diff = end - start
    millis = diff.days * 24 * 60 * 60 * 1000
    millis += diff.seconds * 1000
    millis += diff.microseconds / 1000
    return millis


def meow():
    return str(datetime.datetime.now())


def meow2():
    return datetime.datetime.strftime(datetime.datetime.now(),"%Y-%m-%d-%H-%M-%S")

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
    fileNames = [f for f in os.listdir(dirName) if os.path.isfile(os.path.join(dirName, f))]
    fileNames.sort()

    goodfiles = dict()
    # все файлы старше 5 минут притягиваются к началу 5-минутного интервала с :Х0 или :Х5 минуты
    for item in fileNames:
        dt = datetime.datetime.strptime(item[:16]+'-00', "%Y-%m-%d-%H-%M-%S")
        dtnow = datetime.datetime.now()
        file_processor_logger.warning(f"nowdt = {dtnow}, filedt = {dt}, item={item} = {dtnow - dt}")

        nmstr = '0' # к нему притягиваются [0,1,2,3,4]
        if int(item[15]) in [5,6,7,8,9]:
            nmstr = '5'
        file_processor_logger.warning(f"test = {item}, [15]={item[15]}")
        dtnode = item[:15] + nmstr
        dtnode_next = datetime.datetime.strptime(dtnode + '-00', "%Y-%m-%d-%H-%M-%S") + datetime.timedelta(minutes=6)
        # Если текущее время выползло за границу следующего интервала то позволяется использовать все файлы текущего
        if dtnow >= dtnode_next:
            # print(item, nmstr, dtnode)
            if dtnode not in goodfiles:
                goodfiles[dtnode] = list()
            goodfiles[dtnode].append(item)
        # if not_ready_mark:
        #     goodfiles[dtnode].append("NOTREADY")
    # result = dict()
    # for k,v in goodfiles.items():
    #     if "NOTREADY" in v:
    #         file_processor_logger.warning(f"{k} not ready")
    #     else:
    #         result[k] = v
    return goodfiles


def integrate_filegroups_withmaster_true(files_dict):
    final_records_array = list()
    dirName, fName = os.path.split(os.path.realpath(__file__))
    dirName = os.path.join(dirName, 'csv')
    alltagnames = tag_typer["sumbyavg"] + tag_typer["sum"]+tag_typer["avg"]+tag_typer["bool"]
    # словарь типа каждого имени тэга
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
        file_processor_logger.info(f"new minute node: {minute_node_key}")
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

                    ### СПЕЦИАЛЬНОЕ НЕБОЕВОЕ ТЕСТОВОЕ ПРЕОБРАЗОВАНИЕ
                    rowx = row[1].replace("$", ".")

                    if rowx in alltagsTypes:
                        megadict[rowx].append(row)
                    #else:
                    #    print(f"Not Found type for {row[1]}")
        for tagname, values_array in megadict.items():

            tagnameX = tagname

            valtype = alltagsTypes.get(tagnameX, -1)
            #print(f"Tagname = {tagname}; valtype = {valtype}")
            if valtype > -1:
                dt_begin = datetime.datetime.strptime(minute_node_key + '-00', "%Y-%m-%d-%H-%M-%S")
                dt_end = dt_begin + datetime.timedelta(minutes=5)
                result_value = prepare_value(array_values=values_array, values_type=valtype, tn = tagnameX, dt_beginX = dt_begin)
                if result_value == -0.000001:
                    file_processor_logger.info(f"No VALUE {tagnameX}; valtype = {valtype}; array len = {len(values_array)}")
                else:
                    final_records_array.append([dt_begin, dt_end, tagnameX,  result_value])
    return final_records_array

def prepare_value(array_values=None, values_type=-1, tn="", dt_beginX =None):
    result = -0.000001

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
                    sumx += float(item[2].replace(",","."))
                except:
                    file_processor_logger.error(f"Exception in file_processor item: {item}", exc_info=True)
            result = sumx/len(array_values)
            # среднее, делим сумму значений на количество
            pass
        elif values_type == 2:
            # накопительные - берем последнее
            result = array_values[-1][2]
        elif values_type == 3:
            #накопления вычисляемые из средних
            dt_end = dt_begin + datetime.timedelta(minutes=5)
            xlen = len(array_values)
            result = 0
            if xlen > 0:
                for i in range(0, xlen):
                    row_dt = datetime.datetime.strptime(array_values[i][0][:19], '%d-%m-%Y %H:%M:%S')
                    row_value = float(array_values[i][2].replace(",","."))
                    if i+1 < xlen:
                        row_dt_next = datetime.datetime.strptime(array_values[i+1][0][:19], '%d-%m-%Y %H:%M:%S')
                    else:
                        row_dt_next = dt_end

                    # 5 кубометров в час это 0.001389 кубометров в секунду
                    # у нас есть кубометры в час и секунды -> м3/ч надо делить на 3600 и умножать на секунды интервала
                    result += (row_dt_next-row_dt).total_seconds() * row_value / 3600
    return result

def kill_files(files_dict):
    dirName, fName = os.path.split(os.path.realpath(__file__))
    dirName = os.path.join(dirName, 'csv')
    for k, myfiles in files_dict.items():
        for xfile in myfiles:
            os.remove(os.path.join(dirName, xfile))


def opc_file_processor_loop():
    global file_processor_logger
    # Inside a while loop, wait for incoming events.
    file_processor_logger.info('Start opc_file_processor_loop')

    while True:
        try:
            try:
                file_processor_logger.info(f" opc_files_handle begin")
                filesX = make_filegroups()
                file_processor_logger.info(f" file groups: {filesX}")
                alldata = integrate_filegroups_withmaster_true(files_dict=filesX)
                if len(alldata)>0:
                    file_processor_logger.info(f" rows to handle = {len(alldata)}")
                    res = insert_5_minutes(array_of_rows=alldata)
                    if res:
                        kill_files(filesX)
                    file_processor_logger.info(f" opc_files_handle result = {res}")
                else:
                    file_processor_logger.info(f"No data to process")
            except:
                file_processor_logger.error(f"Exception in file_processor", exc_info=True)
        finally:
            sleep(60)


def parallel_file_processor_main():
    global file_processor_logger
    p = Process(target=opc_file_processor_loop)
    p.start()  # start execution of myFunc() asychronously







