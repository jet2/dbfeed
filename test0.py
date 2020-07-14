import json
from tools import make_filegroups, integrate_filegroups_withmaster_true, kill_files
from orm_pypyodbc import insert_5_minutes
from datetime import datetime
import os

sss='''
{"timestamp":1584902411,"source_name":"WINCC","tagname":"@RM_MASTER","new_value":true,"old_value":1}
'''

if __name__ == '__main__':

    # dirName, fName = os.path.split(os.path.realpath(__file__))
    # dirName = os.path.join(dirName, 'csv')
    # files = [f for f in os.listdir(dirName) if os.path.isfile(os.path.join(dirName, f))]
    #
    # files.sort()
    # print(files)

    print(f"{datetime.now()} Test begin")
    filesX = make_filegroups()
    print(f"files - {filesX}")
    alldata = integrate_filegroups_withmaster_true(files_dict = filesX)
    res = insert_5_minutes(array_of_rows=alldata)
    if res:
        kill_files(filesX)

    print(f"{datetime.now()} Test result = {res}")

    # dt_begin = "01-01-2020 15:14:59.000"[:16]
    # dt_begin2 = "01-01-2020 15:14:59.000"[:19]
    # # "01-01-2020 15:15:15"[:16] = "01-01-2020 15:15"
    # if int(dt_begin[-1]) in [5,6,7,8,9]:
    #     dt_begin = dt_begin[:15]+"5:00"
    # else:
    #     dt_begin = dt_begin[:15] + "0:00"
    # print(dt_begin2, list(range(1,15)) )




