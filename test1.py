import json
from tools import make_filegroups, integrate_filegroups_withmaster_true
from orm_pypyodbc import insert_5_minutes
from datetime import datetime
sss='''
{"timestamp":1584902411,"source_name":"WINCC","tagname":"@RM_MASTER","new_value":true,"old_value":1}
'''

if __name__ == '__main__':
    # json.loads(sss)
    print(f"{datetime.now()} Test begin")
    filesX = make_filegroups()
    alldata = integrate_filegroups_withmaster_true(files_dict = filesX)
    res = insert_5_minutes(array_of_rows=alldata)
    print(f"{datetime.now()} Test result = {res}")




