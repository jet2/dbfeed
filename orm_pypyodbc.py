import datetime
import json
import os
import pyodbc
import logging
from rolloverlogs import create_timed_rotating_log
from settings import mssql_connection

dirName, fName = os.path.split(os.path.realpath(__file__))
dirName = os.path.join(dirName, 'opc_file_processor')
file_processor_logger = create_timed_rotating_log(dirName, "opc_file_processor")


con = None
connectionstring = 'DRIVER={SQL Server}' + \
                   ';SERVER=' + mssql_connection["hostname"] + \
                   ';DATABASE=' + mssql_connection["database"] + \
                   ';UID=' + mssql_connection["username"] + \
                   ';PWD=' + mssql_connection["password"]

markerset=list()

def meow():
    return str(datetime.datetime.now())


def writeTagChangeDisk(date_time=datetime.datetime.now(),
                       source_name='unknown',
                       opc_name='unknown',
                       cur_value=0.0,
                       prev_value=0.0):
    global con
    global cursor
    res=0
    try:

        SQLCommand = ("INSERT INTO opc_data "
                      "(date_time, source_name, opc_name, cur_value, prev_value) "
                      "VALUES (?,?,?,?,?)")
        Values = [date_time, source_name,opc_name, cur_value, prev_value]
        cursor.execute(SQLCommand, Values)
    except Exception as e:
        res = 1
        print("cur.execute(): " + str(e) + '\n' + Values)
    try:
        con.commit()
    except Exception as e:
        res = 2
        print("con.commit2(): " + str(e) + '\n' + Values)
        try:
            f = open('dump.txt', 'a')
            f.write(f"{datetime.datetime.now()},{date_time},{opc_name},{cur_value},{prev_value},\n")
            # print('Error '+e,[cursor, xname, xvalue] )
        except:
            pass
    return res


def insert_opc(json_opcvalues):
    global connectionstring
    conx = pyodbc.connect(connectionstring)
    cursorx = conx.cursor()
    cursorx.fast_executemany = True
    # datetime, place, new_opc_value, old_opc_value
    # old_opc_value is to remember value before interval starts
    # print(xlist[0])
    result = False
    xlist = json_opcvalues.split('\n')
    zlist = list()
    print(xlist[0])

    try:
        opc_dict = json.loads(xlist[0])
    except:
        print('non-jsonable ', xlist[0])

    print('list: ', len(xlist), datetime.datetime.fromtimestamp(opc_dict['timestamp']))
    opc_dict = dict()
    res=3
    for item in xlist:
        if item:
            try:
                opc_dict = json.loads(item)
            except:
                print('non-jsonable ', item)

            if opc_dict.get('timestamp', None) and opc_dict.get('tagname', None):

                try:
                    my_tsx = datetime.datetime.fromtimestamp(opc_dict['timestamp'])
                except Exception as e:
                    my_tsx = datetime.datetime.now()
                    print("Timestamp: " + str(e) + '\n' + opc_dict)
                _dt = my_tsx
                _sn = opc_dict.get('source_name',"Unknown")
                _on = opc_dict['tagname']
                _nv = opc_dict['new_value']
                _ov = opc_dict['old_value']
                _vt = opc_dict['tagtype']
                zlist.append((_dt, _sn,_on,_nv,_ov, _vt))

    # qlist = list()
    # qlist.append((datetime.datetime.now(), 'sn', 'tn',1.1,2.2))
    #
    # cursorx.execute("SELECT * FROM opc_data")
    # tables = cursorx.fetchall()
    #
    # print("zlist", tables)

    try:
        cursorx.executemany("INSERT INTO opc_data " + \
                            "(dt, source_name, tag_name, new_value, old_value,tag_type) " + \
                            "VALUES (?,?,?,?,?,?)", zlist)
        cursorx.commit()
        res = 0
    except Exception as e:
        res = 1
        print("cur.execute(): " + str(e) )

    conx.close()

    print(datetime.datetime.now(), "insresutl: ", res)
    return res == 0


def insert_5_minutes(array_of_rows=[]):
    global connectionstring
    conx = pyodbc.connect(connectionstring)
    cursorx = conx.cursor()
    cursorx.fast_executemany = True
    try:
        cursorx.executemany("INSERT INTO opc_data (dt_begin, dt_end, tagname, tagvalue) VALUES (?,?,?,?)", array_of_rows)
        cursorx.commit()
        res = 0
    except Exception as e:
        res = 1

        file_processor_logger.Error(f"cur.execute(): error = {e}")

    conx.close()

    # print(f"{datetime.datetime.now()} 5 min insert result: {res}")
    return res == 0



def db_prepare():
    global connectionstring

    con = None
    try:
        con = pyodbc.connect(connectionstring)
        file_processor_logger.info(f"OK CONNECT database {connectionstring}")
    except:
        file_processor_logger.error(f"Cant connect database {connectionstring}: ", exc_info=True)
    try:
        con.close()
    except:
        pass
    con = None

def initmarkers():
    global markerset
    try:
        with open('markerset.txt', "r") as f:
            markerset = f.read().split('\n')

        print('markerset:',markerset)
    except:
        pass

def markers_extend(marker):
    global markerset
    markerset.append(marker)
    if len(markerset) > 1000:
        markerset.pop(0)
    with open('markerset.txt', mode='wt', encoding='utf-8') as f:
        f.write('\n'.join(markerset))


def get_markerset():
    return markerset