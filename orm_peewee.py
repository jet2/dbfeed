import peewee
import json
import datetime
from settings import postgreconnection

pg_db = peewee.PostgresqlDatabase(postgreconnection['database'], user=postgreconnection['username'],
                                  password=postgreconnection['password'],
                                  host=postgreconnection['ipaddress'], port=postgreconnection['port'])


########################################################################
class OpcData(peewee.Model):
    """
    ORM model of tagchange table
    """

    date_time = peewee.DateTimeField()
    source_name = peewee.CharField()
    opc_name = peewee.CharField()
    cur_value = peewee.FloatField()
    prev_value = peewee.FloatField()

    class Meta:
        database = pg_db


def meow():
    return str(datetime.datetime.now())


def insert_opc(json_opcvalues):
    # datetime, place, new_opc_value, old_opc_value
    # old_opc_value is to remember value before interval starts
    # print(json_opcvalue)
    result = False
    xlist = json_opcvalues.split('\n')
    print('list: ', len(xlist))
    opc_dict = dict()
    for item in xlist:
        if item:
            try:
                opc_dict = json.loads(item)
            except:
                print('non-jsonable ', item)

            if opc_dict.get('timestamp', None) and opc_dict.get('tagname', None):
                try:
                    my_ts = datetime.datetime.fromtimestamp(opc_dict['timestamp'])
                except Exception as e:
                    my_ts = datetime.datetime.now()
                    print("Timestamp: " + str(e) + '\n' + opc_dict)
                try:
                    single_row = OpcData(date_time=my_ts,
                                               source_name=opc_dict.get('source_name','unknown'),
                                               opc_name=opc_dict['tagname'],
                                               cur_value=opc_dict['new_value'],
                                               prev_value=opc_dict['old_value'])

                    single_row.save()
                    result = True
                except Exception as e:
                    print("Cant save opc: " + str(e))

    return result


def db_prepare():
    if not OpcData.table_exists():
        OpcData.create_table(safe=True)

    print("db prepared")
