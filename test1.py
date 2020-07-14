from datetime import datetime
from tools import millis_interval, meow
import redis
from time import sleep
#r = redis.StrictRedis(host='192.168.0.119', port=6379, db=0)
r = redis.StrictRedis(host='127.0.0.1', port=6379, db=0)

# пишет в редис, изображая сервер орс из файла
if __name__ == '__main__':
    with open('misc/data.csv','r') as f:
        xlist = f.readlines()
    while True:
        #     номер стартовой строки csv с нуля
        startline = 00
        print(len(xlist))
        myrange = range(startline, len(xlist) - 1)
        for i in myrange:

            xline1 = xlist[i].split(';')
            xline2 = xlist[i+1].split(';')

            dt1 = datetime.strptime(xline1[0][:19], '%d-%m-%Y %H:%M:%S')
            dt2 = datetime.strptime(xline2[0][:19], '%d-%m-%Y %H:%M:%S')
            sleeper = millis_interval(dt1, dt2) / 1000
            opc_name = xline1[1].replace(".","$")
            opc_value = 0
            if opc_name[0:4] != "@RM_":
                if xline1[2] == "True\n":
                    opc_value = "1"
                elif xline1[2] == "False\n":
                    opc_value = "0"
                else:
                    opc_value = str(float(xline1[2])).replace(".",",")
            realdt = meow()

            if opc_name[0:4] != "@RM_":
                print(f'{realdt} [{dt1} | {opc_name} = {opc_value} ] sleep {sleeper} ')
                r.set(opc_name, opc_value)
                r.publish(opc_name, opc_value)
            else:
                print(f'NON PUBLISH - {realdt} [{dt1} | {opc_name} = {opc_value} ] sleep {sleeper} ')
            sleep(sleeper)