import os
from flask import Flask, render_template, request
#from orm_peewee import insert_opc, db_prepare
from orm_pypyodbc import insert_opc, db_prepare, initmarkers, markers_extend, get_markerset
from settings import webport
import datetime
from tools import parallel_file_processor_main

app = Flask(__name__, static_url_path='/static')


SECRET_KEY = os.urandom(32)
app.config['SECRET_KEY'] = SECRET_KEY
#log = logging.getLogger('werkzeug')
#log.setLevel(logging.ERROR)


@app.route('/')
def index():
    user = {'username': 'Anonymous'}
    print('external ping')
    return render_template('index.html')


@app.route('/insopcdata', methods=['POST'])
def insopcdata():
    xres = '333'
    inss =False
    try:
        json_text = request.values['data']
        marker = request.values['marker']
        print(request.remote_addr, 'marker', marker)
        if marker not in get_markerset():
            tick = datetime.datetime.now()
            inss = insert_opc(json_text)
            print("dt: ",datetime.datetime.now()-tick)
            if inss:
                xres = '999'
        else:
            print('non inserted')
        
    except Exception as e:
        print("Cant save opc: " + str(e))
        
    if xres == '999':
        markers_extend(marker)
    print('inserting:', xres, inss )
    return xres


@app.route('/addfile', methods=['POST'])
def addfile():
    xres = '333'
    try:
        csv_text = request.values['data']
        filename = request.values['filename']
        hostname = request.values['hostname']
        dirName, fName = os.path.split(os.path.realpath(__file__))

        if not os.path.exists(os.path.join(dirName, 'csv')):
            os.makedirs(os.path.join(dirName, 'csv'))

        fullpath = os.path.join(dirName, 'csv', os.path.splitext(filename)[0] + '-' + hostname + '.csv')
        # dirName, fName = os.path.split(self.origFileName)
        print(request.remote_addr, 'filename =', fullpath)
        arr = csv_text.split("\r\n")

        try:
            with open(fullpath, 'w') as f:
                f.write("\n".join(([item for item in arr if len(item)>3])))
                f.flush()
            xres = '999'
        except Exception as e:
            print("Cant save opc 1: " + str(e))
            xres = '333'

    except Exception as e:
        print("Cant save opc 2: " + str(e))

    return xres


if __name__ == '__main__':
    db_prepare()
    parallel_file_processor_main()
    app.run(debug=False, host='0.0.0.0', port=webport)
