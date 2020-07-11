import os
from flask import Flask, render_template, request
#from orm_peewee import insert_opc, db_prepare
from orm_pypyodbc import insert_opc, db_prepare, initmarkers, markers_extend, get_markerset
from settings import webport
import logging
import datetime


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


if __name__ == '__main__':

    db_prepare()
    initmarkers()
    print('markerset:', get_markerset())

    app.run(debug=False, host='0.0.0.0', port=webport)
