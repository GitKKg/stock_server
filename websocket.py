from flask import Flask, send_file,current_app
from flask_socketio import SocketIO, emit
import os
import json
import multiprocessing
import time
import StockModal.Spider
from eventlet.green import threading
#eventlet.monkey_patch(socket=True)
static_folder="C:\WebProgramming\quasar_init1\dist\spa-mat"
app = Flask(__name__,static_folder=static_folder, static_url_path='')
app.config['SECRET_KEY'] = 'secret!'

socketio = SocketIO(app)
spider_Thread=None
dates_ready=0
date_seting={}
spider_async=None
spider_percent=0
spider_semaphore = threading.Semaphore(1)


@app.route('/')
def index():
    return send_file(static_folder + '\index.html')

@socketio.on('connect')
def test_connect():
    print('ws Connected')
    emit('news',{'data':'hello'})
#    socketio.emit('progress', 50)

@socketio.on('disconnect')
def test_disconnect():
    print('ws disConnected')


@socketio.on('spider')
def spider(data):
    global spider_async
    # flask web container thread reenterable,here make sure at the same time only one index page could spider
    if not spider_semaphore.acquire(blocking=False):
        print("not get spider_semaphore")
        emit('progress', 80)
        return
    print(data)
    print("get spider_semaphore")
    return
   # emit('progress', 0)
   # emit('progress', 1)
    emit('progress', 1)
    UpdateSpiderProgress(2)
    StartSeason = (data['date_start_month'] - 1) // 3 + 1
    EndSeason = (data['date_end_month'] - 1) // 3 + 1
    print()
    print("spider on pid and ppid", os.getpid(), os.getppid())
    spider_async = multiprocessing.Process(
        target=StockModal.Spider.Spider_main,
        kwargs={'StartYear':data['date_start_year'],
                'EndYear':data['date_end_year'],'StartSeason':StartSeason,'EndSeason':EndSeason
                }
    )
    spider_async.start()
    t = threading.Thread(target=report, name='report')
    t.setDaemon(True)
    t.start()


    '''
    date_seting['StartYear']=data['date_start_year']
    date_seting['EndYear']=data['date_end_year']
    date_seting['StartSeason'] = StartSeason
    date_seting['EndSeason'] = EndSeason

    spider_Thread = threading.Thread(target=StockModal.Spider.Spider_main, name='Spider_main',
                                     args=(date_seting,)
                                     )
    spider_Thread.setDaemon(True)
    spider_Thread.start()
    '''
    if 0:
        StockModal.Spider.Spider_main(
        StartYear=data['date_start_year'],EndYear=data['date_end_year'], StartSeason= StartSeason, EndSeason= EndSeason
        )
#    for i in range(0,1000):
#        emit('progress', i)

    print('spider_Thread started')

def UpdateSpiderProgress(percent):
    global app
    print('UpdateSpiderProgress emmited')
    with app.app_context():
        socketio.emit('progress', percent)


def report():
    global spider_percent
    print("report  pid and ppid", os.getpid(), os.getppid())
    while True:
        with app.app_context():
            socketio.emit('progress', spider_percent)
        socketio.sleep(5)

    spider_semaphore.release()

if __name__ == '__main__':

    print("__main__  pid and ppid", os.getpid(), os.getppid())
    socketio.run(app,host='0.0.0.0',
            port=85,

            )
    global spider_async
    if spider_async.is_alive():
        spider_async.terminate()
        print('spider killed')
    print('socketio out')