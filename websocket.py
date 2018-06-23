from flask import Flask, send_file
from flask_socketio import SocketIO, emit
import os
import StockModal.Spider
import StockModal.GeneratorSINA
from eventlet.green import threading
from eventlet.queue import Queue



#eventlet.monkey_patch(socket=True)
static_folder="C:\WebProgramming\quasar_init1\dist\spa-mat"
app = Flask(__name__,static_folder=static_folder, static_url_path='')
app.config['SECRET_KEY'] = 'secret!'

socketio = SocketIO(app)
spider_Thread=None
dates_ready=0
date_seting={}
spider_async=None
global spider_percent
spider_percent = 0
spider_semaphore = threading.Semaphore(1)

#no work for emit,tested
import GSym
GSym._init()
#GSym.set_value('flask_app',app)
#GSym.set_value('socketio',socketio)
GSym.set_value('disconnected',False)

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
    GSym.set_value('disconnected', True)

@socketio.on('SaveDB')
def Save_DB():
    print('Save_DB')
    Save_DB_Thread=threading.Thread(target=StockModal.GeneratorSINA.StartSaveDB, name='StartSaveDB')
    Save_DB_Thread.setDaemon(True)
    Save_DB_Thread.start()
    print('Save_DB_Thread started')

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

   # emit('progress', 0)
   # emit('progress', 1)
    emit('progress', 1)
    UpdateSpiderProgress(2)
    StartSeason = (data['date_start_month'] - 1) // 3 + 1
    EndSeason = (data['date_end_month'] - 1) // 3 + 1
    print("spider on pid and ppid", os.getpid(), os.getppid())
    '''
    spider_async = multiprocessing.Process(
        target=StockModal.Spider.Spider_main,
        kwargs={'StartYear':data['date_start_year'],
                'EndYear':data['date_end_year'],'StartSeason':StartSeason,'EndSeason':EndSeason
                }
    )
    spider_async.start()
    '''
    spider_progress_q = Queue()
    spider_Thread = threading.Thread(target=StockModal.Spider.Spider_main, name='Spider_main',
                                    kwargs={'StartYear':data['date_start_year'],
                                            'EndYear':data['date_end_year'],
                                            'StartSeason':StartSeason,
                                            'EndSeason':EndSeason,
                                            'progress_que':spider_progress_q,
                                            'semaphore':spider_semaphore}
                                     )
    spider_Thread.setDaemon(True)
    spider_Thread.start()
    t = threading.Thread(target=report, name='report', args=(spider_progress_q,))
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


def report(progress_que):
    print("report  pid and ppid", os.getpid(), os.getppid())
    while True:
        data=progress_que.get()
        print(data)
        if (isinstance(data,str)):
            with app.app_context():
                socketio.emit('stockname', data)
            print('emit name')
            if (data=='end'):
                break
        else:
            print('progress')
            with app.app_context():
                socketio.emit('progress', data)
        #socketio.sleep(2)

    spider_semaphore.release()

if __name__ == '__main__':

    print("__main__  pid and ppid", os.getpid(), os.getppid())
    socketio.run(app,host='0.0.0.0',
            port=85,

            )

    print('socketio out')