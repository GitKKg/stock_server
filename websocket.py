from flask import Flask, send_file,request
from flask_socketio import SocketIO, emit
import os
import sys
import platform
import StockModal.Spider
import StockModal.GeneratorSINA
import StockModal.DBLoader
#import StockModal.Scanner
from eventlet.green import threading
from celery import Celery
import json
from eventlet.queue import Queue
from collections import namedtuple
from eventlet import monkey_patch
monkey_patch(socket=True)#only patch socket related c lib,is required for celery and amqp,or else socket connection timeout forever

#Note:how to run
#1.celery worker -A websocket.celery --loglevel=info
#2.python websocket.py


#eventlet.monkey_patch(socket=True)
static_folder="C:\WebProgramming\quasar_init1\dist\spa-mat"
app = Flask(__name__,static_folder=static_folder, static_url_path='')
app.config['SECRET_KEY'] = 'secret!'
app.config.update(
    CELERY_BROKER_URL='amqp://localhost//',
    CELERY_RESULT_BACKEND='amqp://localhost//'
)
socketio = SocketIO(app,async_mode='eventlet',message_queue='amqp://')#when no debug ,async_mode='eventlet' is actually default



#no need to pass app.app_context for socket.emit if got sid,but need for emit,just backup here
def make_celery(app):
    celery = Celery(app.import_name, backend=app.config['CELERY_RESULT_BACKEND'],
                    broker=app.config['CELERY_BROKER_URL'])
    celery.conf.update(app.config)
    TaskBase = celery.Task
    class ContextTask(TaskBase):
        abstract = True
        def __call__(self, *args, **kwargs):
            with app.app_context():
                return TaskBase.__call__(self, *args, **kwargs)
    celery.Task = ContextTask
    return celery

celery = Celery('my_task',broker=app.config['CELERY_BROKER_URL'])
#celery.conf.update(app.config)

spider_Thread=None
dates_ready=0
date_seting={}
spider_async=None
global spider_percent
spider_percent = 0
spider_semaphore = threading.Semaphore(1)
LoadDB_semaphore = threading.Semaphore(1)

#no work for emit,tested
import GSym
GSym._init()
#GSym.set_value('flask_app',app)
#GSym.set_value('socketio',socketio)
GSym.set_value('disconnected',False)
GSym.set_value('socketio',socketio)



client_g={}

GSym.set_value('client_g',client_g)

#every scaning processes share it for read not write!
DB_memconn = None




@app.route('/')
def index():
    print('http Connected ')
    print(request)
    #print(request.sid)
    return send_file(static_folder + '\index.html')

@app.route('/scan' , methods=['POST'])
def scan_request():
    print('post http  Connected sid is 0x', request.data)
    return 'ok'

@socketio.on('scan')
def start_scan(ScanParameter,sid):#scan should be forked by self,emit be handed over to celery
    print('ws start_scan')
    print(ScanParameter)
    print(sid)
    print(request.sid)#should be same
    '''
    scan_async = multiprocessing.Process(
        target=StockModal.Scanner.Scaner_main,
        kwargs={'ScanParameter': ScanParameter,
                'sid': request.sid
                }
    )
    scan_async.daemon=True
    scan_async.start()
    '''

@socketio.on('connect')
def test_connect():
    print('ws Connected sid is 0x', request.sid)
    print(request)
    session={'connected':True}
    #session.connected=True
    client_g[request.sid]=session
    return {'sid':request.sid}

@socketio.on('disconnect')
def test_disconnect():
    print('ws disConnected')
    if request.sid in client_g:
        #since message queue is introduced,global here become dangerous,but
        client_g[request.sid]['connected']=False
        '''
        if 'out_que' in client_g[request.sid]:
            print('send disconnected')
            client_g[request.sid]['out_que'].put('disconnected')
        '''
    #dagerous pop here,moved to progress thread
    #client_g.pop(request.sid)
    #need thread event here,Gym not work

    #GSym.set_value('disconnected', True)

@socketio.on('LdDb')
def Load_DB():
    global DB_memconn
    print('Load_DB')
    systype=platform.platform().upper()
    print(systype)
    if 'WINDOW' in systype :#get no way to share mem copy between process in widows,so put DB in SSD without load
        UpdateLoadDBProgress(100, request.sid)
        emit('loaded')
        return

    if (DB_memconn!=None):
        emit('loaded')
        return
    if not LoadDB_semaphore.acquire(blocking=False):
        print("not get spider_semaphore")
        emit('loading')
        return
    print('Got LoadDB_semaphore')
    DB_memconn=StockModal.DBLoader.loadDB(request.sid)
    print(DB_memconn)

def UpdateLoadDBProgress(percent,sid):
    GSym.get_value('socketio').emit('db_progress', percent)# broadcast due to DB memory backup read is shared.   room=sid
    GSym.get_value('socketio').sleep()#give chance to flush out

@socketio.on('SaveDB')#not finished,do nothing to mutex protect,leave after scaning
def Save_DB():
    print('Save_DB')
    Save_DB_Thread=threading.Thread(target=StockModal.GeneratorSINA.StartSaveDB, name='StartSaveDB')
    Save_DB_Thread.setDaemon(True)
    Save_DB_Thread.start()
    print('Save_DB_Thread started')

@socketio.on('spider')
def spider(data):
    global spider_async
    print('spider sid is 0x',request.sid)
    # flask web container thread reenterable,here make sure at the same time only one index page could spider
    if not spider_semaphore.acquire(blocking=False):
        print("not get spider_semaphore")
        emit('progress', 80)
        return
    print(data)
    print("get spider_semaphore")

   # emit('progress', 0)
   # emit('progress', 1)
    #emit('progress', 1)
    #UpdateSpiderProgress(2)
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
    #client_g[request.sid]['out_que']=Queue()
    #client_g[request.sid]['spider_progress_que'] = Queue()
    client_g[request.sid]['spider_thread'] = threading.Thread(target=StockModal.Spider.Spider_main, name='Spider_main',
                                    kwargs={'sid':request.sid,
                                            'StartYear':data['date_start_year'],
                                            'EndYear':data['date_end_year'],
                                            'StartSeason':StartSeason,
                                            'EndSeason':EndSeason,
                                            #'progress_que':client_g[request.sid]['spider_progress_que'],
                                            #'out_que':client_g[request.sid]['out_que'],
                                            'semaphore':spider_semaphore}
                                     )

    client_g[request.sid]['spider_thread'].setDaemon(True)
    client_g[request.sid]['spider_thread'].start()

    '''
    client_g[request.sid]['sp_progress_thread']= threading.Thread(target=sp_progress_thread,
                                                                  name='sp_progress_thread',
                                                                  args=(request.sid,client_g[request.sid]['spider_progress_que'],))
    client_g[request.sid]['sp_progress_thread'].setDaemon(True)
    client_g[request.sid]['sp_progress_thread'].start()
    '''
#start_background_task also works,but parameter assign got bug deep inside,
# args and kwargs will induce err in 3.4 lib,could only use serial assigning,and start could'nt be call manually
    '''client_g[request.sid]['spider_thread'] = socketio.start_background_task(StockModal.Spider.Spider_main,
                                                                            data['date_start_year'],
                                                                            data['date_end_year'],
                                                                             StartSeason,
                                                                             EndSeason,
                                                                             client_g[request.sid][
                                                                                 'spider_progress_que'],
                                                                             client_g[request.sid]['out_que'],
                                                                             spider_semaphore) #name='Spider_main',


    #client_g[request.sid]['spider_thread'].setDaemon(True)
    #client_g[request.sid]['spider_thread'].start()
    client_g[request.sid]['sp_progress_thread']= socketio.start_background_task(sp_progress_thread,
                                                                  #name='sp_progress_thread',
                                                                  request.sid,client_g[request.sid]['spider_progress_que'])
    #client_g[request.sid]['sp_progress_thread'].setDaemon(True)
    #client_g[request.sid]['sp_progress_thread'].start()
    '''
    print('handle spider finished')

#even in socketio orginal file,still need GSym way,if ref to socketio directly ,will get no effect when these 2 function called in other file
def UpdateSpiderProgress(percent,sid):
    GSym.get_value('socketio').emit('progress', percent, room=sid)
    GSym.get_value('socketio').sleep()#give chance to flush out

def UpdateSpideredName(stockName,sid):
    GSym.get_value('socketio').emit('stockname', stockName, room=sid)
    GSym.get_value('socketio').sleep()

'''
def sp_progress_thread(sid,progress_que):
    print("sp_progress_thread  pid and ppid", os.getpid(), os.getppid())
    while True:
        data=progress_que.get()
        print(data)
        if (isinstance(data,str)):
            with app.app_context():
                socketio.emit('stockname', data,room=sid)
            print('emit name')
            if (data=='end'):
                break
        else:
            print('progress')
            with app.app_context():
                socketio.emit('progress', data,room=sid)
        #socketio.sleep(2)
    #now save to pop because spider is out
    print('session over on ',sid)
    client_g.pop(sid)
    spider_semaphore.release()
'''

if __name__ == '__main__':

    print("__main__  pid and ppid", os.getpid(), os.getppid())
    socketio.run(app,host='0.0.0.0',
            port=85,

            )

    print('socketio out')