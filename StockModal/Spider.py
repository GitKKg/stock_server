import os, apsw
import time
import sys
#from PyQt4 import QtCore

from StockModal.SinaHTMLParser import SinaHTMLParser
from urllib.request import urlopen
import urllib
import websocket
import eventlet.queue

# kyle added for ban
import six
import socks
import socket
from urllib.error import URLError

import traceback  # kyle added for occasionally url time out check
StockCodesName='E:/SA/StockAssist_0304/stock_codes/stock_codes.db'
SinaDirPath='E:/SA/StockAssist_0304/sina'
baseURL=r"http://vip.stock.finance.sina.com.cn/corp/go.php/vMS_FuQuanMarketHistory/stockid/%s.phtml?year=%d&jidu=%d"
#sina_path=os.getcwd()+"\sina_dir"
class NoName(Exception): pass


import GSym
#Flask_app=GSym.get_value('flask_app')
#Flask_app=GSym.get_value('flask_app')

class Spider:
    def __init__(self, sid,sina_path, stock_total,semaphore):  # progress_que,out_que, kyle added sina_path
        self.parser = SinaHTMLParser(sid,sina_path)  # ,progress_que
        self.baseURL = baseURL
        self.sid=sid
        self.cancel = False
        #self.que=progress_que
        #self.out_que =out_que
        self.semaphore = semaphore
        #self.name = name
        self.stock_total=stock_total
        self.spider_percent=0
        self.spidered_sum = 0
        # self.default_sock_proxy=socks.get_default_proxy()
        self.nowsocks = 1;
        # self.logFile = open("d:/spider/"+self.name+".log", "w")

    def setStocksList(self, stocksList):
        self.stocksList = stocksList
        # self.logFile.write(str(self.stocksList)+"\n")

    def setDateRange(self, four):
        self.yearFirst, self.yearLast, self.seasonFirst, self.seasonLast = four

    def run(self):
        old_percent=-1
        spider_percent=0
        for stock in self.stocksList:
            '''
            try:
                if self.out_que.get(block=False)=='disconnected':
                    print('spider know disconnected')
                    break
            except eventlet.queue.Empty:
                print('connection keep ok')
                pass
            '''
            if GSym.get_value('client_g')[self.sid]['connected']==False:
                print('spider know disconnected')
                break
            if stock.startswith(("010", "019", '1', '2', '3', '4', '5', '7', '8', '9')):  # kyle
                self.spidered_sum+=1
                spider_percent=int(100*self.spidered_sum/self.stock_total)
                if(spider_percent>old_percent):
                    old_percent = spider_percent
                    print(spider_percent)
                    #self.que.put(spider_percent)
                    websocket.UpdateSpiderProgress(spider_percent,self.sid)
                #websocket.UpdateSpiderProgress(self.spider_percent)
                continue
            self.parser.newStock()
            if (self.spidered_sum>10):
                #self.que.put('end')
                print('test version,just spider 10 stocks,finished')
                break
            try:
                self.spidered_sum+=1
                spider_percent = int(100 * self.spidered_sum / self.stock_total)
                if (spider_percent > old_percent):
                    old_percent = spider_percent
                    print(spider_percent)
                    #self.que.put(spider_percent)
                    websocket.UpdateSpiderProgress(spider_percent, self.sid)
                #websocket.UpdateSpiderProgress(websocket.spider_percent)

                if 1:
                  for year in range(self.yearFirst, self.yearLast + 1):
                    seasonStart = self.seasonFirst if year == self.yearFirst else 1
                    seasonEnd = self.seasonLast if year == self.yearLast else 4
                    for season in range(seasonStart, seasonEnd + 1):
                        # before 2006 season 2, the HTML formats are different
                        if GSym.get_value('client_g')[self.sid]['connected'] == False:
                            print('spider know disconnected')
                            break
                        if year * 10 + season >= 20062:
                            self.parser.setParserFromat(new=True)
                        else:
                            self.parser.setParserFromat(new=False)
                        # print(self.name + " %s: %d, %d\n" % (stock, year, season))
                        # self.logFile.write("%s: %d, %d\n" % (stock, year, season))
                        ''''proxy_support = urllib.request.ProxyHandler({'sock5': '127.0.0.1:1088'})
                        opener = urllib.request.build_opener(proxy_support)
                        urllib.request.install_opener(opener)
                        print('use proxy\n')'''
                        # inforMation = urllib.urlopen("http://tianya.cn", proxies={'http':proxyConfig})
                        # html = urlopen(self.baseURL % (stock, year, season),proxies={'http':'http://proxy-us.intel.com:912'}).read()

                        # kyle added proxy switch due to sina blocking
                        for retry in range(0, 10):
                            if GSym.get_value('client_g')[self.sid]['connected'] == False:
                                print('spider know disconnected')
                                break
                            if self.nowsocks == 1:
                                self.nowsocks = 0
                                socks.set_default_proxy(socks.SOCKS5, '127.0.0.1', 1088)
                                socket.socket = socks.socksocket
                                try:
                                    print('use socks5\n')
                                    html = urlopen(self.baseURL % (stock, year, season), timeout=6).read()
                                    print(self.baseURL % (stock, year, season))
                                    print('ban sleep 2s\n')
                                    # kyle, socketio.sleep both prevent banning and give other thread to emit percent,but time.sleep will hold emit until spider out
                                    GSym.get_value('socketio').sleep(2)

                                    self.parser.feed(html.decode("gbk"))
                                    break
                                except:
                                    print("Error: %s %d %d" % (stock, year, season))
                                    traceback.print_exc()
                                    continue
                            else:
                                self.nowsocks = 1
                                socks.set_default_proxy()  # no use proxy,direct link
                                socket.socket = socks.socksocket
                                try:
                                    print('direct link\n')
                                    html = urlopen(self.baseURL % (stock, year, season), timeout=6).read()
                                    print(self.baseURL % (stock, year, season))
                                    print('ban sleep 3s\n')
                                    # kyle, socketio.sleep both prevent banning and give other thread to emit percent,but time.sleep will hold emit until spider out
                                    GSym.get_value('socketio').sleep(3)
                                    self.parser.feed(html.decode("gbk"))
                                    break
                                except:
                                    print("Error: %s %d %d" % (stock, year, season))
                                    traceback.print_exc()
                                    continue

                        if self.parser.noName:
                            raise NoName()

            except NoName:
                print("%s has no data" % stock)
                continue
            except:
                print("Error: %s %d %d" % (stock, year, season))
                traceback.print_exc()
                continue

        if GSym.get_value('client_g')[self.sid]['connected'] == False:
            print('session disconnected,so drop client info ')
            GSym.get_value('client_g').pop(self.sid)
        #self.que.put('end')
        websocket.UpdateSpideredName('end',self.sid)
        #GSym.get_value('socketio').emit('stockname', 'end', room=self.sid)
        self.semaphore.release()

#def Spider_main(datesetting):
def Spider_main(sid,StartYear,EndYear,StartSeason,EndSeason,semaphore):#progress_que,out_que,
    global StockCodesName
    global SinaDirPath
    print('Spider_main in')
    print("Spider_main  pid and ppid", os.getpid(), os.getppid())

    #give chance to caller view function to finish,to reslove zombie connection issue when client click close or f5 when just connected
    GSym.get_value('socketio').sleep(0.1)
   # datesetting['StockCodesName']=StockCodesName
   # datesetting['SinaDirPath'] = SinaDirPath
    connection = apsw.Connection(StockCodesName)
    cursor = connection.cursor()
    cursor.execute('select name from stock')
    stocksList = list(map(lambda x: x[0], cursor.fetchall()))
    print("Start retriving data...")
    total = len(stocksList)
 #   numberOfThreads = 1
 #   print("Spider_main ban numberOfThreads limit...\n")
 #   spiders = []

    s = Spider(sid,SinaDirPath,total,semaphore)#progress_que,out_que,
        #s.parser.file=open(SinaDirPath+""+self.stockName+".sina", "wb")
   # s.setDateRange((datesetting['StartYear'],datesetting['EndYear'],datesetting['StartSeason'],datesetting['EndSeason']))
    s.setDateRange(
        (StartYear, EndYear, StartSeason, EndSeason)
    )
    s.setStocksList(stocksList)
    s.run()

    print("Spider_main has finished.")

if __name__ == "__main__":
    print("Loading database...")
    connection = apsw.Connection("d:/spider/stock_codes.db")
    cursor = connection.cursor()
    cursor.execute('select name from stock')
    stocksList = list(map(lambda x: x[0], cursor.fetchall()))
    #     stocksList = ["000034"]
    print("Start retriving data...")
    total = len(stocksList)
    numberOfThreads = 2
    spiders = []
    index = 0
    for i in range(0, numberOfThreads):
        s = Spider("Spider%d" % i)
        s.setDateRange([2014, 2015, 3, 1])
        if i == numberOfThreads - 1:
            s.setStocksList(stocksList[index:])
        else:
            s.setStocksList(stocksList[index:index + (total // numberOfThreads)])
            index = index + (total // numberOfThreads)
        s.start()
        spiders.append(s)

    for s in spiders:
        s.wait()
        print(s.name + " has finished.")