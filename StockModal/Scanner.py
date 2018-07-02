from __future__ import division

from datetime import date

import apsw

import numpy as np

import json

import websocket
from . import Algorithm as Algo
#from PyQt4 import QtCore

DB_Path='C:/WebProgramming/DB/final.db'

def Scaner_main(ScanParameter,sid):
    scanner=Scanner(ScanParameter,sid)
    scanner.setConnection()
    scanner.setParameters(ScanParameter)
    scanner.run()

class Scanner:
    '''
    progressChanged = QtCore.Signal(int)
    progressCanceled = QtCore.Signal()
    progressFinished = QtCore.Signal()
    progressError = QtCore.Signal(str)
    progressText = QtCore.Signal(str, bool)
    foundMatch = QtCore.Signal(list)
    '''

    def __init__(self, ScanParameter,sid):
        #QtCore.QThread.__init__(self, parent)
        self.cancel = False
        self.ScanParameter=ScanParameter
        self.sid=sid

    def setConnection(self):
        self.connection = apsw.Connection(DB_Path)

    def setParameters(self, args):
        self.argsDict = args
        print(self.argsDict)
        Algo.setParameters(args)

    def run(self):
        #self.progressText.emit("Load table...", False)
        print("Load table...")
        #self.progressChanged.emit(10)
        websocket.UpdateScanerProgress(10,self.sid)
        cursor = self.connection.cursor()
        print('cursor')
        print(self.connection)
        print(cursor)
        print('overall start')
        print(self.argsDict["overall"]["start"])
        print(self.argsDict["overall"]["end"])
        cursor.execute('select code,name from stock where shares>0 and date>=%d and date<=%d'
                        % (self.argsDict["overall"]["start"], self.argsDict["overall"]["end"]))
        stocksList = np.array(cursor.fetchall())

        stocksList = stocksList.T

        stocksList = dict(zip(stocksList[0], stocksList[1]))
        #stocksList = {"000408" : "hehehe"}

        #self.progressText.emit("Scanning... (%p%)", True)


        topFilter = Algo.makeCriticalFilter(prev=self.argsDict["overall"]["prev"], after=self.argsDict["overall"]["next"], predicate=(lambda x, y, fuzzy: (x >= y) or fuzzy(x,y)), fuzzy=Algo.makeFuzzy(self.argsDict["overall"]["fuzzy"]))
#
#                                                                                                                                                                                             def makeFuzzy(threshold):
#                                                                                                                                                                                                 def _lambda(base, test):
#                                                                                                                                                                                                     if (base == 0):
#                                                                                                                                                                                                         print(base, test)
#                                                                                                                                                                                                     return (np.abs((base-test)/base) <= threshold)
#
#                                                                                                                                                                                             return _lambda
#
        bottomFilter = Algo.makeCriticalFilter(prev=self.argsDict["overall"]["prev"], after=self.argsDict["overall"]["next"], predicate=(lambda x, y, fuzzy: (x <= y) or fuzzy(x,y)), fuzzy=Algo.makeFuzzy(self.argsDict["overall"]["fuzzy"]))

# progress calculate vars
        total = len(stocksList)
        i = 0
        last_percentage = 0
        scanned_percentage = 0

        for code in sorted(stocksList.keys()):
            if self.cancel:
                #self.progressCanceled.emit()
                return

            cursor.execute('select date,value,shares,factor,average,fuquan_average from stock where code = "%s" and shares>0 and date>=%d and date<=%d'
                        % (code, self.argsDict["overall"]["start"], self.argsDict["overall"]["end"]))

            data = np.array(cursor.fetchall())
            #Fetch all (remaining) rows of a query result, returning them as a list of tuples. An empty list is returned if there is no more record to fetch

            if len(data) == 0:
                continue                                                            #data = np.array(cursor.fetchall())
            dates, values, shares, factors, averages, fuquan_averages = Algo.transposeSINA(data)
            topSeq = topFilter(fuquan_averages)
            bottomSeq = bottomFilter(fuquan_averages)

            for key in self.argsDict.keys():
                enabled = self.argsDict[key].get("enable")

                '''
                    rectangle_keys = ["enable", "tophits", "topfuzzy", "bottomhits", "bottomfuzzy", "high", "low", 
                          "rightmargin", "rightdrawback"]
                        rectangle_vals = [self.rectEnableCheckBox.checkState(),
                '''
                if enabled == None or enabled == False:
                    continue
                graphs = Algo.findGraph(key, fuquan_averages, topSeq, bottomSeq)
                if (len(graphs) != 0):
                    name = stocksList[code]
                    print('found!!!')
                    #self.foundMatch.emit([code, name, np.array(dates), np.array(values), np.array(shares), np.array(factors), np.array(averages), np.array(fuquan_averages), np.array(topSeq), np.array(bottomSeq), graphs])

                    dates=dates.tolist()
                    dates=list(map( lambda obj:obj.strftime('%Y%m%d'),dates))

                    #graphs=list(graphs)
                    print(graphs)
                    #graphs=json.dumps(list(graphs))
                    #graph = []
                    #rect_left = max(56,21)
                    #graph.append(['rect', rect_left, 89, 19.41946436588902, 17.374500569026804, [59, 71], [66, 80]])

                    websocket.UpdatedScanMatch(
                        [code, name, dates, values.tolist(),
                         shares.tolist(), factors.tolist(), averages.tolist(),
                         fuquan_averages.tolist(), topSeq.tolist(), bottomSeq.tolist(), str(graphs)],
                                     self.sid)
                    #websocket.UpdatedScanMatch(graphs)


            i = i + 1
            scanned_percentage = 10 + i * 90 // total
            if scanned_percentage != last_percentage:
                        last_percentage = scanned_percentage
                        websocket.UpdateScanerProgress(scanned_percentage, self.sid)
                        #self.progressChanged.emit(scanned_percentage)

        #self.progressFinished.emit()
