import os, apsw
#from PyQt4 import QtCore
import websocket

#DB_Path='E:/SA/StockAssist_0304/DB/final.db'
DB_Path='C:/WebProgramming/DB/final.db'

def loadDB(sid):
    db = DBLoad(sid)
    db.setFileName()
    db.run()
    return db.memconn

class DBLoad:
    def __init__(self,sid, parent=None):
        self.cancel = False
        self.sid=sid
    
    def setFileName(self, fileName=DB_Path):
        self.fileName = fileName
        #self.progressText.emit("Loading %s (%%p%%)" % fileName, True)
        
    def run(self):
        self.phyconn = apsw.Connection(self.fileName)
        self.memconn = apsw.Connection(":memory:")
        try:#backup.__exit__() just make sure copy if finished,not close backup,so with is good,memconn is still exist when out
            with self.memconn.backup("main", self.phyconn, "main") as backup:
                # call with 0 to get the total pages
                backup.step(0)
                total = backup.pagecount
                
                stepped = 0
                one_percent = total if total < 100 else total // 100
                last_percentage = 0
                while stepped <= total:
                    if self.cancel:
                        #self.progressCanceled.emit()
                        self.memconn=None
                        return
                    backup.step(one_percent)
                    stepped = stepped + one_percent
                    stepped_percentage = stepped*100//total
                    if stepped_percentage != last_percentage:
                        last_percentage = stepped_percentage
                        #self.progressChanged.emit(stepped_percentage)
                        websocket.UpdateLoadDBProgress(stepped_percentage,self.sid)
                # Done
                #self.progressFinished.emit()
        except :
            #self.progressError.emit("Error when loading database!")
            self.memconn=None
            pass
            