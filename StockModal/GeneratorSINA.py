import os, apsw
from struct import unpack
#from PyQt4 import QtCore
import traceback

# del name in UNIQUE,because stock name may be changed due to ST or shell-borrowing such shit happen
_table_prop = """(name TEXT, code TEXT, date INTEGER, 
                    shares FLOAT, value FLOAT, factor INTEGER, 
                    open INTEGER, high INTEGER, close INTEGER, 
                    low INTEGER, average FLOAT, fuquan_average FLOAT,
                    UNIQUE(code, date))"""


SinaDirPath = 'E:/SA/StockAssist_0304/sina'
DB_Path = 'C:/WebProgramming/DB/final.db'
def StartSaveDB():
    sina=GeneratorSINA()
    sina.setSinaDir(SinaDirPath)
    sina.set_DB_name(DB_Path)
    sina.run()
class GeneratorSINA():
    def __init__(self, parent=None):
        self.cancel = False
    
    def setSinaDir(self, dir):
        self.dir = dir
        #self.progressText.emit("Converting %s (%%p%%)" % dir, True)
        #self.progressChanged.emit(0)

    def set_DB_name(self,path):
        self.db_dir = path    
        
    def run(self):
        try:
            print('db start')
            connection = apsw.Connection(":memory:")
            cursor = connection.cursor()
            cursor.execute("create table if not exists stock %s" % (_table_prop))

            self.savedFileName = self.db_dir  # "d:/19901219_to_20140725_SINA.db"# % (minDate, maxDate)
            cursor.execute('attach database "%s" as local_db' % self.savedFileName)
            cursor.execute("create table if not exists local_db.stock %s" % _table_prop)
            
            filelist = os.listdir(self.dir)
            os.chdir(self.dir)
            
            last_percentage = 0
            i = 0
            minDate = 30000000 # hope this program can survive then...
            maxDate = 0
            for stock in filelist:
                if self.cancel:
                    #self.progressCanceled.emit()
                    return
                f = open(stock, "rb")
                buffer = f.read()
                index = 0
                while index < len(buffer):
                    # according to SinHTMLParser for data format
                    # ("date", "shares", "value", "factor", "open", "high", "close", "low")
                    data = unpack("<lfflllll", buffer[index:index+32])
                    # remove .sina ext, ')', '_', and make '(' as separator.
                    nameAndCode = stock.split(".")[0].rstrip(")").replace("_", "*").replace("(", " ").split(" ")
                    # save date range
                    minDate = min(minDate, data[0])
                    maxDate = max(maxDate, data[0])
                    if data[1] == 0:
                            print("%s: %d has shares = 0" % (nameAndCode[1], data[0]))
                    cursor.execute("insert or ignore into stock values(?,?,?,?,?,?,?,?,?,?,?,?)",
                                   nameAndCode+list(data)+[data[2]/data[1] if data[1] > 0 else 0, 0])
                    index = index + 32

                # filling the fuquan_average data
                # 1. get the latest factor
                # 2. fuquan_average = average * (factor/latest_factor)
                cursor.execute('select factor from stock where code="%s" order by date asc' % nameAndCode[1])
                # fetchall will return data in tuples even if you're requesting 1 element
                latestFactor = cursor.fetchall()[-1][0]
                cursor.execute('update stock set fuquan_average=average*factor/%d where code="%s" ' % (latestFactor, nameAndCode[1]))
                cursor.execute('update local_db.stock set fuquan_average=average*factor/%d where code="%s" ' % (latestFactor, nameAndCode[1]))
                i = i + 1
                converted_percentage = i * 70 // len(filelist)
                if converted_percentage != last_percentage:
                            last_percentage = converted_percentage
                            #self.progressChanged.emit(converted_percentage)

            
            # save to local disk file
            #self.progressText.emit("Flushing to disk... (%p%)", False)
            # get the data's date range

            cursor.execute("""insert or ignore into local_db.stock (name, code, date, shares, value, factor, open, high, close, low, average, fuquan_average)
                            select * from stock""")  # insert or replace
            cursor.execute("detach database local_db")
            connection.close()
            print('db save over')
            
            #self.progressChanged.emit(100)
            #self.progressFinished.emit()
        except:
            pass
            print('db save exception')
            traceback.print_exc()
            #self.progressError.emit("Convert directory failed")