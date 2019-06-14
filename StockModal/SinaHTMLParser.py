import os
import apsw  #  get no choice , must calculate ex factor inside
# del name in UNIQUE,because stock name may be changed due to ST or shell-borrowing such shit happen
_table_prop = """(name TEXT, code TEXT, date INTEGER, 
                    shares FLOAT, value FLOAT, factor INTEGER, 
                    open INTEGER, high INTEGER, close INTEGER, 
                    low INTEGER, average FLOAT, fuquan_average FLOAT,
                    UNIQUE(code, date))"""
from html.parser import HTMLParser
# from html.entities import name2codepoint
from struct import pack
from collections import namedtuple
# import threading
import websocket
import GSym
TTagState = namedtuple("TTagState", "tag attr value next data")

# DB_Path = 'C:/WebProgramming/DB/Test/final.db'
DB_Path = 'C:/WebProgramming/DB/final.db'

class Matched(Exception): pass

class SinaHTMLParser(HTMLParser):
    def __init__(self, exRightParser, sid, sina_dir, stockCode=0, year=0, season=0, parent=None):#progress_que,
        #self.que=progress_que

        self.stockCode = stockCode
        # get latest factor
        self.curretFactor = 1
        self.connection = apsw.Connection(DB_Path)
        self.cursor = self.connection.cursor()
        self.cursor.execute("create table if not exists stock %s" % (_table_prop))
        # self.cursor.execute("create table if not exists stock %s" % _table_prop)
        # self.cursor.execute('select factor from stock where code="%s" order by date desc ' % self.stockCode)
        # self.latestFactor, = self.cursor.fetchone()
        # if self.latestFactor is None:
        self.firstFactor = None
        self.secondFactor = None
        self.currentDate = None  # the latest date spider crawled

        self.dividendShareFactor = None
        self.shareGrantingFactor = None
        self.latestFactor = None
        self.latestDate = None
        self.recordDayClose = None
        self.exPrice = None
        self.recordDayClose2 = None
        self.exPrice2 = None
        self.gotErrData = False

        self.exParser = exRightParser


        self.sid=sid
        self.endTagStates = ("table", None, None, None, False)
        self.setParserFromat(True)
        self.stockName = None
        self.file = None
        self.sina_dir_path = sina_dir

        # remove factor below
        self.fileFormat = ("date", "shares", "value", "open", "high", "close", "low")

        self.noName = False

        # super(SinaHTMLParser, self).__init__(parent)
        super(SinaHTMLParser, self).__init__()  # Python 3.7 no need parent para here any more



    def newStock(self):
        if self.file is not None:
            self.file.close()
        self.stockName = None
        self.file = None

    def setParserFromat(self, new):
        if new:
            self.startTagStates = {"begin" : ("table", "id", "FundHoldSharesTable", "name", False),
                                   "name"  : ("th", "colspan", "7", "date", True),
                                   "date"  : ("a", "target", "_blank", "open", True),
                                   "open"  : ("div", None, None, "high", True),
                                   "high"  : ("div", None, None, "close", True),
                                   "close" : ("div", None, None, "low", True),
                                   "low"   : ("div", None, None, "shares", True),
                                   "shares": ("div", None, None, "value", True),
                                   "value" : ("div", None, None, "date", True)}
                                  # "factor": ("div", None, None, "date", True)}  #  outdate since sina not provide factor
        else:
            self.startTagStates = {"begin" : ("table", "id", "FundHoldSharesTable", "name", False),
                                   "name"  : ("th", "colspan", "8", "tr1", True),
                                   "tr1"   : ("tr", None, None, "tr2", False),
                                   "tr2"   : ("tr", None, None, "date", False),
                                   "date"  : ("div", None, None, "open", True),
                                   "open"  : ("div", None, None, "high", True),
                                   "high"  : ("div", None, None, "close", True),
                                   "close" : ("div", None, None, "low", True),
                                   "low"   : ("div", None, None, "shares", True),
                                   "shares": ("div", None, None, "value", True),
                                   "value" : ("div", None, None, "date", True)}
                                   # "factor": ("div", None, None, "date", True)} #  outdate since sina not provide factor
        self.startTagKey = "begin"
        self.startTag = TTagState._make(self.startTagStates[self.startTagKey])
        self.endTag = None
        self.dataAvial = False
        self.hasRealData = False
        self.dataEnd = False
        self.row = {}

    def resetFactorExPrices(self):

        self.cursor.execute('select factor,date from stock where code="%s" order by date desc ' % self.stockCode)
        try:
            self.latestFactor, self.latestDate = self.cursor.fetchone()
        except:
            print("no data in database,this stock may  be already delisted  or new in market \n")
            self.latestFactor = 1
            self.latestDate = 0

        self.currentDate = None
        self.recordDayClose = None
        self.exPrice = None
        self.recordDayClose2 = None
        self.exPrice2 = None
        self.dividendShareFactor = None
        self.shareGrantingFactor = None
        self.firstFactor = None
        self.secondFactor = None
        self.gotErrData = False

    def decode(self, tag, attrs):
        if tag == self.startTag.tag:
            try:
                if self.startTag.attr is None:
                    raise Matched()
                for attr in attrs:
                    if attr[0] == self.startTag.attr and attr[1] == self.startTag.value:
                        raise Matched()
            except Matched:
                if self.endTag is None:
                    self.endTag = TTagState._make(self.endTagStates)
                self.dataAvial = self.startTag.data
                # only update when data is not cared
                if not self.dataAvial:
                    self.startTagKey = self.startTag.next
                    self.startTag = TTagState._make(self.startTagStates[self.startTagKey])

    def processData(self):  # updated per row ,i.e.,per day
        self.row["date"] = (lambda x, y, z: x*10000+y*100+z)(*list(map(int, self.row["date"].split("-"))))
        self.currentDate = self.row["date"]
        self.row["shares"] = float(self.row["shares"])
        self.row["value"] = float(self.row["value"])
        if self.row["value"] != 0 and self.row["shares"] != 0:  # see 002015 20190319,fucking 0!
            self.row["average"] = self.row["value"] / self.row["shares"]
            for field in self.fileFormat[3:]:
                self.row[field] = int(float(self.row[field])*1000)
        else:
            print("Got fucking error data!\n")
            self.gotErrData = True

        # self.currentFactor = self.latestFactor


    def flushData(self):
        # updated per row ,i.e.,per day,
        # and date of row in html is descended downward, so factor and fuquan_average can't be updated here
        if not self.gotErrData:
            self.gotErrData = False
            if self.latestDate == 0 or self.row["date"] > self.latestDate:  # actually no need if, for ignore op will avoid overwriting
                self.cursor.execute('insert or ignore into stock (name,code,date,shares,value,open,'
                    'high,close,low,average)'
                    'VALUES ("{}","{}",{},{},{},{},{},{},{},{})'.format(
                    self.row["name"], self.stockCode, self.row["date"], self.row["shares"],
                    self.row["value"], self.row["open"], self.row["high"],
                    self.row["close"], self.row["low"], self.row["average"]
                    )
            )

    # historically,factor is int in database, now converted it into float in new db,so now can use factor directly below
    def updateCurrentSeasonFuquanAndFactor(self):  # all prices unit are multiplied by 1000,for 1000* in processData()
        # self.cursor.execute(
        #     'update stock set fuquan_average=average*factor/%d where code="%s" and date>=%d'
        #     % (self.latestFactor, self.stockCode,
        #        self.exParser.year * 10000+(self.exParser.season-1)*3*100)
        # )
        if self.exParser.gotDividendShareInfo:
            if self.latestDate <= self.exParser.row["recordDate"] and self.currentDate >= self.exParser.row["exRightDate"] :
                if self.recordDayClose is None:
                    self.cursor.execute('select close from stock where date =%d and code ="%s" '
                                        % (self.exParser.row["recordDate"], self.stockCode))
                    self.recordDayClose, = self.cursor.fetchone()
                    self.exPrice = (self.recordDayClose -
                                    1000*self.exParser.row["dividend"]/10) / (
                                    1+self.exParser.row["sharesSent"]/10 +
                                    self.exParser.row["sharesTranscent"]/10)
                    self.dividendShareFactor = self.recordDayClose / self.exPrice
                    # self.latestFactor = self.currentFactor * self.latestFactor
            else:
                self.exParser.gotDividendShareInfo = False
                self.dividendShareFactor = False

        if self.exParser.gotShareGrantingInfo:
            if self.latestDate <= self.exParser.row["recordDate2"] and self.currentDate >= self.exParser.row["exRightDate2"]:
                if self.recordDayClose2 is None:
                    self.cursor.execute('select close from stock where date =%d and code ="%s" '
                                        % (self.exParser.row["recordDate2"], self.stockCode))
                    self.recordDayClose2, = self.cursor.fetchone()
                    self.exPrice2 = (self.recordDayClose2 +
                                     1000*self.exParser.row["offeringPrice"] *
                                     self.exParser.row["buyOfferingRatio"]/10
                                    ) / (1+self.exParser.row["buyOfferingRatio"]/10)
                    self.shareGrantingFactor = self.recordDayClose2 / self.exPrice2
                    # self.latestFactor = self.currentFactor * self.latestFactor
            else:
                self.exParser.gotShareGrantingInfo = False
                self.shareGrantingFactor = False

        if self.shareGrantingFactor and self.dividendShareFactor:
            if self.exParser.row["recordDate"] >= self.exParser.row["recordDate2"]:
                self.firstFactor = self.shareGrantingFactor * self.latestFactor
                self.secondFactor = self.dividendShareFactor * self.firstFactor
                self.cursor.execute(
                    'update stock set factor=%f where code="%s" and date >%d and date <=%d '
                    % (self.latestFactor, self.stockCode, self.latestDate, self.exParser.row["recordDate2"])
                )
                self.cursor.execute(
                    'update stock set factor=%f where code="%s" and date >%d and date <=%d '
                    % (self.firstFactor, self.stockCode, self.exParser.row["recordDate2"], self.exParser.row["recordDate"])
                )
                self.cursor.execute(
                    'update stock set factor=%f where code="%s" and date >%d '
                    % (self.secondFactor, self.stockCode, self.exParser.row["recordDate"])
                )
            else:
                self.firstFactor = self.dividendShareFactor * self.latestFactor
                self.secondFactor = self.shareGrantingFactor * self.firstFactor
                self.cursor.execute(
                    'update stock set factor=%f where code="%s" and date >%d and date <=%d '
                    % (self.latestFactor, self.stockCode, self.latestDate, self.exParser.row["recordDate"])
                )
                self.cursor.execute(
                    'update stock set factor=%f where code="%s" and date >%d and date <=%d '
                    % (self.firstFactor, self.stockCode, self.exParser.row["recordDate"],
                       self.exParser.row["recordDate2"])
                )
                self.cursor.execute(
                    'update stock set factor=%f where code="%s" and date >%d '
                    % (self.secondFactor, self.stockCode, self.exParser.row["recordDate2"])
                )
            self.latestFactor = self.secondFactor

        elif self.shareGrantingFactor:
            self.firstFactor = self.shareGrantingFactor * self.latestFactor
            self.secondFactor = None
            self.cursor.execute(
                'update stock set factor=%f where code="%s" and date >%d and date <=%d '
                % (self.latestFactor, self.stockCode, self.latestDate, self.exParser.row["recordDate2"])
            )
            self.cursor.execute(
                'update stock set factor=%f where code="%s" and date >%d '
                % (self.firstFactor, self.stockCode, self.exParser.row["recordDate2"])
            )
            self.latestFactor = self.firstFactor

        elif self.dividendShareFactor:
            self.firstFactor = self.dividendShareFactor * self.latestFactor
            self.secondFactor = None
            self.cursor.execute(
                'update stock set factor=%f where code="%s" and date >%d and date <=%d '
                % (self.latestFactor, self.stockCode, self.latestDate, self.exParser.row["recordDate"])
            )
            self.cursor.execute(
                'update stock set factor=%f where code="%s" and date >%d '
                % (self.firstFactor, self.stockCode, self.exParser.row["recordDate"])
            )
            self.latestFactor = self.firstFactor
        else:
            self.cursor.execute(
                'update stock set factor=%f where code="%s" and date >%d '
                % (self.latestFactor, self.stockCode, self.latestDate)
            )

        self.cursor.execute(
            'update stock set fuquan_average=average*factor/%f where code="%s" '
            % (self.latestFactor, self.stockCode)
        )

    def handle_starttag(self, tag, attrs):
        self.decode(tag, attrs)

    def handle_endtag(self, tag):
        if self.endTag is not None and self.endTag.tag == tag:
            self.dataEnd = True

    def handle_data(self, data):
        if not self.dataEnd and self.dataAvial:
            if self.startTagKey == "name":
                if self.stockName is None:
                    self.stockName = data.rstrip(" \r\t\n").lstrip(" \r\t\n").replace("*", "_")
                    if self.stockName != "()": # means this stock has no FuQuan data
                        websocket.UpdateSpideredName(self.stockName, GSym.get_value('current_sid'))
                        # self.que.put(self.stockName)
                        print(self.stockName)
                        self.row[self.startTagKey] = self.stockName
                        self.noName = False
                    else:
                        self.noName = True
            else:
                self.hasRealData = True
                self.row[self.startTagKey] = data.rstrip(" \r\t\n").lstrip(" \r\t\n")
            self.dataAvial = False
            self.startTagKey = self.startTag.next
            self.startTag = TTagState._make(self.startTagStates[self.startTagKey])
            # all data fields are collected
            if self.startTagKey == "date" and self.hasRealData:  # updated per row ,i.e.,per day
                self.processData()
                self.flushData()
