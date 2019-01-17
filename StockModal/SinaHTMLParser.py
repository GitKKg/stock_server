from html.parser import HTMLParser
# from html.entities import name2codepoint
from struct import pack
from collections import namedtuple
# import threading
import websocket
import GSym
TTagState = namedtuple("TTagState", "tag attr value next data")

class Matched(Exception): pass

class SinaHTMLParser(HTMLParser):
    def __init__(self, sid, sina_dir, parent=None):#progress_que,
        #self.que=progress_que
        self.sid=sid
        self.endTagStates = ("table", None, None, None, False)
        self.setParserFromat(True)
        self.stockName = None
        self.file = None
        self.sina_dir_path = sina_dir
        self.fileFormat = ("date", "shares", "value", "factor", "open", "high", "close", "low")
        
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
                                   "name"  : ("th", "colspan", "8", "date", True),
                                   "date"  : ("a", "target", "_blank", "open", True),
                                   "open"  : ("div", None, None, "high", True),
                                   "high"  : ("div", None, None, "close", True),
                                   "close" : ("div", None, None, "low", True),
                                   "low"   : ("div", None, None, "shares", True),
                                   "shares": ("div", None, None, "value", True),
                                   "value" : ("div", None, None, "factor", True),
                                   "factor": ("div", None, None, "date", True)}
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
                                   "value" : ("div", None, None, "factor", True),
                                   "factor": ("div", None, None, "date", True)}
        self.startTagKey = "begin"
        self.startTag = TTagState._make(self.startTagStates[self.startTagKey])
        self.endTag = None
        self.dataAvial = False
        self.hasRealData = False
        self.dataEnd = False
        self.row = {}
        
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
                    
    def processData(self):
        self.row["date"] = (lambda x, y, z: x*10000+y*100+z)(*list(map(int, self.row["date"].split("-"))))
        self.row["shares"] = float(self.row["shares"])
        self.row["value"] = float(self.row["value"])
        for field in self.fileFormat[3:]:
            self.row[field] = int(float(self.row[field])*1000)
    
    def flushData(self):
        if self.file is None:
            self.file = open(self.sina_dir_path+"//"+self.stockName+".sina", "wb")
            # self.file = open("d:/sina/"+self.stockName+".sina", "wb")
        self.file.write(pack("<lfflllll", *list(map(self.row.get, self.fileFormat))))    
        
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
            if self.startTagKey == "date" and self.hasRealData:
                self.processData()
                self.flushData()
