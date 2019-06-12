from html.parser import HTMLParser
# from html.entities import name2codepoint
from struct import pack
from collections import namedtuple
# import threading
# import websocket
# import GSym

# Warning here, this parser just aims to get latest time exRight info due to implement complexity,
# so should be used one time by on season

import requests

TTagState = namedtuple("TTagState", "tag attr value next data")  # data is bool,decide if need this element data


class Matched(Exception): pass


class DividendShareGrantingParser(HTMLParser):
    def __init__(self, year, season, parent=None):  # progress_que,
        # self.que = progress_que
        # self.noNewExRight = False # inform get no exRight info
        self.endTableId = None
        self.sharesSentTranscentRatio = 0
        self.dividend = 0
        self.exRightDate = 0
        self.recordDate = 0
        self.buyOfferingRatio = 0
        self.offeringPrice = 0
        self.exRightDate2 = 0
        self.recordDate2 = 0
        self.gotDividendShareInfo = False  # inform if get  exRight info
        self.gotShareGrantingInfo = False  # # inform if get  exRight info
        self.year = year
        self.season = season
        # self.sid = sid
        self.startTagkey = None
        self.endTagStates = ("table", None, None, None, False)
        self.setParserFromat()
        self.stockName = None
        self.file = None
        self.dataAvial = False
        # self.sina_dir_path = sina_dir
        # self.fileFormat = ("date", "shares", "value", "factor", "open", "high", "close", "low")
        self.noName = False
        # super(SinaHTMLParser, self).__init__(parent)
        super(DividendShareGrantingParser, self).__init__()  # Python 3.7 no need parent para here any more

    def setParserFromat(self):
        self.startTagStates = {
            "begin": ("table", "id", "sharebonus_1", "AnnouncementDate", False),
            "nextRow": ("tr", None, None, "AnnouncementDate", False),
            # "tbody": ("tbody", None, None, "AnnouncementDate", True), # redundant
            # "AnnouncementDate": ("td", "class", "head", "sharesSent", False),
            "AnnouncementDate": ("td", None, None, "sharesSent", False),  # attribute just disappear in request,compared with F12,weird
            "sharesSent": ("td", None, None, "sharesTranscent", True),
            "sharesTranscent": ("td", None, None, "dividend", True),
            "dividend": ("td", None, None, "process", True),
            "process": ("td", None, None, "exRightDate", True),
            "exRightDate": ("td", None, None, "recordDate", True),
            "recordDate": ("td", None, None, "sharebonus2", True),
            "sharebonus2": ("table", "id", "sharebonus_2", "AnnouncementDate2", False),
            # "AnnouncementDate2": ("td", "class", "head", "buyOfferingRatio", False),
            "AnnouncementDate2": ("td", None, None, "buyOfferingRatio", False),  # attribute just disappear in request,compared with F12,weird
            "buyOfferingRatio": ("td", None, None, "offeringPrice", True),
            "offeringPrice": ("td", None, None, "capitalStock", True),
            "capitalStock": ("td", None, None, "exRightDate2", False),
            "exRightDate2": ("td", None, None, "recordDate2", True),  # Only get latest info, no loop
            "recordDate2": ("td", None, None, "end", True),
        }
        self.startTagKey = "begin"
        self.startTag = TTagState._make(self.startTagStates[self.startTagKey])
        self.endTag = None
        self.dataAvial = False
        # self.hasRealData = False
        self.dataEnd = False
        self.row = {}
        self.gotDividendShareInfo = False
        self.gotShareGrantingInfo = False

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
                self.dataAvial = self.startTag.data  # inform if tag's data are needed
                # only update when data is not cared
                if not self.dataAvial: # the data not needed, so update next tag here but not in handle_data
                    # if self.startTag.next != "end":
                    self.startTagKey = self.startTag.next
                    self.startTag = TTagState._make(self.startTagStates[self.startTagKey])
                    # else:# all spider are finished
                    #    self.dataEnd = True

    def processDividendShareData(self):
        self.row["exRightDate"] = (lambda x, y, z: x * 10000 + y * 100 + z)(
            *list(map(int, self.row["exRightDate"].split("-"))))
        self.row["recordDate"] = (lambda x, y, z: x * 10000 + y * 100 + z)(
            *list(map(int, self.row["recordDate"].split("-"))))
        self.row["dividend"] = float(self.row["dividend"])
        self.row["sharesSent"] = float(self.row["sharesSent"])  # maybe not Integer,just so weird
        self.row["sharesTranscent"] = float(self.row["sharesTranscent"])  # maybe not Integer,just so weird

    def processShareGrantingData(self):
        self.row["exRightDate2"] = (lambda x, y, z: x * 10000 + y * 100 + z)(*list(map(int, self.row["exRightDate2"].split("-"))))
        self.row["recordDate2"] = (lambda x, y, z: x * 10000 + y * 100 + z)(
            *list(map(int, self.row["recordDate2"].split("-"))))
        self.row["buyOfferingRatio"] = float(self.row["buyOfferingRatio"])  # maybe not Integer,just so weird
        self.row["offeringPrice"] = float(self.row["offeringPrice"])
        # self.row["capitalStock"] = float(self.row["capitalStock"])


    def flushData(self):
        if self.file is None:
            self.file = open(self.sina_dir_path + "//" + self.stockName + ".sina", "wb")
            # self.file = open("d:/sina/"+self.stockName+".sina", "wb")
        self.file.write(pack("<lfflllll", *list(map(self.row.get, self.fileFormat))))

    def handle_starttag(self, tag, attrs):
        if not self.dataEnd:
            self.decode(tag, attrs)

    def handle_endtag(self, tag):
        if self.endTag is not None and self.endTag.tag == tag:
            if self.endTableId == "sharebonus_2":
                self.dataEnd = True

    def handle_data(self, data):
        if not self.dataEnd and self.dataAvial:
            # self.hasRealData = True
            self.row[self.startTagKey] = data.rstrip(" \r\t\n").lstrip(" \r\t\n")  # save the element data
            if self.startTagKey == "sharebonus2":  # note table end tag for handle_endtag to exit
                self.endTableId = "sharebonus_2"


            if self.startTagKey == "process":# for debug
                print(self.row[self.startTagKey])
            if self.startTagKey == "process" and self.row[self.startTagKey] != "实施":  # get nosense data,so loop back
                self.startTagKey = "nextRow"
                self.startTag = TTagState._make(self.startTagStates[self.startTagKey])
                self.dataAvial = False
            elif self.startTagKey == "exRightDate":
                try:
                    exRightYear , exRightSeason= (lambda x, y, z: (x, (y+2)//3))(*list(map(int, self.row[self.startTagKey].split("-"))))
                    print("exRightYear is %d,exRightSeason is %d" %(exRightYear, exRightSeason))  # for debug
                except:
                    print("Got implemented notice,but no exDate data!\n")
                    exRightYear =0
                    exRightSeason=0
                if exRightYear != self.year or exRightSeason != self.season:
                    self.startTagKey = "sharebonus2"
                    self.startTag = TTagState._make(self.startTagStates[self.startTagKey])
                    self.gotDividendShareInfo = False
                    self.dataAvial = False
                else:
                    self.gotDividendShareInfo = True
                    self.dataAvial = False
                    self.startTagKey = self.startTag.next
                    self.startTag = TTagState._make(self.startTagStates[self.startTagKey])
            elif self.startTagKey == "exRightDate2":
                exRightYear2 , exRightSeason2= (lambda x, y, z: (x, (y+2)//3))(*list(map(int, self.row[self.startTagKey].split("-"))))
                print ("exRightYear2 is %d,exRightSeason2 is %d" %(exRightYear2,exRightSeason2)) # for debug
                if exRightYear2 != self.year or exRightSeason2 != self.season:
                    self.startTagKey = "AnnouncementDate2"
                    self.startTag = TTagState._make(self.startTagStates[self.startTagKey])
                    self.gotShareGrantingInfo = False
                    self.dataAvial = False
                    self.dataEnd = True  # first exRightData outdate, just exit
                else:
                    self.gotShareGrantingInfo = True
                    self.dataAvial = False
                    self.startTagKey = self.startTag.next
                    self.startTag = TTagState._make(self.startTagStates[self.startTagKey])

            elif self.startTagKey == "recordDate2":
                self.dataEnd = True  # get 1st group effect data already , so exit
            else:
                self.dataAvial = False
                self.startTagKey = self.startTag.next
                self.startTag = TTagState._make(self.startTagStates[self.startTagKey])
                # all data fields are collected
                # if self.startTagKey == "end":
                #    self.dataEnd = True
                    # self.processData()
                    # self.flushData()

if __name__ == "__main__":
    print("start")
    html = requests.get("http://money.finance.sina.com.cn/corp/go.php/vISSUE_ShareBonus/stockid/002166.phtml")
    html.encoding = 'gbk'
    html = html.text
    parser = DividendShareGrantingParser(2019, 2)
    parser.setParserFromat()
    parser.feed(html)
    print(parser.row)
    print("end")

