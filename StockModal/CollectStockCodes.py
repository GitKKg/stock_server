import apsw, os
import re

ZsDayFilePath = 'C:\WebProgramming\ZS_Data\day'
stockCodesFilename = 'C:\WebProgramming\ServerPy3.6\StockModal\stock_codes\stock_codes.db'


def GotStockCodes():
    print("Loading...")

    filesList = os.listdir(ZsDayFilePath)
    codesList = list(map((lambda str: (re.findall("\d+", str)[0])), filesList))

    codesList = sorted(list(set(codesList)))

    newconnection = apsw.Connection(":memory:")
    newcursor = newconnection.cursor()
    newcursor.execute("create table if not exists stock %s" % "(name TEXT)")

    print("Inserting")
    for code in codesList:
        newcursor.execute("insert or ignore into stock values(?)", (code,))

    print("Syncing")
    newcursor.execute('attach database "%s" as local_db' % stockCodesFilename)
    newcursor.execute("create table if not exists local_db.stock %s" % "(name TEXT)")
    newcursor.execute("""insert or ignore into local_db.stock (name) select * from stock""")
    newcursor.execute("detach database local_db")

    print("Got stock codes done!\n")

    # clean database, except vol and amount,the open to close price data are all wrong in database
    # before this date due to struct .sina operation,need re crawl
    # newcursor.execute('delete from stock where date >= 20190101')
