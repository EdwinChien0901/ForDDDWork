import json
import Util
import pymongo
from datetime import datetime, timedelta
import sys

def InsertMongo(item):
    myclient = pymongo.MongoClient("mongodb://itadminuser1:520ituser@10.198.213.15/admin?retryWrites=true&w=majority")
    mydb = myclient["test"]
    mycol = mydb["ForeignNews"]

    x = mycol.insert_one(item)
    print(x)
    myclient.close()

if __name__ == "__main__":
    logPath = r"./log/Json_to_Mongo_{0}.log"
    type_ = ["foreign"]
    dataPath = r"./data/json"
    fileName = "foreign_2020-11-15_2020-11-16.json"
    logFile = logPath.format(datetime.now().strftime('%Y%m%d'))
    # 下一頁
    npUrl = ""
    source = {''}
    SAnews = []
    Util.writeLog(logFile, "Begin to insert MongoDB")
    myclient = pymongo.MongoClient("mongodb://itadminuser1:520ituser@10.198.213.15/admin?retryWrites=true&w=majority")
    mydb = myclient["test"]
    mycol = mydb["ForeignNews"]
    try:
        Util.writeLog(logFile, "Json File:{0}/{1}".format(dataPath, fileName))

        newjson = json.load(open(r'{0}/{1}'.format(dataPath, fileName), 'r', encoding="cp950"))
        Util.writeLog(logFile, "New Counts:{0}".format(len(newjson["data"])))
        x = mycol.insert_many(newjson["data"])
        Util.writeLog(logFile, "Result:{0}".format(x))
        Util.writeLog(logFile, "Finish to insert MongoDB")

    except Exception as e:
        Util.writeLog(logFile, "Unexpected Error:{0}".format(Util.getExceptMsg(e)))
    finally:
        myclient.close()


