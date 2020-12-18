import requests
import json
import urllib3
import time
from datetime import datetime, timedelta
import random
from dateutil.relativedelta import relativedelta
import Util
import pytz
import math
import pymongo

urllib3.disable_warnings()

tpe = pytz.timezone('Asia/Taipei')

headers = {
'content-type': 'application/json',
'accept':       'application/json',
'auth-header': 'eyJhbGciOiJIUzUxMiIsInR5cCI6IkpXVCJ9.eyJpZCI6IjllMjkyOTI1LWEwM2YtNDFmYi1iNjI0LTBlNTFkMjE4ZDA3NiIsImlhdCI6MTYwNjM5MTE1MiwiZXhwIjoxNjM3OTQ4NzUyfQ.BiFBLlx5X-fOvDtB2JIHjZAP2aqPg0AghXTu6mar_KhEbGSZ2bQ4TkMA8ZSohUNp4212UHKoUgBEeViVg6ijqQ',
'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/86.0.4240.198 Safari/537.36'
}

payload = {"channel_id": "foreign", "sorted": "tm", "withPushs": False}
payload['group_id'] = "3bd797ba-4bf6-4ad4-851a-739755b09589"

def InsertMongo(item):
    myclient = pymongo.MongoClient("mongodb://itadminuser1:520ituser@10.198.213.15/admin?retryWrites=true&w=majority")
    mydb = myclient["test"]
    mycol = mydb["ForeignNews"]

    x = mycol.insert_one(item)
    print(x)
    myclient.close()

if __name__ == "__main__":
    logPath = r"./log/ForeignNew_{0}_Json_{1}.log"
    type_ = ["foreign"]

    # 下一頁
    npUrl = ""
    source = {''}
    SAnews = []

    for t in type_:
        logFile = logPath.format(t, datetime.now().strftime('%Y%m%d'))
        payload["channel_id"] = t

        DateStr = '2020-12-02'
        startDate = datetime.strptime(DateStr, '%Y-%m-%d')

        for i in range(0, 1):
            #nextDate = startDate + relativedelta(months=1)
            nextDate = startDate + relativedelta(days=1)
            Util.writeLog(logFile, "StartTime:{0} & EndTime:{1}".format(startDate.strftime('%Y-%m-%d'), nextDate.strftime('%Y-%m-%d')))

            payload['startDate'] = startDate.strftime('%Y-%m-%d')
            payload['endDate'] = nextDate.strftime('%Y-%m-%d')
            payload['page'] = 1

            res = requests.post('https://api.infominer.io/search', json=payload, headers=headers, verify=False, timeout=30)
            jd = res.json()
            pageNum = math.ceil(jd["total_count"] / 100)

            data = jd["data"]

            for d in jd["data"]:
                source.add(d["source"])
                InsertMongo(d)
                #if d["source"] == "seekingalpha":
                #    print(d)
                    # SAnews.extend(d)

            for p in range(1, pageNum):
                Util.writeLog(logFile, "page : {0}".format(p + 1))
                payload['page'] = p + 1

                resForNext = requests.post('https://api.infominer.io/search', json=payload, headers=headers, verify=False, timeout=30)
                jdNext = resForNext.json()
                data.extend(jdNext["data"])

                for d in jdNext["data"]:
                    source.add(d["source"])
                    InsertMongo(d)
                    #if d["source"] == "seekingalpha":
                    #    print(d)
                        # SAnews.extend(d)

            jd["data"] = data
            #jd["source"] = source
            Util.writeLog(logFile, "source : {0}".format(source))

            #file = r'./json/{0}_{1}_{2}.json'
            #filePath = file.format(t, startDate.strftime('%Y-%m-%d'), nextDate.strftime('%Y-%m-%d'))
            #if len(data) > 0:
            #    with open(filePath, 'w', encoding='CP950') as f:
            #        json.dump(jd, f)

            startDate = nextDate
            time.sleep(random.randint(2, 5))
