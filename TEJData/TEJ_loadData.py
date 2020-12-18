from datetime import datetime
from dateutil.relativedelta import relativedelta
import pandas as pd
import requests
import urllib3
import json
from sqlalchemy import create_engine
import time, random
import Util

urllib3.disable_warnings()

apiKey = Util.getApiKey()

if __name__ == "__main__":
    logPath = r"./log/TEJ_LOAD_{0}.log"
    tableDf = pd.read_csv(r"D:\EdwinChien\Projects\AI_Profolio\TEJ\table_list_AFUNDS.csv")
    otTable = ["TWN/ANPRCSTD", "TWN/AIACC", "TWN/AIND"]
    #tableDf = tableDf[1:2]
    #tableDf = ['TWN/ATINST1','TWN/APRCD','TWN/APRCD1','TWN/APRCD2','TWN/APRCDA','TWN/APRCM','TWN/APRCW','TWN/APRCY','TWN/AAPRCDA','TWN/AAPRCM1','TWN/AAPRCW1','TWN/AAPRCY1','TWN/AFINSTD','TWN/AIACC','TWN/AIFIN']

    #engine = create_engine('mysql://edwin:ed0685@localhost/tej_test', encoding="utf-8")
    engine = create_engine('mysql://edwin:ed0685@localhost/tej_test?charset=utf8')

    url = "https://api.tej.com.tw/api/datatables/{0}.json?api_key={1}&mdate.gte={2}&mdate.lt={3}"
    #url = "https://api.tej.com.tw/api/datatables/{0}.json?api_key={1}&coid=1101,1102,1103,1104,2330,2449&mdate.gte={2}&mdate.lt={3}"
    #下一頁
    npUrl = "https://api.tej.com.tw/api/datatables/{0}.json?api_key={1}&opts.cursor_id={2}"
    p = {"http": "http://vwcg.cathaysite.com.tw:8080"}

    tName = tableDf["tableName"].to_list()
    #tName = ['TWN_ATINST1','TWN_APRCD','TWN_APRCD1','TWN_APRCD2','TWN_APRCDA','TWN_APRCM','TWN_APRCW','TWN_APRCY','TWN_AAPRCDA','TWN_AAPRCM1','TWN_AAPRCW1','TWN_AAPRCY1','TWN_AFINSTD','TWN_AIACC','TWN_AIFIN']

    sqlList = []
    for i, t in enumerate(tableDf["table"].to_list()):
        #print(t)
        startDate = datetime.strptime('2020-06-01', '%Y-%m-%d')
        logFile = logPath.format(datetime.now().strftime('%Y%m%d'))
        Util.writeLog(logFile, "table name:{0}".format(t))

        if t in otTable:
            continue

        try:
            for j in range(1, 3):
                nextDate = startDate + relativedelta(months=1)
                sdate = startDate.strftime('%Y-%m-%d')
                ndate = nextDate.strftime('%Y-%m-%d')
                if t in otTable:
                    urlStr = "https://api.tej.com.tw/api/datatables/{0}.json?api_key={1}".format(t, apiKey)
                else:
                    urlStr = url.format(t, apiKey, sdate, ndate)

                #print(urlStr)
                Util.writeLog(logFile, urlStr)

                try:
                    res = requests.get(urlStr, verify=False)
                    res.encoding = "utf-8"
                except requests.exceptions.SSLError as se:
                    Util.writeLog(logFile, str(se))
                    Util.writeLog(logFile, Util.getExceptMsg(se))
                    time.sleep(120)  # 遇到SSLError,直接睡120秒
                    continue
                except requests.exceptions.ProxyError as pe:
                    Util.writeLog(logFile, str(pe))
                    Util.writeLog(logFile, Util.getExceptMsg(pe))
                    time.sleep(120)  # 遇到ProxyError,直接睡120秒
                    continue

                text = json.loads(res.text)
                if j == 1:
                        colInfo = Util.getTableColumns(text["datatable"]["columns"])
                # print(len(text["datatable"]["data"]))

                df = pd.DataFrame(text["datatable"]["data"], columns=colInfo["col_ename"])
                next_cursor = str(text["meta"]["next_cursor_id"])
                #如果還有下一頁,用next_cursor_id繼續抓資料
                while next_cursor != "None":
                    nextUrl = npUrl.format(t, apiKey, next_cursor)
                    Util.writeLog(logFile, "nextPage:{0}".format(nextUrl))
                    try:
                        nexthtml = requests.get(nextUrl, verify=False)
                        nexttext = json.loads(nexthtml.text)
                    except requests.exceptions.SSLError as se:
                        Util.writeLog(logFile, str(se))
                        Util.writeLog(logFile, Util.getExceptMsg(se))
                        time.sleep(120)  # 遇到SSLError,直接睡120秒
                        continue
                    except requests.exceptions.ProxyError as pe:
                        Util.writeLog(logFile, str(pe))
                        Util.writeLog(logFile, Util.getExceptMsg(pe))
                        time.sleep(120)  # 遇到ProxyError,直接睡120秒
                        continue

                    #抓到的資料跟原來的dataframe合併
                    if "datatable" in nexttext.keys():
                        df = df.append(pd.DataFrame(nexttext["datatable"]["data"], columns=colInfo["col_ename"]))
                        next_cursor = str(nexttext["meta"]["next_cursor_id"])
                    else:
                        Util.writeLog(logFile, "datatable doesn't exist : {0}".format(str(nexttext)))
                        #(nexttext)

                    time.sleep(random.randint(5, 10))  # 抓完先sleep

                for k, ctype in enumerate(colInfo["col_type"]):
                    if ctype == "datetime":
                        df[colInfo["col_ename"][k]] = df[colInfo["col_ename"][k]].apply(lambda x : datetime.strptime(str(x), '%Y-%m-%dT%H:%M:%SZ') if x != None else None)

                #print(len(df))
                Util.writeLog(logFile, "筆數:{0}".format(len(df)))
                if t in otTable:
                    df.to_sql(con=engine, name=tName[i].lower(), if_exists='replace', index=False)
                else:
                    df.to_sql(con=engine, name=tName[i].lower(), if_exists='append', index=False)

                startDate = nextDate
                time.sleep(random.randint(5, 10))  # 抓完先sleep
        except Exception as e:
            Util.writeLog(logFile, str(e))
            Util.writeLog(logFile, Util.getExceptMsg(e))
            continue


