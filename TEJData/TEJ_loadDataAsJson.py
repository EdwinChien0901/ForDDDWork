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
    logPath = r"./log/TEJ_LoadToJson_{0}.log"
    tableDf = pd.read_csv(r"D:\EdwinChien\Projects\AI_Profolio\TEJ\table_list3.csv", encoding="utf-8")
    otTable = ["TWN/ANPRCSTD", "TWN/AIACC", "TWN/AIND"]
    IFRSTable = ["TWN/AIFIN", "TWN/AIFINQ"]
    #tableDf = tableDf[1:4]
    #tableDf = ['TWN/ATINST1','TWN/APRCD','TWN/APRCD1','TWN/APRCD2','TWN/APRCDA','TWN/APRCM','TWN/APRCW','TWN/APRCY','TWN/AAPRCDA','TWN/AAPRCM1','TWN/AAPRCW1','TWN/AAPRCY1','TWN/AFINSTD','TWN/AIACC','TWN/AIFIN']
    engine = create_engine('mysql://edwin:ed0685@localhost/tej_test?charset=utf8')

    url = "https://api.tej.com.tw/api/datatables/{0}.json?api_key={1}&mdate.gte={2}&mdate.lt={3}"
    #下一頁
    npUrl = "https://api.tej.com.tw/api/datatables/{0}.json?api_key={1}&opts.cursor_id={2}"

    tName = tableDf["tableName"].to_list()
    #tName = ['TWN_ATINST1','TWN_APRCD','TWN_APRCD1','TWN_APRCD2','TWN_APRCDA','TWN_APRCM','TWN_APRCW','TWN_APRCY','TWN_AAPRCDA','TWN_AAPRCM1','TWN_AAPRCW1','TWN_AAPRCY1','TWN_AFINSTD','TWN_AIACC','TWN_AIFIN']


    sqlList = []
    for i, t in enumerate(tableDf["table"].to_list()):
        #print(t)
        startDate = datetime.strptime('2017-12-01', '%Y-%m-%d')
        ysdate = "{0}-01-01".format(startDate.year)
        yedate = "{0}-01-01".format(startDate.year + 1)

        logFile = logPath.format(datetime.now().strftime('%Y%m%d'))
        Util.writeLog(logFile, "table name:{0}".format(t))

        #if t == "TWN/AIFIN":
        if t in IFRSTable:
            qStr = "select * from TWN_AIND where list_day2 between '{0}' and '{1}' union select * from TWN_AIND where mkt in ('TSE','OTC','ROTC') "
            df_com = pd.read_sql_query(qStr.format(ysdate, yedate), engine)

        try:
            for j in range(1, 2):
                nextDate = startDate + relativedelta(months=1)
                sdate = startDate.strftime('%Y-%m-%d')
                ndate = nextDate.strftime('%Y-%m-%d')
                if t in otTable:
                    urlStr = "https://api.tej.com.tw/api/datatables/{0}.json?api_key={1}".format(t, apiKey)
                #elif t == "TWN/AIFIN":
                elif t in IFRSTable:
                    strIte = Util.getComListStr(df_com, 200)
                    urlStr = url.format(t, apiKey, sdate, ndate) + "&coid={0}&opts.pivot=true".format(next(strIte))
                else:
                    urlStr = url.format(t, apiKey, sdate, ndate)

                Util.writeLog(logFile, urlStr)
                colInfo = None
                try:
                    res = requests.get(urlStr, verify=False)
                    assert res.status_code == 200, "Web Error:{0}".format(res.text)
                    res.encoding = "utf-8"
                except requests.exceptions.SSLError as se:
                    Util.writeLog(logFile, str(se))
                    Util.writeLog(logFile, Util.getExceptMsg(se))
                    Util.writeReList(tName[i], t, sdate, ndate, urlStr)
                    time.sleep(120)  # 遇到SSLError,直接睡120秒
                    continue
                except requests.exceptions.ProxyError as pe:
                    Util.writeLog(logFile, str(pe))
                    Util.writeLog(logFile, Util.getExceptMsg(pe))
                    Util.writeReList(tName[i], t, sdate, ndate, urlStr)
                    time.sleep(120)  # 遇到ProxyError,直接睡120秒
                    continue
                except Exception as e:
                    Util.writeLog(logFile, Util.getExceptMsg(e))
                    Util.writeReList(tName[i], t, sdate, ndate, urlStr)
                    time.sleep(60)
                    continue

                text = json.loads(res.text)
                if j == 1 or colInfo == None:
                    colInfo = Util.getTableColumns(text["datatable"]["columns"])

                iPage = 1 #頁碼
                if "datatable" in text.keys():
                    df = pd.DataFrame(text["datatable"]["data"], columns=colInfo["col_ename"])
                    if len(df) > 0:
                        if t in otTable:
                            Util.writeJson(tName[i].lower(), None, None, iPage, res.text)
                        else:
                            Util.writeJson(tName[i].lower(), sdate, ndate, iPage, res.text)
                else:
                    Util.writeLog(logFile, "datatable doesn't exist : {0}".format(str(text)))

                #if t == "TWN/AIFIN":
                if t in IFRSTable:
                    try:
                        next_cursor = next(strIte)
                    except StopIteration as si:
                        next_cursor = "None"
                else:
                    next_cursor = str(text["meta"]["next_cursor_id"])

                #如果還有下一頁,用next_cursor_id繼續抓資料
                while next_cursor != "None":

                    #if t == "TWN/AIFIN":
                    if t in IFRSTable:
                        nextUrl = url.format(t, apiKey, sdate, ndate) + "&coid={0}&opts.pivot=true".format(next_cursor)
                    else:
                        nextUrl = npUrl.format(t, apiKey, next_cursor)

                    Util.writeLog(logFile, "nextPage:{0}".format(nextUrl))
                    try:
                        nexthtml = requests.get(nextUrl, verify=False)
                        assert nexthtml.status_code == 200, "Web Error:{0}".format(nexthtml.text)
                        nexthtml.encoding = "utf-8"

                        nexttext = json.loads(nexthtml.text)
                    except requests.exceptions.SSLError as se:
                        #writeLog(logFile, str(se))
                        Util.writeLog(logFile, Util.getExceptMsg(se))
                        Util.writeReList(tName[i], t, sdate, ndate, urlStr)
                        time.sleep(120)  # 遇到SSLError,直接睡120秒
                        break
                    except requests.exceptions.ProxyError as pe:
                        #writeLog(logFile, str(pe))
                        Util.writeLog(logFile, Util.getExceptMsg(pe))
                        Util.writeReList(tName[i], t, sdate, ndate, urlStr)
                        time.sleep(120)  # 遇到ProxyError,直接睡120秒
                        break
                    except Exception as e:
                        Util.writeLog(logFile, Util.getExceptMsg(e))
                        Util.writeReList(tName[i], t, sdate, ndate, urlStr)
                        time.sleep(60)
                        break

                    #抓到的資料跟原來的dataframe合併
                    if "datatable" in nexttext.keys():
                        df2 = pd.DataFrame(nexttext["datatable"]["data"], columns=colInfo["col_ename"])
                        df = df.append(df2)

                        #if t == "TWN/AIFIN":
                        if t in IFRSTable:
                            try:
                                next_cursor = next(strIte)
                            except StopIteration as si:
                                next_cursor = "None"
                        else:
                            next_cursor = str(nexttext["meta"]["next_cursor_id"])

                        if len(df2) > 0:
                            iPage += 1
                            if t in otTable:
                                Util.writeJson(tName[i].lower(), None, None, iPage, nexthtml.text)
                            else:
                                Util.writeJson(tName[i].lower(), sdate, ndate, iPage, nexthtml.text)
                    else:
                        Util.writeLog(logFile, "datatable doesn't exist : {0}".format(str(nexttext)))

                    time.sleep(random.randint(5, 10))

                Util.writeLog(logFile, "筆數:{0}".format(len(df)))

                startDate = nextDate
                time.sleep(random.randint(15, 30))
        except Exception as e:
            Util.writeLog(logFile, Util.getExceptMsg(e))
            time.sleep(random.randint(10, 20))
            continue


