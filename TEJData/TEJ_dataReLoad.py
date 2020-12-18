from datetime import datetime
import pandas as pd
import requests
import urllib3
import json
from sqlalchemy import create_engine
import time, random
import Util

urllib3.disable_warnings()
logPath = r"./log/TEJ_ReLOAD_{0}.log"
logFile = logPath.format(datetime.now().strftime('%Y%m%d'))
apiKey = Util.getApiKey()

if __name__ == "__main__":
    reloadFile = r"C:\Users\Edwin.Jian\PycharmProjects\TEJData\log\redoList_2020-09-11.csv"
    listDF = pd.read_csv(reloadFile, delimiter=";")
    listDF = listDF.drop_duplicates()

    tName = listDF["tableName"].to_list()
    tList = listDF["table"].to_list()
    startDStr = listDF["startDate"].to_list()
    endDStr = listDF["endDate"].to_list()
    urlList = listDF["url"].to_list()
    npUrl = "https://api.tej.com.tw/api/datatables/{0}.json?api_key={1}&opts.cursor_id={2}"
    otTable = ["TWN/ANPRCSTD", "TWN/AIACC", "TWN/AIND"]

    for t, url in enumerate(urlList):
        try:
            Util.writeLog(logFile, url)
            if t == "TWN/AIFIN":
                engine = create_engine('mysql://edwin:ed0685@localhost/tej_test?charset=utf8')
                df_com = pd.read_sql_query("select coid from TWN_AIND where mkt in ('TSE','OTC','ROTC')", engine)
                strIte = Util.getComListStr(df_com, 300)

            try:
                html = requests.get(url, verify=False)
                assert html.status_code == 200, "Web Error:{0}".format(html.text)
                html.encoding = "utf-8"
                jtext = json.loads(html.text)
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
            except Exception as e:
                Util.writeLog(logFile, str(e))
                Util.writeLog(logFile, Util.getExceptMsg(e))
                time.sleep(120)
                continue

            colInfo = Util.getTableColumns(jtext["datatable"]["columns"])
            # df = pd.DataFrame(jtext["datatable"]["data"], columns=colInfo["col_ename"])
            next_cursor = str(jtext["meta"]["next_cursor_id"])

            iPage = 1  # 頁碼
            if "datatable" in jtext.keys():
                df = pd.DataFrame(jtext["datatable"]["data"], columns=colInfo["col_ename"])
                if len(df) > 0:
                    if t in otTable:
                        Util.writeJson(tName[t].lower(), None, None, iPage, html.text)
                    else:
                        Util.writeJson(tName[t].lower(), startDStr[t], endDStr[t], iPage, html.text)
            else:
                Util.writeLog(logFile, "datatable doesn't exist : {0}".format(str(jtext)))

            # 如果還有下一頁,用next_cursor_id繼續抓資料
            while next_cursor != "None":
                nextUrl = npUrl.format(tList[t], apiKey, next_cursor)
                Util.writeLog(logFile, nextUrl)
                try:
                    nexthtml = requests.get(nextUrl, verify=False)
                    assert nexthtml.status_code == 200, "Web Error:{0}".format(nexthtml.text)
                    nexthtml.encoding = "utf-8"
                    nexttext = json.loads(nexthtml.text)
                except requests.exceptions.SSLError as se:
                    Util.writeLog(logFile, str(se))
                    Util.writeLog(logFile, Util.getExceptMsg(se))
                    time.sleep(120)  # 遇到SSLError,直接睡120秒
                    break
                except requests.exceptions.ProxyError as pe:
                    Util.writeLog(logFile, str(pe))
                    Util.writeLog(logFile, Util.getExceptMsg(pe))
                    time.sleep(120)  # 遇到ProxyError,直接睡120秒
                    break
                except Exception as e:
                    Util.writeLog(logFile, str(e))
                    Util.writeLog(logFile, Util.getExceptMsg(e))
                    time.sleep(120)
                    break

                # 抓到的資料跟原來的dataframe合併
                if "datatable" in nexttext.keys():
                    df2 = pd.DataFrame(nexttext["datatable"]["data"], columns=colInfo["col_ename"])
                    df = df.append(df2)

                    if t == "TWN/AIFIN":
                        try:
                            next_cursor = next(strIte)
                        except StopIteration as si:
                            next_cursor = "None"
                    else:
                        next_cursor = str(nexttext["meta"]["next_cursor_id"])

                    if len(df2) > 0:
                        iPage += 1
                        if t in otTable:
                            Util.writeJson(tName[t].lower(), None, None, iPage, nexthtml.text)
                        else:
                            Util.writeJson(tName[t].lower(), startDStr[t], endDStr[t], iPage, nexthtml.text)
                else:
                    Util.writeLog(logFile, "datatable doesn't exist : {0}".format(str(nexttext)))

                time.sleep(random.randint(5, 10))

            Util.writeLog(logFile, "筆數:{0}".format(len(df)))

            time.sleep(random.randint(5, 10))
        except Exception as e:
            Util.writeLog(logFile, Util.getExceptMsg(e))
            time.sleep(random.randint(10, 20))
            continue
