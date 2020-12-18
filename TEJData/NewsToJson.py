import requests
import json
import urllib3
import time
from datetime import datetime
import random
from dateutil.relativedelta import relativedelta
import Util

urllib3.disable_warnings()

if __name__ == "__main__":
    logPath = r"./log/New_{0}_Json_{1}.log"
    #type_ = ["digiLarge", "digiTimes"]
    type_ = ["mops"]

    # 下一頁
    npUrl = ""

    for t in type_:
        logFile = logPath.format(t, datetime.now().strftime('%Y%m%d'))

        if t == "digiLarge":
            url = "http://10.198.212.123/news?start_time={0}&end_time={1}"
        elif t == "mops":
            url = "http://10.198.212.123/mops_search?start_time={0}&end_time={1}&page={2}"
        else:
            url = "http://10.198.212.123/api?start_time={0}&end_time={1}"

        startDate = datetime.strptime('2020-01-01', '%Y-%m-%d')
        for i in range(0, 2):
            # nextPageCount for mops
            nextPageCount = 1
            nextDate = startDate + relativedelta(months=1)
            Util.writeLog(logFile, "StartTime:{0} & EndTime:{1}".format(startDate.strftime('%Y-%m-%d'), nextDate.strftime('%Y-%m-%d')))

            sdate = "%d" % time.mktime(startDate.timetuple())
            ndate = "%d" % time.mktime(nextDate.timetuple())
            if t == "mops":
                urlStr = url.format(sdate, ndate, nextPageCount)
            else:
                urlStr = url.format(sdate, ndate)

            Util.writeLog(logFile, urlStr)
            try:
                newsText = requests.get(urlStr)
                assert newsText.status_code == 200, "Web Error:{0}".format(newsText.text)
                newsJson = json.loads(newsText.text)
            except requests.exceptions.SSLError as se:
                Util.writeLog(logFile, Util.getExceptMsg(se))
                time.sleep(120)  # 遇到SSLError,直接睡120秒
                continue
            except requests.exceptions.ProxyError as pe:
                Util.writeLog(logFile, Util.getExceptMsg(pe))
                time.sleep(120)  # 遇到ProxyError,直接睡120秒
                continue
            except Exception as e:
                Util.writeLog(logFile, Util.getExceptMsg(e))
                time.sleep(60)
                continue

            data = newsJson["data"]
            if t == "mops":
                if len(newsJson["data"]) > 0:
                    nextPageCount = nextPageCount + 1
                    npUrl = url.format(sdate, ndate, nextPageCount)
                else:
                    npUrl = ""
            else:
                npUrl = newsJson["next_page"]

            while npUrl != "":
                try:
                    Util.writeLog(logFile, "NextUrl:{0}".format(npUrl))
                    newsNext = requests.get(npUrl)
                    assert newsNext.status_code == 200, "Web Error:{0}".format(newsNext.text)
                    nextJson = json.loads(newsNext.text)
                    if len(nextJson["data"]) == 0:
                        break

                    # print('1,', len(json4["data"]))
                    data.extend(nextJson["data"])
                    if t == "mops":
                        nextPageCount = nextPageCount + 1
                        npUrl = url.format(sdate, ndate, nextPageCount)
                    else:
                        npUrl = newsJson["next_page"]
                except requests.exceptions.SSLError as se:
                    Util.writeLog(logFile, Util.getExceptMsg(se))
                    time.sleep(120)  # 遇到SSLError,直接睡120秒
                    continue
                except requests.exceptions.ProxyError as pe:
                    Util.writeLog(logFile, Util.getExceptMsg(pe))
                    time.sleep(120)  # 遇到ProxyError,直接睡120秒
                    continue
                except Exception as e:
                    Util.writeLog(logFile, Util.getExceptMsg(e))
                    time.sleep(60)
                    continue

            newsJson["data"] = data
            file = r'./json/{0}_{1}_{2}.json'
            filePath = file.format(t, startDate.strftime('%Y-%m-%d'), nextDate.strftime('%Y-%m-%d'))
            if len(data) > 0:
                with open(filePath, 'w', encoding='CP950') as f:
                    json.dump(newsJson, f)

            startDate = nextDate
            time.sleep(random.randint(2, 5))
