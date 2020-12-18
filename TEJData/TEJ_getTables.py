import pandas as pd
import requests
import urllib3
import json
import os
import time, random
import Util

urllib3.disable_warnings()

apiKey = Util.getApiKey()

def getCreateSql(tableName, tcName, colInfo):
    ename = colInfo["col_ename"]
    cname = colInfo["col_cname"]
    col_type = colInfo["col_type"]

    createTab = "create table {0} ( "
    colStr = "{0} {1} null comment \'{2}\'"
    #commentStr = str(colInfo["col_map"])

    for i in range(len(col_type)):
        colStr2 = colStr.format(ename[i], col_type[i], cname[i])
        if i == (len(col_type) - 1):
            createTab += colStr2 + " ) comment=\"{1}\";"
            #createTab += colStr2 + " );"
        else:
            createTab += colStr2 + ", "

    return createTab.format(tableName, tcName)
    #return createTab.format(tableName, tcName, commentStr)

if __name__ == "__main__":
    tableDf = pd.read_csv(r"D:\EdwinChien\Projects\AI_Profolio\TEJ\table_list3.csv")
    #tableDf = pd.read_csv(r"D:\EdwinChien\Projects\AI_Profolio\TEJ\table_list_AFUNDS.csv")
    url = "https://api.tej.com.tw/api/datatables/{0}.json?api_key={1}&coid=null{2}"

    tName = tableDf["tableName"].to_list()
    tCName = tableDf["Description"].to_list()
    sqlList = []
    for i, t in enumerate(tableDf["table"].to_list()):
        print(t)
        #if t == "TWN/AIFIN":
        #    urlStr = url.format(t, "&opts.pivot=true")
        if t == "TWN/AIACC":
            urlStr = "https://api.tej.com.tw/api/datatables/{0}.json?api_key={1}&code=null".format(t, apiKey)
        else:
            urlStr = url.format(t, apiKey, " ")

        res = requests.get(urlStr, verify=False)
        text = json.loads(res.text)
        colInfo = Util.getTableColumns(text["datatable"]["columns"])
        sqlStr = getCreateSql(tName[i], tCName[i], colInfo)
        sqlList.append(sqlStr)

    tableDf["sqlstring"] = pd.DataFrame(sqlList, columns=["sqlstring"])
    if os.path.exists(r"./data/tej_tablesql_AIFINQ.csv"):
        os.remove(r"./data/tej_tablesql_AIFINQ.csv")

    tableDf.to_csv(r"./data/tej_tablesql_AIFINQ.csv", encoding="utf-8", index=False)