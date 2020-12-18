from datetime import datetime
import sys, os
import traceback
import math
import json
from cryptography.fernet import Fernet
from sqlalchemy import create_engine
import pandas as pd
import pymongo

def getComListStr(df_com, x):
    n = math.floor(len(df_com) / x) + 1

    base_ = 0
    for i in range(0, n):
        upperBase = base_ + x
        if i == (n - 1):
            comSer = df_com[base_:]["coid"]
        else:
            comSer = df_com[base_:upperBase]["coid"]

        comList = comSer.to_list()
        base_ = upperBase
        yield ",".join(comList)

def getExceptMsg(e):
    error_class = e.__class__.__name__  # 取得錯誤類型
    detail = ""
    if len(e.args) > 0:
        detail = e.args[0]  # 取得詳細內容

    cl, exc, tb = sys.exc_info()  # 取得Call Stack
    lastCallStack = traceback.extract_tb(tb)[-1]  # 取得Call Stack的最後一筆資料
    fileName = lastCallStack[0]  # 取得發生的檔案名稱
    lineNum = lastCallStack[1]  # 取得發生的行號
    funcName = lastCallStack[2]  # 取得發生的函數名稱
    errMsg = "File \"{}\", line {}, in {}: [{}] {}".format(fileName, lineNum, funcName, error_class, detail)

    return errMsg

def writeLog(logFile, errMsg):
    with open(logFile, "a", encoding="utf-8") as f:
        f.write("------{0}-----\n".format(datetime.now().strftime('%Y-%m-%d %H:%M:%S')))
        f.write("{0}\n".format(errMsg))

def writeJson(tableName, startDStr, endDStr, pageNum, JsonStr):
    filePath = r"./json/{0}_{1}_{2}_{3}.json"
    if startDStr == None:
        nowStr = datetime.now().strftime('%Y-%m-%d')
        fileName = filePath.format(tableName, nowStr, nowStr, pageNum)
    else:
        fileName = filePath.format(tableName, startDStr, endDStr, pageNum)

    with open(fileName, "a", encoding="utf-8") as f:
        f.write(JsonStr)

def writeReList(tableName, table, startDStr, endDStr, urlStr):
    listPath = r"./log/redoList_{0}.csv"
    nowStr = datetime.now().strftime('%Y-%m-%d')
    relist = listPath.format(nowStr)
    conList = [tableName, table, startDStr, endDStr, urlStr]

    if os.path.isfile(relist):
        with open(relist, "a", encoding="utf-8") as f:
            f.write(";".join(conList) + "\n")
    else:
        with open(relist, "a", encoding="utf-8") as f:
            f.write("tableName;table;startDate;endDate;url\n")
            f.write(";".join(conList) + "\n")

def getApiKey():
    apiFileContentJson = json.load(open(r"./static/api_key", 'r', encoding="utf-8"))
    aKey = apiFileContentJson.get("api_key")
    key = apiFileContentJson.get("key")
    fernet = Fernet(key)
    apiKey = fernet.decrypt(bytes(aKey, 'utf-8')).decode("utf-8")

    return apiKey

def getTableColumns(colDes):
    reserve_str = "ADD,ALL,ALTER,ANALYZE,AND,AS,ASC,ASENSITIVE,BEFORE,BETWEEN,BIGINT,BINARY,BLOB,BOTH,BY,CALL,CASCADE,CASE,CHANGE,CHAR,CHARACTER,CHECK,COLLATE,COLUMN,CONDITION,CONNECTION,CONSTRAINT,CONTINUE,CONVERT,CREATE,CROSS,CURRENT_DATE,CURRENT_TIME,CURRENT_TIMESTAMP,CURRENT_USER,CURSOR,DATABASE,DATABASES,DAY_HOUR,DAY_MICROSECOND,DAY_MINUTE,DAY_SECOND,DEC,DECIMAL,DECLARE,DEFAULT,DELAYED,DELETE,DESC,DESCRIBE,DETERMINISTIC,DISTINCT,DISTINCTROW,DIV,DOUBLE,DROP,DUAL,EACH,ELSE,ELSEIF,ENCLOSED,ESCAPED,EXISTS,EXIT,EXPLAIN,FALSE,FETCH,FLOAT,FLOAT4,FLOAT8,FOR,FORCE,FOREIGN,FROM,FULLTEXT,GOTO,GRANT,GROUP,HAVING,HIGH_PRIORITY,HOUR_MICROSECOND,HOUR_MINUTE,HOUR_SECOND,IF,IGNORE,IN,INDEX,INFILE,INNER,INOUT,INSENSITIVE,INSERT,INT,INT1,INT2,INT3,INT4,INT8,INTEGER,INTERVAL,INTO,IS,ITERATE,JOIN,KEY,KEYS,KILL,LABEL,LEADING,LEAVE,LEFT,LIKE,LIMIT,LINEAR,LINES,LOAD,LOCALTIME,LOCALTIMESTAMP,LOCK,LONG,LONGBLOB,LONGTEXT,LOOP,LOW_PRIORITY,MATCH,MEDIUMBLOB,MEDIUMINT,MEDIUMTEXT,MIDDLEINT,MINUTE_MICROSECOND,MINUTE_SECOND,MOD,MODIFIES,NATURAL,NOT,NO_WRITE_TO_BINLOG,NULL,NUMERIC,ON,OPTIMIZE,OPTION,OPTIONALLY,OR,ORDER,OUT,OUTER,OUTFILE,PRECISION,PRIMARY,PROCEDURE,PURGE,RAID0,RANGE,READ,READS,REAL,REFERENCES,REGEXP,RELEASE,RENAME,REPEAT,REPLACE,REQUIRE,RESTRICT,RETURN,REVOKE,RIGHT,RLIKE,SCHEMA,SCHEMAS,SECOND_MICROSECOND,SELECT,SENSITIVE,SEPARATOR,SET,SHOW,SMALLINT,SPATIAL,SPECIFIC,SQL,SQLEXCEPTION,SQLSTATE,SQLWARNING,SQL_BIG_RESULT,SQL_CALC_FOUND_ROWS,SQL_SMALL_RESULT,SSL,STARTING,STRAIGHT_JOIN,TABLE,TERMINATED,THEN,TINYBLOB,TINYINT,TINYTEXT,TO,TRAILING,TRIGGER,TRUE,UNDO,UNION,UNIQUE,UNLOCK,UNSIGNED,UPDATE,USAGE,USE,USING,UTC_DATE,UTC_TIME,UTC_TIMESTAMP,VALUES,VARBINARY,VARCHAR,VARCHARACTER,VARYING,WHEN,WHERE,WHILE,WITH,WRITE,X509,XOR,YEAR_MONTH,ZEROFILL"
    # mysql 保留字
    reserve_word = reserve_str.lower().split(",")

    cname = []
    ename = []
    colType = []
    colInfo = {}

    for col in colDes:
        cname.append(col["cname"].strip())
        if col["name"].strip() in reserve_word:
            ename.append("col_{0}".format(col["name"].strip()))
        else:
            ename.append(col["name"].strip())

        colType.append(col["type"])

    colDict = dict(zip(cname, ename))
    colInfo["col_map"] = colDict
    colInfo["col_cname"] = cname
    colInfo["col_ename"] = ename
    colInfo["col_type"] = colType

    return colInfo

def getMongoDB():
    myclient = pymongo.MongoClient("mongodb://127.0.0.1:27017")

    return myclient

def InsertMongo(myclient, item):
    mydb = myclient["test"]
    mycol = mydb["RawNews"]
    x = mycol.insert_one(item)
    myclient.close()

if __name__ == "__main__":
    engine = create_engine('mysql://edwin:ed0685@localhost/tej_test?charset=utf8')
    df_com = pd.read_sql_query("select coid from TWN_AIND where mkt in ('TSE','OTC','ROTC')", engine)
    print("len:", len(df_com))
    strIte = getComListStr(df_com, 300)
    i = 1
    while True:
        try:
            print(i, next(strIte))
            i += 1
        except Exception as e:
            print(getExceptMsg(e))
            break;