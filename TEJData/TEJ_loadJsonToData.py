import pandas as pd
from datetime import datetime
import json
from sqlalchemy import create_engine
import glob
import Util
import os

if __name__ == "__main__":
    logPath = r"./log/TEJ_LOAD_JSON_{0}.log"
    logFile = logPath.format(datetime.now().strftime('%Y%m%d'))
    engine = create_engine('mysql://edwin:ed0685@localhost/tej_test?charset=utf8')
    filePath = r"C:\Users\Edwin.Jian\PycharmProjects\TEJData\json\TEJ_ASHR1\*.json"

    fileList = glob.glob(filePath)

    try:
        for f in fileList:
            Util.writeLog(logFile, f)

            basename = os.path.basename(f)
            filename = basename.split(".")[0].split("_")
            tableName = '_'.join(filename[0:2])
            Util.writeLog(logFile, "table name: {0}".format(tableName))

            jsonData = json.load(open(f, encoding="utf-8"))
            colInfo = Util.getTableColumns(jsonData["datatable"]["columns"])
            df = pd.DataFrame(jsonData["datatable"]["data"], columns=colInfo["col_ename"])
            for k, ctype in enumerate(colInfo["col_type"]):
                if ctype == "datetime":
                    df[colInfo["col_ename"][k]] = df[colInfo["col_ename"][k]].apply(lambda x: datetime.strptime(str(x), '%Y-%m-%dT%H:%M:%SZ') if x != None else None)


            df.to_sql(con=engine, name=tableName, if_exists='append', index=False)

    except Exception as e:
        Util.writeLog(logFile, Util.getExceptMsg(e))