# coding=utf-8
import ConfigParser
import cx_Oracle
import VehPassRecField
import json
import sys
import time
import os


os.environ['NLS_LANG'] = 'SIMPLIFIED CHINESE_CHINA.UTF8'

reload(sys)
sys.setdefaultencoding('utf-8')

cf = ConfigParser.ConfigParser()
cf.read("config.conf")


class JSONObject:
    def __init__(self, d):
        self.__dict__ = d


def get_db_connect():
    conn = cx_Oracle.connect(
            cf.get("db", "db_username"),
            cf.get("db", "db_password"),
            cf.get("db", "db_url")
    )
    return conn


def __process_line(line_str):
    jo = None
    begin = line_str.find("{")
    end = line_str.find("}")
    if begin > 0 and end > 0:
        json_str = line_str[begin:end + 1]
        jo = json.loads(json_str)
    return jo


def log(msg):
    with open("console.log", "a") as loggerfile:
        loggerfile.write(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()) + "   " + msg + "\n")


def build_insert_sql(row):
    tc = {}
    tablename = cf.get("base", "tablename")
    sql = "insert into " + tablename
    columnlist = []
    valueslist = []
    vprf = VehPassRecField.VehPassRecField()
    if cf.get("base", "clientId") == "GD":
            tc = vprf.get_tablecolumn_for_gd()
    else:
        tc = vprf.get_tablecolumn_for_hn()
    for tablecolumn_name in tc.keys():
        tablecolumn_value = tc.get(tablecolumn_name)
        tv = tablecolumn_value.lower()
        if row.has_key(tv):
            columnlist.append(tablecolumn_name)
            insert_value = row.get(tv)
            if tv == "srcinputtime" \
                    or tv == "inputtime" \
                    or tv == "receivedtime"\
                    or tv == "passtime":
                x = time.localtime(insert_value/1000)
                y = time.strftime("%Y-%m-%d %H:%M:%S", x)
                valueslist.append("to_date('" + y + "','yyyy-mm-dd hh24:mi:ss')")
            else:
                valueslist.append("\'" + str(insert_value) + "\'")
    columnstr = ",".join(columnlist)
    valuesstr = ",".join(valueslist)
    sql = sql + "(" + columnstr + ") values (" + valuesstr + ")"
    return sql


def do_insert(vehlist):
    conn = get_db_connect()
    cursor = conn.cursor()
    try:
        for row in vehlist:
            insert_sql = build_insert_sql(row)
            log("insert_sql:" + insert_sql)
            try:
                cursor.execute(insert_sql)
                log("插入成功,成功数据为[%s]" %str(row))
            except cx_Oracle.Error, e:
                log("插入失败,失败数据为[%s]" %str(row))
                log("失败原因[%s]" % e)
        conn.commit()
    except cx_Oracle.Error, ex:
        log(ex)
    conn.close()


try:
    logfilepath = cf.get("base", "logfile_path")
    with open(logfilepath, "r") as logfile:
        log("已启动,读取文件中..")
        print("已启动,读取文件中..")
        vehlist = []
        for line in logfile:
            veh = __process_line(line)
            if veh is not None:
                vehlist.append(veh)
        if len(vehlist) > 0:
            do_insert(vehlist)
            print("入库已完成,详细内容已输出到console.log")
except IOError, e:
    log(e)

