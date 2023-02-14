# 获取要比对的时间区间以及配置

import argparse
import traceback
from functools import wraps
import time

import os
import paramiko
import parser
import psycopg2
import sys
import hashlib
from utils import stringUtils

# from logger import logger
from datetime import datetime
from datetime import timedelta
from psycopg2.extras import DictCursor,DictRow,NamedTupleCursor
from log.log import get_logger


""" 连接数据库 """
# @decoratore
def connectGP4():
    # # try:
    #     global conn
    #     conn = psycopg2.connect(database="qmstst", user="sys", password="sysadmin", host="10.50.10.163", port="5432")
    #     # print("GP  database opend successfully")
    #     cur = conn.cursor()
    #     logger.info("GP-TEST  database connect successfully")
    # # except Exception as e:
    # #     print(e)
    # # finally:
    # #     conn.close()
    try:
        global conn4
        conn4 = psycopg2.connect(database="qmsprd", user="cimuser", password="cimuser", host="10.50.10.169", port="5432")
        get_logger().info("connectGP - GP :%s database connect successfully",conn4)
    except psycopg2.DatabaseError as e:
        get_logger().error("could not connect to Greenplum server:%s", e)
    finally:
        if conn4 in dir():
            conn4.close

""" 连接数据库 """
# @decoratore
def connectGP6():
    # # try:
    #     global conn
    #     conn = psycopg2.connect(database="qmstst", user="sys", password="sysadmin", host="10.50.10.163", port="5432")
    #     # print("GP  database opend successfully")
    #     cur = conn.cursor()
    #     logger.info("GP-TEST  database connect successfully")
    # # except Exception as e:
    # #     print(e)
    # # finally:
    # #     conn.close()
    try:
        global conn6
        conn6 = psycopg2.connect(database="qmsprd", user="cimuser", password="cimuser", host="10.50.10.110", port="5432")
        get_logger().info("connectGP - GP :%s database connect successfully",conn6)
    except psycopg2.DatabaseError as e:
        get_logger().error("could not connect to Greenplum server:%s", e)
    finally:
        if conn6 in dir():
            conn6.close

""" 获取job配置信息 """
# @decoratore
def selectOperate(jobName):
    # parser = argparse.ArgumentParser(description='manual to this script')
    # parser.add_argument("--jobName", type=str, default="0")
    # args = parser.parse_args()
    # print(args.jobName)
    # conn = psycopg2.connect(database="qmstst", user="sys", password="sysadmin", host="10.50.10.163", port="5432")
    cursor = conn6.cursor(cursor_factory=psycopg2.extras.DictCursor)
    # cursor = conn.cursor(cursor_factory=psycopg2.extras.NamedTupleCursor)
    # cursor = conn.cursor()
    # cursor.execute("select opera_type,start_timestamp, end_timestamp,duration,status from eda.transferStateTable where table_name='job' and status = 'Y' and schema_nmae = "+ sys.argv[1] )
    # sql = "select * from eda.transferStateTable where table_name='job' and schema_nmae =  '" + jobType + "'" + "\""
    print(jobName)
    sql = "select * from eda.transferStateTable where table_name='job' and schema_nmae ='" + jobName + "'and status = 'Y'"
    cursor.execute(sql)
    names = [f[0] for f in cursor.description]
    jobRes = cursor.fetchall()
    get_logger().info("selectOperate - jobInfo:%s",jobRes)
    # for row in jobRes:
    # #     # print('opera_type=', row[0], ',start_timestamp=', row[1], ',end_timestamp=', row[2], ',duration=', row[3],
    # #     #       ',status=', row[4])
    # #     # jobInfoList = [row[0],row[1],row[2],row[3],row[4]]
    #     for pair in zip(names, row):
    #         print('{}: {}'.format(*pair))
    # print(jobRes)
    return jobRes


""" 获取需要比对的tableList """
# @decoratore
def getTableList(jobName):
        jobInfoList = selectOperate(jobName)
        cursor = conn4.cursor(cursor_factory=psycopg2.extras.DictCursor)
    # for ret in jobInfoList:
    #     get_logger().info("getTableList - oper_type:%s",ret[5])
    #         # print('opera_type=', row[0], ',start_timestamp=', row[1], ',end_timestamp=', row[2], ',duration=', row[3],
    #         #       ',status=', row[4])
    #         # jobInfoList = [row[0],row[1],row[2],row[3],row[4]]
    #
    #     # print('opera_type=', row[0], ',start_timestamp=', row[1], ',end_timestamp=', row[2], ',duration=', row[3],
    #     #   ',status=', row[4])
    # cursor.execute("select opera_type,start_timestamp, end_timestamp,duration,status from eda.transferStateTable where table_name='job' and schema_nmae = '"+ sys.argv[1] + "'")
        sql = "select * from eda.transferTable where opera_type = '" + jobInfoList[0]['opera_type'] + "' and duration = '" + str(jobInfoList[0]['duration']) + "' and valid_flg = 'Y'"
        ret_sql = cursor.mogrify(sql)
        get_logger().info("getTableList - 获取tableList SQL:%s",sql)
        cursor.execute(sql)
        names = [f[0] for f in cursor.description]
        tablesRes=cursor.fetchall() # 以包含序列的序列（如元组列表）的方式获取所有结果行。
        # for row in tablesRes:
        #     get_logger().info('-----------------------')
        #     for pair in zip(names, row):
        #         get_logger().info('{}: {}'.format(*pair))
        #     get_logger().info('-----------------------')
        # conn.close()
        return tablesRes

""" 记录迁移履历 """
def runHistory(start, end,table ,cnt, duration, opeType, errormessage):
    try:
       cursor = conn4.cursor()
       # insert_sql = "INSERT INTO eda.transferstatetable (db_name, schema_nmae, table_name, start_timestamp, end_timestamp, opera_type, status, duration, cnt, errormessage, db_timestamp)\
       #               VALUES('qmstst', 'sor', 'wpp_adefect_glass_f', '2020-04-01 00:00:00.000', '2020-04-01 04:00:00.000', 'psql_std', 'Y', 9999, NULL, , NULL);"
       # 执行的添加sql语句
       insert_sql = """ INSERT INTO eda.transferstatetable (db_name, schema_nmae, table_name, start_timestamp, end_timestamp, opera_type, status, duration, cnt, errormessage)\
                     VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s);   """

       values = (table['db_name'], table['schema_nmae'], table['table_name'], start, end,  opeType, 'Y', str(duration), cnt, errormessage)
       cursor.execute(insert_sql,values)
       conn4.commit()
    except  Exception as e:
        # get_logger().error("runHistory failed！%s", e)
        get_logger().error(e)


""" 更新job时间 """
def updateJob(start, end, final_end, jobName):
    try:
        now_tm = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
        now = stringUtils.date_delta_before(str(now_tm),int(5),"%Y-%m-%d %H:%M:%S")
        jobInfoList = selectOperate(jobName)
        start_new = end
        start_new_date = stringUtils.str2date(str(start_new), "%Y-%m-%d %H:%M:%S")
        interval = jobInfoList[0]['duration']
        end_new = stringUtils.date_delta(str(start_new),int(interval),"%Y-%m-%d %H:%M:%S")
        cursor = conn4.cursor()
        if start <= final_end and start < now:
            update_sql = " update eda.transferStateTable set start_timestamp = %s , end_timestamp = %s , db_timestamp = now(),run_flg = 'N' where schema_nmae= '" + jobName + "' "
            values = (start_new,end_new)
            cursor.execute(update_sql, values)
            get_logger().info("updateJob- copy_gzip2GP6_job_dataCompare 更新完成")
            conn4.commit()
            conn4.close()
        else:
            update_sql = " update eda.transferStateTable set start_timestamp = %s , end_timestamp = %s , db_timestamp = now(),run_flg = 'N' where schema_nmae= '" + jobName + "' "
            cursor.execute(update_sql)
            update_sql_end = " update eda.transferStateTable set  db_timestamp = now(),status  = 'N' where schema_nmae= '" + jobName + "'"
            cursor.execute(update_sql_end)
            get_logger().info("updateJob- copy_gzip2GP6_job_dataCompare 数据比对完成")
            conn4.commit()
            conn4.close()
    except Exception as e:
        UnLockjob(jobName)
        get_logger().error("update Job copy_gzip failed！%s", e)
        # runHistory(start, end,'', 0, 0, jobName, str(e))
        get_logger().error(e)


""" Lockjob """
def lockJob(jobName):
    try:
        cursor = conn4.cursor(cursor_factory=psycopg2.extras.DictCursor)
        update_sql = " update eda.transferStateTable set db_timestamp = now(),run_flg = 'Y' where schema_nmae = '" + jobName + "' "
        cursor.execute(update_sql)
        conn4.commit()
    except Exception as e:
        get_logger().error("update Job copy_gzip failed！%s", e)
        get_logger().error(e)

""" UnLockjob """
def UnLockjob(jobName):
    try:
        cursor = conn4.cursor(cursor_factory=psycopg2.extras.DictCursor)
        update_sql = " update eda.transferStateTable set db_timestamp = now(),run_flg = 'N' where schema_nmae = '" + jobName + "' "
        cursor.execute(update_sql)
        conn4.commit()
    except Exception as e:
        get_logger().error("UnLockjob Job  failed！%s", e)
        get_logger().error(e)

""" 获取copy 数量时间 """
def getCopyCnt(start, end, ssh ,tableList):
    try:
        get_cnt = 'sh /home/scripts/dataTransfer/getCnt_0204.sh ' \
                  + " '" + str(start) + "' '" \
                  + str(end) + "' '" \
                  + tableList['table_name'] + "' '" \
                  + tableList['schema_nmae'] + "' '" \
                  + tableList['db_name'] + "' '"\
                  + tableList['condition_query_column'] + "'"
        get_logger().info("getCopyCnt - shell命令: %s", get_cnt)
        stdin, stdout, stderr = ssh.exec_command(get_cnt)
        channel = stdout.channel
        cnt = stdout.read().decode("utf-8", "ignore").strip('\n')
        return cnt
    except Exception as e:
        get_logger().error("getCopyCnt - failed！%s", e)
        get_logger().error(e)


"""compareOpeDiff & return ope_idlist """
# filename $DB_NAME^$SCHEMA_NAME^$TABLE_NAME_COMPGP6.csv
def compareOpeDiff(fileName4,fileName6 ,ssh):
    try:
        opelistTmp = []
        opelist = []
        get_diff = 'sh /home/scripts/dataCompare/compareOpeDiff.sh ' \
                  + " '" + str(fileName4) + "' '" \
                  + str(fileName6) + "'"
        get_logger().info("get diff - shell命令: %s", get_diff)
        stdin, stdout, stderr = ssh.exec_command(get_diff)
        channel = stdout.channel
        opesStr = stdout.read().decode("utf-8", "ignore").strip('\n')
        opelistTmp = opesStr.split('\n')
        for filename in opelistTmp:
            # print(filename)
            list = filename.split(',')
            opelist.append(list)
        # print(opelist)

        return opelist
    except Exception as e:
        get_logger().error("get diff - failed！%s", e)
        get_logger().error(e)


"""compareEvtDiff & return ope_idlist """
# filename $DB_NAME^$SCHEMA_NAME^$TABLE_NAME_COMPGP6.csv
def compareEvtDiff(fileName4,fileName6 ,ssh):
    try:
        ope_evtListTmp = []
        ope_evtList = []
        get_diff = 'sh /home/scripts/dataCompare/compareEvtDiff.sh ' \
                  + " '" + str(fileName4) + "' '" \
                  + str(fileName6) + "'"
        get_logger().info("get diff - shell命令: %s", get_diff)
        stdin, stdout, stderr = ssh.exec_command(get_diff)
        channel = stdout.channel
        opeEvtStr = stdout.read().decode("utf-8", "ignore").strip('\n')
        ope_evtListTmp = opeEvtStr.split('\n')
        for index in ope_evtListTmp:
            list = index.split(',')
            ope_evtList.append(list)
        return ope_evtList
    except Exception as e:
        get_logger().error("get diff - failed！%s", e)
        get_logger().error(e)



"""compareDiff & save diff to db """
# 外部表技术  脚本编辑文件技术 sed awk
def save4Diff(opeEvt,ope_evtList,tableList,ssh,start,end):
    try:
        fileNmae = tableList['db_name'] + "^" + tableList[
            'schema_nmae'] + "^" + tableList['table_name'] + "^COMPRawGP4.csv"
        startStr = ope_evtList[opeEvt][0].split(',')
        startNew = startStr[0]
        opeStr = ope_evtList[opeEvt][1].split(',')
        ope = opeStr[0]
        # end = opeEvt.split(',')
        get_resule = 'sh /home/scripts/dataCompare/rawData_GP4.sh ' \
                     + " '" + str(startNew) + "' '" \
                     + tableList['table_name'] + "' '" \
                     + tableList['schema_nmae'] + "' '" \
                     + tableList['db_name'] + "' '" \
                     + tableList['condition_query_column'] + "' '"\
                     + ope + "' '"\
                     + str(start) + "' '"\
                     + str(end) + "'"
        get_logger().info("get  raw data - shell命令: %s", get_resule)
        stdin, stdout, stderr = ssh.exec_command(get_resule)
        # 休眠2s  保证copy query执行成功
        time.sleep(2)
    except Exception as e:
        get_logger().error("get diff - failed！%s", e)
        get_logger().error(e)


"""compareDiff & save diff to db """
# 外部表技术  脚本编辑文件技术 sed awk
def save6Diff(opeEvt,ope_evtList,tableList,ssh,start,end):
    try:
        fileNmae = tableList['db_name'] + "^" + tableList[
            'schema_nmae'] + "^" + tableList['table_name'] + "^COMPRawGP6.csv"
        startStr = ope_evtList[opeEvt][0].split(',')
        startNew = startStr[0]
        opeStr = ope_evtList[opeEvt][1].split(',')
        ope = opeStr[0]
        # end = opeEvt.split(',')
        get_resule = 'sh /home/scripts/dataCompare/rawData_GP6.sh ' \
                     + " '" + str(startNew) + "' '" \
                     + tableList['table_name'] + "' '" \
                     + tableList['schema_nmae'] + "' '" \
                     + tableList['db_name'] + "' '" \
                     + tableList['condition_query_column'] + "' '"\
                     + ope + "' '"\
                     + str(start) + "' '"\
                     + str(end) + "'"
        get_logger().info("get raw data - shell命令: %s", get_resule)
        stdin, stdout, stderr = ssh.exec_command(get_resule)
        # 休眠2s  保证copy query执行成功
        time.sleep(2)
    except Exception as e:
        get_logger().error("get diff - failed！%s", e)
        get_logger().error(e)



"""getFinalResult from source db """
# 外部表技术  脚本编辑文件技术 sed awk
def getFinalResult(fileNmae4, fileNmae6, ssh):
    try:
        # 非动态外部表的情况下应该把该目录下的文件都删除
        # deleteFileName = 'rm -rf /home/scripts/dataCompare/rawDiff/*'
        # get_logger().info("getFinalResult - shell命令: %s", deleteFileName)
        # stdin, stdout, stderr = ssh.exec_command(deleteFileName)
        # get_logger().info(" deleteFileName - successful！fileName")

        get_result4 = 'sh /home/scripts/dataCompare/getResult.sh ' \
                  +"'" + fileNmae4 + "' '" \
                  + fileNmae6 + "'"
        get_logger().info("getFinalResult - shell命令: %s", get_result4)
        stdin, stdout, stderr = ssh.exec_command(get_result4)
        # http response code 404 from gpfdis  HTTP/1.0 404 file not found？ 文件没copy过来？
        # stdin, stdout, stderr = ssh.exec_command("ll /home/scripts/dataCompare/rawDiff/" )
        fileList = stdout.read().decode("utf-8", "ignore").strip('\n')
        get_logger().info("文件获取成功 successful!!! %s",fileList)
        insertTarget4(fileNmae4, ssh)
        insertTarget6(fileNmae6, ssh)
    except Exception as e:
        get_logger().error(" getFinalResult - failed！%s", e)
        get_logger().error(e)


"""insertTarget by ext """
# 外部表  脚本编辑文件 sed awk
def insertTarget4(fileName, ssh):
    try:
        createExtTable(fileName)
        cursor = conn6.cursor()
        insert_sql = """ INSERT INTO eda.transfer4_diff (select * from eda.ext_transfer4_diff);   """
        get_logger().info("insertTarget4 - sql: %s", insert_sql)
        cursor.execute(insert_sql)
        conn6.commit()
        deleteFileName = 'rm -rf /home/scripts/dataCompare/rawDiff/'+ str(fileName)
        get_logger().info("getFinalResult - shell命令: %s", deleteFileName)
        stdin, stdout, stderr = ssh.exec_command(deleteFileName)
        get_logger().info(" deleteFileName - successful！fileName :%s", fileName)

    except Exception as e:
        conn6.rollback()
        get_logger().error(" insertTarget4 - failed！%s", e)
        get_logger().error(e)


"""insertTarget by ext """
# 外部表技术  脚本编辑文件技术 sed awk
def insertTarget6(fileName, ssh):
    try:
        createExtTable(fileName)
        cursor = conn6.cursor()
        insert_sql = """ INSERT INTO eda.transfer6_diff (select * from eda.ext_transfer6_diff);   """
        get_logger().info("insertTarget6 - sql: %s", insert_sql)
        cursor.execute(insert_sql)
        conn6.commit()
        deleteFileName = 'rm -rf /home/scripts/dataCompare/rawDiff/'+ str(fileName)
        get_logger().info("getFinalResult - shell命令: %s", deleteFileName)
        stdin, stdout, stderr = ssh.exec_command(deleteFileName)
        get_logger().info(" deleteFileName - successful！fileName :%s", fileName)

    except Exception as e:
        conn6.rollback()
        get_logger().error(" insertTarget6 - failed！%s", e)
        get_logger().error(e)



""" createExtTable """
# @decoratore
# create external table sor.ext_wpp_adefect_f_n_new( like sor.wpp_adefect_f_n ) \ location( 'gpfdist://10.50.10.170:8100/qmsprd^sor^wpp_adefect_f_n^*.csv' ) format 'text'( delimiter E',' null E'\\N' escape E'\\' );
# qmsprd^sor^wpp_adefect_f_n^20200502000000^20200502040000
def createExtTable(fileName):
    try:
        cursor = conn6.cursor()
        dropExtTable(fileName)
        if "COMPRawGP6" in fileName:
            sql = "create external table eda.ext_transfer6_diff ( like eda.transfer6_diff )" \
                + "location( 'gpfdist://10.50.10.170:8101/" + fileName + "')" \
                + "format 'text'( delimiter E',' null E'\\\\N' escape E'\\\\' );"
        else:
             sql = "create external table eda.ext_transfer4_diff ( like eda.transfer4_diff )" \
              + "location( 'gpfdist://10.50.10.170:8101/" + fileName + "')" \
              + "format 'text'( delimiter E',' null E'\\\\N' escape E'\\\\' );"
        get_logger().info("createExtTable - 创建外部表 SQL: %s ", sql)
        cursor.execute(sql)
        conn6.commit()
        get_logger().info("create external table successful！外部文件名: %s",fileName)
    except Exception as e:
              get_logger().error("create external table failed！%s",e)
              get_logger().error(e)


def dropExtTable(fileName):
    try:
        cursor = conn6.cursor()
        if "COMPRawGP6" in fileName:
            sql = "drop external table  if exists  eda.ext_transfer6_diff;"
        else:
            sql = "drop external table  if exists  eda.ext_transfer4_diff;"
        get_logger().info("dropExtTable - drop外部表 SQL: %s ", sql)
        cursor.execute(sql)
        conn6.commit()
        get_logger().info("dropExtTable  ext_transfer6_diff successful！%s",fileName)
    except Exception as e:
              get_logger().error("dropExtTable  ext_transfer6_diff failed！%s",e)
              get_logger().error(e)


"""异常时报警  """
def alarm(tablename, start_time , opeId, diff):
    try:

        cursor = conn6.cursor()
        insert_sql = """ INSERT INTO dm.system_alarm (data_seq_id,data_name,evt_timestamp,defect_file_url,ope_id,index_url)\
                             VALUES (%s, %s, %s, %s, %s, %s);   """
        get_logger().info("时间为: %s", start_time)
        tmp = get_md5_value(start_time)
        get_logger().info("md5: %s", tmp)

        values = ( tmp, 'GPTRANSFER', start_time, tablename, opeId, diff)
        cursor.execute(insert_sql, values)
        conn6.commit()

    except Exception as e:
        get_logger().error("alarm 发送 - failed！%s", e)
        get_logger().error(e)


def get_md5_value(src):
    myMd5 = hashlib.md5()
    myMd5.update(src.encode("utf8"))
    myMd5_Digest = myMd5.hexdigest()
    return myMd5_Digest

""" compareJob  """
# @decoratore
def compareJob(jobName):
    try:
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())  # 自动接受远程服务器host key

        ssh.connect('10.50.10.170', 22, 'root', 'chot123')  # 远程主机IP、端口、用户名、密码
        get_logger().info("compareJob - SSHInfo: %s",ssh)
        jobInfoList = selectOperate(jobName)
        if jobInfoList[0]["run_flg"] == 'N':
            # lockJob(jobName);
            start = jobInfoList[0]['start_timestamp']
            startF = str(start).replace('-', '').replace(' ', '').replace(':', '')
            get_logger().info(jobInfoList[0]['start_timestamp'])
            # end = jobInfoList[0]['end_timestamp']
            interval = jobInfoList[0]['duration']
            end = stringUtils.date_delta(str(start), int(interval), "%Y-%m-%d %H:%M:%S")
            final_end = jobInfoList[0]['final_end_timestamp']
            endF = str(end).replace('-', '').replace(' ', '').replace(':', '')
            # get_logger().info(jobInfoList[0]['end_timestamp'])
            path_t = startF + "^" + endF
            tableList = getTableList(jobName) # 获取表
            for index in range(len(tableList)):
                # 获取GP4 的group by站点数据
                    commandGP4 = 'sh /home/scripts/dataCompare/dataGroupByOpe_GP4.sh ' \
                              + " '" + str(start) + "' '" \
                              + str(end)+ "' '" \
                              + tableList[index]['table_name'] + "' '" \
                                 + tableList[index]['schema_nmae'] + "' '" \
                              + tableList[index]['db_name'] + "' '" \
                              + tableList[index]['condition_query_column'] + "'"
                    get_logger().info("compareJobGP4 - shell命令: %s",commandGP4)
                    stdin, stdout, stderr = ssh.exec_command(commandGP4)  # 远程服务器要执行的命令
                    channel = stdout.channel
                    status_copy = channel.recv_exit_status()# 获取返回值 shell 执行状态码
                    get_logger().info("compareJob - dataTrs.sh执行结果: %s",status_copy)
                    fileNmae4 = tableList[index]['db_name'] + "^" + tableList[index]['schema_nmae'] + "^" + tableList[index]['table_name'] + "^COMPGP4.csv"
                    if status_copy == 99:
                        # 获取GP6 的group by站点数据
                        commandGP6 = 'sh /home/scripts/dataCompare/dataGroupByOpe_GP6.sh ' \
                                  + " '" + str(start) + "' '" \
                                  + str(end) + "' '" \
                                  + tableList[index]['table_name'] + "' '" \
                                  + tableList[index]['schema_nmae'] + "' '" \
                                  + tableList[index]['db_name'] + "' '" \
                                  + tableList[index]['condition_query_column'] + "'"
                        get_logger().info("compareJobGP6 - shell命令: %s", commandGP6)
                        stdin, stdout, stderr = ssh.exec_command(commandGP6)  # 远程服务器要执行的命令
                        channel = stdout.channel
                        status_copy = channel.recv_exit_status()  # 获取返回值 shell 执行状态码
                        get_logger().info("compareJob - dataTrs.sh执行结果: %s", status_copy)
                        fileNmae6 = tableList[index]['db_name'] + "^" + tableList[index]['schema_nmae'] + "^" + tableList[index]['table_name'] + "^COMPGP6.csv"
                        # 反向比较时参数传入顺序需要互调 todo 应该先count 再 决定差集 2021年4月26日14:09:44
                        # 2021年4月29日15:58:38 周会决议: 只关注GP6比GP4的issue
                        opelist = compareOpeDiff(fileNmae4,fileNmae6,ssh) # GP4 比GP6多
                        # opelist = compareOpeDiff(fileNmae6, fileNmae4, ssh)
                        if  len(opelist[0][0]) > 0 :
                            # 比对的数量的时候如果差异超过10个站点不需要比对.
                            if len(opelist[0][0]) < 50:
                             for opeId in range(len(opelist)):
                                get_logger().info("ope 第 %s 次,ope_id : %s", opeId ,str(opelist[opeId][0]))
                                # v1.1 wx 有数据差异时警
                                alarm(str(tableList[index]['table_name']), str(start), str(opelist[opeId][0]), str(opelist[opeId][1]))
                                # 获取GP4 的group by evt 的数据
                                commandGP4Evt = 'sh /home/scripts/dataCompare/dataGroupByEvt_GP4.sh ' \
                                             + " '" + str(start) + "' '" \
                                             + str(end) + "' '" \
                                             + tableList[index]['table_name'] + "' '" \
                                             + tableList[index]['schema_nmae'] + "' '" \
                                             + tableList[index]['db_name'] + "' '" \
                                             + tableList[index]['condition_query_column'] + "' '" \
                                             + opelist[opeId][0] + "'"
                                get_logger().info("compareJobGP4 - shell命令: %s", commandGP4Evt)
                                stdin, stdout, stderr = ssh.exec_command(commandGP4Evt)  # 远程服务器要执行的命令
                                channel = stdout.channel
                                status_copy = channel.recv_exit_status()  # 获取返回值 shell 执行状态码
                                get_logger().info("compareJob - dataGroupByEvt_GP4.sh执行结果: %s", status_copy)
                                fileNmae4 = tableList[index]['db_name'] + "^" + tableList[index]['schema_nmae'] + "^" + \
                                            tableList[index]['table_name'] + "^COMPEvtGP4.csv"
                                fileNmae6 = tableList[index]['db_name'] + "^" + tableList[index][
                                    'schema_nmae'] + "^" + tableList[index]['table_name'] + "^COMPEvtGP6.csv"
                                # rawData
                                fileNmae4Raw = tableList[index]['db_name'] + "^" + tableList[index]['schema_nmae'] + "^" + \
                                            tableList[index]['table_name'] + "^COMPRawGP4.csv"
                                fileNmae6Raw = tableList[index]['db_name'] + "^" + tableList[index][
                                    'schema_nmae'] + "^" + tableList[index]['table_name'] + "^COMPRawGP6.csv"
                                if status_copy == 99:
                                    # 获取GP6 的group by evt 的数据
                                    commandGP6Evt = 'sh /home/scripts/dataCompare/dataGroupByEvt_GP6.sh ' \
                                                 + " '" + str(start) + "' '" \
                                                 + str(end) + "' '" \
                                                 + tableList[index]['table_name'] + "' '" \
                                                 + tableList[index]['schema_nmae'] + "' '" \
                                                 + tableList[index]['db_name'] + "' '" \
                                                 + tableList[index]['condition_query_column'] + "' '" \
                                                 + opelist[opeId][0] + "'"
                                    get_logger().info("compareJobGP6 - shell命令: %s", commandGP6Evt)
                                    stdin, stdout, stderr = ssh.exec_command(commandGP6Evt)  # 远程服务器要执行的命令
                                    channel = stdout.channel
                                    status_copy = channel.recv_exit_status()  # 获取返回值 shell 执行状态码
                                    get_logger().info("compareJob - dataTrs.sh执行结果: %s", status_copy)
                                    # 反向比较时参数传入顺序需要互调 todo 应该先count 再 决定差集 2021年4月26日14:09:44
                                    ope_evtList = compareEvtDiff(fileNmae4, fileNmae6, ssh)
                                    # ope_evtList = compareEvtDiff(fileNmae6, fileNmae4, ssh)
                                    # 保存有差异的数据到数据库 。需要包含是哪个时间段的数据、哪个table 以及主键信息
                                    for opeEvt in range(len(ope_evtList)):
                                        save4Diff(opeEvt,ope_evtList,tableList[index],ssh,start,end)
                                        save6Diff(opeEvt, ope_evtList, tableList[index], ssh, start, end)
                                        get_logger().info("opeEvt 第 %s 次,时间: %s",opeEvt,ope_evtList[opeEvt][0].split(','))
                                        getFinalResult(fileNmae4Raw, fileNmae6Raw, ssh)
                    else:
                        get_logger().error('shell执行失败了!!!状态码为:' + str(status_copy))
                        # runHistory(start, end, tableList[index], 0, 0, jobName, str(status_copy))
            updateJob(start, end, final_end, jobName) # 更新job
            ssh.close()  # 关闭ssh连接
        else:
            try:
                get_logger().info("Job:%s  Run_flg为Y.退出...",jobName )
                UnLockjob(jobName)
                os._exit(0)
            except Exception as e:
                get_logger().info("Job:%s Run_flg为Y.退出失败", jobName)
    except Exception as e:
              UnLockjob(jobName)
              get_logger().error("compare failed！%s",e)
              get_logger().error(e)

if __name__ == '__main__':
        # while 1:
            main_start = time.time()
            connectGP4()
            connectGP6()
            # strT = 'copy_gzip2GP6_job_dataCompare'
            strT = sys.argv[1]
            compareJob(strT)
            main_end = time.time()
            duration = round((main_end - main_start), 2)
            get_logger().info("copy_gzip2GP6_job_dataCompare-JobName:  %s, 总共花费%s s", strT,duration)
