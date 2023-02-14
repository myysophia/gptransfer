
import argparse
import traceback
from functools import wraps
import time

import os
import paramiko
import parser
import psycopg2
import sys

from utils import stringUtils

# from logger import logger
from datetime import datetime
from datetime import timedelta
from psycopg2.extras import DictCursor,DictRow,NamedTupleCursor
from log.log import get_logger
""" log 装饰器 """
def decoratore(func):
    @wraps(func)
    def log(*args,**kwargs):
        try:
            get_logger().info("当前运行方法开始")
            # return func(*args,**kwargs) # 魔法函数保证返回调用装饰器方法的doc name....
            # get_logger().info("当前运行方法结束")
        except Exception as e:
            get_logger().error(f"{func.__name__} is error,here are details:{traceback.format_exc()}")
    return log


""" 连接数据库 """
# @decoratore
def connectGP():
    # # try:
    #     global conn
    #     conn = psycopg2.connect(database="", user="sys", password="", host="10.50.10.", port="5432")
    #     # print("GP  database opend successfully")
    #     cur = conn.cursor()
    #     logger.info("GP-TEST  database connect successfully")
    # # except Exception as e:
    # #     print(e)
    # # finally:
    # #     conn.close()
    try:
        global conn
        conn = psycopg2.connect(database="", user="", password="", host="", port="5432")
        get_logger().info("connectGP - GP :%s database connect successfully",conn)
    except psycopg2.DatabaseError as e:
        get_logger().error("could not connect to Greenplum server:%s", e)
    finally:
        if conn in dir():
            conn.close

""" 获取job配置信息 """
# @decoratore
def selectOperate(jobName):
    # parser = argparse.ArgumentParser(description='manual to this script')
    # parser.add_argument("--jobName", type=str, default="0")
    # args = parser.parse_args()
    # print(args.jobName)
    # conn = psycopg2.connect(database="", user="sys", password="", host="", port="5432")
    cursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
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


""" 获取tableList """
# @decoratore
def getTableList(jobName):
        jobInfoList = selectOperate(jobName)
        cursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
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
       cursor = conn.cursor()
       # insert_sql = "INSERT INTO eda.transferstatetable (db_name, schema_nmae, table_name, start_timestamp, end_timestamp, opera_type, status, duration, cnt, errormessage, db_timestamp)\
       #               VALUES('qmstst', 'sor', 'wpp_adefect_glass_f', '2020-04-01 00:00:00.000', '2020-04-01 04:00:00.000', 'psql_std', 'Y', 9999, NULL, , NULL);"
       # 执行的添加sql语句
       insert_sql = """ INSERT INTO eda.transferstatetable (db_name, schema_nmae, table_name, start_timestamp, end_timestamp, opera_type, status, duration, cnt, errormessage)\
                     VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s);   """

       values = (table['db_name'], table['schema_nmae'], table['table_name'], start, end,  opeType, 'Y', str(duration), cnt, errormessage)
       cursor.execute(insert_sql,values)
       conn.commit()
    except  Exception as e:
        get_logger().error("runHistory failed！%s", e)
        get_logger().error(e)


""" 更新job时间 """
def updateJob(start, end, final_end, jobName):
    try:
        jobInfoList = selectOperate(jobName)
        start_new = end
        start_new_date = stringUtils.str2date(str(start_new), "%Y-%m-%d %H:%M:%S")
        interval = jobInfoList[0]['duration']
        end_new = stringUtils.date_delta(str(start_new),int(interval),"%Y-%m-%d %H:%M:%S")
        cursor = conn.cursor()
        if start <= final_end:
            update_sql = " update eda.transferStateTable set start_timestamp = %s , end_timestamp = %s , db_timestamp = now(),run_flg = 'N' where schema_nmae= '" + jobName + "' "
            values = (start_new,end_new)
            cursor.execute(update_sql, values)
            get_logger().info("updateJob- transferStateTable-copy_gzip_job 更新完成")
            conn.commit()
            conn.close()
        else:
            update_sql_end = """ update eda.transferStateTable set  db_timestamp = now(),status  = 'N' where schema_nmae= + "'" jobName + "'" """
            cursor.execute(update_sql_end)
            get_logger().info("updateJob- transferStateTable-copy_gzip_job 数据Load完成")
            conn.commit()
            conn.close()
    except Exception as e:
        UnLockjob(jobName)
        get_logger().error("update Job copy_gzip failed！%s", e)
        runHistory(start, end,'', 0, 0, jobName, str(e))
        get_logger().error(e)


""" Lockjob """
def lockJob(jobName):
    try:
        cursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        update_sql = " update eda.transferStateTable set db_timestamp = now(),run_flg = 'Y' where schema_nmae = '" + jobName + "' "
        cursor.execute(update_sql)
        conn.commit()
    except Exception as e:
        get_logger().error("update Job copy_gzip failed！%s", e)
        get_logger().error(e)

""" UnLockjob """
def UnLockjob(jobName):
    try:
        cursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        update_sql = " update eda.transferStateTable set db_timestamp = now(),run_flg = 'N' where schema_nmae = '" + jobName + "' "
        cursor.execute(update_sql)
        conn.commit()
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
        # copy_cnt = "cat /home/scripts/dataTransfer/result | awk '{print $2}'"
        get_logger().info("getCopyCnt - shell命令: %s", get_cnt)
        stdin, stdout, stderr = ssh.exec_command(get_cnt)
        channel = stdout.channel
        cnt = stdout.read().decode("utf-8", "ignore").strip('\n')
        return cnt
    except Exception as e:
        get_logger().error("getCopyCnt - failed！%s", e)
        get_logger().error(e)

""" copy_gzip落盘 """
# @decoratore
def copyGzip(jobName):
    """ DEMO: sh /home/scripts/dataTransfer/dataTrs.sh  '2020-10-13 14:00:00' '2020-10-13 16:00:00' 'wpp_adefect_f_n' 'sor' 'qmstst' '20201013160000^20201013160000' """
    try:
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())  # 自动接受远程服务器host key

        ssh.connect('', '', '')  # 远程主机IP、端口、用户名、密码
        get_logger().info("copyGzip - SSHInfo: %s",ssh)
        jobInfoList = selectOperate(jobName)
        if jobInfoList[0]["run_flg"] == 'N':
            lockJob(jobName);
            start = jobInfoList[0]['start_timestamp']
            startF = str(start).replace('-', '').replace(' ', '').replace(':', '')
            get_logger().info(jobInfoList[0]['start_timestamp'])
            # end = jobInfoList[0]['end_timestamp']
            interval = jobInfoList[0]['duration']
            end = stringUtils.date_delta(str(start), int(interval), "%Y-%m-%d %H:%M:%S")
            final_end = jobInfoList[0]['final_end_timestamp']
            endF = str(end).replace('-', '').replace(' ', '').replace(':', '')
            get_logger().info(jobInfoList[0]['end_timestamp'])
            path_t = startF + "^" + endF
            tableList = getTableList(jobName)
            for index in range(len(tableList)):
                    command = 'sh /home/scripts/dataTransfer/dataTrs_0204.sh ' \
                       + " '" + str(start) + "' '" \
                              + str(end)+ "' '" \
                              + tableList[index]['table_name']  + "' '" \
                              + tableList[index]['schema_nmae'] + "' '" \
                              + tableList[index]['db_name'] + "' '" \
                              + path_t + "' '" \
                              + tableList[index]['condition_query_column'] + "'"   # 2021年2月4日18:40:41 增加condition_query_column 以适应不同表的不同的迁移栏位
                    get_logger().info("copyGzip - shell命令: %s",command)
                    shell_start = time.time()
                    stdin, stdout, stderr = ssh.exec_command(command)  # 远程服务器要执行的命令
                    channel = stdout.channel
                    status_copy = channel.recv_exit_status()# 获取返回值 shell 执行状态码
                    get_logger().info("copyGzip - dataTrs.sh执行结果: %s",status_copy)
                    result = tableList[index]['db_name'] + "." + tableList[index]['schema_nmae'] + "." + tableList[index]['table_name']
                    # 2020年10月24日16:45:21 脚本中如果将psql 的结果重定向到文件， 脚本执行失败。  如何获取数量考虑其他方法
                    # 2020年10月26日17:02:30 脚本中使用 tee 重定向结果到两个文件，发现shell 返回的状态码不正确
                    # copy_cnt = "cat /home/scripts/dataTransfer/" + result
                    # copy_cnt = "cat /home/scripts/dataTransfer/result | awk '{print $2}'"
                    # get_logger().info("copyGzip - shell命令: %s", copy_cnt)
                    # stdin, stdout, stderr = ssh.exec_command(copy_cnt)
                    # channel = stdout.channel
                    # status_result = channel.recv_exit_status()  # 获取返回值 shell 执行状态码
                    # get_logger().info("copyGzip - dataTrs.sh执行结果: %s", status_result)
                    # # get_logger().info( stdout.read())
                    # cnt = stdout.read().decode("utf-8","ignore").strip('\n')

                    # get_logger().info("copyGzip - %s,%s,%s copy_gzip数量为: %s",tableList[index]['table_name'],tableList[index]['schema_nmae'],tableList[index]['db_name'],cnt)              # 获取copy成功返回的数量
                    # 自定义shell执行返回值 99 -> 成功 100 -> 失败
                    if status_copy == 99:
                        # 获取copy cnt
                        cnt =  getCopyCnt(start, end, ssh,tableList[index])
                        shell_end = time.time()
                        duration = round((shell_end - shell_start), 2)
                        get_logger().info("copyGzip - %s,%s,%s copy_gzip数量: %s , 耗时: %s s", tableList[index]['table_name'],
                                          tableList[index]['schema_nmae'], \
                                          tableList[index]['db_name'], cnt, duration)
                        # 记录履历 : tablename、 时间区间、 操作类型、 总耗时、数量(暂时不记录)、
                        # start, end, tablename, schemaname, dbname, cnt, duration, opeType, errormessage
                        runHistory(start, end, tableList[index],  cnt, duration, jobName, 'Success')
                        # 文件系统层面检查文件是否落盘  -d file
                        # TODO
                    else:
                        get_logger().info('shell执行失败了!!!状态码为:' + str(status_copy))
                        runHistory(start, end, tableList[index], 0, 0, jobName, str(status_copy))
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
              get_logger().error("copy_gzip failed！%s",e)
              get_logger().error(e)

if __name__ == '__main__':
    # start = '2020-10-13 14:00:00'
    # end = '2020-10-13 16:00:00'
    # str2date = stringUtils.str2date(start, "%Y-%m-%d %H:%M:%S")
    # get_logger().error(str2date)
    # get_logger().info(end)
    # start_new = end
    # # interval = jobInfoList[0]['duration']
    # end_new = stringUtils.date_delta(start,10,"%Y-%m-%d %H:%M:%S")
    # end_new = str2date + timedelta(hours=10);
    # print(end_new)
    # tablename = 'wpp_adefect_f_n'
    # schemaname = 'sor'
    # dbname = 'qmstst'
    # path_t = '20201013160000^20201013160000'
    # start1 = str(start)
    # strF = start1.replace('-','').replace(' ','').replace(':','')
    # print(start)
    # print(start1)
    # print(strF)
    # selectOperate()
    # getTableList()
    # copyGzip(start, end, tablename, schemaname, dbname, path_t)

    # while 1:
        main_start = time.time()
        connectGP()
        strT = 'copy_gzip_job_24H_2'
        # strT = sys.argv[1]
        copyGzip(strT)
        main_end = time.time()
        duration = round((main_end - main_start), 2)
        get_logger().info("copyGzip-JobName:  %s, 总共花费%s s", strT,duration)
