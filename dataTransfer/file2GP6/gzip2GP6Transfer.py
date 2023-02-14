# from log import get_logger
import tarfile
import traceback
from functools import wraps
import time
import paramiko
import psycopg2
import sys

from utils import stringUtils
import os
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


""" 连接数据库 配置表"""
# @decoratore
def connectGP():
    try:
        global conn
        conn = psycopg2.connect(database="", user="", password="", host="", port="5432")
        get_logger().info("connectGP - GP :%s database connect successfully",conn)
    except psycopg2.DatabaseError as e:
        get_logger().error("could not connect to Greenplum server:%s", e)
    finally:
        if conn in dir():
            conn.close


""" 连接数据库 目标表GP6 connection"""
# @decoratore
def connectDataGP():
    try:
        global connData
        connData = psycopg2.connect(database="", user="", password="", host="", port="5432")
        get_logger().info("connectGP - 目标表GP6 :%s database connect successfully",connData)
    except psycopg2.DatabaseError as e:
        get_logger().error("could not connect to Greenplum server:%s", e)
    finally:
        if connData in dir():
            connData.close


""" 连接数据库 配置表 """
# @decoratore
def connectGPTEST():
    try:
        global connConf
        connConf = psycopg2.connect(database="", user="", password="", host="", port="5432")
        get_logger().info("connectGP - GP :%s database connect successfully",connConf)
    except psycopg2.DatabaseError as e:
        get_logger().error("could not connect to Greenplum server:%s", e)
    finally:
        if connConf in dir():
            connConf.close



""" 获取job配置信息 """
# @decoratore
def selectOperate(jobName):
    cursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
    print(jobName)
    sql = "select * from eda.transferStateTable where table_name='job' and schema_nmae ='" + jobName + "'and status = 'Y'"
    cursor.execute(sql)
    names = [f[0] for f in cursor.description]
    jobRes = cursor.fetchall()
    get_logger().info("selectOperate - jobInfo:%s",jobRes)
    return jobRes


""" 获取tableList """
# @decoratore
def getTableList(jobName):
        jobInfoList = selectOperate(jobName)
        cursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        sql = "select * from eda.transferTable where opera_type = '" + jobInfoList[0]['opera_type'] + "' and duration = '" + str(jobInfoList[0]['duration']) + "' and job_group = '" + str(jobInfoList[0]['schema_nmae'])+ "' and valid_flg = 'Y'"
        ret_sql = cursor.mogrify(sql)
        get_logger().info("getTableList - 获取tableList SQL:%s",sql)
        cursor.execute(sql)
        names = [f[0] for f in cursor.description]
        tablesRes=cursor.fetchall() # 以包含序列的序列（如元组列表）的方式获取所有结果行。
        return tablesRes


""" 记录迁移履历 """
def runHistory(start, end,table ,cnt, duration, opeType, errormessage):
    try:
       cursor = conn.cursor()
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
        if start < final_end:
            update_sql = " update eda.transferStateTable set start_timestamp = %s , end_timestamp = %s , db_timestamp = now(),run_flg = 'N' where schema_nmae= '" + jobName + "' "
            values = (start_new,end_new)
            cursor.execute(update_sql, values)
            get_logger().info("updateJob- transferStateTable-copy_gzip_job 更新完成")
            conn.commit()
            conn.close()
        else:
            update_sql_end = " update eda.transferStateTable set  db_timestamp = now(),run_flg = 'N' where schema_nmae= '" + jobName + "' "
            cursor.execute(update_sql_end)
            get_logger().info("updateJob- transferStateTable-copy_gzip_job 数据Load完成")
            conn.commit()
            conn.close()
    except Exception as e:
        UnLockjob(jobName)
        get_logger().error("update Job copy_gzip failed！%s", e)
        runHistory(start, end,'', 0, 0, jobName, str(e))
        get_logger().error(e)


""" 从外部表获取数量
2021年2月8日10:30:28 add 
共四个参数 
    tablename=$1
    schemaname=$2
    dbname=$3
    extPrefix=$4

 """
def getCntByExt(ssh ,tableList):
    extPrefix = "ext_"
    try:
        getCntByExt = 'sh /home/scripts/dataTransfer/file2GP6/getCntByExt_0208.sh ' + " '"\
                  + tableList['table_name'] + "' '" \
                  + tableList['schema_nmae'] + "' '" \
                  + tableList['db_name'] + "' '"\
                  + extPrefix + "'"
        # copy_cnt = "cat /home/scripts/dataTransfer/result | awk '{print $2}'"
        get_logger().info("getCntByExt - shell命令: %s", getCntByExt)
        stdin, stdout, stderr = ssh.exec_command(getCntByExt)
        channel = stdout.channel
        cnt = stdout.read().decode("utf-8", "ignore").strip('\n')
        return cnt
    except Exception as e:
        get_logger().error("getCopyCnt - failed！%s", e)
        get_logger().error(e)


""" createExtTable """
# @decoratore
# create external table sor.ext_wpp_adefect_f_n_new( like sor.wpp_adefect_f_n ) \ location( 'gpfdist://10.50.10.170:8100/qmsprd^sor^wpp_adefect_f_n^*.csv' ) format 'text'( delimiter E',' null E'\\N' escape E'\\' );
# qmsprd^sor^wpp_adefect_f_n^20200502000000^20200502040000
def createExtTable(startF, endF, tableList):
    try:
        cursor = connData.cursor()
        dropExtTable(tableList)
        sql = "create external table " + tableList['schema_nmae'] + ".ext_py_" + tableList['table_name'] + "( like "+ tableList['schema_nmae'] + "." + tableList['table_name'] + ")" \
                + "location( 'gpfdist://10.50.10.170:8101/" + tableList['db_name'] + "^" + tableList['schema_nmae'] + "^" + tableList['table_name'] + "^" + startF + "^" + endF + ".csv')" \
                + "format 'text'( delimiter E',' null E'\\\\N' escape E'\\\\' );"
        get_logger().info("createExtTable - 创建外部表 SQL: %s ", sql)
        cursor.execute(sql)
        connData.commit()
        get_logger().info("create external table successful！%s")
    except Exception as e:
              get_logger().error("create external table failed！%s",e)
              get_logger().error(e)


""" dropExtTable """
# @decoratore
# create external table sor.ext_wpp_adefect_f_n_new( like sor.wpp_adefect_f_n ) \ location( 'gpfdist://10.50.10.170:8100/qmsprd^sor^wpp_adefect_f_n^*.csv' ) format 'text'( delimiter E',' null E'\\N' escape E'\\' );
# qmsprd^sor^wpp_adefect_f_n^20200502000000^20200502040000

def dropExtTable(tableList):
    try:
        cursor = connData.cursor()
        sql = "drop external table  if exists " + tableList['schema_nmae'] + ".ext_py_" + tableList['table_name']  + ";"
        get_logger().info("dropExtTable - drop外部表 SQL: %s ", sql)
        cursor.execute(sql)
        connData.commit()
        get_logger().info("dropExtTable successful！%s")
    except Exception as e:
              get_logger().error("dropExtTable failed！%s",e)
              get_logger().error(e)


""" del csv """
# @decoratore
# create external table sor.ext_wpp_adefect_f_n_new( like sor.wpp_adefect_f_n ) \ location( 'gpfdist://10.50.10.170:8100/qmsprd^sor^wpp_adefect_f_n^*.csv' ) format 'text'( delimiter E',' null E'\\N' escape E'\\' );
# qmsprd^sor^wpp_adefect_f_n^20200502000000^20200502040000

def delCsvFile(tableList):
    try:
        cursor = connData.cursor()
        sql = "drop external table sor.ext_" + tableList['table_name']  + ";"
        get_logger().info("dropExtTable - drop外部表 SQL: %s ", sql)
        cursor.execute(sql)
        conn.commit()
        get_logger().info("delCsvFile successful！%s")
    except Exception as e:
              get_logger().error("delCsvFile failed！%s",e)
              get_logger().error(e)



""" inserTargetTable """
# @decoratore
def inserTargetTable(tableList):
    try:
        cursor = connData.cursor()
        ist_sql = "insert into  "+ tableList['schema_nmae'] + "." + tableList['table_name'] + " select * from " + tableList['schema_nmae'] + ".ext_py_"  + tableList['table_name']
        get_logger().info("inserTargetTable - 插入目标表 SQL: %s ", ist_sql)
        cursor.execute(ist_sql)
        connData.commit()
        get_logger().info("insert target table successful")
    except psycopg2.IntegrityError as e:
        conn.rollback()
        get_logger().error("insert target table failed！%s", e)
        get_logger().error(e)
    else:
        conn.commit()



""" make_targz Data """
def make_targz(output_filename, source_dir):
    """
    一次性打包目录为tar.gz
    :param output_filename: 压缩文件名
    :param source_dir: 需要打包的目录
    :return: bool
    """
    try:
        with tarfile.open(output_filename, "w:gz") as tar:
            tar.add(source_dir, arcname=os.path.basename(source_dir))

        return True
    except Exception as e:
        print(e)
        return False


""" untar Data """
def untar(fname, dirs):
    """
    解压tar.gz文件
    :param fname: 压缩文件名
    :param dirs: 解压后的存放路径
    :return: bool
    """
    try:
        t = tarfile.open(fname)
        t.extractall(path = dirs)
        return True
    except Exception as e:
        print(e)
        return False


""" verify Data """
# @decoratore
def verifyData(tableList):
    try:
        conn = psycopg2.connect(database="", user="", password="", host="", port="5432")
        cursor = conn.cursor()
        ist_sql = "insert into  "+ tableList['schema_nmae'] + "." + tableList['table_name'] + " select * from sor.ext_" + tableList['table_name']
        get_logger().info("inserTargetTable - 插入目标表 SQL: %s ", ist_sql)
        cursor.execute(ist_sql)
        conn.commit()
        get_logger().info("insert target table successful")
    except Exception as e:
              get_logger().error("insert target table failed！%s",e)
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


""" gzip2GP6 """
# @decoratore
def gzip2GP6(jobName):
    try:
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())  # 自动接受远程服务器host key

        ssh.connect('10.50.10.', 22, '', '')  # 导入GP 6 远程主机IP、端口、用户名、密码
        # ssh.connect('10.50.10.', 22, '', '')  # 导入GP 4 远程主机IP、端口、用户名、密码
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
                    command_tmp = 'gzip -dc /mnt/dataTransfer/' \
                              +  str(startF)[0:6] + "/" \
                              + str(startF)[6:8] + "/" \
                              + tableList[index]['table_name']  + "/" \
                              + tableList[index]['db_name'] + "^" \
                              + tableList[index]['schema_nmae'] + "^" \
                              + tableList[index]['table_name'] + "^" \
                              + str(startF) + "^" \
                              + str(endF) + ".csv.gz"\
                              + '> '
                    command1 = '/mnt/dataTransfer/file2GP/' \
                              + tableList[index]['db_name'] + "^" \
                              + tableList[index]['schema_nmae'] + "^" \
                              + tableList[index]['table_name'] + "^" \
                              + str(startF) + "^" \
                              + str(endF) + ".csv"
                    command = command_tmp + command1
                    get_logger().info("gzip -dc - 解压命令: %s",command)
                    shell_start = time.time()
                    stdin, stdout, stderr = ssh.exec_command(command)  # 远程服务器要执行的命令
                    channel = stdout.channel
                    status_copy = channel.recv_exit_status()# 获取返回值 shell 执行状态码
                    get_logger().info("gzip解压 - 执行结果: %s",status_copy)
                    if status_copy == 0:
                        shell_end = time.time()
                        duration = round((shell_end - shell_start), 2)
                        get_logger().info("gzip解压耗时: %s s", duration)
                        # 创建外部表 文件名 qmsprd^eda^wpp_fdefect_f_n^20200501000000^20200501040000
                        createExtTable(startF, endF, tableList[index])
                        cretab_end = time.time()
                        duration = round((cretab_end - shell_end), 2)
                        get_logger().info("创建外部表耗时: %s s", duration)
                        #入库并记录履历
                        # 记录履历 : tablename、 时间区间、 操作类型、 总耗时、数量
                        # start, end, tablename, schemaname, dbname, cnt, duration, opeType, errormessage
                        ist_start = time.time()
                        inserTargetTable(tableList[index]);
                        ist_end = time.time()
                        ist_duration = round((ist_end - ist_start), 2)
                        get_logger().info("inserTargetTable耗时: %s s", ist_duration)
                        extCnt = getCntByExt(ssh,tableList[index])
                        dropExtTable(tableList[index])
                        get_logger().info("csvFile - delete successful！！！")

                        # 抽查而非每次检查
                        # verifyData()
                        runHistory(start, end, tableList[index],  extCnt, duration, jobName, 'Success')
                    else:
                        get_logger().info('copy_gzip2GP6_job 失败！！！:' + str(status_copy))
                        runHistory(start, end, tableList[index], 0, 0, jobName, str(status_copy))
            updateJob(start, end, final_end, jobName)  # 更新job
            ssh.close()  # 关闭ssh连接
        else:
            try:
                get_logger().info("Job:%s  Run_flg为Y.退出...", jobName)
                UnLockjob(jobName)
                os._exit(0)
            except Exception as e:
                get_logger().info("Job:%s Run_flg为Y.退出失败", jobName)
    except Exception as e:
              get_logger().error("copy_gzip2GP6_job failed！%s",e)
              get_logger().error(e)



if __name__ == '__main__':
    # while 1:
        main_start = time.time()
        connectGP()
        connectDataGP()
        # sys.path.append(os.pardir)
        # connectGPTEST()
        strT = 'copy_gzip2GP6_job_OC'
        # strT = sys.argv[1]
        gzip2GP6(strT)
        main_end = time.time()
        duration = round((main_end - main_start), 2)
        get_logger().info("copyGzip-JobName:  %s, 总共花费%s s", strT, duration)

