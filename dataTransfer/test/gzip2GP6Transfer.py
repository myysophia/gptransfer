# from log import get_logger
import tarfile
import traceback
from functools import wraps
import time
import paramiko
import psycopg2
from utils import stringUtils

# from logger import logger
from datetime import datetime
from datetime import timedelta
from psycopg2.extras import DictCursor,DictRow,NamedTupleCursor
from log.log import get_logger
# 0、连接数据库
# 1、获取job配置信息
# 2、获取需要迁移的table
# 3、 su - gpadmin
#     psql -h 10.50.10.163 -c "copy (select * from sor.wpp_adefect_f_n where evt_timestamp >= V1 and evt_timestamp < V2) to stdout"|
#       psql -h 10.50.10.170 -d qmstst -c "copy sor.wpp_adefect_f_n from stdin"
# 4、保存履历(耗时以及是否有报错？)
# 0、连接数据库

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
    #     conn = psycopg2.connect(database="qmstst", user="sys", password="sysadmin", host="10.50.10.163", port="5432")
    #     # print("GP  database opend successfully")
    #     cur = conn.cursor()
    #     logger.info("GP-TEST  database connect successfully")
    # # except Exception as e:
    # #     print(e)
    # # finally:
    # #     conn.close()
    try:
        global conn
        conn = psycopg2.connect(database="qmsprd", user="cimuser", password="cimuser", host="10.50.10.169", port="5432")
        get_logger().info("connectGP - GP :%s database connect successfully",conn)
    except psycopg2.DatabaseError as e:
        get_logger().error("could not connect to Greenplum server:%s", e)
    finally:
        if conn in dir():
            conn.close

""" 获取job配置信息 """
# @decoratore
def selectOperate():
    # conn = psycopg2.connect(database="qmstst", user="sys", password="sysadmin", host="10.50.10.163", port="5432")
    cursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
    # cursor = conn.cursor(cursor_factory=psycopg2.extras.NamedTupleCursor)
    # cursor = conn.cursor()
    # cursor.execute("select opera_type,start_timestamp, end_timestamp,duration,status from eda.transferStateTable where table_name='job' and schema_nmae = '"+ sys.argv[1] + "'")
    # sql = "select * from eda.transferStateTable where table_name='job' and schema_nmae =  '" + jobType + "'" + "\""
    # print(sql)
    cursor.execute("""select * from eda.transferStateTable 
                      where table_name='job'
                      and schema_nmae = 'copy_gzip2GP6_job'
                      and status = 'Y'""")
    names = [f[0] for f in cursor.description]
    jobRes = cursor.fetchall()
    get_logger().info("selectOperate - jobInfo:%s",jobRes)
    return jobRes


""" 获取tableList """
# @decoratore
def getTableList():
        jobInfoList = selectOperate()
        cursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        sql = "select * from eda.transferTable where opera_type = '" + jobInfoList[0]['opera_type'] + "'" \
                                                                                                      " and duration = '" + str(jobInfoList[0]['duration']) + "' and valid_flg = 'Y'"
        ret_sql = cursor.mogrify(sql)
        get_logger().info("getTableList - 获取tableList SQL:%s",ret_sql)
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

       values = (table['db_name'], table['schema_nmae'], table['table_name'], start, end,  opeType, 'Y', duration, cnt, errormessage)
       cursor.execute(insert_sql,values)
       conn.commit()
    except  Exception as e:
        get_logger().error("runHistory failed！%s", e)
        get_logger().error(e)


""" 更新job时间 """
def updateJob(start, end):
    try:
        jobInfoList = selectOperate()
        start_new = end
        start_new_date = stringUtils.str2date(str(start_new), "%Y-%m-%d %H:%M:%S")
        interval = jobInfoList[0]['duration']
        end_new = stringUtils.date_delta(str(start_new),interval,"%Y-%m-%d %H:%M:%S")
        cursor = conn.cursor()
        update_sql = """ update eda.transferStateTable set start_timestamp = %s , end_timestamp = %s , db_timestamp = now() where schema_nmae='copy_gzip2GP6_job' """
        values = (start_new,end_new)
        cursor.execute(update_sql, values)
        get_logger().info("updateJob - transferStateTable-copy_gzip2GP6_job 更新完成")
        conn.commit()
        # conn.close()
    except Exception as e:
        get_logger().error("update Job copy_gzip2GP6_job failed！%s", e)
        runHistory(start, end,'', 0, 0, 'copy_gzip2GP6_job', str(e))
        get_logger().error(e)


""" 获取copy 数量时间 """
def getCopyCnt(start, end, ssh ,tableList):
    try:
        get_cnt = 'sh /home/scripts/dataTransfer/getCnt.sh ' \
                  + " '" + str(start) + "' '" \
                  + str(end) + "' '" \
                  + tableList['table_name'] + "' '" \
                  + tableList['schema_nmae'] + "' '" \
                  + tableList['db_name'] + "'"
        # copy_cnt = "cat /home/scripts/dataTransfer/result | awk '{print $2}'"
        get_logger().info("getCopyCnt - shell命令: %s", get_cnt)
        stdin, stdout, stderr = ssh.exec_command(get_cnt)
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
        conn = psycopg2.connect(database="qmstst", user="cimuser", password="cimuser", host="10.50.10.170", port="5432")
        cursor = conn.cursor()
        sql = "create external table sor.ext_" + tableList['table_name'] + "( like "+ tableList['schema_nmae'] + "." + tableList['table_name'] + ")" \
                + "location( 'gpfdist://10.50.10.170:8100/" + tableList['db_name'] + "^" + tableList['schema_nmae'] + "^" + tableList['table_name'] + "^" + startF + "^" + endF + ".csv')" \
                + "format 'text'( delimiter E',' null E'\\\\N' escape E'\\\\' );"
        get_logger().info("createExtTable - 创建外部表 SQL: %s ", sql)
        cursor.execute(sql)
        conn.commit()
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
        conn = psycopg2.connect(database="qmstst", user="cimuser", password="cimuser", host="10.50.10.170", port="5432")
        cursor = conn.cursor()
        sql = "drop external table sor.ext_" + tableList['table_name']  + ";"
        get_logger().info("dropExtTable - drop外部表 SQL: %s ", sql)
        cursor.execute(sql)
        conn.commit()
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
        conn = psycopg2.connect(database="qmstst", user="cimuser", password="cimuser", host="10.50.10.170", port="5432")
        cursor = conn.cursor()
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
        conn = psycopg2.connect(database="qmstst", user="cimuser", password="cimuser", host="10.50.10.170", port="5432")
        cursor = conn.cursor()
        ist_sql = "insert into  "+ tableList['schema_nmae'] + "." + tableList['table_name'] + " select * from sor.ext_" + tableList['table_name']
        get_logger().info("inserTargetTable - 插入目标表 SQL: %s ", ist_sql)
        cursor.execute(ist_sql)
        conn.commit()
        get_logger().info("insert target table successful")
    except Exception as e:
              get_logger().error("insert target table failed！%s",e)
              get_logger().error(e)

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
        conn = psycopg2.connect(database="qmstst", user="cimuser", password="cimuser", host="10.50.10.170", port="5432")
        cursor = conn.cursor()
        ist_sql = "insert into  "+ tableList['schema_nmae'] + "." + tableList['table_name'] + " select * from sor.ext_" + tableList['table_name']
        get_logger().info("inserTargetTable - 插入目标表 SQL: %s ", ist_sql)
        cursor.execute(ist_sql)
        conn.commit()
        get_logger().info("insert target table successful")
    except Exception as e:
              get_logger().error("insert target table failed！%s",e)
              get_logger().error(e)


""" gzip2GP6 """
# @decoratore
def gzip2GP6():
    try:
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())  # 自动接受远程服务器host key

        ssh.connect('10.50.10.170', 22, 'root', 'chot123')  # 远程主机IP、端口、用户名、密码
        get_logger().info("copyGzip - SSHInfo: %s",ssh)
        jobInfoList = selectOperate()

        start = jobInfoList[0]['start_timestamp']
        startF = str(start).replace('-', '').replace(' ', '').replace(':', '')
        get_logger().info(jobInfoList[0]['start_timestamp'])
        end = jobInfoList[0]['end_timestamp']
        endF = str(end).replace('-', '').replace(' ', '').replace(':', '')
        get_logger().info(jobInfoList[0]['end_timestamp'])
        path_t = startF + "^" + endF
        # tablename = 'wpp_adefect_f_n'
        # schemaname = 'sor'
        # dbname = 'qmstst'
        tableList = getTableList()
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
                command1 = '/mnt/dataTransfer/' \
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
                    dropExtTable(tableList[index])
                    stdin, stdout, stderr = ssh.exec_command('rm -f '+command1)  # 远程服务器要执行的命令
                    get_logger().info("csvFile - delete successful！！！")
                    # 抽查而非每次检查
                    # verifyData()
                    runHistory(start, end, tableList[index],  9999, duration, 'copy_gzip2GP6_job', 'Success')
                else:
                    get_logger().info('copy_gzip2GP6_job 失败！！！:' + str(status_copy))
                    runHistory(start, end, tableList[index], 0, 0, 'copy_gzip2GP6_job', str(status_copy))
        updateJob(start, end) # 更新job
        ssh.close()  # 关闭ssh连接
    except Exception as e:
              get_logger().error("copy_gzip2GP6_job failed！%s",e)
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
    # startF = '20201013160000'
    # tmp = startF[0:6]
    # tmp1 = startF[6:8]
    # start1 = str(start)
    # strF = start1.replace('-','').replace(' ','').replace(':','')
    # print(tmp)
    # print(tmp1)
    # print(strF)
    connectGP()
    n = 0;
    while True:
        n+=1
        # createExtTable()
        gzip2GP6()
        if n > 100:
            break
