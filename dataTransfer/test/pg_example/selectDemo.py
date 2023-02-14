import os
import sys
import psycopg2
import datetime
import time
import db.GP
from psycopg2.extras import DictCursor,DictRow,NamedTupleCursor
def connectGreenplum():
    try:
        db = psycopg2.connect(database="qmstst", user="sys", password="sysadmin", host="10.50.10.163", port="5432")
        # connect()也可以使用一个大的字符串参数,
        # 比如”host=localhost port=5432 user=postgres password=postgres dbname=test”
        global conn
        conn = db
    except psycopg2.DatabaseError as e:
        print("could not connect to Greenplum server", e)

#     global conn
#     conn = psycopg2.connect(database="qmstst", user="sys", password="sysadmin", host="10.50.10.163", port="5432")
#     print
#     'connect successful!'
#     # cursor = conn.cursor()
# #     cursor.execute('''create table public.member1(
# # id integer not null primary key,
# # name varchar(32) not null,
# # password varchar(32) not null,
# # singal varchar(128)
# # )''')
# #     conn.commit()
# #     conn.close()
#     print
#     'table public.member is created!'


def insertOperate():
    conn = psycopg2.connect(database="qmstst", user="sys", password="sysadmin", host="10.50.10.163", port="5432")
    cursor = conn.cursor()
    cursor.execute("insert into public.member(id,name,password,singal)\
values(1,'member0','password0','signal0')")
    cursor.execute("insert into public.member(id,name,password,singal)\
values(2,'member1','password1','signal1')")
    cursor.execute("insert into public.member(id,name,password,singal)\
values(3,'member2','password2','signal2')")
    cursor.execute("insert into public.member(id,name,password,singal)\
values(4,'member3','password3','signal3')")
    conn.commit()
    conn.close()

    print
    'insert records into public.memmber successfully'


def selectOperate():
    shell_start = time.time()
    # conn = connectGreenplum.gp_connect()
    print(conn)
    # conn = psycopg2.connect(database="qmstst", user="sys", password="sysadmin", host="10.50.10.163", port="5432")
    dict_cur = conn.cursor()
    dict_cur.execute("select * from eda.transferStateTable where table_name='job'")
    print(dict_cur .rowcount)
    # rows = list(cursor)
    # print(rows)
    rows = dict_cur.fetchone()
    print(rows)
    # for row in rows:
    #     print('id=', row[0], ',name=', row[1], ',pwd=', row[2], ',singal=', row[3])
    # conn.close()
    shell_end = time.time()

    # sec = (end_start - shell_start).second
    print(round((shell_start - shell_end),2))

def selectOperateORM():
    sql = 'select * from eda.transferStateTable where table_name=%s;'
    parms = ("job",)  # 如果参数是只有一个元素的元组，逗号必须;字典方式传递参数，请看文档
    data = db.GP.GPModel().getAll(sql, parms)

    print(data)


def insertHisByORM():
    insert_sql = """INSERT INTO eda.transferstatetable 
                                      (db_nmae, schema_nmae, table_name, start_timestamp, end_timestamp, opera_type, status, duration, cnt, errormessage, db_timestamp)
                              VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);   """
    parms = (('qmstst', 'sor', 'wpp_adefect', '2020-10-13 14:00:00', '2020-10-13 14:00:10',  'copy_gzip', 'Y', '20', 100, 'no error'))  # 如果参数是只有一个元素的元组，逗号必须;字典方式传递参数，请看文档
    data = db.GP.GPModel().writeDb(insert_sql, parms)
    print(data)

    # postgres returning id 可以返回刚刚执行语句的id
    # sql = "UPDATE company SET name=%s WHERE mobileno=%s RETURNING id;"
    # parms = (‘测试一下‘, ‘18611111111‘)
    # data = DbModel().writeGetId(sql, parms)
    # print
    # data


if __name__ == '__main__':
    connectGreenplum()
    # insertOperate()
    selectOperate()
    # selectOperateORM()
    # insertHisByORM()