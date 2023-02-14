import psycopg2
from random import random

conn = psycopg2.connect(database="qmstst", user="sys", password="sysadmin", host="10.50.10.163", port="5432")
cur = conn.cursor()


def insertData():
    names = ['路人甲', '王尼玛', '唐马儒']
    zbfms = ['年龄', '身高', '体重']

    for i in range(100):
        sqlstr = 'insert into testRowToColumn(name, zbfm, value) values'
        for j in range(100):
            for name in names:
                for zbfm in zbfms:
                    sqlstr += "('%s','%s',%d)," % (name + str(i * 100 + j), zbfm, int(100 * random()))
        cur.execute(sqlstr[:-1])
        conn.commit()
        print(i)


if __name__ == '__main__':
    insertData()
