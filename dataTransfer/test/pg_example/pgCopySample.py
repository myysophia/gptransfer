#encoding=utf-8
import psycopg2
import StringIO
if __name__=='__main__':
    s = ""
    for i in range(0,1000000):
        s += str(i)+"\taaa\t13434\t1\t2013-01-11\t1\t2\n"
    conn = psycopg2.connect(database="qmstst", user="sys", password="sysadmin", host="10.50.10.163", port="5432")
    cur = conn.cursor()
    cur.copy_from(StringIO.StringIO(s),'tb_user',columns=('id','userame','passwd','roleid','lasttime','failnum','info'))
    conn.commit()
    cur.close()
    conn.close()
    print('done')