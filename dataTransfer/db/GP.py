import psycopg2
import psycopg2.extras
class GPModel(object):

    def __init__(self, autocommit=True):
        # self.conn = psycopg2.connect(host=current_app.config['HOST'],
        #                         port=current_app.config['PORT'],
        #                         user=current_app.config['USER'],
        #                         password=current_app.config['PASSWORD'],
        #                         database=current_app.config['DATABASE'])

        self.conn = psycopg2.connect(host="10.50.10.",
                                     port="5432",
                                     user="",
                                     password="",
                                     database="")
        #是否执行后立即提交，默认为True;如果应用场景中需要使用事务，设置为False。在最后执行commit()方法或者rollback()方法
        self.autocommit = autocommit
        self.cur = self.conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) # 返回值为字典而非 list tuple

    def getOne(self, sql='',data=()):
        self.cur.execute(sql, data)
        self.data = self.cur.fetchone()
        return self.data

    def getAll(self, sql='', data=()):
        self.cur.execute(sql, data)
        self.data = self.cur.fetchall()
        return self.data

    def writeDb(self, sql='', data=()):
        self.cur.execute(sql, data)
        self.data = self.cur.rowcount
        if self.autocommit:
            self.commit()
        return self.data

    def writeGetId(self, sql='', data=()):
        self.cur.execute(sql, data)
        self.data = self.cur.fetchone()
        if self.autocommit:
            self.commit()
        return self.data

    def getSql(self, sql="", data=()):
        return self.cur.mogrify(sql, data)

    def commit(self):
        self.conn.commit()
        self.closeall()

    def closeall(self):
        self.cur.close()
        self.conn.close()