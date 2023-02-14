import mysql.connector.pooling

config = {
    "host": "localhost",
    "port": 3306,
    "user": "root",
    "password": "abc123456",
    "database": "demo"
}
try:
    pool = mysql.connector.pooling.MySQLConnectionPool(
        **config,
        pool_size=10
    )
    con = pool.get_connection()
    con.start_transaction()
    cursor = con.cursor()
    # sql="DROP TABLE t_emp_new"
    # cursor.execute(sql)
    sql = "CREATE TABLE t_emp_new LIKE t_emp"
    cursor.execute(sql)

    # 使用INSERT语句，把部门平均底薪超过公司平均底薪的这样部门里的
    # 员工信息导入到t_emp_new表里面，并且让这些员工隶属于sales部门
    sql = "SELECT AVG(sal) AS avg FROM t_emp"
    cursor.execute(sql)
    temp = cursor.fetchone()
    avg = temp[0]  # 公司平均底薪
    sql = "SELECT deptno FROM t_emp GROUP BY deptno HAVING AVG(sal)>=%s"
    cursor.execute(sql, [avg])
    temp = cursor.fetchall()
    sql = "INSERT INTO t_emp_new SELECT * FROM t_emp WHERE deptno IN ("
    for index in range(len(temp)):
        one = temp[index][0]
        if index < len(temp) - 1:
            sql += str(one) + ","
        else:
            sql += str(one)
    sql += ")"
    cursor.execute(sql)

    sql = "DELETE FROM t_emp WHERE deptno IN ("
    for index in range(len(temp)):
        one = temp[index][0]
        if index < len(temp) - 1:
            sql += str(one) + ","
        else:
            sql += str(one)
    sql += ")"
    cursor.execute(sql)
    sql = "SELECT deptno FROM t_dept WHERE dname=%s"
    cursor.execute(sql, ["SALES"])
    deptno = cursor.fetchone()[0]
    sql = "UPDATE t_emp_new SET deptno=%s"
    cursor.execute(sql, [deptno])
    con.commit()
except Exception as e:
    if "con" in dir():
        con.rollback()
    print(e)