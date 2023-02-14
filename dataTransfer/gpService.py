import psycopg2
import sys
from random import random



# 0、连接数据库
# 1、获取job配置信息
# 2、获取需要迁移的table
# 3、 su - gpadmin
#     psql -h 10.50.10.163 -c "copy (select * from sor.wpp_adefect_f_n where evt_timestamp >= V1 and evt_timestamp < V2) to stdout"|
#       psql -h 10.50.10.170 -d qmstst -c "copy sor.wpp_adefect_f_n from stdin"
# 4、保存履历(耗时以及是否有报错？)

# 0、连接数据库
from coverage.python import os
def copyGzip():
    cmd = "ls"
    val = os.system(cmd)
    print(val)

if __name__ == '__main__':
    # connectGP()
    # selectOperate()
    copyGzip()
