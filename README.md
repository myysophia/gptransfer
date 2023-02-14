# gptransfer
GP to GP  客制化迁移程序、跨集群数据比对 
语言: python
### 获取配置信息

mysql  -h 10.50.10.163 -P3306 -u -p -Dch_qms -e "set names utf8;select JOB_NAME,RUN_START_TIMESTAMP,RUN_END_TIMESTAMP from etl_conf_d where VALID_FLG = 'Y' and JOB_NAME = 'datatransfer'
"

[root@P1QMSPL2RTM01 ~]# mysql  -h 10.50.10.163 -P3306 -u -p -Dch_qms -e "set names utf8;select JOB_NAME,RUN_START_TIMESTAMP,RUN_END_TIMESTAMP from etl_conf_d where VALID_FLG = 'Y' and JOB_NAME = 'datatransfer'
> "
+--------------+---------------------+---------------------+
| JOB_NAME     | RUN_START_TIMESTAMP | RUN_END_TIMESTAMP   |
+--------------+---------------------+---------------------+
| datatransfer | 2020-04-01 00:00:00 | 2020-04-01 04:00:00 |
+--------------+---------------------+---------------------+

### 获取tablelist

create table sor.schemaName( schemaName varchar( 10 ), tableName varchar( 100 ), valid_flag varchar( 1 ), dbtimestamp timestamp not null )
DISTRIBUTED BY (tableName)


### linux shell 执行状态码
-- 0 命令成功结束
-- 1 一般性未知错误
    -- No such file or directory
-- 2 不适合的shell命令
-- 126 命令不可执行
-- 127 没找到命令
-- 128 无效的退出参数
-- 128+x 与Linux信号x相关的严重错误
-- 130 通过Ctrl+C终止的命令
-- 255 正常范围之外的退出状态码

### postgresql 连接、使用
-- https://www.psycopg.org/docs/extras.html?highlight=realdictcursor


### 远程连接主机文档
-- http://docs.paramiko.org/en/stable/
-- exec_command（命令，bufsize = -1，超时=无，get_pty = False，环境=无）
-- 在SSH服务器上执行命令。一个新Channel的打开并执行所请求的命令。该命令的输入和输出流以类似于Pythonfile的对象的形式返回，它们代表stdin，stdout和stderr。
#### issue
-- Permission denied
 [root@gptest01 scripts]# sh /home/scripts/dataTransfer/dataTrs_1031.sh  '2020-10-13 14:00:00' '2020-10-13 16:00:00' 'wpp_fdefect_f_n' 'eda' 'qmstst' '20201013140000^20201013150000'
full path: /mnt/dataTransferqmstst^eda^wpp_fdefect_f_n^20201013140000^20201013150000
ERROR:  command error message: sh: /mnt/dataTransfer/wpp_fdefect_f_n.csv.gz: Permission denied
shell执行结果状态码为: 1
You have mail in /var/spool/mail/root

在linux 不同服务器中，切换同一个用户，其UID GID可能不一样，这需要注意。例如A服务器的 user1的uid是530 gid也是530，B服务商上user1的uid是540，gid是2180.
如果在B服务器上对目录chown user1:user1 共享盘目录。A服务器去写这个共享目录是会提示Permission denied。 这个花费了我很久去思考。

## 获取table list逻辑修改 2020年11月5日17:14:40
-- getTableList时关联job表的duration
-- 不同table 应该使用不同的duration 这样效率会更高
-- transferTable新增duration字段
!!!! 主表子表的关联条件为opera_type、duration、job_group


## update job时更新dbtimestamp

## 如何将项目打包成docker 镜像
### 导出程序依赖包
-- pip freeze > requirements.txt

## 2021年2月4日20:58:20 新增 query_condition_column适应用不同表不同栏位
## 2021年2月5日14:18:45 增加final_end 用于补数据
## 2021年2月7日14:50:29 记录履历的duration 需要记录到小数点之后两位数

issue
## duration 支持 days, seconds, microseconds, milliseconds, minutes, hours, weeks
## todo 数据导入时需要记录数量
## 数据导入目录新增一个层级由/mnt/dataTransfer/变为/mnt/dataTransfer/file2GP/  避免目录混乱.

V1.2 新增gplaoder for pk violent

## compare 程序issue
### compare 程序在insert into target时  如果外部表中没有数据 会报错？2021年4月22日11:57:22
### 没有站点上的差异就没有  evt上的差异. 2021年4月22日11:57:24

V1.3 compare新增alarm逻辑 2021年5月15日13:55:12
当比对到有差异时将差异的数据写入alarm表
stetp2 找到有差异之后将报警信息写入110 的system_alarm<SYS=GPTRANSFER>表.

报警频率为15分钟检查一次，检查当前时间距离过去24H 是否有异常.
报警sql为:
select
case when t.qty > '0' then 'ABNORMAL_Transfer 存在数量差异,请联系王旭处理!!'
else 'NORMAL' end
 from ( select count( 1 ) qty
from system_alarm
where evt_timestamp >= now()- interval '72 h'
and data_name = 'GPTRANSFER' )t
todo : reset 时修改diff 为0


## V2.0 GP6 to GP6
## 2021年8月25日09:10:05 insert失败后需要回滚
except psycopg2.IntegrityError:
        conn.rollback()
    else:
        conn.commit()
