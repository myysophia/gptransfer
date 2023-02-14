#!/bin/bash
# @date 2020年10月18日13:49:16
# @author ninesun
# @desc
#   1. copy by gzip
# dbname^schemaname^tablename^start^end
#su - gpadmin <<EOF
#psql -d qmstst -v v1="'$dateFrom'"  -f /home/scripts/geneSqlCnt.sql >> usrSqlCnt.txt;
#exit;
#EOF
if [ $# == 6 ]; then
    datebeg=$1
    dateend=$2
    tablename=$3
    schemaname=$4
    dbname=$5
    path_t=$6
else
    #echo  "plese input start && end time && tablename,eg '2020-05-18 09:00:00' '2020-05-18 09:20:00' 'wpp_adefect_glass_f'"
    exit 1
fi
EXPORT_START_DATE=$1
EXPORT_END_DATE=$2
TABLE_NAME=$3
SCHEMA_NAME=$4
DB_NAME=$5
PATH_T=$6
path="/mnt/dataTransfer"
path_local="/mnt/dataTransfer"
current_path=`pwd`
#echo $current_path
echo $EXPORT_START_DATE
echo $EXPORT_END_DATE


year=${EXPORT_START_DATE:0:4}
month=${EXPORT_START_DATE:5:2}
day=${EXPORT_START_DATE:8:2}
#echo "substr : $year$month$day"
#su - gpadmin <<EOF
echo "user name = ${USER}"
if [ -d $path_local/$year$month/$day/$TABLE_NAME ];then
	echo "The  $path_local/$year$month/$day/$TABLE_NAME directory exists"
	#chown -R gpadmin:gpadmin $path_local/$year$month/$day/$TABLE_NAME
#	cd $path_local/$year$month/$day/
#	ls -l
su - gpadmin <<EOF
#cd $path_local/$year$month/$day/$TABLE_NAME
#ls
#sleep 10s
#psql -h 10.50.10.163 -c "copy (select * from $DB_NAME.$SCHEMA_NAME.$TABLE_NAME where evt_timestamp >= '$EXPORT_START_DATE' and evt_timestamp <= '$EXPORT_END_DATE') to PROGRAM 'gzip > $path_local/$year$month/$day/$TABLE_NAME/$DB_NAME^$SCHEMA_NAME^$TABLE_NAME^$PATH_T.csv.gz'"

#echo "psql -h 10.50.10.163 -c "copy (select * from $DB_NAME.$SCHEMA_NAME.$TABLE_NAME where evt_timestamp >= '$EXPORT_START_DATE' and evt_timestamp <= '$EXPORT_END_DATE') to PROGRAM 'gzip > $path_local/$year$month/$day/$TABLE_NAME/$DB_NAME^$SCHEMA_NAME^$TABLE_NAME^$PATH_T.csv.gz'"/"

psql -h 10.50.10.163 -c "copy (select * from $DB_NAME.$SCHEMA_NAME.$TABLE_NAME where evt_timestamp >= '$EXPORT_START_DATE' and evt_timestamp <= '$EXPORT_END_DATE') to PROGRAM 'gzip > $path_local/$year$month/$day/$TABLE_NAME/$DB_NAME^$SCHEMA_NAME^$TABLE_NAME^$PATH_T.csv.gz'"
EOF
# dir 不存在，重建文件夹再执行
else 
#su - gpadmin <<EOF
	echo "The $path_local/$year$month/$day/$TABLE_NAME directory does not exist,begin create dir"
	mkdir -p $path_local/$year$month/$day/$TABLE_NAME
	chown -R gpadmin $path_local/$year$month/$day/
	chgrp -R gpadmin $path_local/$year$month/$day/
su - gpadmin <<EOF
#psql -h 10.50.10.163 -c "copy (select * from $DB_NAME.$SCHEMA_NAME.$TABLE_NAME where evt_timestamp >= '$EXPORT_START_DATE' and evt_timestamp <= '$EXPORT_END_DATE') to PROGRAM 'gzip > $path_local/$year$month/$day/$TABLE_NAME/$DB_NAME^$SCHEMA_NAME^$TABLE_NAME^$PATH_T.csv.gz'"
psql -h 10.50.10.163 -c "copy (select * from $DB_NAME.$SCHEMA_NAME.$TABLE_NAME where evt_timestamp >= '$EXPORT_START_DATE' and evt_timestamp <= '$EXPORT_END_DATE') to PROGRAM 'gzip > $path_local/$DB_NAME^$SCHEMA_NAME^$TABLE_NAME^$PATH_T.csv.gz'"
EOF
# 获取状态码
t=$?
echo "shell执行结果状态码为: $t"
fi

if [[ $t -eq 0 ]];then
	 exit 99; # shell 执行返回指定状码
	echo "table name : $TABLE_NAME dump Successfully"
else
	 exit 100; # shell 执行返回指定状码
	echo "table name : $TABLE_NAME dump failed"
fi

