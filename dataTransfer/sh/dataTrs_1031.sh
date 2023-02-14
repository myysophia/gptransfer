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
current_path=`pwd`
#echo $current_path
#echo $EXPORT_START_DATE > starttime
#echo $EXPORT_END_DATE > endtime
#echo "tmp $tmp"
#echo ${USER} 
#path_t1=cat $current_path/starttime|sed 's/-//g'|sed 's/ //g'|sed 's/://g'
#path_t1=sudo cat $current_path/starttime|sed 's/-//g'|sed 's/ //g'|sed 's/://g'
#path_t2=sudo cat $current_path/endtime|sed 's/-//g'|sed 's/ //g'|sed 's/://g'
#echo "$path"
#echo "$path_t1"
#echo "$path_t2"
year=${EXPORT_START_DATE:0:4}
month=${EXPORT_START_DATE:5:2}
day=${EXPORT_START_DATE:8:2}

#cd $path
echo "full path: $path$DB_NAME^$SCHEMA_NAME^$TABLE_NAME^$PATH_T"
if [ -d $path/$year$month/$day/$TABLE_NAME ];then
chgrp -R gpadmin $path
chown -R gpadmin $path
su - gpadmin <<EOF
#psql -h 10.50.10.163 -c "copy (select * from $DB_NAME.$SCHEMA_NAME.$TABLE_NAME where evt_timestamp >= '$EXPORT_START_DATE' and evt_timestamp <= '$EXPORT_END_DATE') to PROGRAM 'gzip >  $path/$DB_NAME^$SCHEMA_NAME^$TABLE_NAME^$PATH_T.csv.gz'"
# |tee $current_path/result
psql -h 10.50.10.163 -c "copy (select * from $DB_NAME.$SCHEMA_NAME.$TABLE_NAME where evt_timestamp >= '$EXPORT_START_DATE' and evt_timestamp <= '$EXPORT_END_DATE') to PROGRAM 'gzip >  $path/$TABLE_NAME.csv.gz'"
EOF
else
mkdir -p $path/$year$month/$day/$TABLE_NAME
su - gpadmin <<EOF
psql -h 10.50.10.163 -c "copy (select * from $DB_NAME.$SCHEMA_NAME.$TABLE_NAME where evt_timestamp >= '$EXPORT_START_DATE' and evt_timestamp <= '$EXPORT_END_DATE') to PROGRAM 'gzip >  $path/$year$month/$day/$TABLE_NAME/$DB_NAME^$SCHEMA_NAME^$TABLE_NAME^$PATH_T.csv.gz'"
EOF
fi
# 获取状态码
t=$?
echo "shell执行结果状态码为: $t"

#执行返回值
#path_t3=sudo more $current_path/result|awk '{print $2}'
#res=sudo cat $current_path/result|awk '{print $2}'
#echo "copy_gzip result: $path_t3"

if [ $t -eq 0 ];then
	 exit 99; # shell 执行返回指定状码
	echo "table name : $TABLE_NAME dump Successfully"
else
	 exit 100; # shell 执行返回指定状码
	echo "table name : $TABLE_NAME dump failed"
fi

