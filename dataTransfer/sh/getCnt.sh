#!/bin/bash
# @date 2020年10月18日13:49:16
# @author ninesun
# @desc
#   1. get copy count

if [ $# == 5 ]; then
    datebeg=$1
    dateend=$2
    tablename=$3
    schemaname=$4
    dbname=$5
else
   echo  "plese input correct params!!!"
    exit 1
fi
EXPORT_START_DATE=$1
EXPORT_END_DATE=$2
TABLE_NAME=$3
SCHEMA_NAME=$4
DB_NAME=$5
PATH_T=$6
path="/mnt/dataTransfer/"
current_path=`pwd`
su - gpadmin <<EOF
psql -t -h 10.50.10.163 -c "select count(1) from $DB_NAME.$SCHEMA_NAME.$TABLE_NAME where evt_timestamp >= '$EXPORT_START_DATE' and evt_timestamp <= '$EXPORT_END_DATE'" 
#> $current_path/result
#$DB_NAME.$SCHEMA_NAME.$TABLE_NAME

#echo "copyCnt-path: $current_path/$DB_NAME.$SCHEMA_NAME.$TABLE_NAME"
EOF
# 获取状态码
t=$?
#echo "shell执行结果状态码为: $t"
