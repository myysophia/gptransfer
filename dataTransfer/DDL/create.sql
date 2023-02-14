# job主表
CREATE TABLE eda.transferstatetable (
	db_name varchar(50) NOT NULL,
	schema_nmae varchar(50) NOT NULL,
	table_name varchar(100) NOT NULL,
	start_timestamp timestamp NULL,
	end_timestamp timestamp NULL,
	opera_type varchar(200) NOT NULL,
	status varchar(20) NULL,
	duration varchar(50) NULL,
	cnt varchar(50) NULL,
	errormessage text NULL,
	db_timestamp timestamp NULL DEFAULT now(),
	final_end_timestamp timestamp NULL,
	run_flg varchar(1) NULL
)
WITH (
	OIDS=FALSE
)
TABLESPACE others
 ;
example:
INSERT INTO eda.transferstatetable
(db_name, schema_nmae, table_name, start_timestamp, end_timestamp, opera_type, status, duration, cnt, errormessage, db_timestamp, final_end_timestamp, run_flg)
VALUES('qmsprd', 'hms_compare__ARRAY', 'job', '2021-08-10 00:00:00.000', '2021-08-10 04:00:00.000', 'copy_gzip2GP6', 'Y', '4', '1', 'gzip导入169 GP6 CF', '2021-08-26 13:12:04.000', '2021-08-10 00:00:00.000', 'N');


# 子表 tablelist
CREATE TABLE eda.transfertable (
	db_name varchar(50) NULL,
	schema_nmae varchar(50) NOT NULL,
	table_name varchar(100) NOT NULL,
	opera_type varchar(200) NOT NULL,
	valid_flg varchar(1) NULL,
	db_timestamp timestamp NULL DEFAULT now(),
	duration varchar(100) NULL,
	job_group varchar(100) NULL,
	condition_query_column varchar(20) NULL,
	"comment" varchar(300) NULL
)
WITH (
	OIDS=FALSE
)
TABLESPACE others
 ;
example :
INSERT INTO eda.transfertable
(db_name, schema_nmae, table_name, opera_type, valid_flg, db_timestamp, duration, job_group, condition_query_column, "comment")
VALUES('qmsprd', 'sor', 'wpp_adefect_glass_f', 'copy_gzip2GP6', 'Y', '2020-10-14 09:44:23.000', '4', 'hms_compare__ARRAY', 'evt_timestamp', NULL);
