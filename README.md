# COVID-19 Data Analysys

CSVベースで、データ集取、グラフ作成を行っていたが、さすがに重たくなってきたのでデータベース化をしています。

	mysql  Ver 14.14 Distrib 5.7.32, for Linux (x86_64) using  EditLine wrapper

## Main Program
covid19.pl

## データベース操作ユーティリティ
dbutil.pl

##	共通ライブラリ
use dbinfo;		# DB information, DB_NAME, DB_PASSWORD, DB_USER, DB_PORT, WIN_PATH
use tbdef;		# Table Defintion
use dblib;		# Library
use dp;			# print with filename and line number

###	dbutil.pl

	dbuil.pl [table_name] options

	option format
	-params  : set PARASMS{$_} = 1;				# -debug
	--params : set PARASMS{$_} = $ARGV[+1];		# --debug 2
	
	table_name  	: Select Table_name for operate, Default all tables
	-table_info		: Display Table information, % dbutil.pl -table_info shows all tables
	-dump		 	: Simple Dump of the table

	-create_table	: Create Tables listed on @MASTER_DEFS and @RECORD_DEFS
	-buid_master	: Insert Data and build @MASTER_DEFS
	-load_master	: Load Master (This may default, not yet)
	-delete_data	: Delete all datas from @MASTER_DEFS and @RECORD_DEFS
	-insert_csv		: load (reformed ) csv to @RECORD_DEFS
	-reform_csv		: Reform data srouce format fit to this program
	-debug
	-verbose

	Examples
	./dbutil..pl -create_table -build_master -reform_csv -insert_csv
	./dbutil..pl AreaInfo -table_info
	./dbutil..pl DataSource -table_info --debug 2
	./dbutil..pl AreaInfo -dump 

	./dbutil..pl CCSE-NC -reform_csv -insert_csv

## 
