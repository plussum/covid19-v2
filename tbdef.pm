#
#
#
package tbdef;
use Exporter;
@ISA = (Exporter);
@EXOIORT = qw(tbdef);

use strict;
use warnings;

use dbinfo;

#
#	MASTER TABLES
#
#	01: ID
#	02: KEY for Reference (join, relation in perl)
#	03 --- not affect for Reference
#
our @MASTER_DEFS  = (
	{type => "master", table_name => "AreaInfo",   hash => {}, csvf => "$dbinfo::WIN_PATH/pop.csv",
			columns => "AreaID INTEGER PRIMARY KEY, AreaName VARCHAR(64), Population INTEGER", }, 
	{type => "master", table_name => "DataSource", hash => {}, csvf => "DataSource.csv",
			columns => "SourceID INTEGER, SourceName CHAR(8), SoruceFullName VARCHAR(256)", },
	{type => "master", table_name => "DataKind",   hash => {}, csvf => "DataKind.csv",
			columns => "KindID INTEGER, KindName CHAR(32), KindFullName VARCHAR(256)", },
);

#
#	Set array number of Master Tables and set AreaInfo
#
our %MASTER_INDEX = ();
for(my $i = 0; $i <= $#MASTER_DEFS; $i++){
	my $p = $MASTER_DEFS[$i];
	my $name = $p->{table_name};
	$MASTER_INDEX{$name} = $i;
}

our $areainfo = $MASTER_INDEX{AreaInfo};
our $AreaName = $MASTER_DEFS[$areainfo]->{hash};


#
#	TRANSACTIONS
#
our @RECORD_DEFS = (
	{type => "transaction", name => "CCSE-NC", table_name => "RawRecord", csvf => "testdb.csv", csv_src => "$dbinfo::WIN_PATH/CSV/time_series_covid19_confirmed_global.csv",
		columns => "RecordNo INTEGER, Date DATE, Source CHAR(8), Kind CHAR(4),AreaID SMALLINT UNSIGNED, Count INTEGER, Avr7 DECIMAL(10,2)",
		select => "SELECT Date Source Kind  AreaInfo.AreaName Count Avr7 FROM RawRecord, AreaInfo WHERE RawRecird.AreID=AreaInfo.AreaID",
		source => "ccse", kind => "NC", 
	},
);

our %RECORD_INDEX = ();
for(my $i = 0; $i <= $#RECORD_DEFS; $i++){
	my $p = $RECORD_DEFS[$i];
	my $name = $p->{table_name};
	$RECORD_INDEX{$name} = $i;
	#dp::dp "$name, $i\n";
}

1;
