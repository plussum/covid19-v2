#!/usr/bin/perl
#
#	Proto Type of Covid-19 data analysis by mysql
#
#	Command Line Paramerts
#	-params  : set PARASMS{$_} = 1;
#	--params : set PARASMS{$_} = $ARGV[+1];
#	
#	table_name  	: Select Table_name for operate, Default all tables
#
#	-table_info		: Display Table information
#	-dump		 	: Simple Dump of the table
#
#	-create_table	: Create Tables listed on @MASTER_DEFS and @RECORD_DEFS
#	-buid_master	: Insert Data and build @MASTER_DEFS
#	-delete_data	: Delete all datas from @MASTER_DEFS and @RECORD_DEFS
#	-load_master	: Load Master (This may default, not yet)
#	-reform_csv		: Reform data srouce format fit to this program
#	-insert_csv		: load (reformed ) csv to @RECORD_DEFS
#
#	Examples
#	./TestCov.pl -create_table -build_master -reform_csv -insert_csv
#	./TestCov.pl CCSE-NC -reform_csv 
#	./TestCov.pl AreaInfo -table_info
#	./TestCov.pl DataSource -table_info --debug 2
#	./TestCov.pl AreaInfo -dump 
#
#
use	strict;
use warnings;
use DBI;
use Data::Dumper;

use config;
use dbinfo;
use dp;

my $DEBUG = 0;
my $VERBOSE = 0;
my $SEARCH_KEY = "";
my $DISP_LINES = 50;

#
#	Set Database Parameter for connect
#
my $DB_NAME = $dbinfo::DB_NAME;
my $password  = $dbinfo::DB_PASSWORD;
my $user = $dbinfo::DB_USER;
my $port = $dbinfo::DB_PORT;
my $dsn = "dbi:mysql:database=$DB_NAME;host=localhost;port=$port";

my %AreaInfo = ();
my %DataSource = ();
my %DataKind = ();
my @MASTER_DEFS  = (
	{table_name => "AreaInfo",   hash => {}, csvf => "$config::WIN_PATH/pop.csv",
			columns => "AreaID INTEGER PRIMARY KEY, AreaName VARCHAR(64), Population INTEGER", }, 
	{table_name => "DataSource", hash => {}, csvf => "DataSource.csv",
			columns => "SourceID INTEGER, SourceName CHAR(8), SoruceFullName VARCHAR(256)", },
	{table_name => "DataKind",   hash => {}, csvf => "DataKind.csv",
			columns => "KindID INTEGER, KindName CHAR(32), KindFullName VARCHAR(256)", },
);

#
#	Set array number of Master Tables and set AreaInfo
#
my %MASTER_INDEX = ();
for(my $i = 0; $i <= $#MASTER_DEFS; $i++){
	my $p = $MASTER_DEFS[$i];
	my $name = $p->{table_name};
	$MASTER_INDEX{$name} = $i;
}

my $areainfo = $MASTER_INDEX{AreaInfo};
my $AreaName = $MASTER_DEFS[$areainfo]->{hash};

my @RECORD_DEFS = (
	{name => "CCSE-NC", table_name => "RawRecord", csvf => "testdb.csv", csv_src => "$config::CSV_PATH/time_series_covid19_confirmed_global.csv",
		columns => "RecordNo INTEGER, Date DATE, Source CHAR(8), Kind CHAR(4),AreaID SMALLINT UNSIGNED, Count INTEGER, Avr7 DECIMAL(10,2) " 
				. ", PRIMARY KEY (RecordNo)",
		select => "SELECT Date Source Kind  AreaInfo.AreaName Count Avr7 FROM RawRecord, AreaInfo WHERE RawRecird.AreID=AreaInfo.AreaID",
		source => "ccse", kind => "NC", comvert => \&comv_ccse,
	},
);
my %RECORD_INDEX = ();
for(my $i = 0; $i <= $#RECORD_DEFS; $i++){
	my $p = $RECORD_DEFS[$i];
	my $name = $p->{table_name};
	$RECORD_INDEX{$name} = $i;
	#dp::dp "$name, $i\n";
}

my @QUERY_DATA = (
	{query => "SELECT * FROM RawRecord WHERE AreaInfo WHERE RawRecird.AreID=AreaInfo.AreName"},
);

#
#	Default Paramers of DO
#
my %DISP = (
	default => {disp_result => 1, disp_line_no => 1, disp_query => 0, disp_name => 0},
	silent  => {disp_result => 0, disp_line_no => 0, disp_query => 0, disp_name => 0},		# no dislay, use result
	verbose => {disp_result => 1, disp_line_no => 1, disp_query => 1, disp_name => 0},
	name    => {disp_result => 1, disp_line_no => 0, disp_query => 0, disp_name => 1},
);


#
#	Command Line Paramerts
#
#	-params  : set PARASMS{$_} = 1;
#	--params : set PARASMS{$_} = $ARGV[+1];
#
#
my @PARAM_NAMES_LIST = qw(create_table delete_data build_master load_master reform_csv insert_csv dump select verbose debug table_info table_name);
my %PARAM_NAMES = ();
foreach my $k (@PARAM_NAMES_LIST){
	$PARAM_NAMES{$k} = 1;
}

#
#	Argument handling
#
my %PARAMS = ();
for(my $i = 0; $i <= $#ARGV; $i++){
	$_ = $ARGV[$i];
	if(/-h/){
		print "usage: $0 " . join(" | ", @PARAM_NAMES_LIST) . "\n";
		exit 1;
	}
	if(/^--/){
		s/$&//;
		if(!defined $PARAM_NAMES{$_}){
			dp::dp "Unkown parameter --$_\n";
			exit;
		}
		$PARAMS{$_} = $ARGV[++$i];
	}
	elsif(/^-/){
		s/$&//;
		if(!defined $PARAM_NAMES{$_}){
			dp::dp "Unkown parameter -$_\n";
			exit;
		}
		$PARAMS{$_} = "";
	}
	else{
		if(defined $MASTER_INDEX{$_} || defined $RECORD_INDEX{$_}){
			$PARAMS{table_name} = $_;
		}
		else {
			dp::dp "Unkonwn parameter or table_name: $_\n";
			exit 1;
		}
	}
	if(/^=/){
		s/^=//;
		$SEARCH_KEY = $_;
	}
}

#
#	form Parameters
#
if(defined $PARAMS{verbose}){
	$VERBOSE = $PARAMS{verbose};
}

if(defined $PARAMS{debug}){
	$DEBUG = $PARAMS{debug};
}

dp::dp %PARAMS . "\n" if($DEBUG);
if((keys %PARAMS) <= 0){
	print "usage: $0 " . join(" | ", @PARAM_NAMES_LIST) . "\n";
	exit;
}
if(! defined $PARAMS{table_name}){
	$PARAMS{table_name} = "";
}

foreach my $k (keys %PARAMS){
	dp::dp "PARAMS: $k = $PARAMS{$k} \n"if($DEBUG);
}



#
#	Connect to Database
#
my $dbh = DBI->connect($dsn, $user, $password, {RaiseError => 0, AutoCommit =>0}) || die $DBI::errstr;
#my $dbh = DBI->connect($dsn, $user, $password ) || die $DBI::errstr;
&DO($dbh, "USE $DB_NAME", $DISP{silent});


#
#	Remove All Data for test
#
my $pname = "delete_data";
if(defined $PARAMS{$pname}){
	foreach my $p (@MASTER_DEFS, @RECORD_DEFS){
		next if($PARAMS{table_name} && $PARAMS{table_name} ne $p->{table_name});

		&DO($dbh, qq{DELETE FROM $p->{table_name};});
	}
}

#
#	Create Table
#
$pname = "create_table";
if(defined $PARAMS{$pname}){
	dp::dp "## create_table\n";
	&DO($dbh, "show tables;") if($VERBOSE);
	foreach my $p (@MASTER_DEFS, @RECORD_DEFS){
		next if($PARAMS{table_name} && $PARAMS{table_name} ne $p->{table_name});

		&DO($dbh, "DROP TABLE IF EXISTS $p->{table_name}", $DISP{verbose});
		&DO($dbh, "CREATE TABLE $p->{table_name} ($p->{columns})", $DISP{verbose});
		&DO($dbh, "SHOW TABLES from $DB_NAME;", $DISP{verbose});
		&DO($dbh, "DESC $p->{table_name};", $DISP{verbose});
	}
	&DO($dbh, "show tables;") if($VERBOSE);
}

#
#	Simple Test of DB operation
#
#$dbh->do("INSERT INTO  $TABLE_NAME VALUES (1, " . $dbh->quote("Tokyo") . " , 12345) ");
#$dbh->do("INSERT INTO  $TABLE_NAME VALUES (1, " . $dbh->quote("東京") . " , 12345) ");
#$dbh->do(qq{INSERT INTO  $TABLE_NAME VALUES (1, "名古屋", 12345) });

#
#	Insert Master Data 
#
$pname = "build_master";
if(defined $PARAMS{$pname}){
	foreach my $p (@MASTER_DEFS){
		next if($PARAMS{$pname} && $PARAMS{$pname} ne $p->{table_name});

		dp::dp "insert_master; $PARAMS{$pname}, $p->{table_name}\n";
		&insert_table($p);
	}
}


#
#	Load Mater Tables
#
$pname = "load_master";
if(defined $PARAMS{$pname}){
	dp::dp "load_master\n";
	foreach my $p (@MASTER_DEFS){
		next if($PARAMS{table_name} && $PARAMS{table_name} ne $p->{table_name});

		dp::dp "load_master; $PARAMS{$pname}, $p->{table_name}\n";
		&load_master($p);
	}
}


#
#	Reformat Data
#
$pname = "reform_csv";
if(defined $PARAMS{reform_csv}){
	my $target = $PARAMS{reform_csv};
	foreach my $p (@RECORD_DEFS){
		next if($PARAMS{table_name} && $PARAMS{table_name} ne $p->{table_name});

		$p->{comvert}->($p);
	}
}

#
#	Insert Recors 
#
$pname = "insert_csv";
if(defined $PARAMS{$pname}){
	dp::dp "insert_csv\n";
	&load_master();
	foreach my $p (@RECORD_DEFS){
		next if($PARAMS{table_name} && $PARAMS{table_name} ne $p->{table_name});

		dp::dp "insert_csv; $PARAMS{$pname}, $p->{table_name}\n";
		&insert_table($p);
	}
}

#
#	Table Info
#
$pname = "table_info";
if(defined $PARAMS{$pname}){
	dp::dp "table_info\n";
	foreach my $p (@MASTER_DEFS, @RECORD_DEFS){
		next if($PARAMS{table_name} && $PARAMS{table_name} ne $p->{table_name});

		&table_info($p);
	}
}

#
#	Dump 
#
$pname = "dump";
if(defined $PARAMS{$pname}){
	#if(! $PARAMS{$pname}){
	#	dp::dp "slect need to select table --select RawRecord\n";
	#	exit 1;
	#}
	&load_master();
	dp::dp "select $PARAMS{table_name}\n" if($DEBUG);
	foreach my $p (@MASTER_DEFS, @RECORD_DEFS){
		dp::dp "### select $PARAMS{table_name} : $p->{table_name}\n" if($DEBUG);
		next if($PARAMS{table_name} && $PARAMS{table_name} ne $p->{table_name});

		dp::dp "### select $p->{table_name}\n" if($DEBUG);
		&load_table($p, {hash => 0, disp => 2});
	}
}

#
#	Dissconnect the DB
#
$dbh->disconnect();
exit 0;

###############################################################

#
#
#
sub	table_info
{
	my($p) = @_;

	my $table_name = $p->{table_name};
	dp::dp "$table_name\n" if($DEBUG);

	my @rows = &DO($dbh, "select TABLE_ROWS from information_schema.tables where table_name = '$table_name';", $DISP{silent});

	print "$table_name ( $rows[0]records )\n";
	&DO($dbh, "select * from INFORMATION_SCHEMA.TABLES where TABLE_SCHEMA='$DB_NAME' and TABLE_NAME='$table_name'", $DISP{name});
	print "-" x 20 . "\n";
	my $sql_str = "desc $p->{table_name}";
	&DO($dbh, "desc $table_name");
	print "\n";
}



#
#	Insert values to Table
#
sub	insert_table
{
	my($p) = @_;

	my $SNO =  &record_number($dbh, $p->{table_name});
	my @vals = ();
	
	my @strs = ();
	my @cl = split(/\s*,\s*/, $p->{columns});
	for(my $i = 0; $i <= $#cl; $i++){
		dp::dp "[$cl[$i]]\n" if($DEBUG > 1);
		push(@strs, $i) if($cl[$i] =~ /CHAR|DATE|TIME/i);
	}
	dp::dp "STRS: " . join(",", @strs) . "\n";
	dp::dp "CSV_FILE : $p->{csvf} \n";

	open(FD, $p->{csvf}) || die "Cannot open $p->{csvf}";
	my $balk = 10;
	<FD>;
	while(<FD>){
		$SNO++;
		# last if($SNO > 100);

		s/[\r\n]+$//;
		my @w = split(/,/, $_);
		if($#w <= 0){
			dp::dp "Error?: " . $#w . "; $_   $p->{csvf}\n";
			next;
		}
		foreach my $i (@strs){
			$w[$i-1] = qq{"$w[$i-1]"} // '""';		# val -> "val" for Charcter
		}
		my $vs = join(", ", $SNO, @w);
		dp::dp "[$vs]\n" if($DEBUG > 2);
		push(@vals,  "($vs)");
		if(($SNO % $balk) == 0){
			&insert_vals($p, $dbh, \@vals);
			@vals = ();
		}
	}
	close(FD);
	if($#vals >= 0){
		&insert_vals($p, $dbh, \@vals);
	}
}

#
#
#
sub	insert_vals
{
	my ($p, $dbh, $valp) = @_;

	my $sql_str = "INSERT INTO  $p->{table_name} VALUES " . join(",", @$valp);
	dp::dp $sql_str . "\n" if($DEBUG > 2);
	my $sth = $dbh->prepare($sql_str);
	$sth->execute();
	$dbh->commit();
}

#
#
#	Combert Dataformat and genarate CSV file for load
#
sub	comv_ccse
{
	my ($p) = @_;

	my $src = $p->{csv_src};
	my $dst = $p->{csvf};
	my $source = $p->{source};
	my $source_id = $DataSource{$source};
	my $kind = $p->{kind};
	my $kind_id = $DataKind{$kind};

	my $filed_names = $p->{hash}->{filed_names};

	&load_master();
	print "-" x 20 . "\n";
	#print Dumper $AreaName;
	print "-" x 20 . "\n";

	dp::dp "src: $src\n";
	dp::dp "dst: $dst\n";

	open(FD, $src) || die "cannot open $src";
	open(CSV, ">$dst") || die "cannot create $dst";
	my $first_line = <FD>;
	$first_line =~ s/[\r\n]+$//;
	my $SNO = 0;
	my ($dsc_area2, $dsc_area1, $dsc_lat, $dsc_long, @dates) = split(/,\s*/, $first_line);

	for(my $i = 0; $i <= $#dates; $i++){
		my($m,$d, $y) = split(/\//, $dates[$i]);
		$dates[$i] = sprintf("%04d-%02d-%02d", 2000 + $y, $m, $d);
	}
	#	columns => "RecordNo INTEGER, Date DATE, SourceID CHAR(4), KIND CHAR(4),AreaID SMALLINT UNSIGNED, Count INTEGER, AVR7 INTEGER",
	my @cl = split(/\s*,\s*/, $p->{columns});
	for(my $i = 0; $i <= $#cl; $i++){
		$cl[$i] =~ s/\s+.*$//;
	}
	print CSV "#" . join(",", @cl) . "\n";

	while(<FD>){
		s/[\r\n]$//;
		#"RawRecord", columns => "RecordNo INTEGER, Date DATE, SourceID TINYINT, KIND TINYINT, AreaID SMALLINT, Count INTEGER, AVR7 INTEGER",
		if(/"/){	#  ,"Korea, South",3
			dp::dp substr($_, 0, 100). "\n";
			s/"([^",]+)(,\s*)([^"]+)"/$1;$3/g;
			dp::dp substr($_, 0, 100). "\n";
		}
		my ($area2, $area1, $lat, $long, @count) = split(/,/, $_);
		my $area_name = ($area2) ? join(";", $area2, $area1) : $area1;
		my $area_id = $AreaName->{$area_name}->{AreaID} // -1;
		dp::dp "[$area_name][" . $AreaName->{$area_name}->{AreaID} . "]\n" if($DEBUG);
		if($area_id == -1){
			dp::dp "Error at AreaCode : [$area_name]\n";
		}
		for(my $i = 0; $i <= $#count; $i++){
			$SNO++;
			my $v = $count[$i] // "Nan";
			my $line = join(",", $dates[$i], $source, $kind, $area_id, $v, 0);
			print CSV $line . "\n";
			dp::dp $line . "\n" if($VERBOSE && $i < 2); # if($SNO < $DISP_LINES) ;
		}
	}
	close(FD);
	close(CSV);

	#dp::dp Dumper $AreaName;
}

#
#	Load Master Tables
#
sub	load_master
{
	my ($p) = @_;

	if(defined $p){
		return &load_table($p, {hash => 1, disp => 0});
	}
	foreach my $p (@MASTER_DEFS){
		&load_table($p, {hash => 1, disp => 0});
	}
	return;
}

#
#
#
sub	load_table
{
	my ($p, $params) = @_;

	dp::dp "# $p->{table_name} . $p->{hash} \n";
	my $mp = $p->{hash} // "";
	%{$mp} = () if($mp);		# clear for recall

	my %dmparam = ();
	$params = \%dmparam if(!defined $params);
	dp::dp "load_table $p->{table_name}\n" if($DEBUG);
	foreach my $k (keys %$params){
		dp::dp "# params: $k $params->{$k}\n" if($DEBUG);
	}

	#
	#	Get Item Name from Definition
	#
	my @cl = split(/\s*,\s*/, $p->{columns});
	for(my $i = 0; $i <= $#cl; $i++){
		$cl[$i] =~ s/\s+.*$//;
	}
	#dp::dp "Columns for Query: " . join(",", @cl ) . "\n";

	#
	#	Execute Query
	#
	my $sql_str = "SELECT *  FROM $p->{table_name}";
	my $sth = $dbh->prepare($sql_str);
	dp::dp $sql_str . "\n" if($VERBOSE || $DEBUG > 2);
	$sth->execute();

	#
	#	Set Table Information
	#
	my @max_len = (0, 0);
	my $names = $sth->{NAME};
	my $numFields = $sth->{NUM_OF_FIELDS} - 1;
	$p->{FieldNames} = [];
	my $fnp = $p->{FiledNames};
	for my $i (0..$numFields){
		push(@$fnp, $$names[$i]);
		$max_len[$i] = 0;
	}

	#
	#	Load and Set Table
	#	
	while(my $ref = $sth->fetchrow_arrayref()) {
		my @vals = ();
		my $master_key = $$ref[1];			# error
		for my $i (0..$numFields){
			my $field = $$names[$i];
			my $v = $$ref[$i];
			$mp->{$master_key}->{$field} = $v if($mp && ($params->{hash} // "")) ;
			# dp::dp "## $p->{table_name} - $master_key - $field - $v : $mp->{$master_key}->{$field}\n";
			push(@vals, "$field = $v");
			if($DEBUG){
				my $len = length($v);
				$max_len[$i] = $len if($len > $max_len[$i]);
			}
		}
		print "> " . join("\t", @vals) . "\n" if(($params->{disp} // "") == 1);
		print join("\t", @$ref) . "\n" if(($params->{disp} // "") == 2);
		dp::dp "# " . join("\t", $master_key, @vals) . "\n" if($DEBUG > 2);
	}
	$sth->finish();
	dp::dp "MAX_LENGTH: " . join(", ", @max_len[0..$numFields]) . "\n" if($VERBOSE || $DEBUG > 2);
}

#
#	Get Record Number of the tabel
#
sub	record_number
{
	my ($dbh, $table_name) = @_;

	my $sql_str = "select TABLE_ROWS from information_schema.tables where table_name = '$table_name'";

	my @res = &DO($dbh, $sql_str, $DISP{silent});
	my $row = ($#res < 0) ? 0 : $res[0];
	#dp::dp "record_namer $table_name : $row \n";
	return $row;
}


#
#	Execute Sql Query
#
#	default => {disp_result => 1, disp_line_no => 0, disp_query => 0, disp_name => 0},
#
sub	DO
{
	my($dbh, $sql_str, $disp) = @_;
	
	$disp = $disp // $DISP{default};

	if($DEBUG){
		my ($package_name, $file_name, $line) = caller;
		dp::dp "Called from: $file_name: $line\n";
		dp::dp Dumper $disp;
	}

	dp::dp "# " .  $sql_str ."\n" if($disp->{disp_query} // "");
	my @result = ();
	if(!defined $sql_str){
		my ($package_name, $file_name, $line) = caller;
		dp::dp "may forgot verbose or sql_str: $file_name #$line\n";
		exit ;
	}
	
#	my $result = $dbh->do($sql_str);
#	if(! $result){
#		print $DBI::errstr
#	}

	my $sth = $dbh->prepare($sql_str);
	$sth->execute();
	if(! defined $sth->{NUM_OF_FIELDS}){
		dp::dp "no result\n" if($disp->{disp_result} // "");
		return;
	}
	my $numFields = $sth->{NUM_OF_FIELDS} - 1;
	my $names = $sth->{NAME};
	print join(" ", @$names) . "\n" if($disp->{disp_name} // "");

	my $rno = 0;
	while(my $row = $sth->fetchrow_arrayref()) {
		$rno++;
		my @w = ();
		for my $i (0..$numFields){
			push(@w, $$row[$i] // "");
		}
		#print "[$numFields] " . join(" " , @w) . "\n" if($verbose);
		my $rs = (($disp->{disp_line_no} // "" ) ? "$rno: " : "") . join(" " , @w) . "\n";
		print $rs if($disp->{disp_result} // "" );
		push(@result, join(" ", @w));
	}
	return (@result);
}

