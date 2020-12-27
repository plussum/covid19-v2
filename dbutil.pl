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
#	./dbutil..pl -create_table -build_master -reform_csv -insert_csv
#	./dbutil..pl CCSE-NC -reform_csv 
#	./dbutil..pl AreaInfo -table_info
#	./dbutil..pl DataSource -table_info --debug 2
#	./dbutil..pl AreaInfo -dump 
#
#
use	strict;
use warnings;
use DBI;
use Data::Dumper;

use config;
use dbinfo;		# DB_NAME, DB_PASSWORD, DB_USER, DB_PORT, WIN_PATH
use tbdef;
use dblib;
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

my $AreaName = $tbdef::AreaName;



#
#	COMMAND DEFINITION
#	order: 	execute order when multiple functions are called
#	name:  	name of fucntion
#	func:	address of function (subroutine)
#
#
my @PARAM_NAMES_LIST = (
	{order => "", name => "debug", 			func => ""},
	{order => "", name => "verbose", 		func => ""},

	{order => 10, name => "create_table", 	func => \&create_table},
	{order => 20, name => "delete_data", 	func => \&delete_data},
	{order => 30, name => "build_master", 	func => \&build_master},
	{order => 40, name => "load_master", 	func => \&load_master},
	{order => 50, name => "reform_csv", 	func => \&reform_csv},
	{order => 60, name => "insert_csv",		func => \&insert_csv},
	{order => 70, name => "dump", 			func => \&dump},
	{order => 80, name => "select", 		func => \&select},
	{order => 90, name => "table_info",		func => \&table_info},
);

my %PARAM_NAMES = ();
foreach my $k (@PARAM_NAMES_LIST){
	my $nm = $k->{name};
	$PARAM_NAMES{$nm} = 1;
}

#
#	because of sepalate files for table defintion , cannto set main functions from tbdef.pm 
#	comvert => &comv_ccse,
#
my %FUNCTION_LIST = (
	 "CCSE-NC" => \&comv_ccse,
);

foreach my $p (@tbdef::RECORD_DEFS){
	my $name = $p->{name};
	if($FUNCTION_LIST{$name}){
		$p->{comvert} = $FUNCTION_LIST{$name};
	}
}

#
#	Command Line Paramerts
#
#	-params  : set PARASMS{$_} = 1;
#	--params : set PARASMS{$_} = $ARGV[+1];
#

#	Argument handling
#
my %PARAMS = ();
for(my $i = 0; $i <= $#ARGV; $i++){
	$_ = $ARGV[$i];
	if(/-h/){
		&usage();
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
		if(defined $tbdef::MASTER_INDEX{$_} || defined $tbdef::RECORD_INDEX{$_}){
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
	&usage();
}

if(! defined $PARAMS{table_name}){
	$PARAMS{table_name} = "";
}

if($DEBUG){
	foreach my $k (keys %PARAMS){
		dp::dp "PARAMS: $k = $PARAMS{$k} \n";
	}
}

$dblib::DEBUG = $DEBUG;
$dblib::VERBOSE = $VERBOSE;


#
#	Connect to Database
#
my $dbh = DBI->connect($dsn, $user, $password, {RaiseError => 0, AutoCommit =>0}) || die $DBI::errstr;
dblib::DO($dbh, "USE $DB_NAME", $dblib::DISP{silent});

#
#	Execute instructed functions
#
my $table_name = $PARAMS{table_name};
foreach my $params (@PARAM_NAMES_LIST){
	next if(! ($params->{func} // ""));		# No function allocated (debug, verbose)
	
	my $pname = $params->{name};
	next if(! defined $PARAMS{$pname});		# Function is not instructed

	dp::dp "## $params->{name} $table_name\n" if($VERBOSE || $DEBUG);
	foreach my $p (@tbdef::MASTER_DEFS, @tbdef::RECORD_DEFS){
		next if($table_name && $p->{table_name} ne $table_name);

		dp::dp "## $params->{name} $params->{func}\n";

		$params->{func}->($p); 		#$p->{comvert}->($p);
	}
}

#
#	Dissconnect the DB
#
$dbh->disconnect();
exit 0;

###############################################################
#
#	Assigned functions
#
###############################################################
sub	usage
{
	my @w = ();
	foreach my $p (@PARAM_NAMES_LIST){
		 push(@w, "-" . $p->{name});
	}
	print "usage: table_name " . join(" | ", @w) . "\n";
	exit 1;
}

sub	remove_data
{
	my ($p) = @_;

	dblib::DO($dbh, qq{DELETE FROM $p->{table_name};});

	return 1;
}

sub	dump_table
{
	my ($p) = @_;

	dp::dp "##### dump_table\n";
	dblib::load_master($dbh);
	dblib::load_table($dbh, $p, {hash => 0, disp => 2});

	return 1;
}

sub	reform_csv
{
	my ($p) = @_;

	if($p->{comvert}){
		$p->{comvert}->($p);
	}

	return 1;
}

sub	table_info
{
	my($p) = @_;

	my $table_name = $p->{table_name};
	dp::dp "$table_name\n" if($DEBUG);

	my @rows = dblib::DO($dbh, "select TABLE_ROWS from information_schema.tables where table_name = '$table_name';", $dblib::DISP{silent});

	print "$table_name ( $rows[0]records )\n";
	dblib::DO($dbh, "select * from INFORMATION_SCHEMA.TABLES where TABLE_SCHEMA='$DB_NAME' and TABLE_NAME='$table_name'", $dblib::DISP{name});
	print "-" x 20 . "\n";
	my $sql_str = "desc $p->{table_name}";
	dblib::DO($dbh, "desc $table_name");
	print "\n";

	return 1;
}

sub	create_table
{
	my($p) = @_;

	dblib::DO($dbh, "DROP TABLE IF EXISTS $p->{table_name}", $dblib::DISP{verbose});
	dblib::DO($dbh, "CREATE TABLE $p->{table_name} ($p->{columns})", $dblib::DISP{verbose});
	dblib::DO($dbh, "SHOW TABLES from $DB_NAME;", $dblib::DISP{verbose});
	dblib::DO($dbh, "DESC $p->{table_name};", $dblib::DISP{verbose});

	return 1;
}

sub	build_master
{
	my($p) = @_;
	
	if($p->{type} eq "master"){
		&insert_table($p); 
	}
}

sub	insert_csv
{
	my($p) = @_;
	
	if($p->{type} eq "transaction"){
		&insert_table($p); 
	}
}

sub	dump
{
	my($p) = @_;

	dblib::load_master($dbh);
	dblib::load_table($dbh, $p, {hash => 0, disp => 2});

	return 1;
}


#
#	Insert values to Table
#
sub	insert_table
{
	my($p) = @_;

	my $SNO =  dblib::record_number($dbh, $p->{table_name});
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
sub	insert_vals
{
	my ($p, $dbh, $valp) = @_;

	my $sql_str = "INSERT INTO  $p->{table_name} VALUES " . join(",", @$valp);
	dp::dp $sql_str . "\n" if($DEBUG > 2);
	my $sth = $dbh->prepare($sql_str);
	my $rt = $sth->execute() || die $DBI::errstr;
	#dp::dp "#### Result ($rt)\n";
	$dbh->commit();
}

###################################################
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
	my $kind = $p->{kind};

	my $filed_names = $p->{hash}->{filed_names};

	dblib::load_master($dbh);
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

