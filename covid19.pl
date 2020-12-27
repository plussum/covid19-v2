#!/usr/bin/perl
#
#
#
use strict;
use warnings;

use DBI;
use Data::Dumper;

use dbinfo;		# DB_NAME, DB_PASSWORD, DB_USER, DB_PORT, WIN_PATH
use tbdef;
use dblib;
use dp;

#
#	Initial Values
#
my $DEBUG = 0;
my $VERBOSE = 0;
my $SEARCH_KEY = "";

#
#	Set Database Parameter for connect
#
my $DB_NAME = $dbinfo::DB_NAME;
my $password  = $dbinfo::DB_PASSWORD;
my $user = $dbinfo::DB_USER;
my $port = $dbinfo::DB_PORT;
my $dsn = "dbi:mysql:database=$DB_NAME;host=localhost;port=$port";

my @PARAM_NAMES_LIST = (
	{order => "", name => "debug", 			func => ""},
	{order => "", name => "verbose", 		func => ""},
);
my %PARAM_NAMES = ();
foreach my $k (@PARAM_NAMES_LIST){
	my $nm = $k->{name};
	$PARAM_NAMES{$nm} = 1;
}

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

$dblib::DEBUG = $DEBUG;
$dblib::VERBOSE = $VERBOSE;

#
#	Connect to Database
#
my $dbh = DBI->connect($dsn, $user, $password, {RaiseError => 0, AutoCommit =>0}) || die $DBI::errstr;
dblib::DO($dbh, "USE $DB_NAME", $dblib::DISP{silent});

my $p = $tbdef::RECORD_DEFS[0];
dp::dp "### " . Dumper %$p;

&dump_record($p);

exit;

#
#
#
sub	dump_record
{
	my ($p) = @_;

	dp::dp "## dump_record [$p]\n";
	my $params = {hash => 0, disp => 2};
	my $join = {
		 AreaID => {table => "AreaInfo", column => "AreaID", },
	};
	dblib::load_master($dbh);
	dblib::load_table($dbh, $p, $params, $join);
}
