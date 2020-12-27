#!/usr/bin/perl
#
#
#
use strict;
use warnings;

use DBI;
use Data::Dumper;

use config;
use dbinfo;		# DB_NAME, DB_PASSWORD, DB_USER, DB_PORT, WIN_PATH
use tbdef;
use dblib;

use csvlib;
use dp;
use csvgpl;

#
#	Initial Values
#
my $DEBUG = 3;
my $VERBOSE = 2;
my $SEARCH_KEY = "";

#
#	Set Database Parameter for connect
#
my $DB_NAME = $dbinfo::DB_NAME;
my $password  = $dbinfo::DB_PASSWORD;
my $user = $dbinfo::DB_USER;
my $port = $dbinfo::DB_PORT;
my $dsn = "dbi:mysql:database=$DB_NAME;host=localhost;port=$port";

#
#	Definition of data
#
my $DLM = ",";
my $EXCLUSION = ""; 
my $CCSE_BASE_DIR = "/home/masataka/who/COVID-19/csse_covid_19_data/csse_covid_19_time_series";

my $PARAMS = {			# MODULE PARETER        $mep
	comment => "**** CCSE PARAMS ****",
	src => "Johns Hopkins CSSE",
	src_url => "https://github.com/beoutbreakprepared/nCoV2019",
	prefix => "jhccse_",
	src_file => {
		NC => "$CCSE_BASE_DIR/time_series_covid19_confirmed_global.csv",
		ND => "$CCSE_BASE_DIR/time_series_covid19_deaths_global.csv",
		CC => "$CCSE_BASE_DIR/time_series_covid19_confirmed_global.csv",
		CD => "$CCSE_BASE_DIR/time_series_covid19_deaths_global.csv",
		NR  => "$CCSE_BASE_DIR/time_series_covid19_recovered_global.csv",
		CR => "$CCSE_BASE_DIR/time_series_covid19_recovered_global.csv",
	},
	base_dir => $CCSE_BASE_DIR,

	#new => \&new,
	#aggregate => \&aggregate,
	#download => \&download,
	#copy => \&copy,
	DLM => $DLM,

	SORT_BALANCE => {		# move to config.pm
		NC => [0, 0],
		ND => [0, 0],
	},
#	THRESH => {		# move to config.pm
#		NC => 0,
#		ND => 1,
#	},


	AGGR_MODE => {DAY => 1, POP => 1},									# Effective AGGR MODE
	#MODE => {NC => 1, ND => 1, CC => 1, CD => 1, NR => 1, CR => 1},		# Effective MODE

	graph_mode => {			# FUNCTION PARAMETER    $funcp
		NC => [
	   		{ext => "#KIND# #SRC# 1", start_day => 0,  end_day => 30, lank =>[0, 29] , exclusion => "Others", target => "", label_skip => 3, graph => "lines", term_ysize => 600},
    		{ext => "#KIND# #SRC# 2", start_day => 0,  end_day => 45, lank =>[0, 29] , exclusion => "Others", target => "", label_skip => 3, graph => "lines", term_ysize => 600},
	   		{ext => "#KIND# #SRC# 3", start_day => 0,  end_day => 60, lank =>[0, 29] , exclusion => "Others", target => "", label_skip => 3, graph => "lines", term_ysize => 600},

			{ext => "#KIND# Taiwan (#LD#) #SRC#", start_day => 0, lank =>[0, 999], exclusion => $EXCLUSION, target => "Taiwan", label_skip => 3, graph => "lines"},
			{ext => "#KIND# China (#LD#) #SRC#", start_day => 0,  lank =>[0, 19], exclusion => $EXCLUSION, target => "China", label_skip => 3, graph => "lines"},
		],
		ND => [
			{ext => "#KIND# Taiwan (#LD#) #SRC#", start_day => 0, lank =>[0, 999], exclusion => $EXCLUSION, target => "Taiwan", label_skip => 3, graph => "lines"},
			{ext => "#KIND# China (#LD#) #SRC#", start_day => 0,  lank =>[0, 19], exclusion => $EXCLUSION, target => "China", label_skip => 3, graph => "lines"},
		],
		CC => [
		],
		CD => [
		],
		FT => {
			EXC => "Others",  # "Others,China,USA";
			ymin => 10,
			average_date => 7,
			graphp => [
			],
		},
		ERN => {
			EXC => "Others",
			ip => 5,
			lp => 8,
			average_date => 7,
			graphp => [
			],
		},
		KV => {
			EXC => "Others",
			graphp => [
			],
		},
	},
};

#
#	Parameter of command line
#
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
#dp::dp "### " . Dumper %$p;

#&dump_record($p);
&draw_graph($p);

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


sub	draw_graph
{
	my $mep = $PARAMS;
	my $graph_set = {
		src => "ccse",			# $DATA_SOURCE,
		mode => "NC",			# $MODE,
		sub_mode => "COUNT",	# $SUB_MODE,
		aggr_mode => "DAY",		# $AGGR_MODE,

		mep => $mep,
		#funcp => $mep->{$SUB_MODE},	# May not need 

		src_file 	=> "srcfile.csv",# $SRC_FILE,
		stage1_csvf => "stage1.csv", # $STG1_CSVF,
		stage2_csvf => "stage2.csv", # $STG2_CSVF,
		htmlf => "covdbindex.html",,
		dlm => $DLM,
	};

	&dwg($graph_set);
}

sub	dwg
{
	my ($graph_set) = @_;
	my $mep = $graph_set->{mep};

	#dp::dp "daily \n" ; # Dumper($graph_set);

	#
	#	Load CCSE CSV
	#
	#my $aggr_func = $mep->{aggregate};
	#my ($colum, $record , $start_day, $last_day) = $aggr_func->($graph_set);

	#
	#	グラフとHTMLの作成
	#

	#my $prefix = $mep->{prefix};
	my $mode = $graph_set->{mode};
	my $aggr_mode = $graph_set->{aggr_mode};
	my $name = join("-", csvlib::valdef($config::MODE_NAME->{$mode}, " $mode "), $graph_set->{sub_mode}, $aggr_mode) ;
	dp::dp "[$mode] $name\n" if($config::VERBOSE);

	my $m = $mep->{AGGR_MODE}{$aggr_mode} // 0;
	$name .= "*$m" . "days" if($m > 1);
	my $csvlist = {
		name => 	$name, 
		csvf => 	$graph_set->{stage1_csvf}, 
		htmlf => 	$graph_set->{htmlf},
		kind => 	$graph_set->{mode},
		src_file => $graph_set->{src_file},
		src => 		$mep->{src},
		src_url => 	$mep->{src_url},
	};
	my $graphp = $mep->{graph_mode}{$mode};
	dp::dp ("#" x 10)  . "\n";
	dp::dp "$graphp $mode \n";
	dp::dp. Dumper @$graphp . "\n";

	#dp::dp Dumper $graphp;

	my $params = {
		debug => 	$DEBUG,
		verbose => 	$VERBOSE,
		graph_set => $graph_set,
		src => 		$graph_set->{src},
		clp => 		$csvlist,
		gplp => 	$graphp,
		mep => 		$mep,
		gplp => 	$graphp,	# $graph_set->{funcp}{graphp},
		aggr_mode => $graph_set->{aggr_mode},
		csv_aggr_mode => ($mep->{csv_aggr_mode} // ""),
		sort_balance => 0.5,
		sort_wight => 0.01,
	};
	#dp::dp "### daily: " . Dumper(%params) . "\n";

	# dp::dp Dumper %$params;
	csvgpl::csvgpl($params);
}
