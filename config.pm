#
#
#
package config;
use Exporter;
@ISA = (Exporter);
@EXOIORT = qw(config);

use strict;
use warnings;
#use ccse;
#use who;

our $DB_PASSWORD = 'S=NX8ncM1Ko0lmT1R-dEHmly';
our $VERBOSE = 1;
our $DEBUG = 0;
our $CODE_PATH = "/home/masataka/who/src";
#our $WIN_PATH = "/mnt/f/cov/plussum.github.io";
our $WIN_PATH = "/mnt/c/cov/plussum.github.io";
our $HTML_PATH = "$WIN_PATH/HTML";
our $CSV_PATH  = "$WIN_PATH/CSV";
our $PNG_PATH  = "$WIN_PATH/PNG";
our $PNG_REL_PATH  = "../PNG";		# HTML からの相対パス
our $CSV_REL_PATH  = "../CSV";		# HTML からの相対パス

our $WHO_INDEX = "who_index.html";
our $RT_IP = 5;
our $RT_LP = 8;
our $THRESH_FT = {NC => 9, ND => 3, NR => 3};		# 

our $NO_DATA = "NaN";

our $POPF = "$WIN_PATH/pop.csv";
#our $POPF_JP = "$WIN_PATH/popjp.txt";
#our $POPF_US = "$WIN_PATH/popus.txt";
our $POP_BASE = 100 * 1000;			# 10万人当たりのケース数
our $POP_THRESH = 100 * 1000;		# 人口が少ないと振れ幅が大きいので、この人口より少ない国は対象外にする


our $DLM = "\t";
our $DLM_OUT = "\t";

our $MODE_NAME = {
	NC => "NEW CASES",
	ND => "NEW DEATHS",
	NR => "NEW RECOVERS",
	CC => "CCM CASES",
	CD => "CCM DEATHS",
	CR => "CCM RECOVERS",
};

our %SORT_BALANCE = (
		NC => [0.7,  0.1],		# 0.7 -> 0.9 70% のところから対象として、1日ごとに0.1を累算して評価 
		ND => [0.7,  0.1],
		NR => [0.7,  0.1],
		CC => [0.99, 0.1],
		CD => [0.99, 0.1],
		CR => [0.99, 0.1],

		ERN => [0.99, 0.1],
		FT => [0.5, 0.3],
		KV => [0.99, 0.1],
);

our %THRESH = ( 	# move to config.pm
	NC => 0,		# 6
	ND => 0,		# 6
	NR => 0,
	CC => 0,
	CD => 0,
	CR => 0,

	ERN => 0,
	FT => 0,
	KV => 0,
);

my $CCSE_BASE_DIR = "/home/masataka/who/COVID-19/csse_covid_19_data/csse_covid_19_time_series";

our $CSS = << "_EOCSS_";
    <meta charset="utf-8">
    <style type="text/css">
    <!--
        span.c {font-size: 12px;}
    -->
    </style>
_EOCSS_

our $CLASS = "class=\"c\"";

1;	
