#!/usr/bin/perl
#
#
package csvgpl;
use Exporter;
@ISA = (Exporter);
@EXOIORT = qw(csvgpl);

use strict;
use warnings;
use Data::Dumper;
use config;
use csvlib;
use dp;

my $DEBUG = 0;
my $VERBOSE = 0;
my $WIN_PATH = "";
my $NO_DATA = "NaN";

my %CNT_POP = ();

my $DEFUALT_SORT_BALANCE = 0.5;		# ALL = 0; 0.7 = 後半の30%のデータでソート
my $DEFUALT_SORT_WEIGHT  = 0.05;	# 0: No Weight, 0.1: 10%　Weight -0.1: -10% Wight
my $SORT_BALANCE = 0;		# ALL = 0; 0.7 = 後半の30%のデータでソート
my $SORT_WEIGHT  = 0;	# 0: No Weight, 0.1: 10%　Weight -0.1: -10% Wight

my $DLM = $config::DLM;

sub	csvgpl
{
	my ($csvgplp) = @_;
 
	$DEBUG = $csvgplp->{debug}// $DEBUG;
	$VERBOSE = $csvgplp->{verbose} // $VERBOSE;

	dp::dp "($DEBUG, $VERBOSE)\n";

	if($DEBUG){
		print "###### csvgpl #####\n";
		open(PARA, "> param.txt") || die "param.txt";
		print PARA Dumper $csvgplp ;
		close(PARA);
	}
	my $aggr_mode = $csvgplp->{aggr_mode} // "";		# Added 05/03 for Population
	if($aggr_mode eq "POP"){
		#dp::dp( "###### $aggr_mode: " . $csvgplp->{src} . "\n") if(1 || $DEBUG > 1);
		csvlib::cnt_pop(\%CNT_POP);
	}

	$WIN_PATH = $config::WIN_PATH;
	my $PNG_PATH = $config::PNG_PATH; 
	my $PNG_REL_PATH = $config::PNG_REL_PATH; 
	my $IMG_PATH = $config::PNG_REL_PATH;
	my $CSV_REL_PATH = $config::CSV_REL_PATH; 
	my $CSS = $config::CSS;
	my $class = $config::CLASS;

	my $clp = $csvgplp->{clp};
	my $gplp = $csvgplp->{gplp};
	my $mep = $csvgplp->{mep};
	my $graph_set = $csvgplp->{graph_set};

	my $src_url = $clp->{src_url};
    my $src_ref = "<a href=\"$src_url\">$src_url</a>";
	my $TBL_SIZE = 10;
	my $mode = $graph_set->{mode};
	my $sub_mode = $graph_set->{sub_mode};

	$DLM = csvlib::valdef($graph_set->{dlm}, $DLM);

	my @references = (defined $mep->{references}) ? (@{$mep->{references}}) : ();
	#dp::dp "REFERENCE: [" . join(",", @references) . "]\n";
 

	$SORT_BALANCE =  $DEFUALT_SORT_BALANCE;
	$SORT_WEIGHT  =  $DEFUALT_SORT_WEIGHT;
	dp::dp "### $mode\n";
	if(defined $mep->{SORT_BALANCE}{$mode}){
		my $sbp = $mep->{SORT_BALANCE}{$mode};
		$SORT_BALANCE = $sbp->[0];
		$SORT_WEIGHT = $sbp->[1];
		#dp::dp "MEP\n";
	}
	elsif(defined $config::SORT_BALANCE{$sub_mode}){
		my $sbp = $config::SORT_BALANCE{$sub_mode};
		$SORT_BALANCE = $sbp->[0];
		$SORT_WEIGHT = $sbp->[1];
		#dp::dp "CONFIG:SUBMODE\n";
	}
	elsif(defined $config::SORT_BALANCE{$mode}){
		my $sbp = $config::SORT_BALANCE{$mode};
		$SORT_BALANCE = $sbp->[0];
		$SORT_WEIGHT = $sbp->[1];
		#dp::dp "CONFIG:MODE\n";
	}
	dp::dp "SORT_BALANCE($mode): $SORT_BALANCE, $SORT_WEIGHT\n";

	dp::dp "TITLE: $clp->{name} \n $clp->{htmlf}\n" if($VERBOSE);
	open(HTML, "> $clp->{htmlf}") || die "Cannot create file $clp->{htmlf}";
	print HTML "<HTML>\n";
	print HTML "<HEAD>\n";
	print HTML "<TITLE> $clp->{name} </TITLE>\n";
	print HTML $CSS;
	print HTML "</HEAD>\n";
	print HTML "<BODY>\n";
	my $now = csvlib::ut2d4(time, "/") . " " . csvlib::ut2t(time, ":");

	#print HTML "SOURCE: <a href = \"$WHO_PAGE\"> WHO situation Reports</a>\n<br>\n";
#
	my $graph_no = 0;
	foreach my $gplitem (@$gplp){
		dp::dp "## gplitem: $gplitem\n";
		if($gplitem->{ext} eq "EOD"){
			print "#### EOD ###\n";
			last;
		}
		$graph_no++;
		my ($png, $plot, $csv, @legs) = &csv2graph($graph_no, $clp->{csvf}, $PNG_PATH, $clp->{name}, $gplitem, $clp, $mep, $aggr_mode, $graph_set);
		next if(!$png);

		my $THRESH_SIZE = 1 * 1024;
		
		if(csvlib::file_size($config::PNG_PATH . "/$png") < $THRESH_SIZE){
			die "Failed to create file\n" . "$png : " . csvlib::file_size($config::PNG_PATH . "/$png");
		}

		###########
		print HTML "<!-- " . $gplitem->{ext} . " -->\n";
		print HTML "<span class=\"c\">$now</span><br>\n";
		print HTML "<img src=\"$IMG_PATH/$png\">\n";
		print HTML "<br>\n";
		print HTML "<span $class> Data Source $src_ref </span>\n";
		print HTML "<hr>\n";
		print HTML "<TABLE>";
		for(my $l = 0; $l <= $#legs; $l++){
			$_ = $legs[$l];
			print HTML "<TR>" if(($l % $TBL_SIZE) == 0);
			print HTML "<TD> <span class=\"c\"> $_ </span> </TD>";
			print HTML "</TR>\n" if($l == $#legs || ($l % $TBL_SIZE) == ($TBL_SIZE - 1));
		}
		print HTML "</TABLE>";
		############

		print HTML "<span $class>";

		my $csvf = $clp->{csvf};
		my $srcf = $clp->{src_file} // "";
		dp::dp $srcf , "\n" if($srcf && $VERBOSE);
		$csvf =~ s#.*/##;
		$srcf =~ s#.*/##;
		my @refs = (join(":", "PNG", $PNG_REL_PATH, $png),
					join(":", "CSV", $PNG_REL_PATH, $csv),
					join(":", "PLT", $PNG_REL_PATH, $plot),
					join(":", "REF", $CSV_REL_PATH, $csvf),
					join(":", "SRC", $CSV_REL_PATH, $srcf),
		);
		#dp::dp "##### SRC:[$srcf][" . $clp->{srcf} . "]\n";
		#dp::dp "##### refs \n" . Dumper(@refs) . "]\n";
		foreach my $r (@refs){
			my ($tag, $path, $fn) = split(":", $r);
			#dp::dp "r:[$r]  $tag, $path, $fn\n";
			#dp::dp "$tag:<a href=\"$path/$fn\">$fn</a>\n"; 
			if($fn){
				print HTML "$tag:<a href=\"$path/$fn\" target=\"blank\">$fn</a>\n"; 
			}
			else {
				print HTML "$tag:\n"; 
			}
		}
		print HTML "<br>\n" if($#references >= 0);
		foreach my $r (@references){
			#dp::dp "REF " . $#references . ":[$r]\n";
			if($r =~ /^http/){
				print HTML "REF <a href=\"$r\" target=\"blank\">$r</a><br>\n"; 
			}
			else {
				print HTML "REF $r<br>\n"; 
			}
		}
		print HTML "</span>\n";
		print HTML "<br><hr>\n\n";
	}
	print HTML "</BODY>\n";
	print HTML "</HTML>\n";
	close(HTML);
}

#
#
#
sub	csv2graph
{
	my ($graph_no, $csvf, $png_path, $kind, $gplitem, $clp, $mep, $aggr_mode, $graph_set) = @_;

	dp::dp join(", ", $gplitem->{ext}, $gplitem->{start_day}, $gplitem->{lank}[0], $gplitem->{lank}[1], $gplitem->{exclusion}, 
			"[" . $clp->{src} . "]", $mep->{prefix}), "\n" if($VERBOSE);
	
	my $src = $gplitem->{src} // "";
	my $ext = sprintf("#%02d ", $graph_no) . $mep->{prefix} . " " . $gplitem->{ext};
	#dp::dp $ext . ":$kind\n";
	my $mode = $graph_set->{mode};
	my $sub_mode = $graph_set->{sub_mode};
	#my $src = $graph_set->{src};

	my $dbh = $graph_set->{dbh};
	my $tabledef = $graph_set->{tabledef};

	$ext =~ s/#KIND#/$kind/;
	$ext =~ s/#SRC#/$src/;
	$ext .= " rl-avr " . $gplitem->{avr_date} if(defined $gplitem->{avr_date});
	#dp::dp $ext . "\n";
	my $fname = $ext;
	$fname =~ s/#LD#//;
	$fname =~ s#/#-#g;
	$fname =~ s/[^0-9a-zA-Z]+/_/g;
	$fname =~ s/^_//;

	#
	#	Mode check
	#
	my %mode_check = ( src => $graph_set->{src}, mode => $mode, sub_mode => $sub_mode, aggr_mode => $aggr_mode);
	foreach my $mn ("src", "mode", "sub_mode", "aggr_mode"){
		my $ml = csvlib::valdef($gplitem->{$mn}, "");
		#dp::dp "MODE_CHECK[$mn]: ml:$ml ($mode, $sub_mode, $aggr_mode)\n";
		if($ml =~ /^!/){
			#dp::dp "EXCLUSE :", join(", ", index("-", $ml) , index($ml, $mode_check{$mn}), index("$src-$ml", $mode_check{$mn})) . "\n";
			if(index($ml, "-") < 0){
				#dp::dp "EXCLUSE+[$ml]$mode_check{$mn} ", index($ml, $mode_check{$mn}) . ", " . index("$src-$ml", $mode_check{$mn}) . "\n";
				return("") if(index($ml, $mode_check{$mn}) >= 0 );	# mode is in the excluse list
			}
			else {
				#dp::dp "EXCLUSE-:$ml:$mode_check{$mn} ", index($ml, $mode_check{$mn}) . ", " . index("$src-$ml", $mode_check{$mn}) . "\n";
				return("") if(index($ml, "$src-" . $mode_check{$mn}) >= 0 );	# "ccse-ND", "ccse-FT", mode is in the excluse list
			}
		}
		else {
			#dp::dp "TARGET :", index($ml, $mode_check{$mn}) . "\n";
			next if(!$ml || $ml eq "*"); 					# Undef or "*"
			return("") if(index($ml, $mode_check{$mn}) < 0 );	# no mode in target list
		}
	}
	#dp::dp "TARGET:[$src, $mode, $sub_mode, $aggr_mode]\n\n";
	

	my $plot_pngf = $png_path . "/" . $fname . ".png";
	my $plot_cmdf = $png_path . "/" . $fname . "-plot.txt";
	my $plot_csvf = $png_path . "/" . $fname . "-plot.csv.txt";
	my $plot_valf = $png_path . "/" . $fname . "-plot.val.txt";
	if($DEBUG){
		dp::dp $plot_pngf , "\n" ;		#### 
		dp::dp "SRC CSV [$csvf]\n" ;
		dp::dp "DST CSV [$plot_csvf]\n";
		dp::dp "DST CMD [$plot_cmdf]\n";
		dp::dp "DST PNG [$plot_pngf]\n";
	}
	my $style = csvlib::valdef($gplitem->{graph}, "lines");

	my $thresh_mode = csvlib::valdef($config::THRESH{$mode}, 0);
	$thresh_mode = csvlib::valdef($mep->{THRESH}{$mode}, $thresh_mode);
	$thresh_mode = csvlib::valdef($gplitem->{thresh}, $thresh_mode);
	#dp::dp "thresh_mode[$thresh_mode]\n";

#	$plot_pngf =~ s/[\(\) ]//g;
	#
	#	Read CSV DATA
	#
	my @DATE_LABEL = ();
	my @DATA = ();
	my $DATE_COL_NO = 2;

######### CSV
#	dp::dp "$csvf\n";
#
#	open(CSV, $csvf) || die "cannot open $csvf";
#	my $cls = <CSV>; 
#	$cls =~ s/,*[\r\n]+$//;
#	for(split(/$DLM/, $cls)){
#		# s#[0-9]{4}/##;
#		push(@DATE_LABEL,  $_);
#	}
#	for(my $i = 0; $i <$DATE_COL_NO; $i++){
#		shift(@DATE_LABEL);
#	}
#	#dp::dp "DATE_LABE:  $#DATE_LABEL: " . join(",", @DATE_LABEL), "\n" ;# if($DEBUG > 2);
#
#	my $DATE_NUMBER = $#DATE_LABEL;
#	my $LAST_DATE = $DATE_LABEL[$DATE_NUMBER];
#	$ext =~ s/#LD#/$LAST_DATE/;
#
#	my $l;
#	for($l = 0; <CSV>; $l++){
#		chop;
#		my @w = split(/$DLM/, $_); 
#		for(my $i = 0; $i <= $#w; $i++){
#			$DATA[$l][$i] = $w[$i];
#		}
#	}
#	close(CSV);
#
######## SQL
	my $first_date_str = dblib::first_date($dbh, $tabledef, $graph_set->{src}, $graph_set->{mode});
	my $final_date_str = dblib::final_date($dbh, $tabledef, $graph_set->{src}, $graph_set->{mode});
	my $first_date = csvlib::date2ut($first_date_str);
	my $final_date = csvlib::date2ut($final_date_str);


	my $date_from = $gplitem->{start_day} // 0;
	my $date_till = $gplitem->{end_day} // 0;

	dp::dp "Date: $date_from, $date_till\n";

	$date_till = $final_date if($date_till == 0);
	$date_from = $first_date if($date_from == 0);
	
	$date_from = $final_date + $date_from * 60 * 60 if($date_from =~ /^[-]?[^\d]+$/);
	$date_till = $date_from + $date_till * 60 * 60 if($date_till =~ /^[+]?[^\d]+$/);

	my $date_from_str = csvlib::ut2d($date_from, "-");
	my $date_till_str = csvlib::ut2d($date_till, "-");
	my $DATE_NUMBER = int((0.99999999 + $date_till - $date_from) / (24 * 60 * 60));

	dp::dp "Date: " . join(",", $date_from, $date_till, $date_from_str, $date_till_str) . "\n";
	my %local_params = %{$graph_set->{load_params}};
	dp::dp "Query: $local_params{query}\n";
	$local_params{query} =~ s/#DATE_FROM#/$date_from_str/;	
	$local_params{query} =~ s/#DATE_TILL#/$date_till_str/;	
	dp::dp "Query: $local_params{query}\n";

	my $START_DATE = $date_from;
	my $LAST_DATE = $date_till;
	# $LAST_DATE = $DATES[$#DATES];
	#dp::dp "LAST_DATE: " . join("," , $LAST_DATE, $end_day, $#DATES) .  "\n";
	#dp::dp join(",", @DATES) . "\n";
	if($style eq "boxes"){
		$START_DATE = &date_offset($START_DATE, -24 * 60 * 60);
		$LAST_DATE  = &date_offset($LAST_DATE,   24 * 60 * 60);
	}

	my %RESULTS = ();
	my @RESULT_ITEMS = ();
	$local_params{result_hash}  = \%RESULTS;
	$local_params{result_items}  = \@RESULT_ITEMS;

	dblib::load_table($graph_set->{dbh}, $graph_set->{tabledef}, \%local_params, $graph_set->{join});
	
	my $COUNTRY_NUMBER = 99999999;

	if($DEBUG > 1){
		dp::dp "CSV: DATA\n";
		#dp::dp "     :" , join(",", @DATE_LABEL) , "\n";
		#dp::dp "### ITEMS : " . join(",", keys %RESULTS) . "\n";
		foreach my $k (@RESULT_ITEMS){
			dp::dp "### RESULT: $k " . join(",", keys %$k) . "\n";
		}

		dp::dp "-" x 20 , "\n";
	}
	exit;

	#
	#	Graph Parameter set
	#
	my $std = (defined $gplitem->{start_day}) ? $gplitem->{start_day} : 0;
	if($std =~ /[0-9]+\/[0-9]+/){
		my $n = csvlib::search_list($std, @DATE_LABEL);
		#dp::dp ">>>> $std: $n " . $DATE_LABEL[$n-1] . "\n";
		#dp::dp ">>> " . join(",", @DATE_LABEL) . "\n";
		if($n && $n > 0){
			$std = $n - 1;
		}
	}
	elsif($std < 0){
		$std = -$std;
		$std = $DATE_NUMBER - $std
	}	

	my $end_day = (defined $gplitem->{end_day}) ? $gplitem->{end_day} : ($DATE_NUMBER + 1);
	#dp::dp "END_DAY: $end_day  $std\n";
	if($end_day =~ /[0-9]+\/[0-9]+/){
		my $n = csvlib::search_list($end_day, @DATE_LABEL);
		#dp::dp ">>>> $std: $n " . $DATE_LABEL[$n-1] . "\n";
		#dp::dp ">>> " . join(",", @DATE_LABEL) . "\n";
		if($n && $n > 0){
			$std = $n - 1;
		}
	}
	elsif($end_day >= ($DATE_NUMBER - $std)){
		$end_day = $DATE_NUMBER - $std ;
	}	
	#dp::dp "END_DAY: $end_day\n";

	my $end = $DATE_NUMBER;
	my $dates = $end - $std + 1;
	my $tgcs = $gplitem->{lank}[0];						# 対象ランク
	my $tgce = $gplitem->{lank}[1];						# 対象ランク 
	#dp::dp "[$tgcs:$tgce]\n";
	$tgce = $COUNTRY_NUMBER if($tgce > $COUNTRY_NUMBER);

	my @exclusion = split(/,/, $gplitem->{exclusion});	# 除外国
	my @target = split(/,/, csvlib::valdefs($gplitem->{target}, ""));			# 明示的対象国
	my @add_target = split(/,/, csvlib::valdefs($gplitem->{add_target}, ""));	# 追加する対象国

	dp::dp "EXCLUS: " , $gplitem->{exclusion}, "  " . $#exclusion, "\n" if($DEBUG > 1);
	dp::dp "TARGET: " , $gplitem->{target}, "  " . $#target, "\n" if($DEBUG > 1);
	dp::dp "ADD_TARGET: " , $gplitem->{add_target}, "  " . $#add_target, "\n" if($DEBUG > 1);

	my @DATES = @DATE_LABEL[$std..$end];
	#dp::dp "DATES: ", join(",", @DATES) , "\n";# if($DEBUG > 1);

	my @LEGEND_KEYS = ();
	my $MAX_COUNT = 0;
	my %CTG = ();
	my %COUNT_D = ();
	my %TOTAL = ();
	my @COUNTRY = ();
	my @UNDEF_POP = ();
	my $avr_date = csvlib::valdef($gplitem->{avr_date});
	#dp::dp "mode: " . $clp->{kind} . "\n";
	my $count_mode = ($clp->{kind} =~ /^N[A-Z]/) ? "DAY" : "CCM";					# 日次/累計 Cumelative

	#
	#	ソート用の配列の作成
	#
	my $pop_thresh = (defined $mep->{POP_THRESH}) ? $mep->{POP_THRESH} : $config::POP_THRESH;
	for(my $cn = 0; $cn <= $COUNTRY_NUMBER; $cn++){
		my $country = $DATA[$cn][0];
		next if(! $country);

		#
		#	POP	rewrite data with POP_COUNT
		#
		my $POP_UNIT = 10^10-1;
		if($aggr_mode eq "POP"){
			if(! defined $CNT_POP{$country}){
				push(@UNDEF_POP, $country);

			}
			if(defined $CNT_POP{$country} && csvlib::num($CNT_POP{$country}) > $pop_thresh){
				$POP_UNIT = $CNT_POP{$country} / $config::POP_BASE;
			}
		}

		#
		#
		#dp::dp join(",", @{$DATA[$cn]}) . "\n" if($country =~ /Sint/ || $country =~ /Japan/);
		my $tl = 0;
		my $ccmt = 0;
		my $swc = 0;
		my $sw_start = int($dates * $SORT_BALANCE + 0.0);	#### 2020.07.14 0.5 -> 0
		if($graph_set->{sub_mode} eq "FT"){
			my $as = scalar(@{$DATA[$cn]});					####	for FT 2020.06.27 データ数が違うので配列サイズで計算
			$sw_start = int($as * $SORT_BALANCE + 0.0);		####	for FT 2020.06.27
			 #dp::dp "$dates $as * $SORT_BALANCE => $sw_start\n";
		}

		#dp::dp "DATES: $std:$DATE_COL_NO dates:$dates end_day:$end_day std:$std sw_start:$sw_start\n";
		#for(my $dn = 0; $dn <= $dates; $dn++){
		for(my $dn = 0; $dn <= $end_day; $dn++){
			my $p = $dn+$std+$DATE_COL_NO;
			my $c = csvlib::valdef($DATA[$cn][$p], 0);
			#dp::dp "$cn:$p -> $c\n" if($c =~ /NaN/);
			#if($c =~ /[^0-9-\.]/){
			#	dp::dp "Numeric Check: ($dn:$std:$p:$c:$csvf)\n";
			#}
			#if($c =~ /^[^0-9]+$/){			# Patch for bug of former data, may be
			if($c =~ /[^0-9\-\.]/){
				dp::dp "DATA ERROR at $country($dn) $c\n" ;#if($DEBUG);
				$tl = -1;
				last;
			}
			if($avr_date){
				my $pp = $p - $DATE_COL_NO;
				my $s = ($pp > $avr_date) ? ($p - $avr_date + 1): $DATE_COL_NO;
				my $e = $p;
				$c = csvlib::avr($DATA[$cn], $s, $e);
			}
			if($count_mode eq "CCM"){
				$ccmt += $c;
				$c = $ccmt;
			}
			if($aggr_mode eq "POP"){
				$c = $c / $POP_UNIT ;
			}

			$COUNT_D{$country}[$dn] = $c;
			if($dn >= $sw_start){			# 新しい情報を優先してソートする
				#dp::dp "$tl: $c \n";
				$tl += $c + $c * $SORT_WEIGHT * ($dn - $sw_start);  # 後半に比重を置く
			}
		}
		dp::dp "## " . join(", ", $country, sprintf("%.2f", $tl), $dates, 
				sprintf("P:%.2f", $POP_UNIT), 
				sprintf("C/P: %.2f", $COUNT_D{$country}[$end_day])) . "\n" if(0 && $country =~ /China/);


		#
		#	-$SORT_BALANCE と最終データの比で傾きをソートの鍵としたい	FT用か ?
		#
		if($SORT_BALANCE < 0 && $tl > 0){
			my $as = scalar(@{$DATA[$cn]});
			my $sp = int((1 - $SORT_BALANCE) * $as + 0.5);
			my $sw = int($as * $SORT_WEIGHT + 1);
			my $v1 = csvlib::avr($DATA[$cn], $sp - $sw , $sp);
			my $v2 = csvlib::avr($DATA[$cn], $as - $sw , $as);
			#dp::dp "FT SORT: as:$as, sp:$sp, sw:$sw, v1:$v1, v2:$v2\n";
			$tl = ($v1 == 0) ? $v2 * 2 : $v2 / $v1;
		}
		if($tl > 0){
			$CTG{$country} = $tl;
			$TOTAL{$country} = $DATA[$cn][1];
		}
		#dp::dp "$country : " . $TOTAL{$country} . "\n";
	}

	#
	#	グラフ用の配列の作成
	#
	my @Dataset = (\@DATES);
	my $CNT = -1;
	my $rn = 0;

	my @sc = (sort {$CTG{$b} <=> $CTG{$a}} keys %CTG);

	#	None Sort
	if(defined $gplitem->{nosort}){
		#dp::dp "#" x 20 . "  NONE SORT";
		@sc = ();
		for(my $cn = 0; $cn <= $COUNTRY_NUMBER; $cn++){
			my $country = $DATA[$cn][0];
			next if(! $country);
			push(@sc, $country);
			$TOTAL{$country} = $cn;
		}
	}

	foreach my $country (@sc){
		#dp::dp "$country  " . $CTG{$country} . "\n";
		
		$rn++;
		next if($#exclusion >= 0 && csvlib::search_list($country, @exclusion));
		#next if($#target >= 0 && $#exclusion >= 0 && ! csvlib::search_list($country, @target));
		next if($#target >= 0 && ! csvlib::search_list($country, @target));
		#dp::dp "Yes, Target $CNT $country [$tgcs, $tgce]\n" if($DEBUG && $#target >= 0);

		$CNT++;
		#dp::dp "  " . join(",", $CNT, $tgcs, $tgce, $country) . "\n";
		if($CNT < $tgcs || $CNT > $tgce){
			#dp::dp "# " .join(",", $CNT, $tgcs, $tgce, $country, ":", @add_target) . "\n";
			next if($#add_target < 0);
			next if(! csvlib::search_list($country, @add_target));
		}
		#dp::dp "> " .join(",", $CNT, $tgcs, $tgce, $country) . "\n";

		push(@LEGEND_KEYS, sprintf("%02d:%s", $rn, $country));
		for(my $i = 0; $i <= $#{$COUNT_D{$country}}; $i++){
			my $dtn = $COUNT_D{$country}[$i];
			$MAX_COUNT = $dtn if(defined $dtn && $dtn > $MAX_COUNT);
		}
		push(@Dataset, [@{$COUNT_D{$country}}]);
		push(@COUNTRY, $country);
		dp::dp "COUNT_D: ". join (",", @{$COUNT_D{$country}}) . "\n" if($DEBUG > 1);

	}


	if($DEBUG > 1){
		dp::dp "Dataset: " . $#Dataset, "\n";
		dp::dp join(",", "", "", @{$Dataset[0]}), "\n";
		for (my $i = 1; $i <= $#Dataset; $i++){
			print "Dataset[$i]:" . $Dataset[$i], "  ";
			print join(",", "$i", $LEGEND_KEYS[$i-1], @{$Dataset[$i]}), "\n";
		}
		dp::dp "-" x 20 , "\n";
	}

	#
	#	グラフの生成 CSV
	#
	#dp::dp "AVERAGE_DATE : [" . $gplitem->{average_date} . "]\n";
	my @record = ();
	my $max_data = 0;
	#for(my $dt = 0; $dt <= $#DATES; $dt++){
	for(my $dt = 0; $dt <= $end_day; $dt++){
		my $dts  = $DATES[$dt];
		$dts =~ s#([0-9]{4})([0-9]{2})([0-9]{2})#$1/$2/$3#;
		$dts = $dt if(defined $gplitem->{ft});
		#dp::dp "[[$DATES[$dt]][$dts]\n";
		my @data = ();
		for (my $i = 1; $i <= $#Dataset; $i++){
			my $v = $Dataset[$i][$dt];
			my $country = $COUNTRY[$i-1];
			#dp::dp "### [$country]: ";
			my $item_number = $TOTAL{$country};
			#dp::dp "###### [$country] $item_number : $dt" . "\n";
	
			#
			#	for Average
			#
			if(defined $gplitem->{average_date}){
				my $av = 0;
				if($dt > $item_number){ 
					$v = $NO_DATA;			# for FT, set nodata
				}
				else {
					for(my $ma = $dt - $gplitem->{average_date} + 1; $ma <= $dt; $ma++){		# +1 2020.09.09
						my $d = $Dataset[$i][$dt];
						if($ma >= 0) {
							$d = $Dataset[$i][$ma];
						}
						$av += $d;
						#dp::dp "($i: $dt $ma $av)";
					}
					$v = int(0.5 + $av) / $gplitem->{average_date};
					#$v = int(100 * $av / $gplitem->{average_date}) / 100;
					#$v = 1 if($v < 1 && defined $gplitem->{logscale});
				}
			}
			if($gplitem->{logscale}){
				$v = $NO_DATA if($v < 1);
			}
			push(@data, $v);
			$max_data = $v if($v > $max_data);
		}
		push(@record, join($DLM, $dts, @data));
	}

	#
	#	for FT, 全てデータなしの相対日をグラフに入れないための処理
	#
	my $final_rec = 0;
	for(my $i = 0; $i <= $#record; $i++){
		my $rr = $record[$i];
		$rr =~ s/^[0-9]+$DLM//;
		if($rr =~ /[0-9]/){
			$final_rec = $i;
		}
	}

	#
	#	特異に大きなデータをグラフから外すための処理
	#		https://ai-trend.jp/basic-study/normal-distribution/standardization/
	#
	my $thresh_fag_max = 0;
	my $thresh = 0;
	my $thresh_ymax = 0;
	my $thresh_flag = 0;
	my $thresh_min = 5;
	my $avr = 0;
	my $max = 0;
	my $total = 0;
	my $count = 0;
	my $stdv = 0;
	my $ct = $thresh_mode;							# ct = 2 -> 95.45%

	if($thresh_mode){
		foreach my $rs (@record){
			my @w = split(/$DLM/, $rs);
			for(my $i = 1; $i <= $#w; $i++){
				if($w[$i] > $thresh_min){
					$total += $w[$i];
					$count++;
					$max = $w[$i] if($w[$i] > $max);
				}
			}
		}
		$avr = $total / $count if($count > 0);
		
		my $s = 0;
		foreach my $rs (@record){
			my @w = split(/$DLM/, $rs);
			for(my $i = 1; $i <= $#w; $i++){
				if($w[$i] > $thresh_min){
					$s += (($w[$i] - $avr)**2);
					#print  sprintf("<%d-%.2f:%.2f>", $w[$i], $avr, $w[$i] - $avr);
				}
			}
		}
		#dp::dp $s . "\n";
		$s = $s / $count if($count > 0);
		$stdv = sqrt($s);
		$stdv = 1 if($stdv <= 0);

		$thresh_fag_max = int($count * 0.002) + 1;
		$thresh = $avr + $stdv * $ct;		# 
		$thresh_ymax = csvlib::max_val($avr + $stdv * ($ct - 1), 4);  	# 特異に大きな値の処理
		
		#dp::dp sprintf("total:%d count:%d max:%d avr:%.2f stdev:%.2f thresh:%d ymax:%d\n",
		#				$total,$count,$max, $avr,$stdv,$thresh,$thresh_ymax);
		for(my $r = 0; $r <= $#record; $r++){
			my @w = split(/$DLM/, $record[$r]);
			my $f = "";
			for(my $i = 1; $i <= $#w; $i++){
				my $z = ($w[$i] - $avr) / $stdv;
				if($z > $ct){
					#dp::dp "THRESH: $w[$i]($z) -> $ct:$thresh\n";
					#$w[$i] = $thresh;
					$thresh_flag++;
				}
			}
		}
	}

	#
	#	累積
	#
	if(defined $gplitem->{ruiseki}){
		dp::dp "#### RUIKEI\n" if($DEBUG);
		my @RUI = ();
		my $recno = $#record;
		dp::dp "$recno,  $#Dataset\n" if($DEBUG);
		for(my $i = 0; $i <= $recno; $i++){
			my @w = split(/$DLM/, $record[$i]);
			my $total = 0;
			for(my $r = 0; $r <= $#w; $r++){
				$RUI[$i][$r] = $w[$r];
				$total += $RUI[$i][$r] if($r > 0);
			}
			if(($gplitem->{ruiseki_percent} // "")){
				for(my $r = 1; $r <= $#w; $r++){
					$RUI[$i][$r] = 100 * $RUI[$i][$r]  / $total;
				}
			}
		}

		if($gplitem->{ruiseki} >= 0){
			for(my $i = 0; $i <= $recno; $i++){
				for(my $r = 2; $r <= $#Dataset; $r++){
					#print "[$i,$r,$recno, $#Dataset]\n";
					#print "[" . join(",", $RUI[$i][$r], $RUI[$i][$r] + $RUI[$i][$r-1]) . "]";
					$RUI[$i][$r] += $RUI[$i][$r-1];
				}
			}
		}
		else {
			for(my $i = 0; $i <= $recno; $i++){
				for(my $r = $#Dataset - 1; $r >= 1; $r--){
					$RUI[$i][$r] += $RUI[$i][$r+1];
				}
			}
		}
		@record = ();
		for(my $i = 0; $i <= $recno; $i++){
			my @rw = ();
			for(my $r = 0; $r <= $#Dataset; $r++){
				push(@rw, $RUI[$i][$r]);
			}
			push(@record, join($DLM, @rw));
		}
	}

	#
	#	グラフ生成用のCSVの書き出し
	#
	my $DLM_OUT = $config::DLM_OUT;
	dp::dp "#### " . $#record . ":$final_rec\n" if($DEBUG);
	open(DF, "> $plot_csvf") || die "cannot create $plot_csvf\n";
	print DF join($DLM_OUT, "#", @LEGEND_KEYS), "\n";
	for(my $i = 0; $i <= $final_rec; $i++){
		my $rr = $record[$i];
		print DF $rr . "\n";
	}
	close(DF);

	#
	#	グラフ生成
	#
	my $ymin = csvlib::valdef($gplitem->{ymin}, 0);
	my $ymax = "";
	if(defined $gplitem->{ymax}){
		$ymax = $gplitem->{ymax};
	}
	elsif($graph_set->{sub_mode} ne "FT"){
		if($thresh_flag > 0 && $thresh_flag <= $thresh_fag_max) {
			$ymax = $thresh_ymax;  	# 特異に大きな値の処理
			#dp::dp "SET YMAX $ymax by THRESH LEVEL\n";
		}
	}
	my $TITLE = $ext . "  src:" . $clp->{src} ; # . " <$thresh_flag:$thresh_fag_max:$ymax:" . sprintf("$max:%.1f:%.1f>", $avr,$stdv);
	$TITLE .= "ymax $ymax" if(defined $gplitem->{ymax});
	$TITLE .= "log " if(defined $gplitem->{logscale});
	my $XLABEL = "";
	my $YLABEL = "";

	my $DATE_FORMAT = "set xdata time\nset timefmt '%m/%d'\nset format x '%m/%d'\n";
	my $XRANGE = "set xrange ['$START_DATE':'$LAST_DATE']";
	if(defined $gplitem->{series}){
		$DATE_FORMAT = "";
		$XRANGE = "set xrange [1:" . $final_rec . "]";
	}

	my $TERM_XSIZE = csvlib::valdef($gplitem->{term_xsize}, 1000) ;
	my $TERM_YSIZE = csvlib::valdef($gplitem->{term_ysize}, 300);

my $PARAMS = << "_EOD_";
set datafile separator '$DLM_OUT'
set xtics rotate by -90
$DATE_FORMAT
set mxtics 2
set mytics 2
set grid xtics ytics mxtics mytics
set key below
# second ax
#
set title '$TITLE' font "IPAexゴシック,12" enhanced
set xlabel '$XLABEL'
set ylabel '$YLABEL'
#
set xtics #XTICKS#
#set xrange ['$START_DATE':'$LAST_DATE']
$XRANGE
set yrange [#YRANGE#]
set y2range [#YRANGE#]
set terminal pngcairo size $TERM_XSIZE, $TERM_YSIZE font "IPAexゴシック,8" enhanced
#LOGSCALE#
#LOGSCALE2#
#FILLSTYLE#
set y2tics
#set output '$plot_pngf.png'
set output '/dev/null'
plot #PLOT_PARAM#
Y_MIN = 0
Y_MAX = GPVAL_Y_MAX

#show variables all
set output '$plot_pngf'
#ARROW#
plot #PLOT_PARAM#
#show variables all
exit
_EOD_

	my @w = ();
	if($style eq "boxes"){
		my $fs = 'set style fill solid border lc rgb "white"';
		$PARAMS =~ s/#FILLSTYLE#/$fs/;
	}
	if(defined $gplitem->{ruiseki}){
		my $fs = 'set style fill pattern 5';
		$PARAMS =~ s/#FILLSTYLE#/$fs/;
	}
	for(my $i = 0; $i <= $#LEGEND_KEYS; $i++){
		my $country = $LEGEND_KEYS[$i];
		$country =~ s/'//g;
		if($style eq "boxes"){
			push(@w, sprintf("'%s' using 1:%d with boxes title '%s' ", 
						$plot_csvf, $i+$DATE_COL_NO, $country)
				);
		}
		elsif(defined $gplitem->{ruiseki}){
			if($gplitem->{ruiseki} >= 0){
				unshift(@w, sprintf("'%s' using 1:%d with filledcurves x1 title '%s' linewidth %d ", 
							$plot_csvf, $i+$DATE_COL_NO, $country, 1)
				);
			}
			else {
				push(@w, sprintf("'%s' using 1:%d with filledcurves x1 title '%s' linewidth %d ", 
							$plot_csvf, $i+$DATE_COL_NO, $country, 1)
				);
			}
		}
		else {
			push(@w, sprintf("'%s' using 1:%d with lines title '%s' linewidth %d ", 
						$plot_csvf, $i+$DATE_COL_NO, $country, ($i < 7) ? 2 : 1)
			);
		}
	}
	my $pn = join(",", @w); 
	if(defined $gplitem->{additional_plot} && $gplitem->{additional_plot}){
		#dp::dp "additional_plot: " . $gplitem->{additional_plot} . "\n";
		$pn .= ", " . $gplitem->{additional_plot};
	}
	if($#LEGEND_KEYS < 0 && ! defined $gplitem->{additional_plot}){
		$pn = "1 with lines title 'no-data'";
	}
	$PARAMS =~ s/#PLOT_PARAM#/$pn/g;	


	if($gplitem->{logscale}){
		if($graph_set->{sub_mode} eq "FT" && $max_data <= 10){
			$max_data = 100;
		} 
		$ymax = csvlib::calc_max($max_data, defined $gplitem->{logscale}) if(! $ymax);
		$ymin = 1 if($ymin < 1);
		# dp::dp "YRANGE [$ymin:$ymax]\n";
	}
	$PARAMS =~ s/#YRANGE#/$ymin:$ymax/g;	
	my $logs = "nologscale";
	if(defined $gplitem->{logscale}){
		$logs = "logscale " . $gplitem->{logscale}; 
		$PARAMS =~ s/#LOGSCALE#/set $logs/;

		$logs = "logscale " . $gplitem->{logscale} . "2";
		$PARAMS =~ s/#LOGSCALE2#/set $logs/;
	}

	my $xtics = 3600 * 24;
	if(defined $gplitem->{series}){
		$xtics = csvlib::valdef($gplitem->{label_skip}, 1);
	}
	else {
		if(defined $gplitem->{label_skip}){
			$xtics = $gplitem->{label_skip} * 3600 * 24;;
		}
	}
	$PARAMS =~ s/#XTICKS#/$xtics/;
#	dp::dp "[[[$PARAMS]]]\n";

#	my $PARAMS_SHOW_VALS = $PARAMS;
#	my $SHOW_VALS = "show variables GPVAL_Y_MIN\nshow variables GPVAL_Y_MAX\n";
#	$PARAMS_SHOW_VALS =~ s/#SHOW_VALS#/$SHOW_VALS/;

	#my $gpval_y_min = $GPVAL{GPVAL_Y_MIN} // "";
	#my $gpval_y_max = $GPVAL{GPVAL_Y_MAX} // "";

	if(1){
		my $RELATIVE_DATE = 7;
		#my $MARK_DATE = $DATES[$end_day-$RELATIVE_DATE];
		my @aw = ();
		
		for(my $date = $end_day - $RELATIVE_DATE; $date > 0; $date -= $RELATIVE_DATE){
			my $mark_date = $DATES[$date];
			#my $a = sprintf("set arrow from '%s',%d to '%s',%d nohead lw 1 dt (3,7) lc rgb \"red\"",
			#    $mark_date, $ymin, $mark_date, csvlib::calc_max2($max_data));
			my $a = sprintf("set arrow from '%s',Y_MIN to '%s',Y_MAX nohead lw 1 dt (3,7) lc rgb \"red\"",
				$mark_date,  $mark_date);
			push(@aw, $a);
		}
		my $arw = join("\n", @aw);
		#dp::dp "ARROW: $arw\n";

		$PARAMS =~ s/#ARROW#/$arw/;	
	}
	
	open(PLF, "> $plot_cmdf") || die "cannot create $plot_cmdf";
	print PLF $PARAMS;
	close(PLF);
	print ("gnuplot $plot_cmdf\n") if($VERBOSE || $DEBUG > 1);
	system("gnuplot $plot_cmdf");
	#system("cat $plot_cmdf");
	#unlink("$plot_pngf.png");

	#dp::dp "#" x 30 . "\n";
	#system("echo $plot_valf ; cat $plot_valf");
	#dp::dp "#" x 30 . "\n";
	#exit;
	#	Draw one week before
	#my %GPVAL =();
	#&load_vals($plot_valf, \%GPVAL);

	if($#UNDEF_POP >= 0){
		for(my $i = 0; $i < 5; $i++){
			#dp::dp "#" x 30 . "\n";
		}
		foreach my $country (@UNDEF_POP){
			next if($country =~ /Unassigned/);
			next if($country =~ /Out of/);
			#dp::dp "No POP:[$country]\n";
		}
		#for(my $i = 0; $i < 5; $i++){
		#	dp::dp "#" x 30 . "\n";
		#}
		### exit 1;
	}

	$plot_pngf =~ s#.*/##;
	$plot_cmdf =~ s#.*/##;
	$plot_csvf =~ s#.*/##;
#	return ("$fname.png", "$fname-plot.txt", "$fname-plot.csv", @LEGEND_KEYS);
	return ($plot_pngf, $plot_cmdf, $plot_csvf, @LEGEND_KEYS);
}

sub	date_offset
{
	my($dt, $offset) = @_;

	my ($m, $d) = split("/", $dt);
	my $ut = csvlib::ymd2tm(2020, $m, $d, 0, 0, 0); 
	my $ld = csvlib::ut2d4($ut + $offset, "/");

	$ld =~ s#^[0-9]{4}/##;
	#dp::dp "DATE: $dt : $ld\n";

	return $ld;
}
1;
sub	load_vals
{
	my ($fn, $gp) = @_;

	dp::dp "#### locad_vals: $fn\n";
	my @list = ();
	open(GP, $fn) || die "cannot open $fn";
	while(<GP>){
		dp::dp $_;
		s/[\r\n]+$//;
		push(@list, $_);
	}
	close(GP);

	for(my $i = 0; $i <= $#list; $i){
		if(/Variables beginning with (.*):/){
			#GPVAL_Y_MIN = 0.0
			$i++;
			$list[$i] =~ /([\w_]+) = ([0-9\.])/;
			my( $val_name , $v) = ($1, $2);
			$gp->{$val_name} = $v;
			dp::dp "### $list[$i] \n";
		}
	}
	foreach my $vn (sort keys %{$gp}){
		dp::dp "GPVAL:  $vn => $gp->{$vn}\n";
	}
}
