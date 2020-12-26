#!/usr/bin/perl
#
#
#
use	strict;
use warnings;
use DBI;

use config;
use dp;

my $DEBUG = 1;
my $SEARCH_KEY = "";

my $QUERY = 1;

my $DB_NAME = "TestCov";
my $dsn = "dbi:mysql:database=$DB_NAME;host=localhost;port=3306";
my $user = 'cov19';
my $password  = $config::DB_PASSWORD;

my %AreaInfo = ();
my %DataSource = ();
my %DataKind = ();
my @MASTER_DEFS  = (
	{table_name => "AreaInfo",   hash => \%AreaInfo, csvf => "$config::WIN_PATH/pop.csv",
			columns => "AreaID INTEGER, AreaName VARCHAR(64), Population INTEGER", }, 
	{table_name => "DataSource", hash => \%DataSource, csvf => "DataSource.csv",
			columns => "SourceID INTEGER, SourceName CHAR(32), SoruceFullName VARCHAR(256)", },
	{table_name => "DataKind",   hash => \%DataKind, csvf => "DataKind.csv",
			columns => "KindID INTEGER, KindName CHAR(32), KindFullName VARCHAR(256)", },
);
my @RECORD_DEFS = (
	{table_name => "RawRecord", csvf => "$config::WIN_PATH/testdb.csv", csv_src => "$config::CSV_PATH/time_series_covid19_confirmed_global.csv",
		columns => "RecordNo INTEGER, Date DATE, SourceID CHAR(4), KIND CHAR(4),AreaID SMALLINT UNSIGNED, Count INTEGER, AVR7 INTEGER",
		source => "ccse", kind => "NC", comvert => \&comv_ccse,
	},
);


#
#	Command Line Paramerts
#
#	-params  : set PARASMS{$_} = 1;
#	--params : set PARASMS{$_} = $ARGV[+1];
#
#
my @PARAM_NAMES_LIST = qw(create_table insert_data delete_data insert_master reform_csv load_csv);
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
	dp::dp "$i: $_\n";
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
		$PARAMS{$_} = 1;
	}
	if(/ccse/){
		&comb_ccse();
		exit 0;
	}
	if(/^=/){
		s/^=//;
		$SEARCH_KEY = $_;
	}
}

#
#	Connect to Database
#
my $dbh = DBI->connect($dsn, $user, $password, {RaiseError => 0, AutoCommit =>0}) || die $DBI::errstr;
#my $dbh = DBI->connect($dsn, $user, $password ) || die $DBI::errstr;
&DO($dbh, 0, "USE $DB_NAME");



#
#	Remove All Data for test
#
if($PARAMS{delete_data}){
	foreach my $p (@MASTER_DEFS, @RECORD_DEFS){
		&DO($dbh, 0, qq{DELETE FROM $p->{table_name};});
	}
}

#
#	Create Table
#
if($PARAMS{create_table}){
	&DO($dbh,1, "show tables;");
	foreach my $p (@MASTER_DEFS, @RECORD_DEFS){
		#$dbh->do("DROP TABLE IF EXOSTS $p->{table_name}");
		#$dbh->do("CREATE TABLE $TABLE_NAME (AreaID INTEGER, AreaName CHAR(64), Population INTEGER)");
		#$dbh->do("CREATE INDEX AreaNameIndex on $TABLE_NAME (AreaName)");
#
		#dp::dp "CREATE TABLE $p->{table_name} ($p->{columns})\n";
		&DO($dbh, 0, "DROP TABLE IF EXISTS $p->{table_name}");
		&DO($dbh, 0, "CREATE TABLE $p->{table_name} ($p->{columns})");
		&DO($dbh, 1, "SHOW TABLES from $DB_NAME;");
		&DO($dbh, 1, "DESC $p->{table_name};");
	}
	$dbh->do("show tables;");
}

#
#	Simple Test of DB operation
#
#$dbh->do("INSERT INTO  $TABLE_NAME VALUES (1, " . $dbh->quote("Tokyo") . " , 12345) ");
#$dbh->do("INSERT INTO  $TABLE_NAME VALUES (1, " . $dbh->quote("東京") . " , 12345) ");
#$dbh->do(qq{INSERT INTO  $TABLE_NAME VALUES (1, "名古屋", 12345) });

#
#	Insert data from pop.csv
#
if(defined $PARAMS{insert_master}){
	foreach my $p (@MASTER_DEFS){
		my $SNO = 0;
		my @vals = ();
		
		my @strs = ();
		my @cl = split(/\s*,\s*/, $p->{columns});
		for(my $i = 0; $i <= $#cl; $i++){
			dp::dp "[$cl[$i]]\n" if($DEBUG > 1);
			push(@strs, $i) if($cl[$i] =~ /CHAR/i);
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
			dp::dp "[$vs]\n" if($DEBUG > 1);
			push(@vals,  "($vs)");
			#my $sql_str = qq{INSERT INTO  $TABLE_NAME VALUES ($SNO, "$name", $pop) } ;
			#push(@vals,  qq{($SNO, "$name", $pop)});
			
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
}

#
#	Load Mater Tables
#
if($PARAMS{load_master}){
	&load_master();
}

if($PARAMS{reform_csv}){
	foreach my $p (@RECORD_DEFS){
		$p->{comvert}->($p);
	}
}
exit;

#
#	Dissconnect the DB
#
$dbh->disconnect();
exit 0;

#
#	Insert values to Table
#
sub	insert_vals
{
	my ($p, $dbh, $valp) = @_;

	my $sql_str = "INSERT INTO  $p->{table_name} VALUES " . join(",", @$valp);
	dp::dp $sql_str . "\n" if($DEBUG > 1);
	my $sth = $dbh->prepare($sql_str);
	$sth->execute();
	$dbh->commit();
}

#
#
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

	open(FD, $src) || die "cannot open $src";
	open(CSV, ">$dst") || die "cannot create $dst";
	my $first_line = <FD>;
	$first_line =~ s/[\r\n]+$//;
	my $SNO = 0;
	my ($dsc_area2, $dsc_area1, $dsc_lat, $dsc_long, @dates) = split(/,\s*/, $first_line);
	while(<FD>){
		s/[\r\n]$//;
		#"RawRecord", columns => "RecordNo INTEGER, Date DATE, SourceID TINYINT, KIND TINYINT, AreaID SMALLINT, Count INTEGER, AVR7 INTEGER",
		my ($area2, $area1, $lat, $long, @count) = split(/,/, $_);
		my $area_name = join(";", $area2, $area1);
		my $area_id = $AreaInfo{$area_name}->{Population} // "-1";
		for(my $i = 0; $i <= $#count; $i++){
			my $v = $count[$i];
			$SNO++;
			my $line = join(",", $SNO, $dates[$i], 0, 0, $area_id, $v, 0);
			dp::dp $line . "\n";
			print $line . "\n";
		}
	}
	close(FD);
	close(CSV);
}

#
#	Load Master Tables
#
sub	load_master
{
	foreach my $p (@MASTER_DEFS){
		my @cl = split(/\s*,\s*/, $p->{columns});
		for(my $i = 0; $i <= $#cl; $i++){
			$cl[$i] =~ s/\s+.*$//;
		}
		#dp::dp "Columns for Query: " . join(",", @cl ) . "\n";
		my $sql_str = "SELECT *  FROM $p->{table_name}";
		#if($SEARCH_KEY){
		#	$sql_str .= " WHERE AreaName='$SEARCH_KEY'" ;
		#	dp::dp $sql_str . "\n";
		#}
		my $sth = $dbh->prepare($sql_str);
		dp::dp $sql_str . "\n";
		$sth->execute();

		#
		#	Set Table Information
		#
		my @max_len = (0, 0);
		my $mp = $p->{hash};
		my $names = $sth->{NAME};
		my $numFields = $sth->{'NUM_OF_FIELDS'} - 1;
		my $fnp = $mp->{hash}->{field_names};
		for my $i (0..$numFields){
			push(@$fnp, $$names[$i]);
			$max_len[$i] = 0;
		}

		#
		#	Load and Set Table
		#	
		while(my $ref = $sth->fetchrow_arrayref()) {
			my @vals = ();
			my $master_key = "";			# error
			for my $i (1..$numFields){
				my $field = $$names[$i];
				my $v = $$ref[$i];
				if($i == 1){
					$master_key = $v;
				}
				$mp->{$master_key}->{$field} = $v if($i > 1);
				if($DEBUG){
					my $len = length($v);
					push(@vals, "$field = $v");
					$max_len[$i] = $len if($len > $max_len[$i]);
				}
			}
			dp::dp "> " . join("\t", $master_key, @vals) . "\n" if($DEBUG > 2);
		}
		$sth->finish();
		dp::dp "MAX_LENGTH: " . join(", ", @max_len[0..$numFields]) . "\n" if($DEBUG > 2);
	}
}


#
#	Execute Sql Query
#
sub	DO
{
	my($dbh, $verbose, $sql_str) = @_;
	
	if(!defined $sql_str){
		my ($package_name, $file_name, $line) = caller;
		dp::dp "may forgot verbose or sql_str: $file_name #$line\n";
		exit ;
	}
	
	$verbose = $verbose // "";
	print "# " .  $sql_str ."\n";
#	my $result = $dbh->do($sql_str);
#	if(! $result){
#		print $DBI::errstr
#	}

	my $sth = $dbh->prepare($sql_str);
	$sth->execute();
	if(! defined $sth->{'NUM_OF_FIELDS'}){
		dp::dp "no result\n" if($verbose);
		return;
	}
	my $numFields = $sth->{'NUM_OF_FIELDS'};
	my $rno = 0;
	while(my $row = $sth->fetchrow_arrayref()) {
		my @w = ();
		for my $i (0..$numFields){
			push(@w, $$row[$i] // "");
		}
		#print "[$numFields] " . join(" " , @w) . "\n" if($verbose);
		print "  $rno:" . join(" " , @w) . "\n" if($verbose);
		$rno++;

	}
}
