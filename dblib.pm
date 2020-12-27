#
#
#
package dblib;
use Exporter;
@ISA = (Exporter);
@EXOIORT = qw(dblib);

use strict;
use warnings;

use DBI;
use Data::Dumper;

use dp;

our	$VERBOSE = 0;
our $DEBUG = 0;

#
#	Default Paramers of DO
#
our %DISP = (
	default => {disp_result => 1, disp_line_no => 1, disp_query => 0, disp_name => 0},
	silent  => {disp_result => 0, disp_line_no => 0, disp_query => 0, disp_name => 0},		# no dislay, use result
	verbose => {disp_result => 1, disp_line_no => 1, disp_query => 1, disp_name => 0},
	name    => {disp_result => 1, disp_line_no => 0, disp_query => 0, disp_name => 1},
);

#
#	Load Master Tables
#
sub	load_master
{
	my ($dbh, $p) = @_;

	if(defined $p){
		return &load_table($p, {hash => 1, disp => 0});
	}
	foreach my $p (@tbdef::MASTER_DEFS){
		&load_table($dbh, $p, {hash => 1, disp => 0});
	}
	return;
}

#
#
#
sub	load_table
{
	my ($dbh, $p, $params, $join) = @_;

	&disp_caller();
	dp::dp "# $p->{table_name}  " . ($p->{hash} // "null") . "\n" if($VERBOSE || $DEBUG);
	my $mp = $p->{hash} // "";
	%{$mp} = () if($mp);		# clear for recall

	$join = $join // "";

	my %dmparam = ();
	$params = \%dmparam if(!defined $params);
	dp::dp "load_table $p->{table_name}\n" if($DEBUG);
	foreach my $k (keys %$params){
		dp::dp "# params: $k $params->{$k}\n" if($DEBUG);
	}
	print "TABLE: $p->{table_name}\n" if($params->{disp}//"");

	#
	#	Set interface with csvgpl or genlal query
	#
	my $result_array = $params->{result_array} // "";
	my $result_hash = $params->{result_hash} // "";
	my $result_items = $params->{result_items} // "";
	# my $label_array  = $params->{label_array} // "";		>> Table definition {FieldNames}
	my $query = $params->{query}  // "";

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
	my $sql_str = ($query) ? $query : "SELECT *  FROM $p->{table_name}";
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
		if($p->{type} eq "master"){		# MASTER TABLE Only
			my $key = $$ref[0];			# error
			my $refarence = $$ref[1];
			$mp->{$key} = $refarence;
		}
		my $col_number = @$ref - 1;
		if($result_items){
			for(my $i = 0; $i <= $col_number; $i++){
				$result_items->[$i] = {};			
			}
		}

		my @vals = ();
		for my $i (0..$numFields){
			my $v = $$ref[$i];
			my $field = $$names[$i];
			dp::dp "[$join][$i][$field][$v]\n" if($params->{disp}&& $DEBUG > 3);
			# #[108] dblib.pm [0][AreaID][1]
			# dp::dp "## $p->{table_name} - $master_key - $field - $v : $mp->{$master_key}->{$field}\n";
			#
			#	$join = ( AreaID => {table => "AreaInfo", column => {AreaID}})
			#	$jp = $tbdef:%MASTER_INDEX{AreaInfo}->{AreaID};
			#
			if($join && defined $join->{$field}){
				my $join_table  = $join->{$field}->{table};
				my $join_column = $join->{$field}->{column};
				my $n   = $tbdef::MASTER_INDEX{$join_table};
				my $jtp = $tbdef::MASTER_DEFS[$n]->{hash};
			
				my $vv = $jtp->{$v};
				dp::dp "#### $field, $join_table, $join_column $jtp $v -> $vv\n" if($DEBUG > 3);
				$v = $$ref[$i] = "$vv($v)";
			}
			if($result_items){
				$result_items->[$i]->{$v}++;
			}
			push(@vals, "$field = $v");
			if($DEBUG){
				my $len = length($v);
				$max_len[$i] = $len if($len > $max_len[$i]);
			}
		}
		print "> " . join("\t", @vals) . "\n" if(($params->{disp} // "") == 1);
		print join("\t", @$ref) . "\n" if(($params->{disp} // "") == 2);
		dp::dp "# " . join("\t", @vals) . "\n" if($DEBUG > 2);
		push(@$result_array, [@vals]) if($result_array);
		if($result_hash){
			my $k = join("\t", @vals[0..($col_number-1)]);
			$result_hash->{$k} = $vals[$col_number];
		}
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

	dp::dp "# " .  $sql_str ."\n" if($disp->{disp_query} // "" || $DEBUG);
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

sub	disp_caller
{
	my @level = @_;

	@level = (0..1) if($#level < 0);
	for(my $i = 0; $i < 2; $i++){
		my ($package_name, $file_name, $line) = caller($i);
		print "called from[$i]: $package_name :: $file_name #$line\n";
	}
}
1;
