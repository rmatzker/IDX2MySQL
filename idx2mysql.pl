#!/usr/local/bin/perl

use strict;
use DBI;

# Read dictionary, data and table names from ARGV
my $idx_dic_file = $ARGV[0] or usage();
my $idx_data_file = $ARGV[1] or usage();
my $table = $ARGV[2] or usage();

# Define hash of IDX codes to table names (change this as needed)
my %idxfiles = (
	'R','Residential', 				# idx_1
	'L','Lots/Land',				# idx_2
	'Z','Commercial/Business',		# idx_3
	'C','Condos/Townhomes/Villas',	# idx_5
	'F','Farms',					# idx_6
	'O','Real Estate Brokers'		# idx_off
	);

# Protect any tables starting with mls_ 
if ($table =~ /^mls\_/) { die ("Table name $table is reserved.\n"); }
my $listing_type = (split(/\_/,$table))[-1] or usage();

my $dbh = DBI->connect(
	"DBI:mysql:YOUR_DB_NAME:YOUR_DB_HOST",
	'YOUR_DB_USERNAME','YOUR_DB_PASSWORD',
	{RaiseError => 1}
	);

if ( ! -f $idx_dic_file || ! -f  $idx_data_file) { die("IDX file missing - $!\n"); } 

# Define reusable SQL strings for use during process
my $sql = "";
my $sql2 = "";
my $sql3 = "";

# Drop all existing data if table exists
dbexec("delete from mls_fields where mls_tablename = '$table';");
dbexec("delete from mls_idx where mls_tablename = '$table';");
my $realtor_col = 0;
my $price_col = 0;

###########################
# Process dictionary file #
###########################

open(DICFILE,$idx_dic_file) or die("ERROR: $!\n");
my @lines = <DICFILE>;
close(DICFILE);

shift @lines; # Remove first line
my %headers;
my @column_types;
my $counter = 0;
foreach my $field (@lines)
{
	chomp($field);
	$field =~ tr/\cM//d; # Remove ^M chars

	my ($dname,$desc,$type,$size) = split(/\t/,$field);
	
	# Fix weird stuff like this:
	#replace(replace(LM_MST_INCLUDE,chr(10),' '),chr(13),' ') INCLUDE    Included    Char    100
	if ($dname =~ /^replace\(replace\(([A-Za-z0-9\_]+)/)
	{
		$dname = $1;
	}

	# Make sure the dname is unique. If not, add numbers incrementally until it is!
	if (exists($headers{$dname}))
	{
		do 
		{
			if ($dname =~ /(.*)(\d+)$/) {
					my $incr = $2;
					$incr++;
					$dname = $1.$incr;
			} else {
				$dname = $dname . "1";
			}
			
		} while (exists($headers{$dname}));
	}
	
	# Remove spaces from dname
	$dname =~ s/ /\_/g;
	
	# Clean up type names
	if (lc($type) eq "char" && $size eq 1) { $type = "bit"; }
	if (lc($type) eq "vchar") { $type = "varchar"; }
	if (lc($type) eq "char") { $type = "varchar"; }
	if (lc($type) eq "number") { $type = "int"; }
	if (lc($type) eq "decimal") { $type = "decimal"; }
	
	# Clean up descriptions 
	$desc =~ s/ ?Y\/N//gi; # remove y/n tags
	
	# Fix capitalization
	if (length($desc) > 3)
	{
		$desc = lc($desc);
		$desc =~ s/\b([a-z])/\u$1/g;
	}
	
	# Force varchar "name" fields to be long
	if ($type eq "varchar") {
		if ($desc =~ m/Name/) {
			$size = 100;
		}
	}
	
	##############################
	# Add to hash
	$headers{$dname} = { col => $counter, dname => $dname, desc => $desc, type => $type, size => $size }; 

	##############################
	# Add to array
	push(@column_types,$type);

	##############################
	# Add to SQL
	$sql .= "insert into mls_fields (mls_colnum,mls_dname,mls_desc,mls_type,mls_size,mls_tablename,mls_listing_type) values (";
	$sql .= "'$counter',";
	$sql .= "'".$dname."',";
	$sql .= mysql_escape($desc).",";
	$sql .= "'".$type."',";
	$sql .= "'".$size."',";
	$sql .= "'".$table."',";
	$sql .= "'$listing_type');\n";
	dbexec($sql);
	$sql = "";
	
	# SQL for table creation
	$sql2 .= $dname." ".$type."(".$size."),\n";
	
	# SQL for 
	$sql3 .= "$dname,";
	
	$counter++;
}
my $total_fields = $counter;

# Drop and recreate the table that holds this MLS data
dbexec("drop table if exists $table;");
$sql2 =~ s/,$//g;
dbexec("create table $table (\n$sql2);");
$sql2 = "";



#####################
# Process data file #
#####################

open(TXTFILE,$idx_data_file) or die("ERROR: $!\n");
my @lines = <TXTFILE>;
close(TXTFILE);

# Get ready to count each listing as we insert
my $insert_count = 0;

shift @lines; # Remove first line

$sql3 =~ s/,$//g;

foreach my $field (@lines) # Looping over each line in the file
{
	chomp($field);
	$field =~ tr/\cM//d; # Remove ^M chars
	my @values = split(/\t/,$field);
	
	$sql .= "insert into $table ($sql3) values (";
	$counter = 0;
	my $mlsid = $values[0];
	my $realtorid = "";
	my $mls_price = 0;
	my $mls_city = "";
	my $mls_adtxt = "";
	my %headerinfo;
	
	foreach my $value (@values) # Looping over each value in a listing
	{
		# Grab RealtorID for later
		if ($headers{'LSFRM'}{'col'} eq $counter)
		{
			$realtorid = $value;
		}
		
		# Grab Price for later
		if ($headers{'LM_MST_LIST_PRC'}{'col'} eq $counter)
		{
			$mls_price = $value;
		}
		
		# Grab city for later
		if ($headers{'LM_MST_CITY'}{'col'} eq $counter)
		{
			$mls_city = $value;
		}
		if ($headers{'LM_MST_REMARKS'}{'col'} eq $counter)
		{
			$mls_adtxt = $value;
		}
		# Look for Y/N values
		my $thisdata = "";
		if ($column_types[$counter] =~ /bit/i)
		{
			if ($value =~ /^Y/i)
			{
				$sql .= "1,";
			} else {
				$sql .= "0,";
			}
			$counter++;
			next;
		}
		
		# Escape strings
		if ($column_types[$counter] =~ /char/i)
		{
			$thisdata .= mysql_escape($value).",";
			$thisdata = "NULL," if ($thisdata eq "'',");
			$sql .= $thisdata;
			$counter++;
			next;
		}
		
		# Catch anything else
		$value = "NULL" if ($value eq "");
		$sql .= $value.",";
		$counter++;
	}
	# Check field count against what we have - fill in rest with null
	if ($counter < $total_fields)
	{
		do
		{
			$sql .= "NULL,";
			$counter++;
		} while ($counter < $total_fields);
	}
	
	$sql =~ s/,$//;
	$sql .= ");\n";

	dbexec($sql);
	$insert_count++;
	$sql = "";
	# Finished inserting listing
	
	# Insert the goodies into mls_idx (our own master index of all IDX files being imported)
	if ($listing_type ne "O") # Skip type "O" which is an office vs a real listing
	{
		# MLS idx index
		dbexec("delete from mls_idx where mlsid = $mlsid;");
				
		if ($realtorid == "") { $realtorid = 0; }
			dbexec("insert into mls_idx (mlsid,mls_tablename,mls_listing_type,mls_realtor,mls_price,mls_city,mls_adtxt)".
				" values ('$mlsid','$table','$listing_type',$realtorid,$mls_price,".mysql_escape($mls_city).",".mysql_escape($mls_adtxt).");");

	}
}		


#################
# Stats & Pics

if ($listing_type ne "O")
{
	# Stats
	print $insert_count . " listings imported into table $table (".$idxfiles{$listing_type}.")\n";
	my $listcount = `cat mls_stats.txt 2>/dev/null`;
	chomp($listcount);
	if ($listcount eq "")
	{ 
		$listcount = $insert_count;
	} else {
		$listcount = $listcount + $insert_count;
	}

	`echo $listcount > mls_stats.txt`;
	`echo $insert_count > $table.txt`;
	} else {
	print scalar (@lines) . " total real estate agents\n";
}

$dbh->disconnect();
exit;

# ================ SUBROUTINES ==========
# pass in a string and get back a safer one:
sub mysql_escape
{
	# get the text:
	my $rawText = shift;

	# first, look for any apostrophes and escape them:
	#$rawText =~ s/'/\\'/g;

	# since it's a text field, we want to enclose it in apostrophes:
	#$rawText = qq('$rawText');
	
	#if ($rawText eq "''") { $rawText = "NULL"; }
	$rawText = $dbh->quote($rawText);
	
	return $rawText;
}

sub usage
{
	print "Usage: $0 <idx_dic_file> <idx_data_file> <db_table_name> <listing_type>\n";
	exit 1;
}

sub dbexec
{
	my $DBCMD = shift or return;
	#print "sql->$DBCMD\n";
	$dbh->do($DBCMD);
	return;
}

sub get_header_bycol
{
	my $colnum = shift or return;
	foreach my $key ( keys %headers)
	{
		if ($headers{$key}{'col'} eq $colnum)
		{
			my %data = (
			'col',$headers{$key}{'col'},
			'dname',$headers{$key}{'dname'},
			'desc',$headers{$key}{'desc'},
			'type',$headers{$key}{'type'},
			'size',$headers{$key}{'size'}
			);
			
			return \%data;
		}
	}
}

