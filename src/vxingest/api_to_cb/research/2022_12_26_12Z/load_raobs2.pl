#!/usr/bin/perl
#

use strict;

my $thisDir = $ENV{PBS_O_WORKDIR};
my $qsubbed=1;
unless($thisDir) {
    # we've been called locally instead of qsubbed
    $qsubbed=0;
    use File::Basename; 
    my ($basename,$thisDir2) = fileparse($0);
    $thisDir = $thisDir2;
}
my $output_id = $ENV{PBS_JOBID} || $$;

my $DEBUG=1;
use DBI;


#change to the proper directory
use Cwd 'chdir'; #use perl version so this isn't unix-dependent
chdir ("$thisDir") ||
          die "Content-type: text/html\n\nCan't cd to $thisDir: $!\n";
$thisDir = $ENV{PWD};

#set database connection parameters
$ENV{DBI_DSN} = "DBI:mysql:soundings:wolphin.fsl.noaa.gov";
$ENV{DBI_USER} = "UA_realtime";
$ENV{DBI_PASS} = "newupper";

$ENV{model_sounding_file} = "tmp/model_sounding.$$.tmp";

# connect to the database
my $dbh = DBI->connect(undef,undef,undef, {RaiseError => 1});
my $sth;
my $query="";
my $gzipped_sounding="";
my $gzipped_hydro="";
my $sql_date;
use Compress::Zlib;

$|=1;  #force flush of buffers after each print
$ENV{CLASSPATH} ="/w3/emb/utilities/javalibs/mysql/mysql-connector-java-5.1.6-bin.jar:.";

$SIG{ALRM} = \&my_timeout;
my %month_num = (Jan => 1, Feb => 2, Mar => 3, Apr => 4, May => 5, Jun => 6,
		 Jul => 7, Aug => 8, Sep => 9, Oct =>10, Nov =>11, Dec =>12);


$ENV{'TZ'}="GMT";
my ($aname,$aid,$alat,$alon,$aelev,@description);
my ($found_airport,$lon,$lat,$lon_lat,$time);
my ($location);
my ($startSecs,$endSecs);
my ($desired_filename,$out_file,$fcst_len,$elev,$name,$id,$data,$bad_data);
my ($good_data,$found_sounding_data,$maps_coords,$title,$logFile);
my ($dist,$dir,$differ);
my ($loaded_soundings);
my $BAD=0;
my $all_levels_filled = 0;
my($alat1,$elon1,$elonv,$alattan,$grib_nx,$grib_ny,$grib_nz,$grib_dx,
   $grib_type,$grid_type,$valid_date_from_file,$fcst_len_from_file);
my $tmp_file = "tmp/$$.data.tmp";
my $iso = "";

use lib "./";
#require "timelocal.pl";   #includes 'timegm' for calculations in gmt
use Time::Local;
#get best return address
my $returnAddress = "(Unknown-Requestor)";
if($ENV{REMOTE_HOST}) {
    $returnAddress = $ENV{REMOTE_HOST};
} else {
    # get the domain name if REMOTE_HOST is not set
    my $addr2 = pack('C4',split(/\./,$ENV{REMOTE_ADDR}));
    $returnAddress = gethostbyaddr($addr2,2) || $ENV{REMOTE_ADDR};
}
my $usage = "usage: $0 [number of 12 h periods ago] [1 to reprocess, 0 otherwise]\n";
print $usage;

my $i_arg=0;
my $data_source = "RAOB";

if($qsubbed == 1) {
    my $output_file = "tmp/$data_source.$output_id.out";
# send standard out and stderr to $output_File
    use IO::Handle;
    *STDERR = *STDOUT;		# send standard error to standard out
    open OUTPUT, '>',"$output_file" or die $!;
    STDOUT->fdopen( \*OUTPUT, 'w' ) or die $!;
}

my $twelves_to_subtract = abs($ARGV[$i_arg++]) || 0;

my $reprocess=0;
if(defined $ARGV[$i_arg++]) {
    $reprocess=1;
}

my $time = time();
# put on 12 hour boundary
$time -= $time%(12*3600);
$time -= $twelves_to_subtract*12*3600;
my @valid_times = ($time);

my @fcst_lens = (0);

foreach my $valid_time (@valid_times) {
    foreach my $fcst_len_for_db (@fcst_lens) {
	my $found_files = 0;
	my $special_type = "none";
	my $desired_fcst_len = $fcst_len_for_db;
	my $make_soundings=1;
	if($data_source =~ /RR|RAP/i && $fcst_len_for_db == -99) {
	    $desired_fcst_len = 0;
	    $special_type = "analysis";
	}
	my $run_time = $valid_time -$desired_fcst_len * 3600;
	my $run_date = sql_datetime($run_time);
	my $valid_date = sql_datetime($valid_time);
	if((!raobs_loaded($valid_time)) || $reprocess) {
	    # load interpolated RAOBs at 10mb resolution
	    my $command = "java Verify3 dummy_dir $data_source ".
		"$valid_time $valid_time $fcst_len_for_db";
	    print "$command\n";
	    system($command) &&
		print "problem with |$command|: $!";
	} else {
	    print "RAOBs already loaded\n";
	}
    }
} # end loop over fcst_lens

# now clean up
unlink $tmp_file ||
    print "could not unlink $tmp_file: $!";
unlink $ENV{model_sounding_file} ||
   die "cannot unlink $ENV{model_sounding_file}: $!";
# clean up tmp directory
opendir(DIR,"tmp") ||
    die "cannot open tmp/: $!\n";
my @allfiles = grep !/^\.\.?$/,readdir DIR;
foreach my $file (@allfiles) {
    $file = "tmp/$file";
    #print "file is $file\n";
    # untaint
    $file =~ /(.*)/;
    $file = $1;
    if(-M $file > .7) {
	print "unlinking $file\n";
	unlink "$file" || print "Can't unlink $file $!\n";
    }
}
closedir DIR;
print "NORMAL TERMINATION\n";


sub sql_datetime {
    my $time = shift;
    my($sec,$min,$hour,$mday,$mon,$year) = gmtime($time);
    $mon++;
    $year += 1900;
    return sprintf("%4d-%2.2d-%2.2d %2.2d:%2.2d:%2.2d",
		   $year,$mon,$mday,$hour,$min,$sec);
}
sub sql_date_hour {
    my $time = shift;
    my($sec,$min,$hour,$mday,$mon,$year) = gmtime($time);
    $mon++;
    $year += 1900;
    return (sprintf("%4d-%2.2d-%2.2d",$year,$mon,$mday),$hour);
}

sub soundings_loaded($table,$valid_date,$fcst_len,$special_type,$dby) {
    my ($table,$valid_date,$fcst_len,$special_type,$dbh) = @_;
    if($special_type eq "analysis") {
	$fcst_len = -99;
    }
    my $query=<<"EOI";
	select count(*) from soundings.$table
	where 1=1
	and time = '$valid_date'
	and fcst_len = $fcst_len
EOI
	;
    #print "$query\n";
    my $sth = $dbh->prepare($query);
    $sth->execute();
    my($n);
    $sth->bind_columns(\$n);
    $sth->fetch();
    #print "n returned is $n\n";
    # line below is for debugging (to force reprocessing of an hour).
    return $n;
}

sub stats_generated($data_source,$valid_time,$fcst_len_for_db) {
    my ($data_source,$valid_time,$fcst_len) = @_;
    my($valid_day,$valid_hour) = sql_date_hour($valid_time);
    $dbh->do("use ruc_ua_sums2");
    my $table = "${data_source}__reg%";
    my $query = qq{show tables like "$table"};
    #print "query is $query\n";
    my @result = $dbh->selectrow_array($query);
    #print "result is @result\n";
    if(@result) {
	$table = $result[0];
    }
    $query =<<"EOI"
select count(*) from $table
where 1=1
and mb10 = 50
and fcst_len = $fcst_len
and hour = $valid_hour
and date = '$valid_day'
EOI
;
    #print "query is $query\n";
    my $sth = $dbh->prepare($query);
    $sth->execute();
    my($n);
    $sth->bind_columns(\$n);
    $sth->fetch();
    #print "n returned is $n\n";
    return $n;
}
   
sub raobs_loaded($valid_time) {
    my($valid_time) = @_;
    my($valid_day,$valid_hour) = sql_date_hour($valid_time);
    my $query =<<"EOI"
select count(*) from ruc_ua.RAOB
where 1=1
and press = 500
and hour = $valid_hour
and date = '$valid_day'
EOI
;
    #print "query is $query\n";
    my $sth = $dbh->prepare($query);
    $sth->execute();
    my($n);
    $sth->bind_columns(\$n);
    $sth->fetch();
    my $result=0;
    if($n > 400) {
	$result = $n;
    }
    #print "result is $result\n";
    return $result;
}
    
