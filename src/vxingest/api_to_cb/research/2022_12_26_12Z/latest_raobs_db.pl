#!/usr/bin/perl
use strict;
#
### adapted by WRM from Randall Collander's version, 2 Nov 2001
### and further adapted by WRM to use the ruc_ua database Dec 2007


# PREAMBLE vvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvv

my $DEBUG=1;  # MAKE THIS NON-ZERO TO PRODUCE EXTRA DEBUG PRINTOUT

#for security, must set the PATH explicitly
$ENV{'PATH'}="";

#use CGI;
#require "timelocal.pl";   #includes 'timegm' for calculations in gmt
use Time::Local;

# hack to remain usable on arachnid
use lib "/usr/local/perl5/lib/site_perl/5.005/sun4-solaris";
    
#get directory and URL
use File::Basename;
my ($dum,$thisDir) = fileparse($ENV{SCRIPT_FILENAME} || '.');
$thisDir =~ m|([\-\~\.\w\/]*)|;	# untaint
$thisDir = $1;
my ($basename,$thisURLDir) = fileparse($ENV{'SCRIPT_NAME'} || '.');
$basename =~ m|([\-\~\.\w]*)|;	# untaint
$basename = $1;
$thisURLDir =~ m|([\-\~\.\w\/]*)|;	# untaint
$thisURLDir = $1;

#change to the proper directory
use Cwd 'chdir'; #use perl version so this isn't unix-dependent
chdir ("$thisDir") ||
          die "Content-type: text/html\n\nCan't cd to $thisDir: $!\n";

#get the query
#my $q = new CGI;

#useful DEBUGGING info vvvvvvvvvvvvvv
if($DEBUG) {
    print "Content-type: text/html\n\n<pre>";
    foreach my $key (sort keys(%ENV)) {
    	#print "$key: $ENV{$key}<br>\n";
    }
    print "thisDir is $thisDir\n";
    print "thisURLDir is $thisURLDir\n";
    print "basename is $basename\n";
    #print "the query is " . $q->dump()."\n";  # OLD CGI syntax
    #print "the query is " . $q->Dump."\n";    # NEW CGI syntax
    print "\n";
}
#end useful DEBUGGING info ^^^^^^^^^^^^^^^^^

#get best return address
my $returnAddress = "(Unknown-Requestor)";
if($ENV{REMOTE_HOST}) {
    $returnAddress = $ENV{REMOTE_HOST};
} else {
    # get the domain name if REMOTE_HOST is not set
    my $addr2 = pack('C4',split(/\./,$ENV{REMOTE_ADDR}));
    $returnAddress = gethostbyaddr($addr2,2) || $ENV{REMOTE_ADDR};
}

if($DEBUG) {
    print "returnAddress is $returnAddress\n";
}

#move query string items into %data
#my @keys = $q->param;
#my %data;
#foreach my $key (sort @keys) {
#    $_ = $q->param($key);
#    /^\s*([\w\.\-\@]*)/;
#    $data{$key}=$1;
#    if($DEBUG) {
#        print "$key (cleaned) -> $data{$key}\n";
#    }
#}

    
# END OF PREAMBLE ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
use DBI;
#connect
$ENV{DBI_DSN} = "DBI:mysql:ruc_ua:wolphin";
$ENV{DBI_USER} = "writer";
$ENV{DBI_PASS} = "amt1234";
my $dbh = DBI->connect(undef,undef,undef, {RaiseError => 1});

my $query = "";

#get previous latest times
$query =<<"EOI"
select wmoid,name,lat,lon,elev,
unix_timestamp(latest) from metadata
EOI
    ;
if($DEBUG) {print "query is $query\n";}
my $sth = $dbh->prepare($query);
$sth->execute();
my(%db_name,%db_lat,%db_lon,%db_elev,%prev_secs);
my($wmoid,$db_name,$db_lat,$db_lon,$db_elev,$caltime);
$sth->bind_columns(\$wmoid,\$db_name,\$db_lat,\$db_lon,\$db_elev,\$caltime);
while($sth->fetch()) {
    #print "$wmoid, $caltime\n";
    $db_name{$wmoid} = $db_name;
    $db_lat{$wmoid} = $db_lat;
    $db_lon{$wmoid} = $db_lon;
    $db_elev{$wmoid} = $db_elev;
    $prev_secs{$wmoid} = $caltime;
}

# set up query to put new times in the uaverif database
$query =<<"EOI"
update metadata set latest = ?
 where wmoid = ?
EOI
    ;
my $sth_update_latest = $dbh->prepare($query);
$query =<<"EOI"
insert into metadata
    (wmoid,name,lat,lon,elev,latest)
    values(?,?,?,?,?,?)
    on duplicate key update
    name=?,lat=?,lon=?,elev=?,latest=?
EOI
    ;
my $sth_insert_update = $dbh->prepare($query);

# query the offical database for all current stations
# get current time
my $endSecs = time() - 3600*12*abs($ARGV[0]);
my ($dum,$hour,$mday,$mon,$year);
($dum,$dum,$hour,$mday,$mon,$year)=gmtime($endSecs);
$mon++;  #start at 1, not zero
$year += 1900;
my $edate= sprintf("$year%2.2d%2.2d%2.2d",$mon,$mday,$hour);

# ask for 24 hours worth of data
($dum,$dum,$hour,$mday,$mon,$year)=gmtime($endSecs - 3600*24);
$mon++;  #start at 1, not zero
$year += 1900;
my $bdate= sprintf("$year%2.2d%2.2d%2.2d",$mon,$mday,$hour);

#print "bdate $bdate, edate $edate\n";

#generate the query string:
my %form = (shour => 'All Times',
	 ltype => 'All Levels',
	 wunits => 'Knots',
	 bdate => $bdate,
         edate => $edate,
	 access => 'All Sites',
	 view => 'NO',
	 osort => 'Station Series Sort',
	 oformat => 'FSL format (ASCII text)'
	 );

use URI::URL;
my $curl = url("");
$curl->query_form(%form);

#generate the POST request:
use LWP::UserAgent;
my $ua = new LWP::UserAgent;
$_ = "http://esrl.noaa.gov/raobs/intl/GetRaobs.cgi$curl";
my ($req_string) = m|([:/\w?%.=&()-+]*)|; # untaint
if($DEBUG) {print "request string is $req_string\n";}
my $req = new HTTP::Request 'GET', $req_string;
#make the request and see what came back:

my $tmp_file = "returned$$.tmp";
my $response = $ua->request($req,$tmp_file); # put the response in $tmp_file

my($vid,$flag,$wmo,$idat,$i);

my %month_num = (JAN => 1, FEB => 2, MAR => 3, APR => 4, MAY => 5, JUN => 6,
                 JUL => 7, AUG => 8, SEP => 9, OCT =>10, NOV =>11, DEC =>12);
open(DATA,$tmp_file);
my $icount=0;
my $line;
while (<DATA>) {
    #print;
    if (substr($_,4,3) eq '254') {
	$icount++;

# reset valid ob flag

	$flag = 0;
	my @hdr = split /\s+/;

# extract observation date from header line
	my $month_num = $month_num{$hdr[4]};
	my $raob_secs = timegm(0,0,$hdr[2],$hdr[3],$month_num-1,$hdr[5]);
	my $latest_dt = sql_date($raob_secs);

# extract values from the RAOB test
	my($lat,$lon);
	$line = <DATA>;
	#print "1: $line";
	my $wban = substr($line,9,5); # not used
	my $wmoid = substr($line,16,5);
	$wmoid += 0;		# make it a number
	my $lats = substr($line,23,6);
	my $lons = substr($line,29,7);
	my $elev = substr($line,38,4);
	$elev += 0;		# make it a number
	if($lats =~ /(.*)(N|S)/) {
	    $lat = $1*100;
	    my $ns = $2;
	    if($ns eq "S") {
		$lat = 0 - $lat;
	    }
	} else {
	    $lat = undef;
	}
	if($lons =~ /(.*)(E|W)/) {
	    $lon = $1*100;
	    my $ew = $2;
	    if($ew eq "W") {
		$lon = 0 - $lon;
	    }
	} else {
	    $lon = undef;
	}

# extract station id from observation and concantenate output string

	$line = <DATA>;	# nothing useful here
	$line = <DATA>;
	#print "2: $line";
	my $name = substr($line,17,4);
	$name =~ s/^\s+//;
	$name =~ s/\s+$//;
	if($name eq '9999') {
	    $name = $wmoid;
	}

	if(($db_name{$wmoid} ne $name) ||
	   $db_lat{$wmoid} ne $lat ||
	   $db_lon{$wmoid} ne $lon ||
	   $db_elev{$wmoid} ne $elev) {
	    print "NOT UPDATING metadata for $wmoid: \n".
		"|$db_name{$wmoid}| to |$name|, |$db_lat{$wmoid}| to |$lat|, ".
		"|$db_lon{$wmoid}| to |$lon|, |$db_elev{$wmoid}| to |$elev|\n";
	    print "$wmoid,$name,$lat,$lon,$elev,$latest_dt,".
		"$name,$lat,$lon,$elev,$latest_dt\n";
	    #$sth_insert_update->execute($wmoid,$name,$lat,$lon,$elev,$latest_dt,
		#			$name,$lat,$lon,$elev,$latest_dt);
	    #$db_name{$wmoid} = $name;
	    #$db_lat{$wmoid} = $lat;
	    #$db_lon{$wmoid} = $lon;
	    #$db_elev{$wmoid} = $elev;
	    #$prev_secs{$wmoid} = $raob_secs;
	}
	if($wmoid == 3005) {
	    print "$wmoid, $raob_secs,  $prev_secs{$wmoid}\n";
	}
	if($raob_secs > $prev_secs{$wmoid}) {
	    if($DEBUG) {print "updating |$wmoid| $name from $prev_secs{$wmoid} to $raob_secs\n";}
	    $sth_update_latest->execute($latest_dt,$wmoid);
	    $prev_secs{$wmoid} = $raob_secs;
	}
	#$prev_secs{$wmoid} = $raob_secs
	
    }
}
close(DATA);
unlink($tmp_file);
$sth_update_latest->finish();
$sth_insert_update->finish();

sub sql_date {
    my $time = shift;
    my($sec,$min,$hour,$mday,$mon,$year) = gmtime($time);
    $mon++;
    $year += 1900;
    return sprintf("%4d-%2.2d-%2.2d %2.2d:%2.2d:%2.2d",
		   $year,$mon,$mday,$hour,$min,$sec);
}
    

