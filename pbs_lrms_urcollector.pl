#!/usr/bin/perl

use strict;

use IO::Handle;
use POSIX;
use Getopt::Long;
use Sys::Syslog;
use DBI;
use JSON;
use Time::HiRes qw(usleep ualarm gettimeofday tv_interval);

# variables set up in configuration
my $lrmsLogDir ="";
my $database = "/tmp/collector.sqlite";
my $collectorBufferFileName = "/tmp/collectorBuffer";
my $systemLogLevel = 7;
my $logType = 0;

# global variables used internally.
my $lastLog;
my $logCounter = 0;
my $keepGoing = 1;
my $dryrun;

GetOptions(
	'lrmsLog=s'    => \$lrmsLogDir,
	'l=s'          => \$lrmsLogDir,
	'database=s'   => \$database,
	'd=s'          => \$database,
	'bufferFile=s' => \$collectorBufferFileName,
	'b=s'          => \$collectorBufferFileName,
	"dryrun"  => \$dryrun
);


##-------> sig handlers subroutines <---------##

sub sigINT_handler
{
	&printLog( 3, "got SIGINT" );
	$keepGoing = 0;
}

sub error
{
	if ( scalar(@_) > 0 )
	{
		&printLog( 2, "$_[0]" );
	}
}

sub printLog
{
	#my $logLevel = $_[0];
	#my $log      = $_[1];
	if ( $_[0] <= $systemLogLevel )
	{
		if ( $logType == 1 )
		{
			my $pri = "";
		  SWITCH:
			{
				if ( $_[0] == 0 ) { $pri = 'crit';    last SWITCH; }
				if ( $_[0] == 1 ) { $pri = 'err';     last SWITCH; }
				if ( $_[0] == 2 ) { $pri = 'warning'; last SWITCH; }
				if ( $_[0] == 3 ) { $pri = 'warning'; last SWITCH; }
				if ( $_[0] == 4 ) { $pri = 'notice';  last SWITCH; }
				if ( $_[0] == 5 ) { $pri = 'notice';  last SWITCH; }
				if ( $_[0] == 6 ) { $pri = 'info';    last SWITCH; }
				if ( $_[0] == 7 ) { $pri = 'info';    last SWITCH; }
				if ( $_[0] == 8 ) { $pri = 'debug';   last SWITCH; }
				if ( $_[0] == 9 ) { $pri = 'debug';   last SWITCH; }
				my $nothing = 1;
			}
			syslog( $pri, $_[1] );
		}
		else
		{
			my $localtime = localtime();
			if ( $_[1] ne $lastLog )
			{
				if ( $logCounter != 0 )
				{
					print LOGH
					  "$localtime: Last message repeated $logCounter times.\n";
				}
				$logCounter = 0;
				print LOGH "$localtime: " . $_[1] . "\n";
			}
			else
			{
				$logCounter++;
				if ( $logCounter == 20 )
				{
					print LOGH "$localtime: Last message repeated 20 times.\n";
					$logCounter = 0;
				}
			}
			$lastLog = $_[1];
		}
	}
}

sub putBuffer
{

	# arguments are: 0 = buffer name
	#                1 = last LRMS job id
	#                2 = last LRMS job timestamp (log time)
	my $buffName = $_[0];
	
	if ( $_[1] eq "" )
	{
		&printLog( 1, "ASSERT Write in Buffer $_[0]; EMPTY LRMS ID. Not Updating Buffer.", 1 );
		return;
	}
	#open( OUT, "> $buffName" ) || return 2;
	#print OUT "$_[1]:$_[2]\n";
	#&printLog( 7, "Write in Buffer lrmsId:$_[1];timstamp:$_[2]", 1 );
	#close(OUT);
	open(TMP, ">", "$buffName.tmp") || return 2;
	print TMP "$_[1]:$_[2]\n";
	&printLog( 7, "Write in Buffer lrmsId:$_[1];timstamp:$_[2]", 1 );
	close(TMP);
	rename($buffName, "$buffName.ori");
	rename("$buffName.tmp", $buffName);
	return 0;
}

sub readBuffer
{
	my $buffname = $_[0];
	open( IN, "< $buffname" ) || return 2;
	my $lrmsid;
	my $tstamp;
	while (<IN>)
	{
		if ( $_ =~ /^(.*?):(.*?)$/ )
		{
			$lrmsid = $1;
			$tstamp = $2;
		}
	}
	close(IN);
	&printLog( 8, "buffer: $buffname. First job: id=$lrmsid; timestamp=$tstamp" );
	$_[1] = $lrmsid;
	$_[2] = $tstamp;
	return 0;
}

sub parseUR_pbs
{
	my %pbsRecord = ();
	my $URString = $_[0];
	&printLog( 8, "UR string:\n$URString" );

	my @URArray  = split( ' ', $URString );
	my @tmpArray = split( ';', $URArray[1] );
	$_ = $tmpArray[3];
	if (/^user=(.*)$/) { $pbsRecord{user} = $1; }
	$pbsRecord{lrmsId} = $tmpArray[2];
	$_ = $tmpArray[2];
	if (/^(\d*)\.(.*)$/) { $pbsRecord{server} = $2; }
	foreach (@URArray)
	{
		if (/^queue=(.*)$/) { $pbsRecord{queue} = $1; }
		if (/^resources_used.cput=(.*)$/)
		{
			$_ = $1;
			$_ =~ /(\d*):(\d\d):(\d\d)$/;
			$pbsRecord{cput} = $3 + $2 * 60 + $1 * 3600;
		}
		if (/^resources_used.walltime=(.*)$/)
		{
			$_ = $1;
			$_ =~ /(\d*):(\d\d):(\d\d)$/;
			$pbsRecord{walltime} = $3 + $2 * 60 + $1 * 3600;
		}
		if (/^resources_used.vmem=(.*)$/)
		{
			$_ = $1;
			$_ =~ /(\d*[M.k]b)$/;
			$pbsRecord{vmem} = $1;
		}
		if (/^resources_used.mem=(.*)$/)
		{
			$_ = $1;
			$_ =~ /(\d*[M.k]b)$/;
			$pbsRecord{mem} = $1;
		}
		if (/^Resource_List.ncpus=(\d*)$/)
		{
			$pbsRecord{processors} = $1;

			# attention! might also be list of hostnames,
			# in this case the number of hosts should be
			# counted!? What about SMP machines; is their
			# hostname listed N times or only once??
		}
		if (/^group=(.*)$/)
		{
			$pbsRecord{group} = $1;
		}
		if (/^jobname=(.*)$/)
		{
			$pbsRecord{jobName} = $1;
		}
		if (/^ctime=(\d*)$/)
		{
			$pbsRecord{ctime} = $1;
		}
		if (/^qtime=(\d*)$/)
		{
			$pbsRecord{qtime} = $1;
		}
		if (/^etime=(\d*)$/)
		{
			$pbsRecord{etime} = $1;
		}
		if (/^start=(\d*)$/)
		{
			$pbsRecord{start} = $1;
		}
		if (/^end=(\d*)$/)
		{
			$pbsRecord{end} = $1;
		}
		if (/^exec_host=(.*)$/)
		{
			$pbsRecord{execHost} = $1;
		}
		if (/^Exit_status=(\d*)$/)
		{
			$pbsRecord{exitStatus} = $1;
		}
	}
	return %pbsRecord;
}

sub sqliteRecordInsert
{
	my $recordString = $_[1];
	my $recorddate =$_[2];
	my $lrmsid = $_[3];
	
	my $sqlStatement =
"INSERT INTO records (key, lrmstype, record, recordDate, lrmsId, commandStatus) VALUES (NULL,'torque','$recordString','$recorddate', '$lrmsid',0)";
			my $sth =
			  $_[0]->prepare(
"INSERT INTO records (key, lrmstype, record, recordDate, lrmsId, commandStatus) VALUES (NULL,'torque',?,?,?,0)"
			  );
			my $querySuccesfull = 1;
			my $queryCounter    = 0;
			while ( $querySuccesfull )
			{
				eval {
					my $res =
					  $sth->execute( $recordString, $recorddate,
						$lrmsid );
				};
				if ($@)
				{
					&printLog( 3, "WARN: ($queryCounter) $@" );
					print "Retrying in $queryCounter\n";
					for (
						my $i = 0 ;
						$i < $queryCounter ;
						$i++
					  )
					{
						sleep $i;
					}
					$queryCounter++;
				}
				else
				{
					$querySuccesfull = 0;
					&printLog( 9, "$sqlStatement" );
				}
				last if ( $queryCounter >= 10 );
			}
			return $querySuccesfull;
}

##  MAIN ##

while (@ARGV)
{
        $lrmsLogDir = shift @ARGV;
                # take it as configuration file name
}

my (			$dev,   $ino,     $mode, $nlink, $uid,
				$gid,   $rdev,    $size, $atime, $mtime,
				$ctime, $blksize, $blocks
			);    # these are dummies

my $sigset    = POSIX::SigSet->new();

my $actionInt =
  POSIX::SigAction->new( "sigINT_handler", $sigset, &POSIX::SA_NODEFER );
POSIX::sigaction( &POSIX::SIGINT,  $actionInt );
POSIX::sigaction( &POSIX::SIGTERM, $actionInt );

my $timeToWaitForNewEvents = 5;

my $sqlStatement =
"CREATE TABLE IF NOT EXISTS records (key INTEGER PRIMARY KEY, lrmstype TEXT, record TEXT, recordDate TEXT, lrmsId TEXT, commandStatus INT)";
my $sqliteCmd = "/usr/bin/sqlite3 $database \"$sqlStatement\"";
my $status    = system("$sqliteCmd");

my $dbh = DBI->connect("dbi:SQLite:$database")
  || die "Cannot connect: $DBI::errstr";
$dbh->{AutoCommit} = 1;    # disable transactions.
$dbh->{RaiseError} = 1;

my $lastProcessedLrmsId;
my $lastProcessedDateTime;

&readBuffer( $collectorBufferFileName, $lastProcessedLrmsId, $lastProcessedDateTime );
print "Starting from: $lastProcessedLrmsId:$lastProcessedDateTime\n";

my @lrmsLogFiles;
my %logFInodes = ();
my %logFSizes  = ();
my %logFMod    = ();

print "Using logs from $lrmsLogDir\n";

opendir( DIR, $lrmsLogDir ) || &error("Error: can't open dir $lrmsLogDir: $!");
		while ( defined( my $file = readdir(DIR) ) )
		{
			next if ( $file =~ /^\.\.?$/ );    # skip '.' and '..'
			push @lrmsLogFiles, $file;

			# keep track of last modification timestamp:
			# only inode, size and modification timestamp are interesting!
			(
				$dev,   $logFInodes{$file}, $mode,  $nlink,
				$uid,   $gid,               $rdev,  $logFSizes{$file},
				$atime, $logFMod{$file},    $ctime, $blksize,
				$blocks
			 )
			  = stat("$lrmsLogDir/$file");
		}
		my @sortedLrmsLogFiles =
		  ( sort { $logFMod{$a} <=> $logFMod{$b} } keys %logFMod );
		closedir DIR;

my $canProcess = 0;
if ( ($lastProcessedDateTime eq "") && ($lastProcessedLrmsId eq "") )
{
	#Empty buffer: first run.
	$canProcess = 1;
}
my $mainRecordsCounter = 0;
while ( @sortedLrmsLogFiles && $keepGoing)
{
		
	my $thisLogFile = shift(@sortedLrmsLogFiles);
	my $secsWaited = 0;
	open(FH, "$lrmsLogDir/$thisLogFile") or die "Can't open file $thisLogFile: $!";
	while( $keepGoing )
	{
		my $t1 = [gettimeofday];
		my $date;
		my $lrmsid;
		while (<FH>)
		{
			
			my $event;
			if ( $_ =~ /^(.*);(.);(.*);(.*)$/ )
			{
				$date = $1;
				$event = $2;
				$lrmsid = $3;
				if ( $canProcess && ($event eq "E")) 
				{ 
					my %record = &parseUR_pbs($_);
					my $json_record = to_json(\%record);
					print "$lrmsid\n"; 
					if ( &sqliteRecordInsert ($dbh, $json_record, $date, $lrmsid) != 0 )
					{
						exit 1;
					}
					&putBuffer($collectorBufferFileName,$lrmsid,$date);
					$mainRecordsCounter += 1;
				}
				if ( ($date eq $lastProcessedDateTime ) && ($lrmsid eq $lastProcessedLrmsId) && ($event eq "E") ) 
				{
					$canProcess = 1;
				}
				my $elapsed = 0;
				my $jobs_min = 0;
				my $min_krecords = 0.0;
				my $elapsed      = tv_interval( $t1, [gettimeofday] );
				if ( $elapsed > 0) 
				{
					$jobs_min     = ( $mainRecordsCounter / $elapsed ) * 60;
				}
				if ( $jobs_min > 0 )
				{
					$min_krecords = 1000.0 / $jobs_min;
				}
				$jobs_min     = sprintf( "%.2f", $jobs_min );
				$min_krecords = sprintf( "%.1f", $min_krecords );
				#&printLog( 4,
				#	"Processed: $mainRecordsCounter,Elapsed: $elapsed,Records/min:$jobs_min,min/KRec: $min_krecords"
				#);
				if ( $mainRecordsCounter >= 100 )
				{
					print "Processed: $mainRecordsCounter,Elapsed: $elapsed,Records/min:$jobs_min,min/KRec: $min_krecords\n";
					if ( $keepGoing == 0 )
					{
						&putBuffer($collectorBufferFileName,$lrmsid,$date);
						exit 0;
					}
					$mainRecordsCounter = 0;
					$t1                 = [gettimeofday];
				}
			}
		}
		$secsWaited += 1;
		sleep 1;
		last if @sortedLrmsLogFiles;
		FH->clearerr();
		if ( $secsWaited > $timeToWaitForNewEvents )
		{
			close FH;
			exit 0;	
		}
	}
	close FH;
}

