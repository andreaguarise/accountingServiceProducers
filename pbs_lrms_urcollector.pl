#!/usr/bin/perl

use strict;

use IO::Handle;

my $lrmsLogDir ="";

sub error
{
	print "Error: $_[0]\n";
}

sub printLog
{
	print "Log: $_[1]\n";
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

while (@ARGV)
{
        $lrmsLogDir = shift @ARGV;
                # take it as configuration file name
}

my (			$dev,   $ino,     $mode, $nlink, $uid,
				$gid,   $rdev,    $size, $atime, $mtime,
				$ctime, $blksize, $blocks
			);    # these are dummies

my $timeToWaitForNewEvents = 5;
my $lastProcessedLrmsId = "15542.t2-ce-01.to.infn.it";
my $lastProcessedDateTime = "04/21/2013 23:49:16";

my @lrmsLogFiles;
my %logFInodes = ();
my %logFSizes  = ();
my %logFMod    = ();


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
while ( @sortedLrmsLogFiles )
{
		
	my $thisLogFile = shift(@sortedLrmsLogFiles);
	my $secsWaited = 0;
	open(FH, "$lrmsLogDir/$thisLogFile") or die "Can't open file $thisLogFile: $!";
	for(;;)
	{
		while (<FH>)
		{
			my $date;
			my $lrmsid;
			my $event;
			if ( $_ =~ /^(.*);(.);(.*);(.*)$/ )
			{
				$date = $1;
				$event = $2;
				$lrmsid = $3;
				if ( $canProcess && ($event eq "E")) 
				{ 
					my %record = &parseUR_pbs($_);
					print "$record{lrmsId}\n"; 
				}
				if ( ($date eq $lastProcessedDateTime ) && ($lrmsid eq $lastProcessedLrmsId) ) 
				{
					$canProcess = 1;
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

$canProcess = 0;

