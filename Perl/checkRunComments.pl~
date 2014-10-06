#!/usr/bin/env perl

use strict;
use warnings;

use lib '/nfshome0/smpro/scripts'; #manipulates @INC at compile time, @INC being the hash that tells the compiler where to look for 
#the file.  Tells module where it may be loaded from.
use StorageManager::DB 
#imports all symbols in @EXPORT and @EXPORT_OK in DB.pm
  qw( $debug $fix @states %message %hide $hostname $t0script $force $doChecksum
  $full );
# multiline text file
my $helpText = <<'END_OF_HELP';
    all        => Try to run and fix on all nodes, re-submitting all files
    max=s      => Maximum number of files to fix, useful for testing
    state=s    => Limit to files in this state (regex)
    pattern=s  => Limit to files matching this pattern
    nodbupdate => Do not update the DB when re-submitting (so no duplicate
                  entry in FILES_TRANS_NEW). Also true when --checksum is used
END_OF_HELP
# declaring vars
my $max;
my $all         = 0;
my $nodbupdate  = 0;
my $wantedState = '.*';
my $wantedFiles = '.*';
my %options     = (
    help => $helpText,

    "all"        => \$all,
    "max=s"      => \$max,
    "state=s"    => \$wantedState,
    "pattern=s"  => \$wantedFiles,
    "nodbupdate" => \$nodbupdate,
);
#creates instance of class (again, why ANYONE would choose perl for OO programming is beyond me)
my $book = StorageManager::DB->new(%options);
my $done = 0;

for my $runnumber ( $book->getRunList() ) {
    print "Checking run $runnumber:\n";
    my $badfiles = $book->checkRun($runnumber);
#Here's where things get weird.  Again, knowing regex will come in VERY handy for this project.  I think Obasi worked more on the stuff below.

  STATE:
    for my $state (map { ($_ => 'BLOCKED_' . $_) } @states) {
        next STATE unless exists $badfiles->{$state};
        print scalar keys %{ $badfiles->{$state} },
          " files in $state ($message{$state}):\n";
        next STATE unless $state =~ /$wantedState/;
      FILE:
        for my $filename ( sort keys %{ $badfiles->{$state} } ) {
            next FILE unless $filename =~ /$wantedFiles/;
            my $badfile = $badfiles->{$state}->{$filename};
            my ( $host, $checksum, $size ) = @$badfile{qw(HOSTNAME CHECKSUM FILESIZE)};
            $checksum = '' unless defined $checksum;
            $size = '' unless defined $size;
            print "\t$filename $host $checksum $size\n"
              unless $hide{$state}
                  || (  !$full
                      && $state eq 'FILES_TRANS_CHECKED'
                      && $badfile->{STREAM} eq 'Error' );

            # Trying to fix
            if ( $state =~ /FILES_(NEW|CREATED)/ ) {
                $book->lookForFile( $1 => $filename, $host );
                next FILE;
            }
            next FILE unless $badfile->{PATHNAME};
            my $file = $badfile->{PATHNAME} . "/" . $filename;
            if ( $state eq 'FILES_TRANS_NEW' ) {
                $book->checkChecksum( $file, $badfile, $state );
            }
            if ( !$fix ) {
                next FILE;
            }
            elsif ( !$force && $badfile->{FILES_TRANS_COPIED} ) {
                print "\tGoing to next file\n";
                next FILE;
            }
            next unless $all || -f $file;
            print "\t$file $host $checksum\n";
            if ( defined $max && $done++ > $max ) {
                next FILE;
            }
            $badfile->{HLTKEY} =~ s/^HLTKEY=//;
            $badfile->{HLTKEY} ||= $book->getHltKey($runnumber);

            $badfile->{INDEX} = $badfile
              ->{INDFILENAME};    # Oracle does not allow INDEX as AS clause
            my @notify = ($t0script);
            push @notify, '--nodbupdate' if $nodbupdate || $doChecksum;
            for (
                qw( APP_NAME APP_VERSION RUNNUMBER LUMISECTION FILENAME
                PATHNAME HOSTNAME DESTINATION SETUPLABEL
                STREAM TYPE NEVENTS FILESIZE CHECKSUM HLTKEY )
              )
            {                     #INDEX START_TIME STOP_TIME
                if ( exists $badfile->{$_} and defined $badfile->{$_} ) {
                    push @notify, "--$_=" . $badfile->{$_};
                }
                else {
                    print "WARNING: Missing $_ for $file\n";
                    $done--;
                    next FILE;
                }
            }
            print "Notifying Tier0 for $file\n";
            system( $t0script, @notify );
        }
    }
}
print "Reached maximum number of allowed updates ($max < $done).\n"
  if defined $max && $done > $max;