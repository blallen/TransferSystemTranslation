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

# initialize option variables to be passed into instance of DB
my $max;
my $all         = 0;
my $nodbupdate  = 0;
my $wantedState = '.*';
my $wantedFiles = '.*';

# assigns variables from above into a hash that will be passed into GetOptions in initialize of DB object
# =s means a mandatory argument
my %options     = (
    help => $helpText,

    "all"        => \$all,
    "max=s"      => \$max,
    "state=s"    => \$wantedState,
    "pattern=s"  => \$wantedFiles,
    "nodbupdate" => \$nodbupdate,
);
# creates instance of DB
# returns a new SQL phrase book with a YAML loader class
# and a database handle for "CMS_STOMGR_W" (or another database loaded from .db.conf)
my $book = StorageManager::DB->new(%options);


my $done = 0;

# loop over Runs
# possible values of $book->getRunList()
# whatever you passed as a command line argument (most likely?)
# the latest run number in the database (if you don't pass any command line arguments)
# an array of runs determined by $last and $skip (if you pass these as command line arguments)
for my $runnumber ( $book->getRunList() ) {
    print "Checking run $runnumber:\n";
    my $badfiles = $book->checkRun($runnumber);
    # let's see what DB::checkRun() does
    # it returns a reference to a crazy nested hash with the structure below
    # %badfiles is a hash  with keys that are states
    # each state is a key to an anonymous hash that contains the filenames that are bad for that state as keys
    # each filename is a key to an anonymous hash that contains the row for this file from the query

# and now we do some stuff using the crazy %badfiles hash

# available states:
# FILES_NOT_FOUND
# FILES_CREATED
# FILES_INJECTED
# FILES_TRANS_NEW
# FILES_TRANS_COPIED
# FILES_TRANS_CHECKED
# FILES_TRANS_INSERTED
# FILES_TRANS_REPACKED
# FILES_DELETED

  # loops over a hash containing above states mapped to BLOCKED_* versions of them
  STATE:
    for my $state (map { ($_ => 'BLOCKED_' . $_) } @states) {
        next STATE unless exists $badfiles->{$state};
	# skips states that are not in badfiles hash
        print scalar keys %{ $badfiles->{$state} },
          " files in $state ($message{$state}):\n";
	# prints out number of files in given state (and what the meaning of state is)
        next STATE unless $state =~ /$wantedState/;
	# check is state is the one you are interested in (can be set as an option... is all by default?)
      # gets all bad files in the given state and then goes through them in order (assuming low to high)
      FILE:
        for my $filename ( sort keys %{ $badfiles->{$state} } ) {
            next FILE unless $filename =~ /$wantedFiles/;
	    # checks if file is the one you are interested in (set as an option, is all by default?)
            my $badfile = $badfiles->{$state}->{$filename};
	    # copy this particular file out of hash and into scalar variable $badfile
            my ( $host, $checksum, $size ) = @$badfile{qw(HOSTNAME CHECKSUM FILESIZE)};
	    # gets host machine, checksum, and size of file from the SQL query row for this file 
            $checksum = '' unless defined $checksum;
            $size = '' unless defined $size;
	    # set some safe defaults if things are undefined
            print "\t$filename $host $checksum $size\n"
              unless $hide{$state}
                  || (  !$full
                      && $state eq 'FILES_TRANS_CHECKED'
                      && $badfile->{STREAM} eq 'Error' );
	    # prints file info unless
	    # state is in hide vector --> default: FILES_TRANS_INSERTED, FILES_TRANS_REPACKED, FILES DELETED
	    # or
	    # the full display option is off, STATE is FILES_TRANS_CHECKED, and value of Stream (from query) is Error

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
} # end loop over runs
print "Reached maximum number of allowed updates ($max < $done).\n"
  if defined $max && $done > $max;
