package StorageManager::DB;  #In perl, modules and packages are not the same thing (because Larry Wall hates us, presumably).
# Hence you have to declare that modules exist in a different namespace.  The general rule is declare one namespace per module,
# and no more.

use strict;
use warnings;
use File::Spec::Functions; #imports catfile function, which concatenates directory+filename
use File::Basename; #imports dirname function, which yields directory from file path
use DBI; #used for interacting with Oracle SQL database.  It might be helpful to do a quick review of SQL if you haven't already.
use Getopt::Long; 
use Data::Phrasebook;
#for reference, the qw function creates a list of all of the strings passed as parameters.
use Exporter qw( import ); #no need to call on namespace when using function
use POSIX qw( strftime ); #creates string of date and time

sub help {
    #All parameters passed to a function are automatically assigned to @_, a special built-in var.  When you 
    #use shift without an argument (as below), it makes @_ the default argument and takes the first item in the list.
    my $helpText = shift;
    #below is just a giant block of text to be printed 
    print <<'END_OF_HELP';
Common options:
    h|help  => Displays help
    debug   => Enables debugging output
    deleted => Show also deleted files

    config=s    => Database configuration file, defaults to ~/.db.conf
    runnumber=s => Runnumber to look at. Defaults to last - skip
    last=s      => Last runnumbers to check
    skip=s      => Number of runs to skip, used with last
    hostname=s  => Restricts to hostname of files to check
    force       => Force re-transmission of files, even if already copied
    fix         => Try to fix things, otherwise only display
    full        => Full display, otherwise display only potential errors
    checksum    => When fixing files, re-calculate checksums (slow)
    nochecksum  => When fixing files, set checksum to 0 to force copy
    removebad   => When fixing files, remove bad events before sending
    log         => Check the rfcp\'s output
END_OF_HELP
    if ($helpText) {
        #Here they are using Regex to search $0 for matching patterns.  There is a lot of regex in this code so it might help to learn regex.
        my ($scriptName) = $0 =~ m#([^/]*)\.pl$#;
        print "\nSpecific options for $scriptName:\n$helpText";
    }
    exit;
}

# Exporter stuff.  Tells the code what vars to export automatically and which to export only when told to, respectively.
our @EXPORT    = qw( $debug $fix $config );
our @EXPORT_OK = qw( @states %message $hostre $hostname $t0script $force
  $fix $doChecksum %hide $debug $full $removeBad );
our $VERSION = 1.00;

# Exported variables
our @states = map { "FILES_$_" }
  qw( CREATED INJECTED TRANS_NEW TRANS_COPIED TRANS_CHECKED TRANS_INSERTED
  TRANS_REPACKED DELETED );
our %message = (
    FILES_NOT_FOUND => "File not found in database",
    FILES_CREATED  => "File found in database but not passed over to T0 system",
    FILES_INJECTED => "File found in database and handed over to T0 system",
    FILES_TRANS_NEW =>
      "File found in database and being processed by T0 system",
    FILES_TRANS_COPIED   => "File found in database and copied by T0 system",
    FILES_TRANS_CHECKED  => "File found in database and checked by T0 system",
    FILES_TRANS_INSERTED => "File found in database and inserted into T0ast",
    FILES_TRANS_REPACKED =>
      "File found in database and sucessfully processed by T0 system",
    FILES_DELETED =>
      "File found in database, sucessfully processed and locally deleted",
    map { 'BLOCKED_' . $_ => "File blocked in $_ but further states are done" }
      @states
);
our %hide = (
    FILES_TRANS_INSERTED => 1,
    FILES_TRANS_REPACKED => 1,
    FILES_DELETED        => 1,
);
our $hostre     = qr/^srv-c2(?:c0[67]-\d\d|d05-02)$/i;
our $debug      = 0;
our $force      = 0;
our $full       = 0;
our $showLog    = 0;
our $fix        = $0 =~ /^fix/;
our $doChecksum = 0;
our $removeBad  = 0;
our $t0dir      = "/nfshome0/cmsprod/TransferTest";
our $t0script   = "$t0dir/injection/sendNotification.sh";
# load config settings from config file
our $config     = catfile( $ENV{HOME}, '.db.conf' );
our $hostname;

#our $t0script = '/nfshome0/babar/work/T0/operations/sendNotification.sh';

# Private variables
my ( $help, @runs );
my $last        = undef;
my $skip        = undef;
my $noChecksum  = 0;
my $showDeleted = 0;
my @savedARGV   = @ARGV;
my $readerDatabaseHandler;
my $writerDatabaseHandler;
my $tier0DatabaseHandler;

# To use Harry/Rémi's tools
my $scram_arch = '/nfshome0/cmssw2/slc5_amd64_gcc462';
my $cmssw      = "$scram_arch/cms/cmssw/CMSSW_5_2_6";
$ENV{LD_LIBRARY_PATH} = join ":",
  "$cmssw/external/slc5_amd64_gcc462/lib", "$cmssw/lib/slc5_amd64_gcc462",
  "$scram_arch/external/gcc/4.6.2/lib64",  "$ENV{LD_LIBRARY_PATH}";

# Constructor. Takes options for Getopt as parameter
sub new {
    #unpacks arguments (first one is the class so it's put there, rest are the options so they go in a new options hash)
    my ( $class, %options ) = @_;
    #creates instance of class
    my $self = bless {}, $class;

    # remove help text from options hash and put into its own variable
    my $helpText = delete $options{help};

    #    local @ARGV = @ARGV;
    # searches for the following strings in the list of flags passed when running the program.  If it finds one of these, it assigns a value to the variable the array points to?  It is easiest to substantially change this part of the code in python (this method is considered outdated and hard to read)
    # =s means a mandatory argument
    GetOptions(
        "h|help"  => \$help,
        "debug"   => \$debug,
        "deleted" => \$showDeleted,

        "config=s"    => \$config, # loaded from a config file
        "runnumber=s" => \@runs,
        "last=s"      => \$last,
        "skip=s"      => \$skip,
        "hostname=s"  => \$hostname,
        "force"       => \$force,
        "fix"         => \$fix,
        "full"        => \$full,
        "checksum"    => \$doChecksum,
        "nochecksum"  => \$noChecksum,
        "removebad"   => \$removeBad,
        "log"         => \$showLog,
        %options
    ) or die "GetOptions failed: $!";

    help($helpText) if $help;
    $debug && print "Creating book with config $config\n";

    # calls get book method which intializing a SQL phrasebook to interface with the database
    $self->{book} = $self->getBook($config); # use config file to generate phrasebook and store as $self->{book}
    if ($full) {
        delete @hide{qw(FILES_TRANS_INSERTED FILES_TRANS_REPACKED)};
    }
    if ($showDeleted) {
        delete $hide{FILES_DELETED};
    }
    return $self;
}

# Returns a phrasebook for SQL queries
#I think Obasi did some work on these subroutines, so he might be able to provide some insight here.
sub getBook {
    my ( $self, @args ) = @_; # @args = $config from DB->new()
    # ||= assigns a new phrasebook to self->book if it is unassigned
    return $self->{book} ||= Data::Phrasebook->new(
        class  => 'SQL', # initialize as SQL class
        loader => 'YAML', # use YAML loader class
	# loads a database handler
        dbh    => $self->getDatabaseHandler(@args), # get a database handler based on info in config file 
	                                            # default database is "CMS_STOMGR_W" 
        # load phrasebook library from file
	file =>
          catfile( dirname( $INC{"StorageManager/DB.pm"} ), "sql_lib.yml" ),
        debug => 2,
    );
}

# Proxy queries to the book
sub query {
    my ( $self, @args ) = @_;
    my $book = $self->getBook; # get phrasebook
    return $book->query(@args);
}

# Get the run list based on parameters, or last runs
sub getRunList {
    my $self = shift;
    # check if @runs already exists
    unless (@runs) { 
        my $book = $self->getBook(); # get phrasebook
        my $sth  = $book->query('getLatestRun'); # returns a statement handle with an array of run numbers listed in descending order
        $skip = 1 unless defined $skip; # should be initialized to undef when DB class is initialized
        $last = 2 unless defined $last; # should be initialized to undef when DB class is initialized
        unless (
            @runs = map { $_->[0] } @{ $sth->fetchall_arrayref( [0], $last + $skip ) }
	    # @{stuff} returns an array of array references to the run numbers for the three latest runs 
	    # map { $_->[0] @array grabs first element from array
	    # thus @runs is an array of run numbers for the 3 latest runs in descending order
          )
        {
            die "You did not specify a run number,"
              . " and I could not find the latest one.";
        }
    }

    $skip ||= 0; # sets skip to zero if @runs already existed
    $last ||= @runs; # sets last equal to the number of run numbers in @runs if @runs already existed
    @runs = reverse splice @runs, $skip, $last;
    # splice @array, offset, number of elements to remove
    # so splice command saves latest run number and removes the next two runs
    # so input to reverse should be a array of length 1
    # reverse @array sends $array[0] to $array[n-1], $array[1] to $array[n-2], etc and vice versa 
    # thus reverse does nothing in default case, 
    # but would give you a set of runs in ascending order if you changed the default skip and last parameters
    if ($#runs) { # $#runs is max index of @runs array.... should be 0 at this point????
        print "Will check " . @runs . " runs: " . join( " ", @runs ) . "\n";
    }
    return @runs;
}

# Read configuration file properly
sub readConfig {
    my ( $self, $config, $wantWriter ) = @_; # $config = config file, 
                                             # $wantWriter = option to determine what kind of interface
    # okay let's declare a lot of variables
    my ( $reader, $phrase, $dbi, $writer, $wphrase, $t0writer, $t0phrase );
    # checks if you have a readable config file
    if ( $config && -r $config ) {
	# opens config file and assigns its handle to $fh; returns an error message if this fails
        open my $fh, '<', $config or die "open($config): $!";
	# read entire config file
        while (<$fh>) {
            next if /^\s*#/; # skip comments (lines starting with #)
	    # the below regex translates to
	    # " $var = 'string' ;" or ' $var = "string" ;' or " $var = value ;"
	    # so this config file is basically a list of perl variable declarations which we import 
	    # (I hope, still need to look at actual file)
            if ( my ( $key, $value ) =
                /^\s*\$(\w+)\s*=\s*['"]?(\S+?)["']?\s*;/ )
		# assigns values to the following variables from the config file
            {
                $reader   = $value, next if $key eq "reader";   # default is "CMS_STOMGR_W"
                $phrase   = $value, next if $key eq "phrase";   # pass phrase
                $dbi      = $value, next if $key eq "dbi";      # default is "DBI:Oracle:CMS_RCMS"
                $writer   = $value, next if $key eq "writer";
                $wphrase  = $value, next if $key eq "wphrase";
                $t0writer = $value, next if $key eq "t0writer";
                $t0phrase = $value, next if $key eq "t0phrase";
                $debug
                  && print "Ignoring unknown configuration variable: $key\n";
            }
        }
        close $fh;
    }

    
    if ($wantWriter) {
        # if called from getWriterDatabaseHandler() make sure have appropriate config
	if ( $wantWriter == 1 ) {

            unless ( $writer && $wphrase ) {
                die "No writer DB configuration. Aborting.";
            }
            return ( $writer, $wphrase, $dbi );
        }
	# if called from getTier0DatabaseHandler() make sure have appropriate config
        elsif ( $wantWriter == 2 ) {
            unless ( $t0writer && $t0phrase ) {
                die "No Tier0 writer DB configuration. Aborting.";
            }
            return ( $t0writer, $t0phrase, $dbi );
        }
        else {
            die "Unknown DB configuration requested: $wantWriter";
        }
    }
    # if called from getDatabaseHandler() make sure have appropriate config
    unless ( $reader && $phrase ) {
        die "No DB configuration. Aborting.";
    }
    return ( $reader, $phrase, $dbi );
}

# setup DB connection
sub getDatabaseHandler {
    my ( $self, $config ) = @_; # $config = @args from DB->getBook() = $config from DB->new() which is read from a config file
    return $readerDatabaseHandler if defined $readerDatabaseHandler; # this only happens if a book has been loaded before
    # or at least this variable is uninitialized in this file
    # reads config file and saves them to new variables
    my ( $reader, $phrase, $dbi ) = $self->readConfig($config); # passes config file to readConfig (which makes sense)
    # defaults to:
    # $reader = "CMS_STOMGR_W"
    # $phrase = "a pass word"
    # $dbi = "DBI:Oracle:CMS_RCMS"
    $debug && print "Setting up DB connection for $dbi and $reader\n";
    # get database handler using info from config file
    my $dbh = DBI->connect( $dbi, $reader, $phrase )
      or die("Error: Connection to Oracle DB failed");
    $debug && print "DB connection set up successfully \n";
    return $readerDatabaseHandler = $dbh;
}

sub getWriterDatabaseHandler {
    my ( $self, $config ) = @_; 
    return $writerDatabaseHandler if defined $writerDatabaseHandler;
    my ( $writer, $wphrase, $dbi ) = $self->readConfig( $config, 1 );
    $debug && print "Setting up DB connection for $dbi and $writer\n";
    my $dbh = DBI->connect( $dbi, $writer, $wphrase )
      or die("Error: Connection to Oracle DB failed");
    $debug && print "DB connection set up successfully \n";
    return $writerDatabaseHandler = $dbh;
}

sub getTier0DatabaseHandler {
    my ( $self, $config ) = @_;
    return $tier0DatabaseHandler if defined $tier0DatabaseHandler;
    my ( $t0writer, $t0phrase, $dbi ) = $self->readConfig( $config, 2 );
    $debug && print "Setting up DB connection for $dbi and $t0writer\n";
    my $dbh = DBI->connect( $dbi, $t0writer, $t0phrase )
      or die("Error: Connection to Oracle DB failed");
    $debug && print "DB connection set up successfully \n";
    return $tier0DatabaseHandler = $dbh;
}

sub lookForFile {
    my ( $self, $kind, $file, $host ) = @_;
    my %kinds = ( NEW => 'insert', CREATED => 'close' );
    return if !$fix;
    return if !exists $kinds{$kind};
    $kind = $kinds{$kind};
    $hostname ||= $ENV{HOSTNAME};
    chomp( $hostname = qx{ hostname } ) unless $hostname;
    return if $host ne $hostname;
    my $files = strftime "/store/global/log/%Y%m*-$host-*.log", localtime time;  #what do these strings mean???
    my $fixFile = strftime "/store/global/log/%Y%m%d-$host-fix.log",  #?
      localtime time;

    my @fileList = reverse sort grep { !/-fix\.log$/ } glob($files);
    for my $lookFile ( splice( @fileList, 0, 10 ) ) {
        open( my $fh, '<', $lookFile )
          or die "Cannot open $lookFile for reading: $!";
        while (<$fh>) {
            if (/^\.\/${kind}File\.pl\s+--FILENAME\s+$file\s+/) {
                print "Found $file in $lookFile:\n$_";
                open my $fix, '>>', $fixFile
                  or die "Cannot open $fixFile for writing: $!";
                print $fix $_;
                close $fix;
                if ( $kind eq 'insert' ) {
                    $kind = 'close';
                }
                else {
                    close $fh;
                    return;
                }
            }
        }
        close $fh;
    }
    print "Could not find $file for $kind\n";
}

sub checkStreamerFile {
    my ( $self, $file, $filename ) = @_;
    my $output = qx{ $cmssw/bin/slc5_amd64_gcc462/DiagStreamerFile $file 2>&1 };
    if (
        my (
            $totalEvents,         $badHeadersEvents, $badChecksumEvents,
            $badUncompressEvents, $duplicatedEvents
        )
        = $output =~ /^read \s (\d+) \s events$ \n
            ^and \s (\d+) \s events \s with \s bad \s headers$ \n
            ^and \s (\d+) \s events \s with \s bad \s check \s sum$ \n
            ^and \s (\d+) \s events \s with \s bad \s uncompress$ \n
            ^and \s (\d+) \s duplicated \s event \s Id$/xms
      )
    {
        my $diagLogFile = "/tmp/$filename.diag";
        open my $diag, '>', $diagLogFile
          or die "Cannot open $diagLogFile for writing: $!";
        print $diag $output;
        close $diag;
        my $badEvents =
          $badHeadersEvents + $badChecksumEvents + $duplicatedEvents;
        $badEvents += $badUncompressEvents
          if $badChecksumEvents !=
          $badUncompressEvents;    # Assume bad checksum causes bad uncompress
        my $badRatio = int( $badEvents / $totalEvents * 10000 ) / 100;

        if ( $badEvents == 0 ) {
            print "\t\tNo bad event, sending as-is\n";
            return 0;              # Will continue processing
        }
        elsif ($removeBad) {       # User required fixing
            rename $file, "$file.bad" or die "Cannot rename bogus $file: $!";
            my $fixOutput =
qx{ $cmssw/bin/slc5_amd64_gcc462/DiagStreamerFile $file.bad $file 2>&1 };
            my $diagFixLogFile = "/tmp/$filename.diagFix";
            open my $diagFix, '>', $diagFixLogFile
              or die "Cannot open $diagFixLogFile for writing: $!";
            print $diagFix $fixOutput;
            close $diagFix;
            if (
                my (
                    $totalFixEvents,       $badHeadersFixEvents,
                    $badChecksumFixEvents, $badUncompressFixEvents,
                    $duplicatedFixEvents,  $goodFixEvents
                )
                = $fixOutput =~ /^read \s (\d+) \s events$ \n
                    ^and \s (\d+) \s events \s with \s bad \s headers$ \n
                    ^and \s (\d+) \s events \s with \s bad \s check \s sum$ \n
                    ^and \s (\d+) \s events \s with \s bad \s uncompress$ \n
                    ^and \s (\d+) \s duplicated \s event \s Id$ \n
                    ^Wrote \s (\d+) \s good \s events \s*$/xms
              )
            {
                print "\t\t$badEvents bad events ($badRatio\%) :"
                  . " $goodFixEvents salvaged"
                  . " (Log $diagFixLogFile)\n";
                return $self->calculateChecksum($file)
                  ;    # Sending the new checksum
            }
            else {
                print
                  "File $file have been rewritten, but something went wrong."
                  . " Please check: $diagFixLogFile\n";
                return;
            }
        }
        else {
            print "\t\t$filename contains $badRatio\% bad events:\n";
            printf "\t\t%5d total events\n", $totalEvents if $totalEvents;
            printf "\t\t%5d bad header\n", $badHeadersEvents
              if $badHeadersEvents;
            printf "\t\t%5d bad header\n", $badHeadersEvents
              if $badHeadersEvents;
            printf "\t\t%5d bad checksum\n", $badChecksumEvents
              if $badChecksumEvents;
            printf "\t\t%5d bad uncompress\n", $badUncompressEvents
              if $badUncompressEvents;
            printf "\t\t%5d duplicated\n", $duplicatedEvents
              if $duplicatedEvents;
            print "Please decide that to do. To fix, run:\n";
            print join " ", $0, @savedARGV, "--removebad\n";
            return;
        }

        # Should never happen
        print "File $file contains bad data!"
          . " No idea how we got there! Check $diagLogFile\n";
        return;
    }
    elsif ( $output =~ /dumping bad / ) {
        my $diagLogFile = "/tmp/$filename.diag";
        open my $diag, '>', $diagLogFile
          or die "Cannot open $diagLogFile for writing: $!";
        print $diag $output;
        close $diag;
        print "File $file contains bad data! Won't fix! Check $diagLogFile\n";
        return;
    }
}

sub calculateChecksum {
    my ( $self, $file ) = @_;
    return $self->calculateChecksumC($file);
}

sub calculateChecksumC {
    my ( $self, $file ) = @_;
    my $output = qx{ $cmssw/bin/slc5_amd64_gcc462/CalcAdler32 $file 2>&1 };
    if ( my ($checksum) = $output =~ /^(\S+) \s+ $file $ /xms ) {
        return $checksum;
    }
    else {
        warn "CalcAdler32 returned unknown output: $output."
          . " Falling back to perl version";
        return $self->calculateChecksumPerl($file);
    }
}

sub calculateChecksumPerl {
    my ( $self, $file ) = @_;
    require Digest::Adler32;
    my $a32 = Digest::Adler32->new;
    open my $fh, '<', $file or die "Cannot open $file: $!";
    $a32->addfile($fh);
    close $fh;
    my $checksum = $a32->hexdigest;
    $checksum =~ s/^0*//;
    return $checksum;
}

sub checkChecksum {
    my ( $self, $file, $badfile, $state ) = @_;
    my ( $host, $checksum, $filename, $runnumber, $size ) =
      @$badfile{qw(HOSTNAME CHECKSUM FILENAME RUNNUMBER FILESIZE)};
    if ($showLog) {
        $runnumber = sprintf "%09d", $runnumber;
        my $logfile = sprintf "/store/copyworker/workdir/%03s/%03s/%03s/%s.log",
          ( $runnumber =~ /\d\d\d/g ), $filename;
        if ( -f $logfile ) {
            open my $log, '<', $logfile
              or die "Can't open $logfile for reading: $!";
            while (<$log>) {
                print;
            }
        }
    }
    return unless $doChecksum && -f $file;
    my $cs = $self->calculateChecksum($file);
    $size = '' unless defined $size;
    print "\t$filename $host $checksum $size\n"
      if $hide{$state}
      || (!$full
        && $state eq 'FILES_TRANS_CHECKED'
        && $badfile->{STREAM} eq 'Error' );
    print "\t\tchecksum invalid, should be $cs not $checksum\n"
      if $cs ne $checksum;

    return if !$fix;    # Sanity checking
    my $streamCheck = $self->checkStreamerFile( $file, $filename );
    return if !defined $streamCheck;
    if ($streamCheck) {
        $cs = $streamCheck;
        $badfile->{FILESIZE} = $size = -s $file;
        print "\t\tchecksum updated: $cs, size: $size\n";
    }
    return if ( !$force && $cs eq $checksum );
    $badfile->{CHECKSUM} = $cs;
    my $updateQuery = $self->query(
        updateChecksum => {
            map { ( lc($_), $badfile->{$_} ) } qw( FILENAME CHECKSUM FILESIZE )
        }
    );
    $updateQuery->dbh( $self->getWriterDatabaseHandler($config) );
    my $updatedRows = $updateQuery->execute();
    die "ERROR: Updated $updatedRows and not just one!"
      unless $updatedRows == 1;
    print "\t\tset checksum to $cs in the DB\n";

    if ($noChecksum) {
        delete $badfile->{CHECKSUM};
        print "\t\tRemoving the checksum because of --nochecksum\n";
    }
}

sub checkRun {
    my ( $self, $runnumber ) = @_;

    my $checkHan = $self->query(
        'checkRun',
        bind => { runnumber => $runnumber },
        replace =>
          { hostname => $hostname ? "and HOSTNAME = '$hostname'" : '', }
    );

    #Get query result - array elements will be '' when file not in that table.
    my $total = 0;
    my ( %badfiles, %goodfiles );
    while ( my $result = $checkHan->fetchrow_hashref ) {
        my $host = $result->{HOSTNAME};
        $total++;
        next
          unless $host =~ /$hostre/;    # Host matches what we're interested in
        if ($debug) {
            $result->{$_} and $goodfiles{$_}++ for @states;
        }
        for my $index ( 0 .. $#states ) {
            my $filename     = $result->{'FILES_CREATED'};
            my $state        = $states[$index];
            my $nextstate    = $states[ $index + 1 ];
            my $furtherstate = $states[ $index + 2 ];
            unless ( $nextstate && $result->{$nextstate} )
            {                           # Next column is empty
                 # Special case for files which do not get repacked nor inserted
                if (   $state eq 'FILES_TRANS_CHECKED'
                    && $result->{'FILES_DELETED'} )
                {
                    if ( $result->{STREAM} eq 'Error' ) {
                        $badfiles{FILES_DELETED}->{$filename} = $result;
                        last;
                    }
                }

                # But following column is not empty
                if ( $furtherstate && $result->{$furtherstate} ) {
                    $badfiles{ 'BLOCKED_' . $state }->{$filename} = $result;
                    last;
                }

                $badfiles{$state}->{$filename} = $result;
                last;
            }
        }
    }
    $debug && print "Found a total of $total files for $runnumber\n";
    $debug && print "Found $goodfiles{$_} files in $_\n" for keys %goodfiles;
    return \%badfiles;
}

my %hltkeys;

sub getHltKey {
    my ( $self, $runnumber ) = @_;
    my $hltHandle = $self->query( getHltKey => { runnumber => $runnumber } );
    my ($hltkey) = $hltHandle->fetchrow_array;
    die "Could not find HLT key for run $runnumber"
      unless $hltkey;
    return $hltkeys{$runnumber} = $hltkey;
}

# For CopyManager Fixup
my %nextState     = map { $states[$_] => $states[ $_ + 1 ] } 0 .. $#states - 1;
my %previousState = map { $states[$_] => $states[ $_ - 1 ] } 1 .. $#states;

sub updateStatus {
    my ( $self, $status, $filename, $badfiles, $todo ) = @_;

    # Sanity checks
    return unless $status && $filename && $badfiles && $todo;
    return unless $status =~ /^(NEW|COPIED|CHECKED|INSERTED|REPACKED)$/;

    my $state         = "FILES_TRANS_$status";
    my $nextState     = $nextState{$state};
    my $previousState = $previousState{$state};

    # File was found to advance to $state, do we miss that state?
    return
      unless ( exists $badfiles->{$previousState}
        && delete $badfiles->{$previousState}->{$filename} )
      || ( exists $badfiles->{ 'BLOCKED_' . $previousState }
        && delete $badfiles->{ 'BLOCKED_' . $previousState }->{$filename} );

    $$todo--;
    print "Updating $filename from $state to $nextState (still $$todo to do)\n";
    my $updateQuery = $self->query(
        'updateCopyManager',
        bind    => { filename => $filename },
        replace => { table    => "FILES_TRANS_$status", }
    );
    $updateQuery->dbh( $self->getTier0DatabaseHandler($config) );
    my $updated = $updateQuery->execute();

    return unless $updated;
    return if $status eq 'INSERTED';    # No stored procedure for inserted
    for my $table (qw( SUMMARY INSTANCES )) {
        my $storeProcQuery = $self->query(
            'getCopyManagerStoredProc',
            bind    => { filename   => $filename },
            replace => { storedProc => "TRANS_${status}_PROC_$table", }
        );

        $storeProcQuery->dbh( $self->getTier0DatabaseHandler($config) );
        $storeProcQuery->execute();
    }
}

1;
