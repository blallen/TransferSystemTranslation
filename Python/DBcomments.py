import sys
import re
import os

#Convert Later!
# Exporter stuff
#our @EXPORT    = qw( $debug $fix $config );
#our @EXPORT_OK = qw( @states %message $hostre $hostname $t0script $force
#  $fix $doChecksum %hide $debug $full $removeBad );

#NOTE: This code was written very quickly and was never thoroughly tested.  If you see something that looks wrong or otherwise just
#kind of stupid, there is probably just an issue with the code.


VERSION = 1.00

# Exported variables
states = ["FILES_CREATED", "FILES_INJECTED", "FILES_TRANS_NEW", "FILES_TRANS_COPIED", "FILES_TRANS_CHECKED", "FILES_TRANS_INSERTED", "FILES_TRANS_REPACKED", "FILES_DELETED"]
#Here I have concatenated all of the possible messages into one dictionary.  This was done in a shorter, but much less readable manner
#in the original code using the map function.
message = {
    "FILES_NOT_FOUND" : "File not found in database",
    "FILES_CREATED"  : "File found in database but not passed over to T0 system",
    "FILES_INJECTED" : "File found in database and handed over to T0 system",
    "FILES_TRANS_NEW" : "File found in database and being processed by T0 system",
    "FILES_TRANS_COPIED"   : "File found in database and copied by T0 system",
    "FILES_TRANS_CHECKED"  : "File found in database and checked by T0 system",
    "FILES_TRANS_INSERTED" : "File found in database and inserted into T0ast",
    "FILES_TRANS_REPACKED" : "File found in database and sucessfully processed by T0 system",
    "FILES_DELETED" : "File found in database, sucessfully processed and locally deleted",
    "BLOCKED_FILES_CREATED" : "File blocked in FILES_CREATED but further states are done",
    "BLOCKED_FILES_INJECTED" : "File blocked in FILES_INJECTED but further states are done",
    "BLOCKED_FILES_TRANS_NEW" : "File blocked in FILES_TRANS_NEW but further states are done",
    "BLOCKED_FILES_TRANS_COPIED" : "File blocked in FILES_TRANS_COPIED but further states are done",
    "BLOCKED_FILES_TRANS_CHECKED" : "File blocked in FILES_TRANS_CHECKED but further states are done",
    "BLOCKED_FILES_TRANS_INSERTED" : "File blocked in FILES_TRANS_INSERTED but further states are done",
    "BLOCKED_FILES_TRANS_REPACKED" : "File blocked in FILES_TRANS_REPACKED but further states are done",
    "BLOCKED_FILES_DELETED" : "File blocked in FILES_DELETED but further states are done"
}

hide = {"FILES_TRANS_INSERTED" : 1, "FILES_TRANS_REPACKED" : 1, "FILES_DELETED" : 1}
#This is a regex expression.  It is simply used to parse text documents for specific patterns
hostre     = "qr/^srv-c2(?:c0[67]-\d\d|d05-02)$/i"
debug      = 0
force      = 0
full       = 0
showLog    = 0
fix        = "$0 =~ /^fix/"
doChecksum = 0
removeBad  = 0
t0dir      = "/nfshome0/cmsprod/TransferTest"
t0script   = "$t0dir/injection/sendNotification.sh"

#set to ENV{HOME} + file extension.  May have to modify to get it to work on your system
config     = os.environ['HOME'] + '.py.conf'

hostname   = ""

# Private variables
help = 0
runs = []

last        = None
skip        = None
noChecksum  = 0
showDeleted = 0
# savedARGV hold all command line arguments passed to the function in a list, with index 0 being the first arg and so on.  
#we will use this to mimic the getopt function in the perl script.
savedARGV   = sys.argv

my $readerDatabaseHandler;
my $writerDatabaseHandler;
my $tier0DatabaseHandler;

scram_arch = '/nfshome0/cmssw2/slc5_amd64_gcc462'
cmssw      = "$scram_arch/cms/cmssw/CMSSW_5_2_6"


$ENV{LD_LIBRARY_PATH} = join ":",
  "$cmssw/external/slc5_amd64_gcc462/lib", "$cmssw/lib/slc5_amd64_gcc462",
  "$scram_arch/external/gcc/4.6.2/lib64",  "$ENV{LD_LIBRARY_PATH}";



def help(helpText):
	"Help function, fully functional"
    #print this multiline text
	print """
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
    log         => Check the rfcp's output
    	"""
    	if(helpText):
            #I think __file__ is a special variable holding the file path + file name of the script
    		print "Specific options for", __file__, "\n"
    		print helpText



#Some flags passed when running the script end in "=s" to specificy that a string is REQUIRED immediately after raising the flag.
#The function below was designed to cut this off so you could test for equivalence.
def cutOffS(list):
	for i in range(len(list)):
		if list[i][-2:] == "=s":
			list[i] = list[i][0:-2]
	return list


class SMDB:
	"Initializer class used in old code"
	getOpt = ["h", "help","debug", "deleted", "config=s", "runnumber=s", "last=s", "skip=s", "hostname=s", "force", "fix", "full", "checksum", "nochecksum", "removebad", "log"]
	# NOTE: These vars are initialized in a very different way in the original code
	argv = []
	help = 0
	debug = 0
	showDeleted = 0
	config = ""
	runs = ""
	last = ""
	skip = ""
    hostname = ""
	force = 0
	fix = 0
	full = 0
	checkSum = 0
	noChecksum = 0
	removeBad = 0
	log = 0
	helpText = 0
	allVar = 0
	maxVar = ""
	wantedState = ""
	wantedFiles = ""
	nodbupdate = 0
	helpText = """
	all        => Try to run and fix on all nodes, re-submitting all files
    max=s      => Maximum number of files to fix, useful for testing
    state=s    => Limit to files in this state (regex)
    pattern=s  => Limit to files matching this pattern
    nodbupdate => Do not update the DB when re-submitting (so no duplicate
                  entry in FILES_TRANS_NEW). Also true when --checksum is used
				"""

	def __init__(self, options):
		# initialize values
		self.options = options
		# save current command line args
		SMDB.argv = sys.argv
		# Add options keys to list of searched for terms
		self.cmdOpts = getOpt + self.options.keys()
		# Iterate over all args passed.  I do not iterate naturally over the list because at some instance 
		# in the loop we must take the next arg in the list.  Test each for equivalence to a specific option and 
		# assign values to the corresponding variables if the test returns true.  Just for the record there is a much
		# faster way to do this but I chose this way to increase readability.  This corresponds to getOpt in the original script
        # but here we use a different method to parse flags.
		for args in range(len(SMDB.argv)):
			if SMDB.argv[args] == "h" or SMDB.argv[args] == "help":
				# The help option was passed, so turn help on
				SMDB.help = 1
            elif SMDB.argv[args] == "debug":
            	SMDB.debug = 1
            elif SMDB.argv[args] == "deleted":
            	SMDB.showDeleted = 1
            elif SMDB.argv[args] == "config":
            	# the original getOpt program had config=s (the =s implies that this argument MUST be followed by a string) so 
            	# we must assert that the argument immediately following the one in question is a string and assign it to the config var
            	if type(SMDB.argv[args + 1]) == type('str'):
            		SMDB.config = SMDB.argv[args + 1]
            	else:
            		raise Exception("Arguments passed invalid!  Please pass appropriate args")
            elif SMDB.argv[args] == "runnumber":
            	if type(SMDB.argv[args + 1]) == type('str'):
            		SMDB.runs = SMDB.argv[args + 1]
            	else:
            		raise Exception("Arguments passed invalid!  Please pass appropriate args")
            elif SMDB.argv[args] == "last":
            	if type(SMDB.argv[args + 1]) == type('str'):
            		SMDB.last = SMDB.argv[args + 1]
            	else:
            		raise Exception("Arguments passed invalid!  Please pass appropriate args")
            elif SMDB.argv[args] == "skip":
            	if type(SMDB.argv[args + 1]) == type('str'):
            		SMDB.skip = SMDB.argv[args + 1]
            	else:
            		raise Exception("Arguments passed invalid!  Please pass appropriate args")
            elif SMDB.argv[args] == "hostname":
            	if type(SMDB.argv[args + 1]) == type('str'):
            		SMDB.hostname = SMDB.argv[args + 1]
            	else:
            		raise Exception("Arguments passed invalid!  Please pass appropriate args")
            elif SMDB.argv[args] == "force":
            	SMDB.force = 1
            elif SMDB.argv[args] == "fix":
            	SMDB.fix = 1
            elif SMDB.argv[args] == "full":
            	SMDB.full = 1
            elif SMDB.argv[args] == "checksum":
            	SMDB.checkSum = 1
            elif SMDB.argv[args] == "nochecksum":
            	SMDB.noChecksum = 1
            elif SMDB.argv[args] == "removebad":
            	SMDB.removeBad = 1
            elif SMDB.argv[args] == "log":
            	SMDB.log = 1
            elif SMDB.argv[args] == "help":
            	SMDB.helpText = 1
            elif SMDB.argv[args] == "all":
            	SMDB.allVar = 1
            elif SMDB.argv[args] == "max":
            	if type(SMDB.argv[args + 1]) == type('str'):
            		SMDB.maxVar = SMDB.argv[args + 1]
            	else:
            		raise Exception("Arguments passed invalid!  Please pass appropriate args")
            elif SMDB.argv[args] == "state":
            	if type(SMDB.argv[args + 1]) == type('str'):
            		SMDB.wantedState = SMDB.argv[args + 1]
            	else:
            		raise Exception("Arguments passed invalid!  Please pass appropriate args")
            elif SMDB.argv[args] == "pattern":
            	if type(SMDB.argv[args + 1]) == type('str'):
            		SMDB.wantedFiles = SMDB.argv[args + 1]
            	else:
            		raise Exception("Arguments passed invalid!  Please pass appropriate args")
            elif SMDB.argv[args] == "nodbupdate":
            	SMDB.nodbupdate = 1
            else:
            	raise Exception("Arguments passed invalid!  Please pass appropriate args")

        # We have finished parsing cmd options, run functions as requested
        if SMBD.help:
        	help(SMBD.helpText)
        if SMBD.debug:
        	print "Creating book with config:", SMBD.config





    def readConfig(self, config, wantWriter):
    	self.config = config
    	self.wantWriter = wantWriter
    	self.reader 
        self.phrase
        self.self.dbi
        self.writer
        self.wphrase
        self.t0writer
        self.t0phrase
        #if config exists and corresponds to a file, open the file and assign it to a file handle
        if self.config:
        	fh = open(self.config)
        	if fh:
        		break
        	else:
        		raise Exception("Open Config Failed")







		

























