# Python Version of checkRun.pl
# currently very very very preliminary
# Authors: B. Allen

# generic includes
from pprint import pprint

# dict for SQL queries
book = { 
    'getLatestRun': 
    '''select RUNNUMBER from CMS_STOMGR.RUNS 
       where runnumber > 140400 and runnumber < 1000000 
       group by RUNNUMBER 
       order by 1 desc 
       nulls last''',
    'checkRun':
        '''select fc.HOSTNAME, fc.STREAM,
    fc.FILENAME as FILENAME,
    fc.FILENAME as FILES_CREATED,
    fi.FILENAME as FILES_INJECTED,
    ftne.FILENAME as FILES_TRANS_NEW,
    ftco.FILENAME as FILES_TRANS_COPIED,
    ftch.FILENAME as FILES_TRANS_CHECKED,
    ftin.FILENAME as FILES_TRANS_INSERTED,
    ftre.FILENAME as FILES_TRANS_REPACKED,
    fd.FILENAME as FILES_DELETED,
    fc.SETUPLABEL,
    fc.TYPE,
    fc.STREAM,
    fc.APP_NAME,
    fc.APP_VERSION,
    fc.RUNNUMBER,
    fc.LUMISECTION,
    (cast(fc.CTIME as date) - to_date('19700101','YYYYMMDD')) * 86400 as STARTTIME,
    fi.CHECKSUM,
    fi.PATHNAME,
    fi.DESTINATION,
    fi.NEVENTS,
    fi.FILESIZE,
    (cast(fi.ITIME as date) - to_date('19700101','YYYYMMDD')) * 86400 as STOPTIME,
    fi.INDFILENAME,
    fi.INDFILESIZE,
    fi.COMMENT_STR as HLTKEY
  from CMS_STOMGR.FILES_CREATED fc
    left outer join CMS_STOMGR.FILES_INJECTED fi
      on fc.FILENAME=fi.FILENAME
    left outer join CMS_STOMGR.FILES_TRANS_NEW ftne
      on fc.FILENAME=ftne.FILENAME
    left outer join CMS_STOMGR.FILES_TRANS_COPIED ftco
      on fc.FILENAME=ftco.FILENAME
    left outer join CMS_STOMGR.FILES_TRANS_CHECKED ftch
      on fc.FILENAME=ftch.FILENAME
    left outer join CMS_STOMGR.FILES_TRANS_INSERTED ftin
      on fc.FILENAME=ftin.FILENAME
    left outer join CMS_STOMGR.FILES_TRANS_REPACKED ftre
      on fc.FILENAME=ftre.FILENAME
    left outer join CMS_STOMGR.FILES_DELETED fd
      on fc.FILENAME=fd.FILENAME
  where RUNNUMBER = :runnumber
  :hostname
      '''
    }

# adding option handling
from optparse import OptionParser

parser = OptionParser()
parser.add_option('-R', '--runnumber', help="Run number to check. (Don't pass -1. It's not a real run number and it's the default if you don't pass anything.)", dest='RunNumber', action='store', metavar='<runnumber>')
parser.add_option('-S', '--skip', help='How many of the most recent runs you want to skip.', dest='skip', type='int', action='store', metavar='<skip>')
parser.add_option('-L', '--last', help='How many runs you want to check.', dest='last', type='int', action='store', metavar='<last>')
parser.add_option('-D', '--debug', help='Spit out a bunch of debugging text.', dest='debug', action='store_true')
parser.set_defaults(RunNumber=-1, last=1, skip=0, debug=False)
(opts, args) = parser.parse_args() 

RunList = []
if not (opts.RunNumber == -1): RunList.append(opts.RunNumber)
skip = opts.skip
last = opts.last
debug = opts.debug

# actually start accessing DB

import sqlalchemy
from sqlalchemy import *

import os

# get DB info from config file
conf_file = os.path.join(os.environ['HOME'], '.dbpy.conf')
config = open(conf_file, 'r')
lines = [line for line in config]
user = 'user'
phrase = 'phrase'
hostname = 'hostname'
for line in lines:
    tmp = line.split()
    if (tmp[0] == 'dbi'):    hostname = tmp[2]
    if (tmp[0] == 'reader'): user = tmp[2]
    if (tmp[0] == 'phrase'): phrase = tmp[2]
engine_loc = 'oracle://'+user+':'+phrase+'@'+hostname
#print engine_loc

# access database
engine = sqlalchemy.create_engine(engine_loc)
connection = engine.connect()


#print "# of runs: "+str(len(RunList))
#print "skip: "+str(skip)
#print "last: "+str(last)

# if specific run wasn't specified, grab runs according to skip and last
if (len(RunList) == 0):
    result = connection.execute(book['getLatestRun'])
    i = 0
    for row in result:
        i+=1
        #print i
        if ( i > last+skip):
            break
        if (i < skip+1):
            continue
        RunList.append(row["RunNumber"])
        #print row["RunNumber"]
    result.close()
    RunList.reverse()

# list of states file can be in
states = ['Files_Created', 'Files_Injected', 'Files_Trans_New', 'Files_Trans_Copied', 'Files_Trans_Checked', 'Files_Trans_Inserted', 'Files_Trans_Repacked', 'Files_Deleted' ]

# loop over requested runs and find bad files which need to be fixed
print "# of runs: "+str(len(RunList))
for run in RunList:
    print 'Run Number: '+str(run)   
    # get list of files for the run
    result = connection.execute(book['checkRun'],run)
    
    total = 0
    goodfiles = {}
    
    # set up badfiles dictionary
    badfiles  = {}
    for state in states:
        badfiles[state] = {}
        badfiles['Blocked_'+state] = {}

    # loop over files and check which ones are bad
    for row in result:
        filename = row['Files_Created']
        #print filename
        total+=1
        if (debug and total > 5): break # temporary break so I don't die from output overdose
        
        #print len(states)
        for (index, state) in enumerate(states):
            if (index+2 == len(states)): break
            #print index, state
            nextstate = states[index+1]
            furtherstate = states[index+2]
            
            if (debug): print nextstate, row[nextstate]

            # if next state doesn't exist add file to bad files dictionary
            if not ( nextstate and row[nextstate]):
            
                # special case for files which do not get repacked or inserted
                # adds them to state: Files_Deleted
                if (state == 'Files_Trans_Checked' and row['Files_Deleted']):
                    if (row['Stream'] == 'Error'):
                        badfiles['Files_Deleted'][filename] = row
                        if (debug): print filename, 'added to badfiles list for state Files_Deleted'
                        break

                # if the next column is not empty 
                # e.g. processing continued somehow
                # add file to badfiles for blocked_state
                if (furtherstate and row[furtherstate]):
                    badfiles['Blocked_'+state][filename] = row
                    if (debug): print filename, 'added to badfiles list for state Blocked_'+state
                    break

                # add file to badfiles for state
                badfiles[state][filename] = row
                if (debug): print filename, 'added to badfiles list for state ', state
                break
    pprint(badfiles)
    result.close()

# close connection to database
connection.close()
