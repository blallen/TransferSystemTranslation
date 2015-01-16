# Python Version of checkRun.pl
# currently very very very preliminary
# Authors: B. Allen

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


# get latest runs
result = connection.execute('''
select RUNNUMBER from CMS_STOMGR.RUNS 
where runnumber > 140400 and runnumber < 1000000 
group by RUNNUMBER 
order by 1 desc 
nulls last''')

i = 1
for row in result: 
    print 'Run Number: ', row["RunNumber"]
    i+=1
    if (i > 5): break
result.close()

# close connection to database
connection.close()
