#-------------------------------------------------------------------------------
# Name:        module1
# Purpose:
#
# Author:      Charles Freeman
#
# Created:     29/06/2014
# Copyright:   (c) Charles Freeman 2014
# Licence:     <your licence>
#-------------------------------------------------------------------------------
import DB

#define variable for later use
#Define the multiline text file.
helpText = """
    all        => Try to run and fix on all nodes, re-submitting all files
    max=s      => Maximum number of files to fix, useful for testing
    state=s    => Limit to files in this state (regex)
    pattern=s  => Limit to files matching this pattern
    nodbupdate => Do not update the DB when re-submitting (so no duplicate
    entry in FILES_TRANS_NEW). Also true when --checksum is used
            """
maxVar = 0
allVar = 0
nodbupdate = 0
wantedState = ".*"
wantedFiles = ".*"
#create a dictionary to mimic the array
options = {help:helpText, "all":allVar, "max=s":maxVar, "state=s":wantedState, "pattern=s":wantedFiles, "nodbupdate":nodbupdate}
