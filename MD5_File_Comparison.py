#-------------------------------------------------------------------------------
# Name:        MD5_File_Comparison.py
# Purpose:
#
# Author:      tolga.altinors
#
# Created:     15/05/2019
# Copyright:   (c) tolga.altinors 2019
# Licence:     <your licence>
#-------------------------------------------------------------------------------
import os
import sys
sys.path.append('/homexxx/yyyComponents/eggs/argparse-1.3.0-py2.7.egg')
import argparse  # Above line needed on servers - Python 2.6
import socket
import pymysql
import _mssql
import shutil
from sys import platform as _platform
import logging
import subprocess
import time
import smtplib
import pprint
import collections


##from logutils.queue import QueueHandler, QueueListener
from logging.handlers import TimedRotatingFileHandler
from stat import *
from distutils.util import strtobool
from datetime import datetime
from email.mime.text import MIMEText


class OnMarkRotatingFileHandler(TimedRotatingFileHandler, object):
    """
    Extension of TimedRotatingFileHandler class to let us rotate logs on the
    hour. Using TimedRotatingFileHandler under a CRON job wouldn't rotate as
    TimedRotatingFileHandler sets the next rollover time based on the last
    modified time of the current log file. Inherits from 'object' so we can use
    super() in the constructor (new-style class).
    """
    def __init__(self, filename, when='h', interval=1, backupCount=0,
                 encoding=None, delay=True, utc=False):
        # call TimedRotatingFileHandler constructor which calls computeRollover
        super(OnMarkRotatingFileHandler, self).__init__(
            filename, when, interval, backupCount,
            encoding, delay, utc)

        self.baseFilename = filename  # to get log file name from handler

        # self.rolloverAt set in constructor and used in shouldRollover
        if os.path.exists(filename):
            t = os.stat(filename)[ST_MTIME]
        else:
            t = int(time.time())
        self.rolloverAt = self.computeRollover(t)

    def floor_to(self, seconds, interval):
        """
        Returns the result of floor division of number of seconds and interval
        """
        return int(seconds/interval) * interval

    def computeRollover(self, currTime):
        """
        Sets the next rollover time to be the next interval eg an hourly
        interval will see logs rotate on the hour
        """
        # computeRollover returns next rollover time in seconds since epoch
        tempResult = (
            super(OnMarkRotatingFileHandler, self).computeRollover(currTime))
        if not self.when.startswith('W'):  # not implemented
            result = self.floor_to(tempResult, self.interval)
        else:
            result = tempResult

        return result


def get_logger(loggerName, scriptPath):
    """
    Sets up the .log file in the 'logs' subdirectory (to this script), which
    is written to by using a OnMarkRotatingFileHandler
    """
    scriptDir = os.path.dirname(scriptPath)
    scriptName = os.path.split(scriptPath)[1]
    logDirName = os.path.splitext(scriptName)[0]

    logDir = os.path.join(scriptDir, '{0}_logs'.format(logDirName))
    if not os.path.exists(logDir):  # create a 'logs' subdirectory
        os.mkdir(logDir)

    returnLog = logging.getLogger(loggerName)
    logHandler = OnMarkRotatingFileHandler(  # sends logging output to disk
                     os.path.join(logDir, '{0}.log'.format(logDirName)))

    f = logging.Formatter(
        '%(asctime)s %(name)-2s %(levelname)-2s %(message)s')
    logHandler.setFormatter(f)
    returnLog.addHandler(logHandler)
    returnLog.setLevel(logging.INFO)
    return returnLog


def format_tStamp(timestamp):
    """
    Format a timestamp similar to that used by MSSQL
    """
    return datetime.fromtimestamp(timestamp).strftime('%Y-%m-%d %H:%M:%S')


def add_to_log(mainLog, message, linesBefore, linesAfter):
    """
    Add log message to file
    """
    for i in range(0, linesBefore):
        mainLog.info("")

    mainLog.info(message)

    for i in range(0, linesAfter):
        mainLog.info("")


def site_map(abbr_site):
    """
    Returns full site name
    Args:
        abbr_site (string) : string containing primary site
    Returns:
        location (string) : string containing full site name
    """
    site_maps = {'CPL' : 'Copley',
    'LPL' : 'Liverpool',
    'LML' : 'Leeds'}

    return site_maps[abbr_site]


def split_hostname(hostname):
    """
    Returns primary site environment info such as LIVE, UAT
    Args:
        hostname (string) : string containing hostname
    Returns:
        primarySite (string) : 3 byte primary site
        platform (string)   : 1 byte environment info
    """
    primarySite = hostname[:3]
    platform = hostname[-3]

    return primarySite, platform


def get_db_host_name(primarySite, platform):
    """
    Returns host information for database
    Args:
        primarySite (String) : String containing primary site
        platform (string) : String containing environment

    Returns:
        (String)   : database host name like LPLDPWSWLU01
    """

    return '{0}DPWSQL{1}01'.format(primarySite, platform)


def get_database_connection(db_host, version):
    """
    Returns a connection to the database for this site and environment
    """
    logins = {
              }

    db_login = logins[db_host]
    db_pass = ''
    db_name = ''

    try:
        if version is 'pymssql':
            database_connection = pymssql.connect(
                host=db_host,
                user=db_login,
                password=db_pass,
                database=db_name,
                as_dict=True)
        else:
            database_connection = _mssql.connect(
                server=db_host,
                user=db_login,
                password=db_pass,
                database=db_name)

        #print "Connected to {}".format(db_host)
        return database_connection
    except:
        msg = ("Unable to connect to {0} on {1} with user {2}")
        msg = msg.format(db_name, db_host, db_login)
        print (msg)
        return 'ERROR'


def clear_table_data(db, **kwargs):
    """
    Deletes all data from given table

    Args:
        db (db object): Handle to DB
        table (str): table name

    Returns:
    """
    table_name = kwargs.get('table_name', None)

    try:
        cursor = db.cursor()

        sql = ''
        sql += 'DELETE FROM {0}'.format(table_name)

        cursor.execute(sql)
        db.commit()

    except Exception as e:
        print ("*** Error placing cursor {0}".format(e))
        sys.exit(0)
    else:
        cursor.close()


def update_database(db, **kwargs):
    """
    Updates the DB table with args passed

    Args:
        db (db object): Handle to DB
        pathType (str): Flag to identify path type
        dirPath (str) : Used to apply query results
        chkSum (str)  : String containing check sum value
        runDate (date): Date teh script was run
        action (string): Insert or Update

    Returns:
        String. A string containing the sql query - not required
    """

    tableName = kwargs.get('tableName', None)
    PathType = kwargs.get('PathType', None)
    dirPath = kwargs.get('dirPath', None)
    chkSum = kwargs.get('chkSum', None)
    runDate = kwargs.get('runDate', None)
    action = kwargs.get('action', None)

    if dirPath.endswith('/'):
        dirPath = dirPath[:-1]

    try:
        cursor = db.cursor()
        sql = ''
        if action == 'Update':
            sql += 'UPDATE {0} '.format(tableName)
            #sql += 'UPDATE [XYZXYZ].[dbo].[Sync_Paths] '
            sql += 'SET MD5CheckSum = \'{0}\', '.format(chkSum)
            sql += 'RunDate = \'{0}\''.format(runDate)
            sql += ' WHERE '
            sql += 'PathToSync = \'{0}\''.format(dirPath)

        elif action == 'Insert':
            sql += 'INSERT INTO {0} ('.format(tableName)
            sql += 'PathType, PathToSync) '
            sql += 'VALUES (\'{0}\', \'{1}\')'.format(PathType, dirPath)
        else:
            print ("Unexpected action passed - {}".format(action))

        cursor.execute(sql)
        db.commit()
    except Exception as e:
        print ("*** Error placing cursor {0}".format(e))
        sys.exit(0)
    else:
        cursor.close()
        return sql


def check_lock_file_exist(lockFile, mainLog):
    """
    Checking for the existence of a lock file

    Args:
        lockFile (string) : path of lock file
        mainLog (object)  : log object

    Returns:

    """
    message =''

    if os.path.exists(lockFile):
        message = "Comparison instance already running - exiting script"
        mainLog.warning(message)
        #send_email(message, mainLog)
        print (message)  # so console can see the error
        sys.exit()
    else:
        message = "Comparison script started"
        mainLog.info(message)
        print (message, "at", datetime.now().strftime('%H:%M:%S'))
        open(lockFile, 'w').close()  # write '.RUNNING' file


def unify_path_endings(path_list):
    """
    Make sure all paths end with a slash

    Args:
        path_list (List) : List containing folder paths

    Returns:
        folder_paths (List) : List containing folder paths
    """
    folder_paths = []
    for path in path_list:
        path = str(path)
        if path.endswith('/'):
            folder_paths.append(path)
        else:
            path = path + "/"
            folder_paths.append(path)

    return folder_paths


def run_command(command, **env):
    """Run a command on the terminal.

    Args:
        command (str): the command to execute

    Keyword Args:
        **env (dict): any keyword arguments are collected into a dictionary and passed
        as environment variables directly to the subprocess call.

    Returns:
        tuple.  A tuple containing `(stdoutdata, stderrdata)`, or None if unsuccessful.
    """
    p = subprocess.Popen(
        command,
        shell=True,
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE)
    out,err = p.communicate()
    returncode = p.returncode
    return out, err, returncode


def run_MD5_process(hostname, checkDate, mainLog):
    """
    Retrieves paths from database and updates the MD5 column
    Args:
        hostname (string)      : Database to connect to
        checkDate (date object): used to update the run date column
        mainLog (object)       : log to update
    Returns:
    """
    file_paths = []
    #sefasApps = []

    primarySite, platform = split_hostname(hostname)

    db_host = get_db_host_name(primarySite, platform)
    # # Connect to database and retrieve sefas app paths
    try:

        db = get_database_connection(db_host, 'pymssql')
        cursor = db.cursor()  # connection was made ok
        add_to_log(mainLog, "Connected to database {}".format(db_host), 0, 0)

        # # List of sefas apps
        add_to_log(mainLog, "Retrieving  paths", 0, 0)
        file_paths = get_sync_paths_from_DB(cursor, tableName = '[XYZXYZ].[dbo].[Sync_Paths]')
        add_to_log(mainLog, "{0} paths pulled from database table".format(len(file_paths)), 0, 0)

        # # Add final slash to path if required
        file_paths = unify_path_endings(file_paths)
        cursor.close()

    except Exception as e:
        add_to_log(mainLog, "*** Error connecting to target database {} ***".format(db_host), 0, 0)
        add_to_log(mainLog, Exception(e.message), 0, 0)
        sys.exit(1)

    #__findmd5__ = "find \'%(dirToCheck)s\' -type f ! -path '*spool/*' ! -name '*.pyc' ! -name '*.zip' -exec md5sum {} \; | sort -k 2 | md5sum"
    #__findmd5__ = "find \'%(dirToCheck)s\' -maxdepth 1 ! -type d ! -name '*.pyc' ! -name '*.zip' -exec md5sum {} \; | sort -k 2 | md5sum"
    #__findmd5__ = "find \'%(dirToCheck)s\' -maxdepth 1 -type f ! -name '*.pyc' ! -name '*.zip' -exec md5sum {} \; | sort -k 2 | md5sum"

    numberOfApps = len(file_paths)
    currentApp = 1

    for dirToCheck in file_paths:

        dirToCheck = dirToCheck.strip()

        __findmd5__ = "find \'%(dirToCheck)s\' "

        if dirToCheck == '/opdd/data/traffic/Resources/':
            __findmd5__ += "-maxdepth 1 "
        __findmd5__ += "-type f "
        __findmd5__ += "! -path '*spool/*' "
        __findmd5__ += "! -name '*.pyc' "
        __findmd5__ += "! -name '*.zip' "
        __findmd5__ += "-exec md5sum {} \; | "
        __findmd5__ += "sort -k 2 | "
        __findmd5__ += "md5sum"

        print ("{0} - {1} of {2}".format(dirToCheck, str(currentApp), str(numberOfApps)))
        currentApp += 1

        args = { 'dirToCheck': dirToCheck }

        cmd = __findmd5__ %  args

        msg = "Creating check sum for   : {0}".format(dirToCheck)
        add_to_log(mainLog, msg, 1, 0)

        # # get checksum of folder
        checkSum, msgList, errMsgList = create_check_sum(cmd, mainLog)

        # # Update DB
        update_database(db,
            tableName = '[XYZXYZ].[dbo].[Sync_Paths]',
            dirPath = dirToCheck,
            chkSum = checkSum,
            runDate = checkDate,
            action = 'Update')


def run_rsync_process(comparisonDict, targetServers, hostname, mainLog):
    """
    Loops through paths within the dictionary and compares to target servers
    Args:
        comparisonDict (dict): Dictionary containing file date and size
        targetServers (list) : list of servers to loop through
        primarySite (str)    :
        hostname (str)       :
        mainLog (object)     : log to update
    Returns:
    """
    #comparisonDict = kwargs.get('comparison_dict', None)
    #targetServers = kwargs.get('target_servers', None)
    #primarySite = kwargs.get('primary_site', None)
    #hostname = kwargs.get('host_name', None)

    primarySite, _ = split_hostname(hostname)

    rsyncList = []
    excludePathList = []

    for dirToCompare, value in comparisonDict.iteritems():

        print (" ")
        dirToCompare = dirToCompare.strip()
        msg = "**************************************************************"
        add_to_log(mainLog, msg, 1, 0)
        msg = "**************************************************************"
        add_to_log(mainLog, msg, 0, 0)
        print (msg)
        print (" ")
        msg = "Directory to compare {0}".format(dirToCompare)
        add_to_log(mainLog, msg, 0, 0)
        print (msg)
        print (" ")
        msg = "**************************************************************"
        add_to_log(mainLog, msg, 0, 0)
        msg = "**************************************************************"
        add_to_log(mainLog, msg, 0, 1)

        #__rsyncLocal__ = "rsync -avh --exclude 'spool/' --exclude '*.pyc' --exclude '*.zip' \'%(dirToCompare)s\'"
        #__rsyncRemote__ = "rsync -avh --exclude 'spool/' --exclude '*.pyc' --exclude '*.zip' -e ssh %(remoteUser)s@%(remoteServer)s:\'%(dirToCompare)s\'"
        #__rsync__ = "rsync -avh --dry-run --exclude 'spool/' --exclude '*.pyc' --exclude '*.zip' -e ssh \'%(dirToCompare)s\' %(remoteUser)s@%(remoteServer)s:\'%(dirToCompare)s\'"

        excludeFolder = "--exclude 'spool/' "
        excludeFiles = "--exclude '*.pyc' --exclude '*.zip' "

        if dirToCompare == '/opdd/data/traffic/Resources':
            dirToCompare = '/opdd/data/traffic/Resources/'
            excludeFolder = "--exclude '*/' --exclude 'spool/' "

        # Local
        __rsyncLocal__ = "rsync -avh "
        __rsyncLocal__ += "{excludeFolder}"
        __rsyncLocal__ += "{excludeFiles}"
        __rsyncLocal__ += "\'{dirToCompare}\'"

        # Remote
        __rsyncRemote__ = "rsync -avh "
        __rsyncRemote__ += "{excludeFolder}"
        __rsyncRemote__ += "{excludeFiles}"
        __rsyncRemote__ += "-e ssh {remoteUser}@{remoteServer}:"
        __rsyncRemote__ += "\'{dirToCompare}\'"

        # Whole command
        __rsync__ = "rsync -avh --dry-run "
        __rsync__ += "{excludeFolder}"
        __rsync__ += "{excludeFiles}"
        __rsync__ += "-e ssh \'{dirToCompare}\' "
        __rsync__ += "{remoteUser}@{remoteServer}:"
        __rsync__ += "\'{dirToCompare}\'"

        #localLocation = site_map(primarySite)

        for remoteServer in targetServers:

            # # Get remote servers location LPL, LML etc
            remoteSite, _ = split_hostname(remoteServer)

            # # Create dictionary key description
            noRemoteMatch = 'noRemoteMatch'
            sourceMD5, remoteMD5, sourceRemote = create_some_dict_keys(primarySite, remoteSite)

            remoteLocation = site_map(remoteSite)

            #print "local full          : {}".format(localLocation)
            #print "remote full         : {}".format(remoteLocation)
            #print "local MD5 {}        : {}".format(sourceMD5, value[sourceMD5])
            #print "remote MD5 {}       : {}".format(remoteMD5, value[remoteMD5])
            #print "source remote {}    : {}".format(sourceRemote, value[sourceRemote])
            #print "-------------------"

            args = { 'remoteUser': 'dpw',
                'remoteServer': remoteServer,
                'dirToCompare': dirToCompare,
                'excludeFolder' : excludeFolder,
                'excludeFiles' : excludeFiles}

            noMatchList = ['None', '']

            if value[sourceRemote] == "No Match":

                msg = "**************************************************************"
                add_to_log(mainLog, msg, 1, 0)
                msg = "Checking differences in files in {0} and {1}".format(hostname, remoteServer)
                add_to_log(mainLog, msg, 0, 0)
                msg = "**************************************************************"
                add_to_log(mainLog, msg, 0, 1)

                # # # # # # # # # # # # # # # # # # # # # # # #
                # # GET LOCAL FILE LIST
                # # # # # # # # # # # # # # # # # # # # # # # #
                cmd = __rsyncLocal__.format(**args)
                print (cmd)
                dictLocal = {}
                #excludedDictLocal = {}

                result = get_rsync_file_list(cmd, mainLog, 'Local')

                dictLocal, _ = populate_dict(result, excludePathList, dirToCompare, 'Local')

                # # # # # # # # # # # # # # # # # # # # # # # #
                # # GET REMOTE FILE LIST
                # # # # # # # # # # # # # # # # # # # # # # # #
                cmd = __rsyncRemote__.format(**args)

                dictRemote = {}
                #excludedDictRemote = {}

                result = get_rsync_file_list(cmd, mainLog, 'Remote')

                dictRemote, _ = populate_dict(result, excludePathList, dirToCompare, 'Remote')

                # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
                # # CREATE DICTIONARIES TO FIND OVERLAPS
                # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
                filesInBoth = {k:v for k, v in dictLocal.iteritems() if k in dictRemote}
                filesInLocal = {k:v for k, v in dictLocal.iteritems() if k  not in dictRemote}
                filesInRemote = {k:v for k, v in dictRemote.iteritems() if k not in dictLocal}

                num_of_overlap_files = len(filesInBoth)
                num_of_local_only_files = len(filesInLocal)
                num_of_remote_only_files = len(filesInRemote)

                msg = "* * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * *"
                add_to_log(mainLog, msg, 0, 0)
                msg = "BREAKDOWN FOR DIRECTORY - {0}".format(dirToCompare)
                add_to_log(mainLog, msg, 0, 0)
                msg = "* * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * *"
                add_to_log(mainLog, msg, 0, 0)
                msg = "Files in both          : {0}".format(num_of_overlap_files)
                add_to_log(mainLog, msg, 0, 0)
                msg = "Files in local only    : {0}".format(num_of_local_only_files)
                add_to_log(mainLog, msg, 0, 0)
                msg = "Files in remote only   : {0}".format(num_of_remote_only_files)
                add_to_log(mainLog, msg, 0, 1)

                # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
                # # LOOP THROUGH DICTIONARY TO OUTPUT OVERLAPS
                # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
                filesIdentical = True
                overlap_log = []

                if num_of_overlap_files == 0 and num_of_remote_only_files == 0:
                    msg = "REMOTE SERVER {0} - {1} (DIRECTORY NOT PRESENT)".format(remoteServer, dirToCompare)
                    add_to_log(mainLog, msg, 1, 1)

                if num_of_overlap_files == 0 and num_of_local_only_files == 0:
                    msg = "LOCAL SERVER {0} - {1} (DIRECTORY NOT PRESENT)".format(hostname, dirToCompare)
                    add_to_log(mainLog, msg, 1, 1)


                if num_of_overlap_files > 0:

                    filesIdentical, overlap_log = compare_overlap_files(filesInBoth, dictRemote, hostname, remoteServer)

                    # # files are identical
                    if filesIdentical:
                        msg = "******     No differences in files that overlap     ******"
                        overlap_log.append(msg)
                        msg = "******     No differences in files that overlap     ******"
                        overlap_log.append(msg)

                        # MD5 is not present
                        if value[remoteMD5].strip() in noMatchList:
                            msg = "**********************************************************"
                            overlap_log.append(msg)
                            msg = "****** Check application is present in the database ******"
                            overlap_log.append(msg)
                            msg = "****** Check application is present in the database ******"
                            overlap_log.append(msg)
                            overlap_log.append(" ")

                        # MD5 is present
                        elif value[remoteMD5].strip() not in noMatchList:
                            msg = "**********************************************************"
                            overlap_log.append(msg)
                            msg = "******** Check MD5 value in database is upto date ********"
                            overlap_log.append(msg)
                            msg = "******** Check MD5 value in database is upto date ********"
                            overlap_log.append(msg)
                            overlap_log.append(" ")

                    # # If overlap files not the same then output log header
                    if not filesIdentical:
                        msg = "--------------------------------------------------------------"
                        add_to_log(mainLog, msg, 0, 0)
                        msg = "REMOTE SERVER {0} - {1} (NOT IN SYNC)".format(remoteServer, dirToCompare)
                        add_to_log(mainLog, msg, 0, 0)
                        msg = "--------------------------------------------------------------"
                        add_to_log(mainLog, msg, 0, 1)

                        if value[remoteMD5].strip() in noMatchList:
                            msg = "**********************************************************"
                            add_to_log(mainLog, msg, 0, 0)
                            msg = "****** Check application is present in the database ******"
                            add_to_log(mainLog, msg, 0, 0)
                            msg = "****** Check application is present in the database ******"
                            add_to_log(mainLog, msg, 0, 1)


                    # # Then add remaining info to the log
                    for log_message in overlap_log:
                        add_to_log(mainLog, log_message, 0, 0)

                # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
                # # FILES THAT ARE IN LOCAL ONLY
                # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
                if num_of_local_only_files > 0:
                    list_files_on_one_server(filesInLocal, mainLog, hostname, 'Local')

                # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
                # # FILES THAT ARE IN REMOTE ONLY
                # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
                if num_of_remote_only_files > 0:
                    list_files_on_one_server(filesInRemote, mainLog, remoteServer, 'Remote')

                rsyncList.append(__rsync__.format(**args))

            elif value[sourceRemote] == "Match":
                msg = "REMOTE SERVER {0} - {1} (IN SYNC)".format(remoteServer, dirToCompare)
                add_to_log(mainLog, msg, 2, 2)

            else:
                msg = "UNEXPECTED VALUE IN DICTIONARY {0} - {1}".format(sourceRemote, value[sourceRemote])
                add_to_log(mainLog, msg, 0, 0)

        msg = " "
        add_to_log(mainLog, msg, 0, 2)


    # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
    # # OUTPUT RSYNC COMMANDS FOR REFERENCE
    # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
    msg = "**************************************************************"
    add_to_log(mainLog, msg, 0, 0)
    msg = "**************          RSYNC COMMANDS          **************"
    add_to_log(mainLog, msg, 0, 0)
    msg = "**************************************************************"
    add_to_log(mainLog, msg, 0, 0)
    for syncCommand in rsyncList:
        add_to_log(mainLog, syncCommand, 0, 0)


def get_sefas_apps_from_DB(cursor, **kwargs):
    """
    Returns list of config

    Args:
        cursor (db object): Handle to DB
        primarySites (list) : List of primary sites

    Returns:
        List. A list containing config files
    """

    tableNames = kwargs.get('tableName', None)
    primarySites = kwargs.get('primarySites', None)

    sql = 'SELECT DISTINCT '
    sql += '(CASE WHEN RIGHT(b.ParameterMask, 1) = \'/\' '
    sql += 'THEN SUBSTRING(b.ParameterMask, 1, LEN(b.ParameterMask) -1) '
    sql += 'ELSE b.ParameterMask END) AS ParameterMask '
    sql += 'FROM {0} AS a '.format(tableNames[0])
    sql += 'INNER JOIN {0} AS b '.format(tableNames[1])
    sql += 'ON a.JobType = b.JobType WHERE '
    sql += 'b.ParameterName = \'{0}\' AND '.format('E:opWD')
    sql += 'b.ParameterMask LIKE \'{0}\' AND '.format('%/SefasApps/%')
    sql += 'a.PrimarySite IN ({0}) AND '.format(primarySites)
    sql += 'b.ParameterMask NOT LIKE \'{0}\''.format('%remake_%')

    cursor.execute(sql)

    return [row['ParameterMask'] for row in cursor.fetchall()]


def get_config_files_from_DB(cursor,  **kwargs):
    """
    Returns list of config files

    Args:
        cursor (db object): Handle to DB
        primarySites (list) : List of primary sites

    Returns:
        List. A list containing sync paths
    """

    tableNames = kwargs.get('tableName', None)
    primarySites = kwargs.get('primarySites', None)

    sql = 'SELECT DISTINCT '
    sql += '(CASE WHEN RIGHT(b.ParameterMask, 1) = \'/\' '
    sql += 'THEN SUBSTRING(b.ParameterMask, 1, LEN(b.ParameterMask) -1) '
    sql += 'ELSE b.ParameterMask END) AS ParameterMask '
    sql += 'FROM {0} AS a '.format(tableNames[0])
    sql += 'INNER JOIN {0} AS b '.format(tableNames[1])
    sql += 'ON a.JobType = b.JobType WHERE '
    sql += 'b.ParameterName = \'{0}\' AND '.format('ConfigFile')
    sql += 'a.PrimarySite IN ({0})'.format(primarySites)

    cursor.execute(sql)

    return [row['ParameterMask'] for row in cursor.fetchall()]


def get_sync_paths_from_DB(cursor, **kwargs):
    """
    Returns list of paths from database

    Args:
        cursor (db object): Handle to DB

    Returns:
        List. A list containing sync paths
    """

    pathType = kwargs.get('pathType', None)
    tableName = kwargs.get('tableName', None)

    sql = 'SELECT DISTINCT PathToSync '
    sql += 'FROM {0}'.format(tableName)
    #sql += 'FROM [XYZXYZ].[dbo].[Sync_Paths]'
    if pathType:
        sql += ' WHERE '
        sql += 'PathType = \'{0}\''.format(pathType)

    cursor.execute(sql)

    return [row['PathToSync'] for row in cursor.fetchall()]


def get_JT_sefas_config_from_DB(db_host):
    """
    Returns list of combinations of JT, sefas path and config file
    This uses _mssql to retrieve stored procedure results

    Args: db host

    Returns:
        List. A list containing a dictionary
    """
    try:
        conn = get_database_connection(db_host, '_mssql')

        conn.execute_query('[dbo].[DB_Synchronisation_Configs]')
        result = [row for row in conn]

    except Exception as e:
        print ("*** Error executing procedure {0}".format(e))

    return result


def get_sync_comparison_from_DB(db_host):
    """
    Returns list comparing sync of paths in detail
    This uses _mssql to retrieve stored procedure results

    Args: db host

    Returns:
        List. A list containing sync dictionary
    """
    try:
        conn = get_database_connection(db_host, '_mssql')

        conn.execute_query('[dbo].[DB_Synchronisation_SefasApps]')
        result1 = [row for row in conn]

        conn.execute_query('[dbo].[DB_Synchronisation_SefasResources]')
        result2 = [row for row in conn]

    except Exception as e:
        print ("*** Error executing procedure {0}".format(e))

    return result1 + result2


def create_check_sum(cmdToRun, mainLog):
    """
    Returns the checksum of a folder and error messages

    Args:
        cmdToRun (str) :

    Returns:
        String. A String containing the checksum
        List. A list containing processing message
        List. A list containing error messages
    """
    message = ''
    msg = []
    errMsg = []
    returnValue = ''

    mainLog.info(message)

    try:
        message = "Running command  : {0}".format(cmdToRun)
        msg.append(message)
        mainLog.info(message)
        result, err, returncode = run_command(cmdToRun)
    except subprocess.CalledProcessError:
        message = "Failed to run command: {0}\n{1}".format(cmdToRun, result)
        errMsg.append(message)
        mainLog.info(message)
    else:

        returnValue = result.split(' ')[0]
        message = "Check sum: {0}".format(returnValue)
        mainLog.info(message)

        if len(returnValue.split(' ')[0]) != 32:
            message = "Check Sum not 32 bytes long - {0}".format(returnValue)
            print (message)
            mainLog.info(message)
            message = "Terminating script"
            print (message)
            mainLog.info(message)
            sys.exit()

        if err != "":
            message = "Error: {0}".format(err.strip())
            if 'No such file or directory' in err:
                returnValue = ''
            errMsg.append(message)
            mainLog.info(message)

        if returncode != 0:
            message = "Error code: {0}".format(returncode)
            errMsg.append(message)
            mainLog.info(message)

    return returnValue, msg, errMsg


def get_rsync_file_list(cmd, mainLog, serverLocation):
    """
    Returns list of files / folders for passed location
    Args:
        cmd (string) : string containing rsync command
        mainLog (object) : log to be updated
        serverLocation (string) : string containing info on whether Local or remote
    Returns:
        result (list) : list of folder and files with date / time / size
    """
    try:
        msg = "Running {0} command  : {1}".format(serverLocation, cmd)
        add_to_log(mainLog, msg, 0, 1)
        result = run_command(cmd)
    except subprocess.CalledProcessError:
        msg = ("Failed to find {0} files, using command: {1}\n{2}".format(serverLocation,cmd, result))
        add_to_log(mainLog, msg, 0, 1)
        print (msg)
    else:
        return result


def log_apps_status(mainLog, allMatchList, comparisonDict):
    """
    Updates log with info on matched / unmatched paths
    Args:
        mainLog (object) : log to be updated
        allMatchList (list) : list containing matching apps
        comparisonDict (dict) : dict containing non matching apps
    Returns:
    """
    if len(allMatchList):
        msg = "~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~"
        add_to_log(mainLog, msg, 0, 0)
        msg = "LIST OF APPLICATIONS / RESOURCES THAT MATCH - {}".format(len(allMatchList))
        add_to_log(mainLog, msg, 0, 0)
        msg = "~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~"
        add_to_log(mainLog, msg, 0, 0)
        for dirToCompare in allMatchList:
            msg = "\t{0}".format(dirToCompare)
            add_to_log(mainLog, msg, 0, 0)
        msg = "~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~"
        add_to_log(mainLog, msg, 0, 0)

    if len(comparisonDict):
        msg = "~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~"
        add_to_log(mainLog, msg, 0, 0)
        msg = "LIST OF APPLICATIONS / RESOURCES TO BE CHECKED - {}".format(len(comparisonDict))
        add_to_log(mainLog, msg, 0, 0)
        msg = "~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~"
        add_to_log(mainLog, msg, 0, 0)

        for dirToCompare, _ in comparisonDict.iteritems():
            msg = "\t{0}".format(dirToCompare)
            add_to_log(mainLog, msg, 0, 0)
        msg = "~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~"
        add_to_log(mainLog, msg, 0, 0)


def populate_sync_jobtype_resource_table(db, **kwargs):
    """
    Updates the DB table with args passed

    Args:
        db (db object): Handle to DB
        jobType (str): Flag to identify path type
        ResourcePath (str) : Used to apply query results

    Returns:
        String. A string containing the sql query - not required
    """
    job_type = kwargs.get('JobType', None)
    resource_path = kwargs.get('ResourcePath', None)

    if resource_path.endswith('/'):
        resource_path = resource_path[:-1]

    try:
        cursor = db.cursor()

        sql = ''
        sql += 'INSERT INTO [XYZXYZ].[dbo].[Sync_Jobtype_Resource] ('
        sql += 'JobType, ResourcePath) '
        sql += 'VALUES (\'{0}\', \'{1}\')'.format(job_type, resource_path)

        cursor.execute(sql)
        db.commit()
    except Exception as e:
        print ("*** Error placing cursor {0}".format(e))
        sys.exit(0)
    else:
        cursor.close()


def create_jobtype_resource_link(hostname, config_resource_dict):
    '''
    This is to try and link jobtypes to resource paths to aid in displaying
    the link in the sync email.

    There is no  direct link between job type and resource path. There is an
    indirect link between jobtype and config as the config contains the
    resource path.

    Sync_Jobtype_Resource table is updated with jobtype and resource path

    Args:
        hostname (string) :
        config_resource_dict (dict) : Contains config path as key and
                                       resource path as value

    Return:
    '''

    primarySite, platform = split_hostname(hostname)

    db_host = get_db_host_name(primarySite, platform)

    db = get_database_connection(db_host, 'pymssql')

    # # Returns a list containing sql result as dictionary
    # # Jobtype, Sefas app, config file
    temp_list = get_JT_sefas_config_from_DB(db_host)

    cp_jt_dict = {}     # config as key, jobtypes as value
    temp_dict = {}

    # # Create dictionary with config as key and
    # # corresponding job types as list of values
    for l in temp_list:

        temp_dict = l

        jt = str(temp_dict['JobType'].strip())
        sp = str(temp_dict['SefasPath'].strip())
        cp = str(temp_dict['ConfigPath'].strip())

        # # Add to dictionary with an empty list as value
        if cp not in cp_jt_dict:
            cp_jt_dict[cp] = []

        if jt not in cp_jt_dict[cp]:
            cp_jt_dict[cp].append(jt)


    config_resource = {}
    resource_JT = {}
    # # Create dictionary with
    # # config path as key
    # # list of resource paths as value

    # # only interested if config has a resource path
    # # not all configs contain a resource path
    config_resource = {k:v for k, v in config_resource_dict.iteritems() if v}

    # # create dictionary of resource as keys with empty list
    resource_JT = {r:[] for value in config_resource.itervalues() for r in value}

 
    # # Create dictionary with
    # # resource path as key
    # # List of job types as value

    for config, resources in config_resource.iteritems():

        if config in cp_jt_dict:

            JTs = cp_jt_dict[config]

            for resource in resources:

                for JT in JTs:

                    if JT not in resource_JT[resource]:

                        resource_JT[resource].append(JT)

    # # Delete all entries first
    clear_table_data(db, table_name = '[XYZXYZ].[dbo].[Sync_Jobtype_Resource]')

    # # Update Sync_Jobtype_Resource table
    for res_path, jobTypes in resource_JT.iteritems():

        for jt in jobTypes:

            populate_sync_jobtype_resource_table(db, JobType = jt,
            ResourcePath = res_path)


def update_table_with_paths(hostname, checkDate, mainLog):
    '''

    '''
    primarySite, platform = split_hostname(hostname)

    db_host = get_db_host_name(primarySite, platform)

    # # Connect to database and retrieve config file paths
    try:

        db = get_database_connection(db_host, 'pymssql')
        cursor = db.cursor()  # connection was made ok
        add_to_log(mainLog, "Connected to database {}".format(db_host), 0, 1)

        # # List of sefas apps
        add_to_log(mainLog, "Searching for sefas paths", 0, 0)
        sefas_app_list = get_sefas_apps_from_DB(cursor,
                tableNames = "'[XYZXYZ].[dbo].[JobTypeMaster]', '[XYZXYZ].[dbo].[JobStepParameterMaster]'",
                primarySites = "'LPL', 'LML', 'CPL'")
        add_to_log(mainLog, "{0} sefas app paths found in DB".format(len(sefas_app_list)), 0, 1)

        # # list of exsisting sefas apps
        add_to_log(mainLog, "Checking for existing sefas apps", 0, 0)
        existing_sefas_paths = get_sync_paths_from_DB(cursor,
                tableName = '[XYZXYZ].[dbo].[Sync_Paths]',
                pathType = 'Sefas')
        add_to_log(mainLog, "{0} existing sefas app paths found in DB".format(len(existing_sefas_paths)), 0, 1)

        # # List of config files
        add_to_log(mainLog, "Searching for config file paths", 0, 0)
        config_list = get_config_files_from_DB(cursor, 
                tableNames = "'[XYZXYZ].[dbo].[JobTypeMaster]', '[XYZXYZ].[dbo].[JobStepParameterMaster]'", 
                primarySites = "'LPL', 'LML', 'CPL'")
        add_to_log(mainLog, "{0} config paths found in DB".format(len(config_list)), 0, 1)

        # # list of exsisting external resources
        add_to_log(mainLog, "Checking for existing external resources", 0, 0)
        existing_resource_paths = get_sync_paths_from_DB(cursor,
                tableName = '[XYZXYZ].[dbo].[Sync_Paths]',
                pathType = 'External Resource')
        add_to_log(mainLog, "{0} existing resource paths found in DB".format(len(existing_resource_paths)), 0, 1)

        cursor.close()

    except Exception as e:
        add_to_log(mainLog, "*** Error connecting to target database {} ***".format(db_host), 0, 0)
        add_to_log(mainLog, Exception(e.message), 0, 0)
        sys.exit(1)


    # # # # # # # # # # # # # # # # # # # # # #
    # # Find sefas app paths not in the database
    sefas_apps_to_add = []
    for sefas_apps in sefas_app_list:
        sefas_app = sefas_apps
        if sefas_apps.endswith('/'):
            sefas_app = sefas_apps[:-1]

        if sefas_app not in existing_sefas_paths:
            sefas_apps_to_add.append(sefas_app)

    # # Update database table with new sefas app paths
    for dir_to_insert in sefas_apps_to_add:
        update_database(db, 
            tableName = '[XYZXYZ].[dbo].[Sync_Paths]',
            PathType = 'Sefas',
            dirPath = dir_to_insert,
            runDate = checkDate,
            action = 'Insert')

    # # Add file info to the log
    if len(sefas_apps_to_add) > 0:
        add_to_log(mainLog, "Adding {0} new sefas app paths to the database".format(len(sefas_apps_to_add)), 1, 1)
        for f in sefas_apps_to_add:
            add_to_log(mainLog, "\t{0}".format(f), 0, 0)
    else:
        add_to_log(mainLog, "No new sefas app paths found", 1, 1)


    # # # # # # # # # # # # # # # # # # # # # #
    # # Open files in configList and find external resource folders
    external_resource_tags = ['res_dir','xerox_font_dir','res_dir_path','metrics_dir_path']
    #external_resource_list = []
    external_resource_to_add = []
    missing_configs = []
    config_resource_dict = {}

    for config_file in config_list:

        try:
            with open(config_file, mode='r') as in_f:

                # # add config file as key with a blank list as value
                if config_file not in config_resource_dict:
                    config_resource_dict[config_file] = []

                for line in in_f:

                    line = line.strip()
                    # # Check the line has assignment
                    if '=' in line:
                        parts = line.split('=')

                        ext_resource = parts[1]
                        if ext_resource.endswith('/'):
                            ext_resource = parts[1][:-1]

                        if parts[0] in external_resource_tags and ext_resource not in external_resource_to_add:
                            # # check it is not already i n the database
                            if ext_resource not in existing_resource_paths:
                                external_resource_to_add.append(ext_resource)

                            if ext_resource not in config_resource_dict[config_file]:
                                config_resource_dict[config_file].append(ext_resource)

        except IOError as e:
            missing_configs.append(config_file)


    # # Update database table with new external resources
    for dir_to_insert in external_resource_to_add:
        update_database(db,
            tableName = '[XYZXYZ].[dbo].[Sync_Paths]',
            PathType = 'External Resource',
            dirPath = dir_to_insert,
            runDate = checkDate,
            action = 'Insert')

    # # Add file info to the log
    if len(external_resource_to_add) > 0:
        add_to_log(mainLog, "Adding {0} new external resource paths to the database".format(len(external_resource_to_add)), 1, 1)
        for f in external_resource_to_add:
            add_to_log(mainLog, "\t{0}".format(f), 0, 0)
    else:
        add_to_log(mainLog, "No new external resource paths found", 1, 1)

    if len(missing_configs) > 0:
        add_to_log(mainLog, "Number of config files not found in the file system - {0}".format(len(missing_configs)), 1, 1)
        for f in missing_configs:
            add_to_log(mainLog, "\t{0}".format(f), 0, 0)
        add_to_log(mainLog, "", 1, 1)

    return config_resource_dict


def concat_split_file_path(passedList):
    """
    Concatenates file paths that is split across elements in list

    Args:
        passedList (List) : List containing paths

    Returns:
        List. A list with paths concatenated
    """
    indexOfTimeField = 0

    for ind, item in enumerate(passedList):

        if ':' in item:
            indexOfTimeField = ind

    if indexOfTimeField != 1:
        # bit before time
        tempList = passedList[:2]
        # reverse list to build filename up
        tempList = list(reversed(tempList))
        # bit after time
        tempList2 = passedList[indexOfTimeField:]

        # create new path my merging split items
        spath = ''
        for x in tempList:
            spath = spath + ' ' + x

        spath = spath.strip()

        # re-create reverse list
        returnList = []
        returnList.append(spath)
        for x in tempList2:
            returnList.append(x)
    else:
        print ("")

    return returnList


def populate_dict(result, excludePathList, dir, location):

    outerDict = {}
    excludedDict = {}

    fullPath = ''

    # # get path - last folder
    if not dir.endswith('/'):
        dir = dir + '/'

    dir = os.path.split(os.path.dirname(dir))[0]

    # # set date time format
    dtMask = '%Y/%m/%d %H:%M:%S'

    # # convert returns from command to a list
    resultList = list(result)
    resultList = resultList[0].split('\n')

    # # parse result of command to get directories
    directoryList = [i for i in resultList if i.startswith('d')]

    # # parse result of command to get files and links
    fileList = [i for i in resultList if i.startswith(('l', '-'))]

    print ("{0} directory count {1}".format(location, len(directoryList)))
    print ("{0} file count {1}".format(location, len(fileList)))

    # # Loop through the files and assign to dictionary
    for i in fileList:

        # # split line using space
        parseLine = i.split(' ')

        # # reverse the list so we have, filename, time, date ,size
        reverseList = list(reversed(parseLine))

        # # Check filename does not contain spaces
        # # reverseList[1] should contain time
        #if not ':' in reverseList[1]:
        if not ":" in reverseList[1]:
            reverseList = concat_split_file_path(reverseList)

        # # create date time
        dt = "{0} {1}".format(reverseList[2], reverseList[1])
        datetime_object = datetime.strptime(dt, dtMask)

        key = reverseList[0]

        fullPath = os.path.join(dir, key)
        #path, file = os.path.split(fullPath)

        exclude = False
        for excludePath in excludePathList:

            if excludePath in fullPath:
                exclude = True

        if exclude == False:
            outerDict[key] = {}
            outerDict[key]['DateTime'] = datetime_object
            outerDict[key]['Size'] = reverseList[3]
        else:
            excludedDict[key] = {}
            excludedDict[key]['DateTime'] = datetime_object
            excludedDict[key]['Size'] = reverseList[3]

    orderedDict = collections.OrderedDict(sorted(outerDict.items()))
    orderedDictExc = collections.OrderedDict(sorted(excludedDict.items()))
    return orderedDict, orderedDictExc


def compare_overlap_files(filesInBoth, dictRemote, hostName, remoteServer):
    """
    Adds to the log, the file names, date and size of files that appear only on
    one of the servers being compared
    Args:
        filesInBoth (dict) : dict containing filename as key, size and date as
            value
        dictRemote (dict):   dict containing filename as key, size and date as
            value
        hostName (string)      : Name of local server
        remoteServer (string)  : Name of remote server
    Returns:
        filesIdentical (bool)  : Flag stating whether files are identical or not
        return_log_info (list) : List containing log info
    """
    return_log_info = []

    msg = "* * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * *"
    return_log_info.append(msg)
    msg = "FILE DIFFERENCES IN {0} - {1}".format(hostName, remoteServer)
    msg = msg.upper()
    return_log_info.append(msg)
    msg = "* * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * *"
    return_log_info.append(msg)
    return_log_info.append("")

    filesInBothOD = collections.OrderedDict(sorted(filesInBoth.items()))

    filesIdentical = True

    for outerKey, _ in filesInBothOD.iteritems():

        key = outerKey
        localSize = filesInBothOD[key]['Size']
        localDate = filesInBothOD[key]['DateTime']

        if key in dictRemote:
            remoteSize = dictRemote[key]['Size']
            remoteDate = dictRemote[key]['DateTime']

        if localSize != remoteSize or localDate != remoteDate:
            msg = "*************** INVESTIGATE ***************"
            return_log_info.append(msg)
            msg = "*************** INVESTIGATE ***************"
            return_log_info.append(msg)

            msg = "Main key: {0}".format(key)
            return_log_info.append(msg)
            msg = "\t Local Date    : {0}".format(localDate)
            return_log_info.append(msg)
            msg = "\t Remote Date   : {0}".format(remoteDate)
            return_log_info.append(msg)
            msg = "\t Local Size    : {0}".format(localSize)
            return_log_info.append(msg)
            msg = "\t Remote Size   : {0}".format(remoteSize)
            return_log_info.append(msg)

            sizeMsg = ''
            if localSize == remoteSize:
                sizeMsg = "and size is the same"
            elif localSize > remoteSize:
                sizeMsg = "and local size is greater"
            else:
                sizeMsg = "and remote size is greater"

            if filesInBothOD[key]['DateTime'] > dictRemote[key]['DateTime']:
                msg = "\t Local Date is the latest {0}".format(sizeMsg)
                return_log_info.append(msg)

            elif filesInBothOD[key]['DateTime'] < dictRemote[key]['DateTime']:
                msg = "\t Remote Date is the latest {0}".format(sizeMsg)
                return_log_info.append(msg)

            else:
                msg = "\t Dates are the same {0}".format(sizeMsg)
                return_log_info.append(msg)

            filesIdentical = False

        if localSize != remoteSize or localDate != remoteDate:
            msg = "******************* END *******************"
            return_log_info.append(msg)
            return_log_info.append("")
        else:
            pass

    return filesIdentical, return_log_info


def list_files_on_one_server(filesInOne, mainLog, server, serverLocation):
    """
    Adds to the log, the file names, date and size of files that appear only on
    one of the servers being compared
    Args:
        filesInOne (dict) : dict containing filename as key, size and date as
            value
        mainLog (log object)    :
        server (string)         : Name of server
        serverLocation (string) : Specifies whether remote or local server
    Returns:
    """
    msg = "~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~"
    add_to_log(mainLog, msg, 0, 0)
    msg = "~ ~ FILES IN {0} ONLY - {1}".format(serverLocation, server)
    msg = msg.upper()
    add_to_log(mainLog, msg, 0, 0)
    msg = "~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~"
    add_to_log(mainLog, msg, 0, 1)

    filesInOneOD = collections.OrderedDict(sorted(filesInOne.items()))

    for outerKey, _ in filesInOneOD.iteritems():

        key = outerKey
        fileSize = filesInOneOD[key]['Size']
        fileDate = filesInOneOD[key]['DateTime']

        msg = "Main key: {0}".format(key)
        add_to_log(mainLog, msg, 0, 0)
        msg = "\t {0} Date    : {1}".format(serverLocation, fileDate)
        add_to_log(mainLog, msg, 0, 0)
        msg = "\t {0} Size    : {1}".format(serverLocation, fileSize)
        add_to_log(mainLog, msg, 0, 0)

    msg = " "
    add_to_log(mainLog, msg, 0, 0)


def create_all_dict_keys(primarySite):
    """
    Returns dictionary keys to match the SQL column headers
    Args:
        primarySite (string) : string containing primary site
    Returns:
        sourceMD5 (string) : example - SourceLeedsMD5
        remote1MD5 (string) : example - CopleyMD5
        remote2MD5 (string) : example - LeedsCopley
        sourceRemote1 (string) : example - LiverpoolMD5
        sourceRemote2 (string) : example - LeedsLiverpool
    """
    source = ''
    remote1 = ''
    remote2 = ''
    if primarySite == 'LML':
        source = 'Leeds'
        remote1 = 'Copley'
        remote2 = 'Liverpool'
    elif primarySite == 'LPL':
        source = 'Liverpool'
        remote1 = 'Copley'
        remote2 = 'Leeds'
    elif primarySite == 'CPL':
        source = 'Copley'
        remote1 = 'Leeds'
        remote2 = 'Liverpool'
    else:
        print ("Unrecognised location - {0}".format(primarySite))

    sourceMD5 = "Source{0}MD5".format(source)
    remote1MD5 = "{0}MD5".format(remote1)
    remote2MD5 = "{0}MD5".format(remote2)
    sourceRemote1 = "{0}{1}".format(source, remote1)
    sourceRemote2 = "{0}{1}".format(source, remote2)

    return sourceMD5, remote1MD5, remote2MD5, sourceRemote1, sourceRemote2


def create_some_dict_keys(primarySite, remoteSite):
    """
    Returns dictionary keys to match the SQL column headers
    Args:
        primarySite (string) : string containing primary site
        remoteSite (string) : string containing remote site
    Returns:
        sourceMD5 (string) : example - SourceLeedsMD5
        remoteMD5 (string) : example - CopleyMD5
        sourceRemote (string) : example - LiverpoolMD5
    """
    source = ''

    if primarySite == 'LML':
    	source = 'Leeds'
    if primarySite == 'CPL':
    	source = 'Copley'
    if primarySite == 'LPL':
    	source = 'Liverpool'

    if remoteSite == 'LML':
    	target = 'Leeds'
    if remoteSite == 'CPL':
    	target = 'Copley'
    if remoteSite == 'LPL':
    	target = 'Liverpool'

    sourceMD5 = "Source{0}MD5".format(source)
    remoteMD5 = "{0}MD5".format(target)
    sourceRemote = "{0}{1}".format(source, target)

    return sourceMD5, remoteMD5, sourceRemote


def parse_comparisonList(comparisonList, primarySite):
    """
    Returns parsed information from a list which is populated through a
    Standard Procedure call to the database
    Args:
        comparisonList (List) : List containing dictionary of SP results
        primarySite (string)  : String containing site location

    Returns:
        orderedDict (Dict) : Dict containing folder paths that are not in sync
        matchList (List)   : List containing folder paths that are in sync
    """
    # # assign generic dict keys
    paramMask = 'parametermask'
    allMD5Match = 'AllMD5Match'
    noRemoteMatch = 'noRemoteMatch'

    # # create specific dict keys
    sourceMD5, remote1MD5, remote2MD5, sourceRemote1, sourceRemote2 = create_all_dict_keys(primarySite)

    mainDict = {}
    matchList = []
    sefasPath = ''

    for i in comparisonList:

        innerDict = {}
        tempDict = {}
        noMatchList = ['None', '']

        tempDict = i
        sefasPath =  str(tempDict[paramMask])
        noMatch = ''
        if str(tempDict[allMD5Match]) == 'No Match':
            innerDict[sourceMD5] = str(tempDict[sourceMD5])
            innerDict[remote1MD5] = str(tempDict[remote1MD5])
            innerDict[remote2MD5] = str(tempDict[remote2MD5])
            innerDict[sourceRemote1] = str(tempDict[sourceRemote1])
            innerDict[sourceRemote2] = str(tempDict[sourceRemote2])
            innerDict[allMD5Match] = str(tempDict[allMD5Match])

            if str(innerDict[remote1MD5]) in noMatchList or str(innerDict[remote2MD5]) in noMatchList:
                noMatch = 'No Remote Match'

            innerDict[noRemoteMatch] = noMatch
            mainDict[str(tempDict[paramMask])] = innerDict
        else:
            matchList.append(sefasPath)

    orderedDict = collections.OrderedDict(sorted(mainDict.items()))
    return orderedDict, matchList


def main(arguments):

    fullRun = False
    md5Only = False
    rsyncOnly = False

    if arguments.full:
        fullRun = True
        print ("Starting the MD5 and rsync process...")

    if arguments.md5:
        md5Only = True
        print ("Starting the MD5 process...")

    if arguments.rsync:
        rsyncOnly = True
        print ("Starting the rsync process...")

    if not fullRun and not md5Only and not rsyncOnly:
        fullRun = True
        print ("Starting the MD5 and rsync process...")

    # # get script path and set log
    scriptPath = os.path.abspath(__file__)
    mainLog = get_logger('main', scriptPath)

    # # we don't want >1 instances of this script running at the same time
    lockFile = os.path.abspath(__file__.replace('.py', '.RUNNING'))

    check_lock_file_exist(lockFile, mainLog)

    # # one timestamp for this comparison
    dtNow = datetime.now()
    checkDate = format_tStamp(time.mktime(dtNow.timetuple()))

    # # possible target servers
    hostname = socket.gethostname().upper()
    # # hostname = 'LMLDPWPRDU01'
    primarySite, platform = split_hostname(hostname)
    targetServers = ['LPLDPWPRD~01', 'CPLDPWPRD~01', 'LMLDPWPRD~01']
    targetServers = [srv.replace('~', platform) for srv in targetServers]
    targetServers.remove(hostname)     # # remove hostname from target server

    # # Update dbo.Sync_Paths with Sefas Apps and External Resource information
    config_resource_dict = update_table_with_paths(hostname, checkDate, mainLog)

    # #
    create_jobtype_resource_link(hostname, config_resource_dict)

    #os.remove(lockFile)
    #exit()

    # # Run process to update table with MD5 value
    if fullRun or md5Only:
        run_MD5_process(hostname, checkDate, mainLog)

        if md5Only:
            print ("MD5 process complete")
            os.remove(lockFile)
            exit()

    # # Run DB comparison SP
    db_host = get_db_host_name(primarySite, platform)
    comparisonList = get_sync_comparison_from_DB(db_host)

    # # Return dictionary of non match MD5s
    # # Return list of matched MD5 paths
    comparisonDict, allMatchList = parse_comparisonList(comparisonList, primarySite)

    #compareList = []
    #noCompareList = []
    #excludePathList = []

    msg = "LOCAL SERVER {0}".format(hostname).upper()
    add_to_log(mainLog, msg, 1, 1)

    # # Output a list of all apps to the log - to give a summary of apps
    log_apps_status(mainLog, allMatchList, comparisonDict)

    # # Create list of differences from rsync results
    run_rsync_process(comparisonDict, targetServers, hostname, mainLog)

    os.remove(lockFile)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-f', '--full', action='store_true',
                    help="full comparison including MD5 creation")
    parser.add_argument('-m', '--md5', action='store_true',
                    help="Only create MD5 checksums")
    parser.add_argument('-r', '--rsync', action='store_true',
                    help="Only create rsync comparison reports")

    arguments = parser.parse_args()

    if len(sys.argv) > 2:
        print ("Can't set more than 1 argument")

        for arg in sys.argv:
            print (arg)

        exit()

    main(arguments)
