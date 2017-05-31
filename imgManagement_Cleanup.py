from PgSQL import *
from GetImageStatistics import getImageStat as imgStats
import os
import sys
import logging
import traceback
import PIL
from PIL import Image
import re
import time
import datetime

def ConfigureLogging():
    logFormatter = logging.Formatter("%(asctime)s [%(threadName)-12.12s] [%(levelname)-5.5s]  %(message)s")
    rootLogger = logging.getLogger()
    rootLogger.setLevel(logging.DEBUG)

    # Write log messages into file <pythonScriptName>.log
    fileHandler = logging.FileHandler("{0}.log".format(__file__), mode='a', encoding=None, delay=False )
    fileHandler.setFormatter(logFormatter)
    fileHandler.setLevel(logging.DEBUG)
    rootLogger.addHandler(fileHandler)

    # Write log messages to console: sends logging output to streams such as sys.stdout
    consoleHandler = logging.StreamHandler(sys.stdout)
    consoleHandler.setFormatter(logFormatter)
    consoleHandler.setLevel(logging.DEBUG)
    rootLogger.addHandler(consoleHandler)

    logging.info("Initialized logging")
#

class compareDBvsStorage():

    def __init__(self,in_directory):
        self.directory = in_directory

        # list storage
        logging.info('Gathering storage elements...')
        self.storageElements = self.__getStorageElements()

        # list database
        logging.info('Gathering database elements...')
        self.dbElements = self.__getDBElements()
    #

    def _comparer(self,in_referenceDict,in_compareDict):
        compareDict = {}
        for key,values in in_referenceDict.iteritems():
            for value in values:
                if not value in in_compareDict.get(key):
                    compareDict.setdefault(key,[])
                    compareDict[key].append(value)
        return compareDict
    #

    def compareFolderLevel(self):
        logging.info('Comparing folder level:')
        storageDirectories = [key for key in self.storageElements.keys()]
        logging.info('\t{} directories in {}'.format(len(storageDirectories),self.directory))
        self.dbDirectories = [key for key in self.dbElements.keys()]
        logging.info('\t{} directories in field imgcurrdirectory in database'.format(len(self.dbDirectories)))
        folderLevelDifference = [key for key in self.storageElements.keys() if key not in self.dbElements.keys()]
        if len(folderLevelDifference) != 0:
            logging.warn('Found difference in folder level:')
            for folder in list(folderLevelDifference):
                if folder in self.dbDirectories:
                    logging.warn('\tFolder {} is registered in database but not found in storage'.format(folder))
                if folder in storageDirectories:
                    logging.warn('\tFolder {} found in storage but not registered in database'.format(folder))
        else:
            logging.info('No differences were found')
    #

    def compareFileLevel(self):
        self.folderWithDifferences = {}

        # storage as reference
        logging.info('')
        logging.info('Comparing file level:')
        logging.info('')
        logging.info('\tComparing storage vs database:')
        storageVsDatabase = self._comparer(self.storageElements,self.dbElements)
        for key in sorted(storageVsDatabase.keys()):
            logging.info('\t\tFound {}/{} Images in {} which are not in database ({})'.format(len(storageVsDatabase.get(key)),len(self.storageElements.get(key)),key,len(self.dbElements.get(key))))
            self.folderWithDifferences.update({key:[len(self.storageElements.get(key))]})

        # database as reference
        logging.info('')
        logging.info('\tComparing database vs storage:')
        databaseVsStorage = self._comparer(self.dbElements,self.storageElements)
        for key in sorted(databaseVsStorage.keys()):
            logging.info('\t\tFound {}/{} rows in database that have no equivalent in {} ({})'.format(len(databaseVsStorage.get(key)),len(self.dbElements.get(key)),key,len(self.storageElements.get(key))))
            if not(key in self.folderWithDifferences.keys()):
                self.folderWithDifferences.update({key:[len(self.dbElements.get(key))]})
            else:
                self.folderWithDifferences[key].append(len(self.dbElements.get(key)))

        logging.info('')
        logging.info('\tDetected differences in {} folders:'.format(len(self.folderWithDifferences)))
        for k,v in sorted(self.folderWithDifferences.iteritems()):
            logging.info('\t\t{}: {}'.format(k,v))

        return {'imgNotInDatabase':storageVsDatabase,'rowNotInFilesystem':databaseVsStorage}
    #

    def __getStorageElements(self):
        storage = {}
        for subdir in os.listdir(self.directory):
            yearDir = os.path.join(self.directory,subdir)
            for subdir in os.listdir(yearDir):
                monthDir = os.path.join(yearDir,subdir)
                monthDict = {monthDir:[elem for elem in os.listdir(monthDir)]}
                storage.update(monthDict)
        return storage
    #

    def __getDBElements(self):
        result, dbCount = postgre.selectQry(curs,"imgcurrdirectory,imgfilename", "imgfile")
        dbElements = {}
        for element in result:
            dbElements.setdefault(element['imgcurrdirectory'],[])
            dbElements[element['imgcurrdirectory']].append(element['imgfilename'])
        return dbElements
    #
#

class imageComparsion():

    def __init__(self,in_directory,in_files,in_pattern):
        self.directory = in_directory
        self.fileNames = in_files
        self.pattern = in_pattern

    def _getImgAttributes(self,in_file):
        """
        Returns: Format, Mode, Size ,DateTimeStamp as DictKeys
        """
        try:
            im = Image.open(os.path.join(self.directory,in_file))
            Format = im.format
            Mode = im.mode
            Size = im.size
            try:
                tempTimeStamp = re.findall(self.pattern,str(im.info))
                properTimeStamp = time.mktime(datetime.strptime(min(tempTimeStamp),'%Y:%m:%d %H:%M:%S').timetuple())
                TimeStamp = datetime.fromtimestamp(properTimeStamp).strftime('%Y-%m-%d %H:%M:%S')
            except:
                TimeStamp = '#'
            return {'Format':Format,'Mode':Mode,'Size':Size,'DateTimeStamp':TimeStamp}
        except:
            #pass
            raise EnvironmentError
    #

    def _getCreateTime(self,in_file):
        import time
        import datetime
        from datetime import datetime
        osTimeFormat = datetime.fromtimestamp(os.path.getctime(os.path.join(self.directory,in_file))).strftime('%Y-%m-%d %H:%M:%S')
        return osTimeFormat
    #

    def getOlderImage(self):
        cmpDict = {}

        for fileName in self.fileNames:
            imgCreateDate = self._getCreateTime(fileName)
            imgAttributes = self._getImgAttributes(fileName)

            logging.debug('File: {}\trecording Date: {}, OS create Date: {}'.format(fileName, imgAttributes['DateTimeStamp'], imgCreateDate))

            cmpDict.update({fileName:[imgAttributes['DateTimeStamp'],imgCreateDate]})

        minDate = max(cmpDict.values())

        for key,values in cmpDict.items():
            if values == min(cmpDict.values()):
                return key
    #
#

def getMinLengthRMSValues(in_cursor):
    result, dbCount = postgre.selectQry(in_cursor,'min(char_length(rms1::text)) as min','imgstatistics')
    return [element.get('min') for element in result][0]
#

def getOlderDuplicateFromDB(in_cursor, in_duplicates):
    uuidList = []
    cmpList = []
    count=0
    for img in in_duplicates:
        # Select = 's.imgfileuuid, f.imgfiledate, f.imgfileoscreatedate, f.imgcurrdirectory, f.imgfilename'
        cmpList.append([img['s.imgfileuuid'],os.path.join(img['f.imgcurrdirectory'],img['f.imgfilename']),img['f.imgfiledate'],img['f.imgfileoscreatedate']])
##        if [filePath] == [elem[1] for elem in cmpList]:
##            print 'Same File: {}'.format(filePath)
##            sys.exit()
    dateTimes = [elem[-3:] for elem in cmpList]
    logging.debug('\tComparing:')
    for elem in cmpList:
        logging.debug('\t{}'.format(str(elem)))
        if max(dateTimes) == elem[1:]:
            logging.debug('\tDetected {} as older image. Adding to list.'.format(elem))
            uuidList.append(elem[0])
    logging.info('Detected {} duplicate to delete'.format(len(uuidList)))
    return uuidList
#

def checkFileName(in_reference, in_compare):
    """Return a Boolean:
        True if in values match, else False
    """
    if in_reference == in_compare:
        return True
    if in_reference != in_compare:
        return False


#-----------
# MAIN
#-----------

# Globals
db = 'ImageManagement'
user = "admin"
password = "?|\/btH|Uf"
rootDir = r'\\AAWS0703NAS\Multimedia\Pictures'
dateTimePattern = '[0-9]{4}[:][0-9]{2}[:][0-9]{2}[ ][0-9]{2}[:][0-9]{2}[:][0-9]{2}'


try:
    ConfigureLogging()
    # open db connection and providing cursor
    postgre = PgSQL(db,user,password)
    conn = postgre.connectDB()
    curs = postgre.openCursor(conn)

    mlrmsv = getMinLengthRMSValues(curs)

    # compare storage vs database
    svd = compareDBvsStorage(rootDir)
    # comparing folder level
    svd.compareFolderLevel()
    # comparing file level
    result = svd.compareFileLevel()

    # set compare Dictionaries
    imagesNotInDatabase = result['imgNotInDatabase']
    rowsNotInFilesystem = result['rowNotInFilesystem']


    logging.info('')
    logging.info('Checking Imagefiles which are not in Database:')
    for key,values in sorted(imagesNotInDatabase.items()):
        counter = 1
        for value in values:
            logging.info('{} / {}: {}'.format(counter,len(values),os.path.join(key,value)))
            counter += 1

            # get image statistic
            statistics = imgStats(os.path.join(key,value))

            # check for rows in database with same statistic values
            Select = 's.imgfileuuid, f.imgfiledate, f.imgfileoscreatedate, f.imgcurrdirectory, f.imgfilename'
            From = "imgstatistics s JOIN imgfile f ON s.imgfileuuid = f.uuid"
            Where = "s.rms1::text LIKE '{}%'".format(str(statistics['rms1'])[:mlrmsv-2])
            detected,count = postgre.selectQry(curs,Select,From,Where)
            if count != 0:

                # handling 1:1 reference
                if count == 1:
                    logging.info('\tFound reference in Database with same statistic: {}'.format(os.path.join(detected[0]['f.imgcurrdirectory'],detected[0]['f.imgfilename'])))
                    # check accordance
                    if checkFileName(value,detected[0]['f.imgfilename']) == True:
                        logging.info('\t\tFilename matches. Check for other accordance...')
                    else:
                        logging.info('\tFilename does not match.')
                        logging.info('\tUpdating database: UPDATE imgfile SET imgfilename = {} WHERE uuid = {}'.format(value,detected[0]['s.imgfileuuid']))
                        postgre.updateRow(curs,'imgfile','imgfilename',"'{}'".format(value),"uuid = '{}'".format(detected[0]['s.imgfileuuid']))
                        conn.commit()

                # handling duplicates
                if count == 2:
                    olderUUID = getOlderDuplicateFromDB(curs,detected)
                    logging.info('Older value from DB: {}'.format(olderUUID))

                    olderImg = imageComparsion(key,[detected[0]['f.imgfilename'],value],dateTimePattern).getOlderImage()
                    logging.info('Older Image: {}'.format(olderImg))

                # handling of multiple references
                if count > 2:
                    logging.info('No rule defined for handling {} references. DO SOMTHING HERE!!!'.format(count))

            else:
                logging.info('\tNo references found.')


    postgre.disconnect(conn)
except:
    logging.error('Unexcepted error:')
    logging.error(traceback.format_exc())
logging.shutdown()