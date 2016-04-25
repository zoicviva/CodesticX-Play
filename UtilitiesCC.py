from shutil import copy2
import ntpath
import os
import logging
class UtilitiesCC:
    @staticmethod 
    def writeToFile(filePath,contentList):
        fileObj=open(filePath,'w')
        for line in contentList:
            fileObj.write(line+"\n")
        fileObj.close()
    
    @staticmethod
    def copyFileToWork(filePath):
        copy2(filePath, os.path.expanduser('~')+"/CodeCompliance/work/")
        fileName=ntpath.basename(filePath)
        return fileName
    
    @staticmethod
    def copyFileToOrig(filePath):
        logging.info("copying "+filePath+" to "+os.getcwd()+"orig")
        copy2(filePath, os.path.expanduser('~')+"/CodeCompliance/orig/")
        fileName=ntpath.basename(filePath)
        return fileName
    
    @staticmethod 
    def writeTextToFile(filePath,fileName,fileContent):
        fileObj=open(filePath+'/'+fileName,'w')
        fileObj.write(fileContent)
        fileObj.close()