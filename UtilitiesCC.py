from shutil import copy2
import ntpath
class UtilitiesCC:
    @staticmethod 
    def writeToFile(filePath,contentList):
        fileObj=open(filePath,'w')
        for line in contentList:
            fileObj.write(line+"\n")
        fileObj.close()
    
    @staticmethod
    def copyFileToWork(filePath):
        copy2(filePath, "work/")
        fileName=ntpath.basename(filePath)
        return fileName
    
    @staticmethod
    def copyFileToOrig(filePath):
        copy2(filePath, "orig/")
        fileName=ntpath.basename(filePath)
        return fileName
    
    @staticmethod 
    def writeTextToFile(filePath,fileName,fileContent):
        fileObj=open(filePath+'/'+fileName,'w')
        fileObj.write(fileContent)
        fileObj.close()