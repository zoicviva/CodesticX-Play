import sqlparse
import ntpath
from UtilitiesCC import UtilitiesCC

class CodeFormatter:
    def __init__(self,filePath):
        self.filePath=filePath
        self.fileName=ntpath.basename(filePath)

    
    def formatIt(self):
        fileObj=open(self.filePath,'r')
        fileContent=fileObj.read()
        fileObj.close()
        lvlOneFormattedContent=sqlparse.format(fileContent,reindent=True,keyword_case='upper',identfier_case='lower')
        return lvlOneFormattedContent
        
if __name__=="__main__":
    obj=CodeFormatter("/Users/vivek.keshri/dq_base_7th_march.sql")
    lvlOneFormattedContent=obj.formatIt()
    UtilitiesCC.writeTextToFile("formatted/temp","l1_"+obj.fileName,lvlOneFormattedContent)
    print "done"
    