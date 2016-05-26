import re
import os
class CommentHandler: 
    
    def __init__(self,fileName):
        self.fileName=fileName
        self.filePath=os.path.expanduser('~')+"/CodeCompliance/orig/"+fileName
    
    def removeComments(self):
        fileContent = open(self.filePath, "r")
        fileText=fileContent.read().replace("\r","\n")  #added replace to handle old mac format return \r
        fileContent.close()
        rmvMulComment= re.sub("(/\*([^*]|(\*+[^*/]))*\*+/)|(//.*)","",fileText)
        rmvAllComment=re.sub("--.*","",rmvMulComment)
        return rmvAllComment;
    