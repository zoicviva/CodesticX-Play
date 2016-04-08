import re
class CommentHandler: 
    
    def __init__(self,fileName):
        self.fileName=fileName
        self.filePath='orig/'+fileName
    
    def removeComments(self):
        fileContent = open(self.filePath, "r")
        fileText=fileContent.read()
        fileContent.close()
        rmvMulComment= re.sub("(/\*([^*]|(\*+[^*/]))*\*+/)|(//.*)","",fileText)
        rmvAllComment=re.sub("--.*","",rmvMulComment)
        return rmvAllComment;
    