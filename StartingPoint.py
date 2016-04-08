from StatmentSequencer import StatementSequencer
from UtilitiesCC import UtilitiesCC
from CommentHandler import CommentHandler
from Analyser import Analyser
from OperationCount import OperationCount

# Run this to generate the HTML report
if __name__=="__main__" : 
    fileName=UtilitiesCC.copyFileToOrig("/Users/vivek.keshri/dq_base_7th_march.sql")
    fileContent=CommentHandler(fileName).removeComments()
    UtilitiesCC.writeTextToFile("work", fileName, fileContent)
    fileObj=StatementSequencer(fileName)
    sequencedfilePath=fileObj.sequenceIt()
    print "sequenced file present at : "+sequencedfilePath+" inside project folder"
    analyse=Analyser().startAnalysing(fileName)
    print analyse
    print OperationCount(fileName).tableWiseCount()