from StatmentSequencer import StatementSequencer
from UtilitiesCC import UtilitiesCC
from CommentHandler import CommentHandler
from Analyser import Analyser
from OperationCount import OperationCount

#Run it to get html
if __name__=="__main__" : 
    fileName=UtilitiesCC.copyFileToOrig("/Users/tata.swaroop/Desktop/Desktop/DQ/TAG_CR_26582541_DQ/compile/spl/sp_load_em_carrier_dq_cmptn.sql")
    fileContent=CommentHandler(fileName).removeComments()
    UtilitiesCC.writeTextToFile("work", fileName, fileContent)
    fileObj=StatementSequencer(fileName)
    sequencedfilePath=fileObj.sequenceIt()
    print "sequenced file present at : "+sequencedfilePath+" inside project folder"
    analyse=Analyser().startAnalysing(fileName)
    print analyse
    print OperationCount(fileName).tableWiseCount()