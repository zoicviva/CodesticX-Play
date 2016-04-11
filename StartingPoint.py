from StatmentSequencer import StatementSequencer
from UtilitiesCC import UtilitiesCC
from CommentHandler import CommentHandler
from Analyser import Analyser
from OperationCount import OperationCount

#/Users/tata.swaroop/Desktop/Desktop/DQ/TAG_CR_26582541_DQ/compile/spl/sp_load_em_carrier_dq_cmptn.sql
#/Users/vivek.keshri/Desktop/DQ_ENHANCEMENT\ PHASE\ 2/dq/DQ_ENHANCEMENT\ PHASE\ 2/TAG_CR_26582541_DQ/compile/spl/sp_load_em_carrier_dq_raw_file.sql
if __name__=="__main__" : 
    fileName=UtilitiesCC.copyFileToOrig("/Users/vivek.keshri/Desktop/DQ_ENHANCEMENT\ PHASE\ 2/dq/DQ_ENHANCEMENT\ PHASE\ 2/TAG_CR_26582541_DQ/compile/spl/sp_load_em_carrier_dq_raw_file.sql")
    fileContent=CommentHandler(fileName).removeComments()
    UtilitiesCC.writeTextToFile("work", fileName, fileContent)
    fileObj=StatementSequencer(fileName)
    sequencedfilePath=fileObj.sequenceIt()
    print "sequenced file present at : "+sequencedfilePath+" inside project folder"
    analyse=Analyser().startAnalysing(fileName)
    print analyse
    print OperationCount(fileName).tableWiseCount()