import json
import re
class Analyser:
    def __init__(self):
        self.procSeqNumeber=0
        self.insertSeqNumeber=0
        self.selectSeqNumeber=0
        self.updateSeqNumeber=0
        self.deleteSeqNumeber=0
        self.mergeSeqNumeber=0
        self.callSeqNumeber=0
        self.setSeqNumeber=0
        self.declareSeqNumeber=0
        self.collectSeqNumeber=0
        
    def masterJson(self,fileName):
        dictObj={}
        noCmntFile=open("work/"+fileName,"r")
        fileContent=noCmntFile.read()
        noCmntFile.close()
        preProcName=re.search('replace\s*procedure([^\n]+) \(',fileContent.lower()).group(1)
        finalProcName=re.sub(' ', '', preProcName.strip())
        dictObj["proc_name"]=finalProcName
        cmntFile=open("orig/"+fileName,"r")
        fileContent=cmntFile.read()
        cmntFile.close()
        noOfSinCmnts=len(re.findall('--.*',fileContent))
        noOfMulCmnts=len(re.findall("(/\*([^*]|(\*+[^*/]))*\*+/)",fileContent))
        dictObj["no_of_cmnts"]=noOfSinCmnts+noOfMulCmnts
        dictObj["single_line_cmnts"]=noOfSinCmnts
        dictObj["multi_line_cmnts"]=noOfMulCmnts
        if (re.search('EXIT\s*HANDLER',fileContent) or re.search('CONTINUE\s*HANDLER',fileContent)):
            excptn="Y"
        else:
            excptn="N"
        dictObj["exception_handled"]=excptn
        lineCount=len(re.findall("\n",fileContent))+1
        dictObj["no_of_lines"]=lineCount
        stmtFile=open("temp/"+fileName,"r")
        fileContent=stmtFile.read()
        stmtFile.close()
        stmtCount=len(re.findall("\n",fileContent))
        dictObj["no_of_stmts"]=stmtCount
        jsonFileName="temp/"+fileName+".master.json"
        toJson=open(jsonFileName,"w")
        toJson.write(json.dumps(dictObj))
        toJson.close()
        
    def analyseCall(self,stmt):
        dictObj={}
        dictObj["type"]="others"
        dictObj["subtype"]="call"
        dictObj["proc_seq_nr"]=self.procSeqNumeber
        dictObj["seq_nr"]=self.callSeqNumeber
        preProcName=re.search(r'call \s*([^\n]+) \(', stmt).group(1)
        finalProcName=re.sub(' ', '', preProcName.strip())
        dictObj["proc_name"]=finalProcName
        preArgStr=re.search(r'\(\s*([^\n]+)\)', stmt).group(1)
        finalArgList=re.sub(' ', '', preArgStr.strip()).split(',')
        dictObj["args"]=finalArgList
        return json.dumps(dictObj);
    
    def analyseInsert(self,stmt):
        dictObj={}
        dictObj["type"]="dml"
        dictObj["subtype"]="insert"
        dictObj["proc_seq_nr"]=self.procSeqNumeber
        dictObj["seq_nr"]=self.insertSeqNumeber
        preIntoTableName=re.search(r'into\s*([^\(]*)', stmt).group(1)
        finalIntoTableName=re.sub(' ', '', preIntoTableName.strip())
        dictObj["table_name"]=finalIntoTableName
        return json.dumps(dictObj);
    
    def analyseDelete(self,stmt):
        dictObj={}
        dictObj["type"]="dml"
        dictObj["subtype"]="delete"
        dictObj["proc_seq_nr"]=self.procSeqNumeber
        dictObj["seq_nr"]=self.deleteSeqNumeber
        to_table_stmt=re.sub(' where.*', ' ', stmt)
        preTableName1=re.search(r'delete\s*(.*?from){0,1}([^,;]*)', to_table_stmt).group(2).strip()
        preTableName2=re.search(r'.*\.\s*[^\s]*',preTableName1).group()
        finalTableName=re.sub(' ', '', preTableName2.strip())
        dictObj["table_name"]=finalTableName
        dictObj["alias_name"]=preTableName1[len(preTableName2):].strip()
        return json.dumps(dictObj);
    
    def analyseMerge(self,stmt):
        dictObj={}
        dictObj["type"]="dml"
        dictObj["subtype"]="merge"
        dictObj["proc_seq_nr"]=self.procSeqNumeber
        dictObj["seq_nr"]=self.mergeSeqNumeber
        preTableName1=re.search(r'into(.*?)using', stmt).group(1)
        preTableName2=re.search(r'.*\.\s*[^\s]*',preTableName1).group()
        finalTableName=re.sub(' ', '', preTableName2.strip())
        dictObj["table_name"]=finalTableName
        dictObj["alias_name"]=preTableName1[len(preTableName2):].strip()
        return json.dumps(dictObj);
    
    def analyseUpdate(self,stmt):
        dictObj={}
        dictObj["type"]="dml"
        dictObj["subtype"]="update"
        dictObj["proc_seq_nr"]=self.procSeqNumeber
        dictObj["seq_nr"]=self.updateSeqNumeber
        to_table_stmt=re.sub(' set.*', ' ', stmt)
        preTableName1=re.search(r'update\s*(.*?from){0,1}([^,;]*)', to_table_stmt).group(2).strip()
        preTableName2=re.search(r'.*\.\s*[^\s]*',preTableName1).group()
        finalTableName=re.sub(' ', '', preTableName2.strip())
        dictObj["table_name"]=finalTableName
        dictObj["alias_name"]=preTableName1[len(preTableName2):].strip()
        return json.dumps(dictObj);
    
    def analyseSelect(self,stmt):
        dictObj={}
        dictObj["type"]="dql"
        dictObj["subtype"]="select"
        dictObj["proc_seq_nr"]=self.procSeqNumeber
        dictObj["seq_nr"]=self.selectSeqNumeber
        
        return json.dumps(dictObj);
    
    def analyseDeclare(self,stmt):
        dictObj={}
        dictObj["type"]="others"
        dictObj["subtype"]="declare"
        dictObj["proc_seq_nr"]=self.procSeqNumeber
        dictObj["seq_nr"]=self.declareSeqNumeber
        
        return json.dumps(dictObj);
    
    def analyseCollect(self,stmt):
        dictObj={}
        dictObj["type"]="others"
        dictObj["subtype"]="collect"
        dictObj["proc_seq_nr"]=self.procSeqNumeber
        dictObj["seq_nr"]=self.collectSeqNumeber
        preTableName1=re.search(r'collect\s*stats\s*on\s*([^;]*)',stmt).group(1)
        preTableName2=re.search(r'.*\.\s*[^\s]*',preTableName1).group()
        finalTableName=re.sub(' ', '', preTableName2.strip())
        dictObj["table_name"]=finalTableName
        if(len(preTableName1.strip())==len(preTableName2.strip())):
            dictObj["is_table_level"]="Y"
        else :
            dictObj["is_table_level"]="N"
        return json.dumps(dictObj);
    
    def analyseSet(self,stmt):
        dictObj={}
        dictObj["type"]="others"
        dictObj["subtype"]="set"
        dictObj["proc_seq_nr"]=self.procSeqNumeber
        dictObj["seq_nr"]=self.setSeqNumeber
        dictObj["variable"]=re.search(r'set \s*([^\n]+)=', stmt).group(1).strip()
        dictObj["value"]=re.search(r'=\s*([^\n]+);', stmt).group(1).strip()
        return json.dumps(dictObj);
    
    def startAnalysing(self,fileName):
        #
        self.masterJson(fileName)
        fileContent=open("temp/"+fileName,"r")
        jsonFileName="temp/"+fileName+".json"
        toJson=open(jsonFileName,"w")
        for line in fileContent:
            lowerLine=re.sub("\"", "'",line.lower())
            self.procSeqNumeber+=1
            firstWord=lowerLine.strip().split()[0]
            if(firstWord=='insert'):
                self.insertSeqNumeber+=1
                toJson.write(self.analyseInsert(lowerLine)+"\n")
            elif(firstWord=='select'):
                self.selectSeqNumeber+=1
                toJson.write(self.analyseSelect(lowerLine)+"\n")
            elif(firstWord=='update'):
                self.updateSeqNumeber+=1
                toJson.write(self.analyseUpdate(lowerLine)+"\n")
            elif(firstWord=='delete'):
                self.deleteSeqNumeber+=1
                toJson.write(self.analyseDelete(lowerLine)+"\n")
            elif(firstWord=='merge'):
                self.mergeSeqNumeber+=1
                toJson.write(self.analyseMerge(lowerLine)+"\n")
            elif(firstWord=='call'):
                self.callSeqNumeber+=1
                toJson.write(self.analyseCall(lowerLine)+"\n")
            elif(firstWord=='set'):
                self.setSeqNumeber+=1
                toJson.write(self.analyseSet(lowerLine)+"\n")
            elif(firstWord=='declare'):
                self.declareSeqNumeber+=1
                toJson.write(self.analyseDeclare(lowerLine)+"\n")
            elif(firstWord=='collect'):
                self.collectSeqNumeber+=1
                toJson.write(self.analyseCollect(lowerLine)+"\n")
            
        fileContent.close()
        toJson.close()
            
        return "success"