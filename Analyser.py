import json
import re
import os
import logging
from TableExtractor import TableExtractor

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
        self.userHome=os.path.expanduser('~')
        
    def masterJson(self,fileName):
        dictObj={}
        noCmntFile=open(self.userHome+"/CodeCompliance/work/"+fileName,"r")
        fileContent=noCmntFile.read()
        noCmntFile.close()
        preProcName=re.search('replace\s*procedure([^\n]+) \(',fileContent.lower()).group(1)
        finalProcName=re.sub(' ', '', preProcName.strip())
        dictObj["proc_name"]=finalProcName
        cmntFile=open(self.userHome+"/CodeCompliance/orig/"+fileName,"r")
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
        stmtFile=open(self.userHome+"/CodeCompliance/temp/"+fileName,"r")
        fileContent=stmtFile.read()
        stmtFile.close()
        stmtCount=len(re.findall("\n",fileContent))
        dictObj["no_of_stmts"]=stmtCount
        jsonFileName=self.userHome+"/CodeCompliance/temp/"+fileName+".master.json"
        toJson=open(jsonFileName,"w")
        toJson.write(json.dumps(dictObj))
        toJson.close()
    
    def matches(self,line, opendelim='(', closedelim=')'):
        stack = []
        for m in re.finditer(r'[{}{}]'.format(opendelim, closedelim), line):
            pos = m.start()
            c = line[pos]
            if c == opendelim:
                stack.append(pos+1)

            elif c == closedelim:
                if len(stack) > 0:
                    prevpos = stack.pop()
                    yield (prevpos, pos, len(stack))
                else:
                    print("encountered extraneous closing quote at pos {}: '{}'".format(pos, line[pos:] ))
                    pass

        if len(stack) > 0:
            for pos in stack:
                print("expecting closing quote to match open quote starting at: '{}'".format(line[pos-1:]))
    
    def replaceMacthes(self,line):
        matches_arr=[]
        rplcd_arr=[]
        maxLevel=0
        total_len=len(line)
        for openpos, closepos, level in self.matches(line):
            if(maxLevel<=level):
                maxLevel=level
            query=line[openpos:closepos]
            query="~"*(openpos)+query+"~"*(total_len-closepos)
            matches_arr.append({"query":query,"level":level,"start":openpos,"end":closepos})
    
        for i in range(0,maxLevel):
            for baseItem in matches_arr:
                if(baseItem["level"]==i):
                    mainQry=baseItem["query"]
                    replacedQry=""
                    for replacer in matches_arr:
                        if replacer["level"]==i+1:
                            between_len=replacer["end"]-replacer["start"]
                            between_str="~"*between_len
                            replacedQry=mainQry[0:replacer["start"]]+between_str+mainQry[replacer["end"]:]
                            mainQry=replacedQry
                        else:
                            pass
                    rplcd_arr.append({"query":replacedQry,"level":baseItem["level"],"start":baseItem["start"] ,"end":baseItem["end"]})
                else: 
                    pass
        for item in matches_arr:
            if item["level"]==maxLevel :
                rplcd_arr.append(item)
        return rplcd_arr
    
    def getTablesFromSelect(self,line):
        rplcd_arr=self.replaceMacthes(line)
        tables=[]
        for i in rplcd_arr:
            noTild=re.sub(r'~+','',i["query"].lower())
            if re.search('^\s*select',noTild):
                noBraces=re.sub(r'\(\s*\)',' ~ ',noTild)
                splittedArr=re.sub('\sunion all\s|\sintersect\s|\sminus\s|\sunion\s',";;",noBraces).split(';;')
                for item in splittedArr:
                    noWhere=re.sub(r'select .* from','from',item)
                    noWhere=re.sub(r' where .*| group .*| qualify .*| having .*| order .*',';',noWhere)
                    
                    tables+=re.findall(r'from\s*([\w\d\.\_\$]+)',noWhere)
                    tables+=re.findall(r'join\s*([\w\d\.\_\$]+)',noWhere)
                    tables+=re.findall(r',\s*([\w\d\.\_\$]+)',noWhere)
            
        return tables
    
    def getLevelZeroQuery(self,line):
        matches_arr=[]
        rplcd_arr=[]
        maxLevel=0
        total_len=len(line)
        for openpos, closepos, level in self.matches(line):
            if(maxLevel<=level):
                maxLevel=level
            query=line[openpos:closepos]
            query="~"*(openpos)+query+"~"*(total_len-closepos)
            matches_arr.append({"query":query,"level":level,"start":openpos,"end":closepos})
    
        for i in range(0,1):
            for baseItem in matches_arr:
                if(baseItem["level"]==i):
                    mainQry=baseItem["query"]
                    replacedQry=""
                    for replacer in matches_arr:
                        if replacer["level"]==i+1:
                            between_len=replacer["end"]-replacer["start"]
                            between_str="~"*between_len
                            replacedQry=mainQry[0:replacer["start"]]+between_str+mainQry[replacer["end"]:]
                            mainQry=replacedQry
                        else:
                            pass
                    rplcd_arr.append({"query":replacedQry,"level":baseItem["level"],"start":baseItem["start"] ,"end":baseItem["end"]})
                else: 
                    pass
        return rplcd_arr.join()
        
    def analyseCall(self,stmt):
        dictObj={}
        dictObj["type"]="others"
        dictObj["subtype"]="call"
        dictObj["proc_seq_nr"]=self.procSeqNumeber
        dictObj["seq_nr"]=self.callSeqNumeber
        try:
            preProcName=re.search(r'call \s*([^\n]+) \(', stmt).group(1)
            finalProcName=re.sub(' ', '', preProcName.strip())
            dictObj["proc_name"]=finalProcName
            preArgStr=re.search(r'\(\s*([^\n]+)\)', stmt).group(1)
            finalArgList=re.sub(' ', '', preArgStr.strip()).split(',')
            dictObj["args"]=finalArgList
        except:
            dictObj["proc_name"]=""
            dictObj["args"]=""
            logging.error("analyseCall - "+stmt)
        return json.dumps(dictObj);
    
    def analyseInsert(self,stmt):
        dictObj={}
        dictObj["type"]="dml"
        dictObj["subtype"]="insert"
        dictObj["proc_seq_nr"]=self.procSeqNumeber
        dictObj["seq_nr"]=self.insertSeqNumeber
        try:
            preIntoTableName=re.search(r'into\s*([^\(]*)', stmt).group(1)
            finalIntoTableName=re.sub(' ', '', preIntoTableName.strip())
            dictObj["table_name"]=finalIntoTableName
            noCnstStmt=re.sub("\'.*?\'","''",stmt)
            line=""
            if re.search(r'insert\s*into\s*[\s\w\d\.\_\$]+\s*\(',stmt):
                line=re.sub(r'insert\s*into\s*[\s\w\d\.\_\$]+\s*\(.*?\)','',stmt)
            else:
                line=stmt
            line="("+line[line.find("select"):].strip()+")"
            fromTables=self.getTablesFromSelect(line)
            dictObj["from_table_names"]=fromTables
        except:
            dictObj["table_name"]=""
            dictObj["from_table_names"]=""
            logging.error("analyseInsert - "+stmt)
        return json.dumps(dictObj);
    
    
    def analyseDelete(self,stmt):
        dictObj={}
        dictObj["type"]="dml"
        dictObj["subtype"]="delete"
        dictObj["proc_seq_nr"]=self.procSeqNumeber
        dictObj["seq_nr"]=self.deleteSeqNumeber
        try:
            to_table_stmt=re.sub(' where.*', ' ', stmt)
            preTableName1=re.search(r'delete\s*(.*?from){0,1}([^,;]*)', to_table_stmt).group(2).strip()
            preTableName2=re.search(r'.*\.\s*[^\s]*',preTableName1).group()
            finalTableName=re.sub(' ', '', preTableName2.strip())
            dictObj["table_name"]=finalTableName
            dictObj["alias_name"]=preTableName1[len(preTableName2):].strip()
        except:
            dictObj["table_name"]=""
            dictObj["alias_name"]=""
            logging.error("analyseDelete - "+stmt)
        return json.dumps(dictObj);
    
    def analyseMerge(self,stmt):
        dictObj={}
        dictObj["type"]="dml"
        dictObj["subtype"]="merge"
        dictObj["proc_seq_nr"]=self.procSeqNumeber
        dictObj["seq_nr"]=self.mergeSeqNumeber
        try:
            preTableName1=re.search(r'into(.*?)using', stmt).group(1)
            preTableName2=re.search(r'.*\.\s*[^\s]*',preTableName1).group()
            finalTableName=re.sub(' ', '', preTableName2.strip())
            dictObj["table_name"]=finalTableName
            dictObj["alias_name"]=preTableName1[len(preTableName2):].strip()
        except:
            dictObj["table_name"]=""
            dictObj["alias_name"]=""
            logging.error("analyseMerge - "+stmt)
        return json.dumps(dictObj);
    
    def analyseUpdate(self,stmt):
        dictObj={}
        dictObj["type"]="dml"
        dictObj["subtype"]="update"
        dictObj["proc_seq_nr"]=self.procSeqNumeber
        dictObj["seq_nr"]=self.updateSeqNumeber
        try:
            to_table_stmt=re.sub(' set.*', ' ', stmt)
            preTableName1=re.search(r'update\s*(.*?from){0,1}([^,;]*)', to_table_stmt).group(2).strip()
            preTableName2=re.search(r'.*\.\s*[^\s]*',preTableName1).group()
            finalTableName=re.sub(' ', '', preTableName2.strip())
            dictObj["table_name"]=finalTableName
            dictObj["alias_name"]=preTableName1[len(preTableName2):].strip()
        except:
            dictObj["table_name"]=""
            dictObj["alias_name"]=""
            logging.error("analyseUpdate - "+stmt)
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
        try:
            preTableName=re.search(r"collect\s*(stats|statistics){1}\s*(on){0,1}(.*);", stmt).group(3).strip().split()
            finalTableName=preTableName[0]
            dictObj["table_name"]=finalTableName
            if(len(preTableName)>1):
                dictObj["is_table_level"]="N"
            else :
                dictObj["is_table_level"]="Y"
        except:
            dictObj["table_name"]=""
            dictObj["is_table_level"]=""
            logging.error("analyseCollect - "+stmt)
            
        return json.dumps(dictObj);
    
    def analyseSet(self,stmt):
        dictObj={}
        dictObj["type"]="others"
        dictObj["subtype"]="set"
        dictObj["proc_seq_nr"]=self.procSeqNumeber
        dictObj["seq_nr"]=self.setSeqNumeber
        try:
            dictObj["variable"]=re.search(r'set \s*([^\n]+)=', stmt).group(1).strip()
            dictObj["value"]=re.search(r'=\s*([^\n]+);', stmt).group(1).strip()
        except:
            dictObj["variable"]=""
            dictObj["value"]=""
            logging.error("analyseSet - "+stmt)
        return json.dumps(dictObj);
    
    def startAnalysing(self,fileName):
        #
        self.masterJson(fileName)
        fileContent=open(self.userHome+"/CodeCompliance/temp/"+fileName,"r")
        jsonFileName=self.userHome+"/CodeCompliance/temp/"+fileName+".json"
        toJson=open(jsonFileName,"w")
        for line in fileContent:
            lowerLine=re.sub("\"", "'",line.lower())
            lowerLine=re.sub("\s*\.\s*", ".",lowerLine)
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