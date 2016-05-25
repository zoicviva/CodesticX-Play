import json
import os
class OperationCount:
    def __init__(self,fileName):
        self.jsonFileName=fileName+".json"
        self.masterJsonFileName=fileName+".master.json"
        self.fileName=fileName
        self.userHome=os.path.expanduser('~')
        
    def isTablePresent(self,tableName,tableDictArr):
        index=-1
        i=0
        for i in range(0,len(tableDictArr)):
            if tableDictArr[i]["tableName"]==tableName:
                index=i
                break
        return index
    
    def writeResultToMasterJson(self,tableDictArr):
        masterJson=open(self.userHome+"/CodeCompliance/temp/"+self.fileName+".master.json",'a')
        masterJsonDict={}
        masterJsonDict["type"]="operation_count"
        masterJsonDict["value"]=tableDictArr
        masterJson.write(json.dumps(masterJsonDict))
        masterJson.close()
        return 1
    
    def tableWiseCountHtml(self,tableDictArr):
        masterJsonFileContent=open(self.userHome+"/CodeCompliance/temp/"+self.fileName+".master.json",'r')
        masterJsonDict={}
        for line in masterJsonFileContent:
            tempDictObj=json.loads(line)
            if tempDictObj["type"]=="master_data":
                masterJsonDict=tempDictObj
                break
        masterJsonFileContent.close()
        totalOfProc={'insert':0,'update':0,'delete':0,'merge':0,'collect':0}
        
        htmlHeadFile=open(self.userHome+"/CodeCompliance/html/template/TableCountHead.txt","r")
        htmlHeader=htmlHeadFile.read()
        htmlHeadFile.close()
        htmlMiddleFile=open(self.userHome+"/CodeCompliance/html/template/TableCountMiddle.txt","r")
        htmlMiddle=htmlMiddleFile.read()
        htmlMiddleFile.close()
        htmlTailFile=open(self.userHome+"/CodeCompliance/html/template/TableCountTail.txt","r")
        htmlFooter=htmlTailFile.read()
        htmlTailFile.close()
        
        htmlBody="<tr><th>Table Name</th><th>Inserts</th><th>Updates</th><th>Deletes</th><th>Merges</th><th>Stats</th></tr>"
        for dictObj in tableDictArr:
            htmlBody+="<tr><td>"+dictObj["tableName"]+"</td><td>"+str(dictObj["insert"])+"</td><td>"+str(dictObj["update"])+"</td><td>"+str(dictObj["delete"])+"</td><td>"+str(dictObj["merge"])+"</td><td>"+str(dictObj["collect"])+"</td></tr>\n"
            totalOfProc["insert"]+=dictObj["insert"]
            totalOfProc["update"]+=dictObj["update"]
            totalOfProc["delete"]+=dictObj["delete"]
            totalOfProc["merge"]+=dictObj["merge"]
            totalOfProc["collect"]+=dictObj["collect"]
        htmlBody+="<tr><th>Total</th><th>"+str(totalOfProc["insert"])+"</th><th>"+str(totalOfProc["update"])+"</th><th>"+str(totalOfProc["delete"])+"</th><th>"+str(totalOfProc["merge"])+"</th><th>"+str(totalOfProc["collect"])+"</th></tr>\n"
        bubbleData="["
        bubbleData+="{text: 'Inserts', count: "+str(masterJsonDict["inserts"])+"},"
        bubbleData+="{text: 'Deletes', count: "+str(masterJsonDict["deletes"])+"},"
        bubbleData+="{text: 'Updates', count: "+str(masterJsonDict["updates"])+"},"
        bubbleData+="{text: 'Merges', count: "+str(masterJsonDict["merges"])+"},"
        bubbleData+="{text: 'Tables', count: "+str(len(tableDictArr))+"},"
        bubbleData+="{text: 'Statements', count: "+str(masterJsonDict["no_of_stmts"])+"},"
        bubbleData+="{text: 'Comments', count: "+str(masterJsonDict["no_of_cmnts"])+"},"
        bubbleData+="{text: 'Lines', count: "+str(masterJsonDict["no_of_lines"])+"}"
        bubbleData+="]"
        
        htmlFile=open(self.userHome+"/CodeCompliance/html/TableCount_"+self.fileName+".html",'w')
        htmlFile.write(htmlHeader)
        htmlFile.write(bubbleData)
        htmlFile.write(htmlMiddle)
        htmlFile.write(htmlBody)
        htmlFile.write(htmlFooter)
        htmlFile.close()
        return htmlFile.name
    
    def tableWiseCount(self):
        jsonFileContent=open(self.userHome+"/CodeCompliance/temp/"+self.jsonFileName,'r');
        jsonObjArr=[]
        for line in jsonFileContent:
            jsonObjArr.append(json.loads(line))
        jsonFileContent.close()
        tableDictArr=[]
        for dictObj in jsonObjArr:
            tableDict={}
            if dictObj["subtype"]=="insert" or dictObj["subtype"]=="update" or dictObj["subtype"]=="delete" or dictObj["subtype"]=="collect" or dictObj["subtype"]=="merge":
                index=self.isTablePresent(dictObj["table_name"], tableDictArr)
                if index>=0:
                    if dictObj["subtype"]=="insert":
                        tableDictArr[index]["insert"]+=1
                    elif dictObj["subtype"]=="update":
                        tableDictArr[index]["update"]+=1
                    elif dictObj["subtype"]=="delete":
                        tableDictArr[index]["delete"]+=1
                    elif dictObj["subtype"]=="collect":
                        tableDictArr[index]["collect"]+=1
                    elif dictObj["subtype"]=="merge":
                        tableDictArr[index]["merge"]+=1
                else:
                    tableDict["tableName"]=dictObj["table_name"]
                    if dictObj["subtype"]=="insert":
                        tableDict["insert"]=1
                        tableDict["update"]=0
                        tableDict["delete"]=0
                        tableDict["collect"]=0
                        tableDict["merge"]=0
                    elif dictObj["subtype"]=="update":
                        tableDict["insert"]=0
                        tableDict["update"]=1
                        tableDict["delete"]=0
                        tableDict["collect"]=0
                        tableDict["merge"]=0
                    elif dictObj["subtype"]=="delete":
                        tableDict["insert"]=0
                        tableDict["update"]=0
                        tableDict["delete"]=1
                        tableDict["collect"]=0
                        tableDict["merge"]=0
                    elif dictObj["subtype"]=="collect":
                        tableDict["insert"]=0
                        tableDict["update"]=0
                        tableDict["delete"]=0
                        tableDict["collect"]=1
                        tableDict["merge"]=0
                    elif dictObj["subtype"]=="merge":
                        tableDict["insert"]=0
                        tableDict["update"]=0
                        tableDict["delete"]=0
                        tableDict["collect"]=0
                        tableDict["merge"]=1
                    tableDictArr.append(tableDict)
        
        self.writeResultToMasterJson(tableDictArr)
        htmlFileName=self.tableWiseCountHtml(tableDictArr)
        return "file://"+os.path.abspath(htmlFileName)
# if __name__=="__main__":
#     OperationCount("dq_base_7th_march.sql").tableWiseCount()
