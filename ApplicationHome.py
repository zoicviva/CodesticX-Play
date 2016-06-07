import os
import json
import collections

class ApplicationHome:
    
    def __init__(self,files):
        self.userHome=os.path.expanduser('~')
        self.masterJsons=[]
        self.files=files
        self.buildMasterData()
        
    def getTablesFreq(self):
        tables=[]
        for fileName in self.files:
            jsonFileContent=open(self.userHome+"/CodeCompliance/temp/"+fileName+".json",'r')
            jsonObjArr=[]
            for line in jsonFileContent:
                jsonObjArr.append(json.loads(line))
            jsonFileContent.close()            
            for jsonObj in jsonObjArr:
                if jsonObj["subtype"]=="insert":
                    tables.append(jsonObj["table_name"])
                    tables+=jsonObj["from_table_names"]
                if jsonObj["subtype"]=="update":
                    tables.append(jsonObj["table_name"])
                    tables+=jsonObj["from_table_names"]
                if jsonObj["subtype"]=="delete":
                    tables.append(jsonObj["table_name"])
                    tables+=jsonObj["from_table_names"]
                if jsonObj["subtype"]=="merge":
                    tables.append(jsonObj["table_name"])
                    tables+=jsonObj["from_table_names"]  
        counter=collections.Counter(tables)
        lst=counter.most_common()
        newList=[]
        for (name,value) in lst:
            dictObj={}
            dictObj["name"]=name
            dictObj["value"]=value
            newList.append(dictObj)
        return newList            
        
        
    def buildMasterData(self):
        for file in self.files:
            masterJsonFileContent=open(self.userHome+"/CodeCompliance/temp/"+file+".master.json",'r')
            buildedObj={}
            for line in masterJsonFileContent:
                jsonObj=json.loads(line)
                if jsonObj["type"]=="master_data" :
                    procName=jsonObj["proc_name"]
                    if procName=="":
                        print file
                    buildedObj["name"]=procName
                    buildedObj["master_data"]=jsonObj
                if jsonObj["type"]=="score":
                    buildedObj["score"]=jsonObj
                if jsonObj["type"]=="operation_count":
                    buildedObj["operation_count"]=jsonObj
            self.masterJsons.append(buildedObj)
            
    
    def getComplexityBarDataString(self):
        data="data="
        dataArray=[]
        for jsonObj in self.masterJsons:
            procName=""
            idealScore=0
            actualScore=0
            dataDictObj={}
            procName=jsonObj["name"].split(".")[1]
            idealScore=jsonObj["score"]["ideal_score"]
            actualScore=jsonObj["score"]["actual_score"]
            dataDictObj["Proc_Name"]=str(procName)
            dataDictObj["Ideal"]=idealScore
            dataDictObj["Actual"]=actualScore
            dataArray.append(dataDictObj)
        
        data+=str(dataArray)
        return data
    
    def getBubbleChartString(self):
        bubbleData="["
        insertCount=0
        deleteCount=0
        updateCount=0
        mergeCount=0
        tableCount=0
        procedureCount=len(self.masterJsons)
        for masterJson in self.masterJsons:
            insertCount+=masterJson["master_data"]["inserts"]
            deleteCount+=masterJson["master_data"]["deletes"]
            updateCount+=masterJson["master_data"]["updates"]
            mergeCount+=masterJson["master_data"]["merges"]
            tableCount+=len(masterJson["operation_count"]["value"])
        bubbleData+="{text: 'Inserts', count: "+str(insertCount)+"},"
        bubbleData+="{text: 'Deletes', count: "+str(deleteCount)+"},"
        bubbleData+="{text: 'Updates', count: "+str(updateCount)+"},"
        bubbleData+="{text: 'Merges', count: "+str(mergeCount)+"},"
        bubbleData+="{text: 'Tables', count: "+str(tableCount)+"},"
        bubbleData+="{text: 'Procedures', count: "+str(procedureCount)+"},"
        bubbleData+="]"
        return bubbleData
    
    def buildApplication(self):
        
        htmlHeadFile=open(self.userHome+"/CodeCompliance/html/template/apphome_1.txt","r")
        htmlHeader=htmlHeadFile.read()
        htmlHeadFile.close()
        htmlContent1File=open(self.userHome+"/CodeCompliance/html/template/apphome_2.txt","r")
        htmlContent1=htmlContent1File.read()
        htmlContent1File.close()
        htmlContent2File=open(self.userHome+"/CodeCompliance/html/template/apphome_3.txt","r")
        htmlContent2=htmlContent2File.read()
        htmlContent2File.close()
        htmlTailFile=open(self.userHome+"/CodeCompliance/html/template/apphome_end.txt","r")
        htmlFooter=htmlTailFile.read()
        htmlTailFile.close()
        barChartData=self.getComplexityBarDataString()
        htmlFile=open(self.userHome+"/CodeCompliance/html/AppHome.html","w")
        htmlFile.write(htmlHeader)
        htmlFile.write(self.getBubbleChartString())
        htmlFile.write(htmlContent1)
        htmlFile.write(htmlContent2)
        htmlFile.write(barChartData)
        htmlFile.write(htmlFooter)
        htmlFile.close()
        return htmlFile.name