import os
import json

class ApplicationHome:
    
    def __init__(self,files):
        self.userHome=os.path.expanduser('~')
        self.masterJsons=[]
        self.files=files
        self.buildMasterData()
        
        
    
    def buildMasterData(self):
        for file in self.files:
            masterJsonFileContent=open(self.userHome+"/CodeCompliance/temp/"+file+".master.json",'r')
            buildedObj={}
            for line in masterJsonFileContent:
                jsonObj=json.loads(line)
                if jsonObj["type"]=="master_data" :
                    procName=jsonObj["proc_name"]
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

    def buildApplication(self):
        htmlHeadFile=open(self.userHome+"/CodeCompliance/html/template/apphome_1.txt","r")
        htmlHeader=htmlHeadFile.read()
        htmlHeadFile.close()
        htmlTailFile=open(self.userHome+"/CodeCompliance/html/template/apphome_end.txt","r")
        htmlFooter=htmlTailFile.read()
        htmlTailFile.close()
        barChartData=self.getComplexityBarDataString()
        htmlFile=open(self.userHome+"/CodeCompliance/html/AppHome.html","w")
        htmlFile.write(htmlHeader)
        htmlFile.write(barChartData)
        htmlFile.write(htmlFooter)
        htmlFile.close()
        return htmlFile.name