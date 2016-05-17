import os
import json

class ApplicationHome:
    
    def __init__(self):
        self.userHome=os.path.expanduser('~')
        
    
    def getComplexityBarDataString(self,files):
        data="data="
        dataArray=[]
        for file in files:
            masterJsonFileContent=open(self.userHome+"/CodeCompliance/temp/"+file+".master.json",'r')
            procName=""
            idealScore=0
            actualScore=0
            dataDictObj={}
            for line in masterJsonFileContent:
                jsonObj={}
                jsonObj=json.loads(line)
                if jsonObj["type"]=="master_data" :
                    procName=jsonObj["proc_name"].split(".")[1]
                if jsonObj["type"]=="score":
                    idealScore=jsonObj["ideal_score"]
                    actualScore=jsonObj["actual_score"]
            dataDictObj["Proc_Name"]=str(procName)
            dataDictObj["Ideal"]=idealScore
            dataDictObj["Actual"]=actualScore
            dataArray.append(dataDictObj)
            masterJsonFileContent.close()
        
        data+=str(dataArray)
        return data

    def buildApplication(self,files):
        htmlHeadFile=open(self.userHome+"/CodeCompliance/html/template/apphome_1.txt","r")
        htmlHeader=htmlHeadFile.read()
        htmlHeadFile.close()
        htmlTailFile=open(self.userHome+"/CodeCompliance/html/template/apphome_end.txt","r")
        htmlFooter=htmlTailFile.read()
        htmlTailFile.close()
        barChartData=self.getComplexityBarDataString(files)
        htmlFile=open(self.userHome+"/CodeCompliance/html/AppHome.html","w")
        htmlFile.write(htmlHeader)
        htmlFile.write(barChartData)
        htmlFile.write(htmlFooter)
        htmlFile.close()
        return htmlFile.name