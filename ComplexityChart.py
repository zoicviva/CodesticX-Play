import os
import json

class ComplexityChart:
    
    def csvFile(self,files):
        userHome=os.path.expanduser('~')
        data="Proc_Name,Ideal,Actual \n"
        for file in files:
            masterJsonFileContent=open(userHome+"/CodeCompliance/temp/"+file+".master.json",'r')
            procName=""
            idealScore=0
            actualScore=0
            for line in masterJsonFileContent:
                jsonObj={}
                jsonObj=json.loads(line)
                if jsonObj["type"]=="master_data" :
                    procName=jsonObj["proc_name"].split(".")
                if jsonObj["type"]=="score":
                    idealScore=jsonObj["ideal_score"]
                    actualScore=jsonObj["actual_score"]
            data+=procName[1]+","+str(idealScore)+","+str(actualScore)+"\n"
            masterJsonFileContent.close()
        csvFileObj=open(userHome+"/CodeCompliance/html/data.csv","w")
        csvFileObj.write(data)
        csvFileObj.close()
                
#         if not os.path.exists(userHome+"/CodeCompliance/html/data.csv"):
#             with open(userHome+"/CodeCompliance/html/data.csv",mode='w') as i:
#                 data='''Proc_Name,Ideal,Actual \n'''
#                 i.write(data)
#         
#         for jsonObj in masterJsonObjArr:
#             if jsonObj["type"]=="master_data" :
#                 procName=jsonObj["proc_name"].split(".")
#             if jsonObj["type"]=="score":
#                 idealScore=jsonObj["ideal_score"]
#                 actualScore=jsonObj["actual_score"]
#                 
#         with open(userHome+"/CodeCompliance/html/data.csv",mode='a') as i:       
#             i.write(procName[1]+","+str(idealScore)+","+str(actualScore)+"\n")
    