import json
import os
import re
from ComplexityChart import ComplexityChart


class Complexity:
        
    @staticmethod
    def complexityScore(fileName):
        
        dictObj={}
        userHome=os.path.expanduser('~')
        jsonFileContent=open(userHome+"/CodeCompliance/temp/"+fileName+".json",'r')
        jsonObjArr=[]
        for line in jsonFileContent:
            jsonObjArr.append(json.loads(line))
        jsonFileContent.close()

        masterJsonFileContent=open(userHome+"/CodeCompliance/temp/"+fileName+".master.json",'r')
        masterJsonObjArr=[]
        for line in masterJsonFileContent:
            masterJsonObjArr.append(json.loads(line))
        masterJsonFileContent.close()

        finalActScore=0
        eachTableScore=5
        qualifyScore=2
        idealInsertScore=15
        idealUpdateScore=10
        idealDelScore=5
        idealMergeScore=10
        insCnt=0
        updCnt=0
        delCnt=0
        merCnt=0

        bonus=0

        for jsonObj in jsonObjArr:
            if jsonObj["subtype"]=="insert" :
                actInsertScore=((len(jsonObj["from_table_names"]))*eachTableScore)
                if re.search(r'(case\s*when|extract|coalesce|distinct|group|count)',jsonObj["statement"]):
                    actInsertScore=actInsertScore+len(re.findall(r'(case\s*when|extract|coalesce|distinct|group|count)',jsonObj["statement"]))
                if re.search(r'\s*(qualify|having|rollup|cube)\s*',jsonObj["statement"]):
                    actInsertScore=actInsertScore+(len(re.findall(r'\s*(qualify|having|rollup|cube)\s*',jsonObj["statement"]))*qualifyScore)
                if actInsertScore > idealInsertScore:
                    insCnt=insCnt+1
                finalActScore+=actInsertScore
    
            if jsonObj["subtype"]=="update" :
                actUpdateScore=((len(jsonObj["from_table_names"])+1)*eachTableScore)
                if re.search(r'(case\s*when|extract|coalesce|distinct|group|count)',jsonObj["statement"]):
                    actUpdateScore=actUpdateScore+len(re.findall(r'(case\s*when|extract|coalesce|distinct|group|count)',jsonObj["statement"]))
                if re.search(r'\s*(qualify|having|rollup|cube)\s*',jsonObj["statement"]):
                    actUpdateScore=actUpdateScore+(len(re.findall(r'\s*(qualify|having|rollup|cube)\s*',jsonObj["statement"]))*qualifyScore)
                if actUpdateScore > idealUpdateScore:
                    updCnt=updCnt+1
                finalActScore+=actUpdateScore
     
            if jsonObj["subtype"]=="delete" :
                actDelScore=((len(jsonObj["from_table_names"])+1)*eachTableScore)
                if re.search(r'(case\s*when|extract|coalesce|distinct|group|count|where)',jsonObj["statement"]):
                    actDelScore=actDelScore+len(re.findall(r'(case\s*when|extract|coalesce|distinct|group|count|where)',jsonObj["statement"]))
                if re.search(r'\s*(qualify|having|rollup|cube)\s*',jsonObj["statement"]):
                    actDelScore=actDelScore+(len(re.findall(r'\s*(qualify|having|rollup|cube)\s*',jsonObj["statement"]))*qualifyScore)
                if actDelScore > idealDelScore:
                    delCnt=delCnt+1
                finalActScore+=actDelScore   
    
            if jsonObj["subtype"]=="merge" :
                actMergeScore=((len(jsonObj["from_table_names"]))*eachTableScore)
                if re.search(r'(case\s*when|extract|coalesce|distinct|group|count)',jsonObj["statement"]):
                    actMergeScore=actMergeScore+len(re.findall(r'(case\s*when|extract|coalesce|distinct|group|count)',jsonObj["statement"]))
                if re.search(r'\s*(qualify|having|rollup|cube)\s*',jsonObj["statement"]):
                    actMergeScore=actMergeScore+(len(re.findall(r'\s*(qualify|having|rollup|cube)\s*',jsonObj["statement"]))*qualifyScore)
                if actMergeScore > idealMergeScore:
                    merCnt=merCnt+1
                finalActScore+=actMergeScore
        
        for jsonObj in masterJsonObjArr:
            if jsonObj["type"]=="master_data":
                noOfStmts=jsonObj["inserts"]+jsonObj["updates"]+jsonObj["deletes"]+jsonObj["merges"]
                idealScore=(jsonObj["inserts"]*idealInsertScore)+(jsonObj["updates"]*idealUpdateScore)+(jsonObj["deletes"]*idealDelScore)+(jsonObj["merges"]*idealMergeScore)

        for i in range(0,noOfStmts):
            bonus+=(0.01*i)
            
        finalActScore=finalActScore+bonus
        
        dictObj["type"]="score"
        dictObj["ideal_score"]=idealScore
        dictObj["actual_score"]=finalActScore
        dictObj["complex_stmts"]=insCnt+updCnt+delCnt+merCnt
        dictObj["complex_stmt_percent"]=((insCnt+delCnt+updCnt+merCnt)*100.0/noOfStmts)
        
        if finalActScore > (idealScore*2):
            dictObj["percent_of_ideal"]=200
        else :
            dictObj["percent_of_ideal"]=finalActScore*100.0/idealScore
        dictObj["complex_stmt_percent"]=((insCnt+delCnt+updCnt+merCnt)*100.0/noOfStmts)

        with open(userHome+"/CodeCompliance/temp/"+fileName+".master.json",mode='a') as feedsjson:
            feedsjson.write(json.dumps(dictObj)+"\n")
            
