import re
import json

class Complexity:
    
    def __init__(self):
        self.complxStmts=0
        self.stmtCnt=0
        self.insCnt=0
        self.updCnt=0
        self.delCnt=0
        self.MerCnt=0
        self.finalInsertScore=0
        self.finalUpdateScore=0
        self.finalDelScore=0
        self.finalMergeScore=0
        self.eachTablePoint=5
        self.qualifyPoint=2
        self.idealInsertScore=15
        self.idealUpdateScore=10
        self.idealDeleteScore=5
        self.idealMergeScore=10
        
    def getInsertScore(self,dictObj):
        actInsertScore=((len(dictObj["from_table_names"]))*self.eachTablePoint)
        if re.search(r'(case\s*when|extract|coalesce|distinct|group|count)',dictObj["statement"]):
            actInsertScore=actInsertScore+len(re.findall(r'(case\s*when|extract|coalesce|distinct|group|count)',dictObj["statement"]))
        if re.search(r'\s*(qualify|having|rollup|cube)\s*',dictObj["statement"]):
            actInsertScore=actInsertScore+(len(re.findall(r'\s*(qualify|having|rollup|cube)\s*',dictObj["statement"]))*self.qualifyPoint)
        if actInsertScore > self.idealInsertScore:
            self.complxStmts=self.complxStmts+1
        self.stmtCnt+=1
        self.insCnt+=1
        self.finalInsertScore+=actInsertScore
        return actInsertScore
        
    def getUpdateScore(self,dictObj):
        actUpdateScore=((len(dictObj["from_table_names"])+1)*self.eachTablePoint)
        if re.search(r'(case\s*when|extract|coalesce|distinct|group|count)',dictObj["statement"]):
            actUpdateScore=actUpdateScore+len(re.findall(r'(case\s*when|extract|coalesce|distinct|group|count)',dictObj["statement"]))
        if re.search(r'\s*(qualify|having|rollup|cube)\s*',dictObj["statement"]):
            actUpdateScore=actUpdateScore+(len(re.findall(r'\s*(qualify|having|rollup|cube)\s*',dictObj["statement"]))*self.qualifyPoint)
        if actUpdateScore > self.idealUpdateScore:
            self.complxStmts=self.complxStmts+1
        self.stmtCnt+=1
        self.updCnt=self.updCnt+1
        self.finalUpdateScore+=actUpdateScore
        return actUpdateScore
        
    def getDeleteScore(self,dictObj):
        actDelScore=((len(dictObj["from_table_names"])+1)*self.eachTablePoint)
        if re.search(r'(case\s*when|extract|coalesce|distinct|group|count|where)',dictObj["statement"]):
            actDelScore=actDelScore+len(re.findall(r'(case\s*when|extract|coalesce|distinct|group|count|where)',dictObj["statement"]))
        if re.search(r'\s*(qualify|having|rollup|cube)\s*',dictObj["statement"]):
            actDelScore=actDelScore+(len(re.findall(r'\s*(qualify|having|rollup|cube)\s*',self.stmt))*self.qualifyPoint)
        if actDelScore > self.idealDeleteScore:
            self.complxStmts=self.complxStmts+1
        self.stmtCnt+=1
        self.delCnt=self.delCnt+1
        self.finalDelScore+=actDelScore
        return actDelScore
            
    def getMergeScore(self,dictObj):
        actMergeScore=((len(dictObj["from_table_names"]))*self.eachTablePoint)
        if re.search(r'(case\s*when|extract|coalesce|distinct|group|count)',dictObj["statement"]):
            actMergeScore=actMergeScore+len(re.findall(r'(case\s*when|extract|coalesce|distinct|group|count)',dictObj["statement"]))
        if re.search(r'\s*(qualify|having|rollup|cube)\s*',dictObj["statement"]):
            actMergeScore=actMergeScore+(len(re.findall(r'\s*(qualify|having|rollup|cube)\s*',dictObj["statement"]))*self.qualifyPoint)
        if actMergeScore > self.idealMergeScore:
            self.complxStmts=self.complxStmts+1
        self.stmtCnt+=1
        self.MerCnt+=1
        self.finalMergeScore+=actMergeScore
        return actMergeScore
        
    def getFinalScore(self):
        dictObj={}
        bonus=0
        for i in range(0,self.stmtCnt):
            bonus+=(0.01*i)
        idealScore=(self.insCnt*15)+(self.updCnt*10)+(self.delCnt*5)+(self.MerCnt*10)
        dictObj["ideal_score"]=idealScore
        actualScore=self.finalInsertScore+self.finalUpdateScore+self.finalDelScore+self.finalMergeScore+bonus
        dictObj["actual_score"]=actualScore
        dictObj["complex_stmts"]=self.complxStmts
        if actualScore > (idealScore*2):
            dictObj["percent_of_ideal"]=200
        elif idealScore == 0:
            dictObj["percent_of_ideal"]=0
        else:
            dictObj["percent_of_ideal"]=actualScore*100.0/idealScore
        if self.stmtCnt == 0:
            dictObj["complex_stmt_percent"]=0
        else:
            dictObj["complex_stmt_percent"]=((self.complxStmts*100.0)/self.stmtCnt)    
        return dictObj    