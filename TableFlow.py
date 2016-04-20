import json
import os

class TableFlow:
    def __init__(self,fileName):
        self.jsonFileName=fileName+".json"
        self.fileName=fileName      
    
    def tableFlowHTML(self,dictObjArr):
        htmlHeadFile=open("html/template/TableFlowHead.txt","r")
        htmlHead=htmlHeadFile.read()
        htmlHeadFile.close()
        htmlTailFile=open("html/template/TableFlowTail.txt","r")
        htmlTail=htmlTailFile.read()
        htmlTailFile.close()
        htmlBody=htmlHead
        allTableDup=[]
            
        for dictObj in dictObjArr:
            centerTable=dictObj["center"]
            leftTables=dictObj["left"]
            rightTables=dictObj["right"]
            allTableDup.append(centerTable)
            allTableDup+=leftTables
            allTableDup+=rightTables
            line="<div class='statment'>"
            if len(leftTables) > 0:
                line+="<div class='left talk-bubble tri-right right-top'>"
            else :
                line+="<div class='left'>"
            for ltable in leftTables:
                line+="<div class='left_child'><span class='"+ltable+"'>"+ltable+"</span></div>"
            line+="</div><div class='center'><div class='center_child'><span class='"+centerTable+"'>"+centerTable+"</span></div></div>"
            if len(rightTables) > 0: 
                line+="<div class='right talk-bubble tri-right left-top'>"
            else:
                line+="<div class='right'>"
            for rtable in rightTables:
                line+="<div class='right_child'><span class='"+rtable+"'>"+rtable+"</span></div>"
            line+="</div><div class='clr'></div></div>"
            htmlBody+=line+"\n"
        allTables=list(set(allTableDup))
        scriptLine="<script> var classes = [" 
        tableCount=0
        for table in allTables:
            scriptLine+="'"+table+"'"
            tableCount+=1
            if(tableCount<len(allTables)):
                scriptLine+=","
        scriptLine+="];</script>"
        htmlBody+=scriptLine
        htmlBody+=htmlTail
        htmlFile=open("html/TableFlow_"+self.fileName+".html",'w')
        htmlFile.write(htmlBody)
        htmlFile.close()
        return htmlFile.name
        
    def tableFlowGenerator(self):
        jsonFileContent=open("temp/"+self.jsonFileName,'r');
        jsonObjArr=[]
        for line in jsonFileContent:
            jsonObjArr.append(json.loads(line))
        jsonFileContent.close()
        dictObjArr=[]
        isNoRightTable=True
        for jsonObj in jsonObjArr:
            if(jsonObj["subtype"]=="insert"):
                dictObj={}
                dictObj["center"]=jsonObj["table_name"]
                fromTablesWithAlias=jsonObj["from_table_names"]
                fromTables=[]
                for fromTableObj in fromTablesWithAlias:
                    fromTables.append(fromTableObj["table_name"])
                fromTables=sorted(fromTables, key=len)
                leftTables=[]
                rightTables=[]
                index=0
                while index<len(fromTables):
                    leftTables.append(fromTables[index])
                    if index+1 < len(fromTables):
                        rightTables.append(fromTables[index+1])
                    index+=2
                if(len(leftTables)+len(rightTables)>1):
                    dictObj["left"]=leftTables
                    dictObj["right"]=rightTables 
                else:
                    if(isNoRightTable):
                        dictObj["left"]=leftTables
                        dictObj["right"]=rightTables
                        isNoRightTable=False 
                    else:
                        dictObj["left"]=rightTables
                        dictObj["right"]=leftTables
                        isNoRightTable=True
                dictObjArr.append(dictObj)
        htmlFileName=self.tableFlowHTML(dictObjArr)
        print "open this in web browser : file://"+os.path.abspath(htmlFileName)
        return "file://"+os.path.abspath(htmlFileName)

# if __name__=="__main__":
#     TableFlow("sp_load_em_carrier_dq_raw_file.sql").tableFlowGenerator()
