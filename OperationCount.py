import json
import os
class OperationCount:
    def __init__(self,fileName):
        self.jsonFileName=fileName+".json"
        self.fileName=fileName
        
    def isTablePresent(self,tableName,tableDictArr):
        index=-1
        i=0
        for i in range(0,len(tableDictArr)):
            if tableDictArr[i]["tableName"]==tableName:
                index=i
                break
        return index
    def tableWiseCountHtml(self,tableDictArr):
        totalOfProc={'insert':0,'update':0,'delete':0,'merge':0,'collect':0}
        htmlHeader='''<html><head><style>
        body{background:#f1f1f1}
        #center{margin:auto;width:50%;}
        table { color: #333;font-family: Helvetica, Arial, sans-serif;width: 640px; border: 1px solid black; border-radius: 10px;
        box-shadow: 0px 0px 5px #888888; }
        td, th { border: 1px solid transparent;height: 30px; transition: all 0.3s;}
        th {background: #DFDFDF;font-weight: bold;}
        td {background: #FAFAFA;text-align: center;}
        tr:nth-child(even) td { background: #F1F1F1; }   
        tr:nth-child(odd) td { background: #FEFEFE; }  
        tr td:hover { background: #666; color: #FFF; }
        </style></head><body><div id="center"><table>'''
        htmlFooter="</table><div></body></html>"
        htmlBody="<tr><th>Table Name</th><th>Inserts</th><th>Updates</th><th>Deletes</th><th>Merges</th><th>Stats</th></tr>"
        for dictObj in tableDictArr:
            htmlBody+="<tr><td>"+dictObj["tableName"]+"</td><td>"+str(dictObj["insert"])+"</td><td>"+str(dictObj["update"])+"</td><td>"+str(dictObj["delete"])+"</td><td>"+str(dictObj["merge"])+"</td><td>"+str(dictObj["collect"])+"</td></tr>\n"
            totalOfProc["insert"]+=dictObj["insert"]
            totalOfProc["update"]+=dictObj["update"]
            totalOfProc["delete"]+=dictObj["delete"]
            totalOfProc["merge"]+=dictObj["merge"]
            totalOfProc["collect"]+=dictObj["collect"]
        htmlBody+="<tr><th>Total</th><th>"+str(totalOfProc["insert"])+"</th><th>"+str(totalOfProc["update"])+"</th><th>"+str(totalOfProc["delete"])+"</th><th>"+str(totalOfProc["merge"])+"</th><th>"+str(totalOfProc["collect"])+"</th></tr>\n"
        htmlFile=open("html/TableCount_"+self.fileName+".html",'w')
        htmlFile.write(htmlHeader)
        htmlFile.write(htmlBody)
        htmlFile.write(htmlFooter)
        htmlFile.close()
        return htmlFile.name
    
    def tableWiseCount(self):
        jsonFileContent=open("temp/"+self.jsonFileName,'r');
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
        
#         for tables in tableDictArr:
#             print tables
        htmlFileName=self.tableWiseCountHtml(tableDictArr)
        print "open this in web browser : file://"+os.path.abspath(htmlFileName)
        return "success"
# if __name__=="__main__":
#     OperationCount("dq_base_7th_march.sql").tableWiseCount()
