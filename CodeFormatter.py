import sqlparse
import ntpath
import re
from UtilitiesCC import UtilitiesCC

class CodeFormatter:
    def __init__(self,filePath):
        self.filePath=filePath
        self.fileName=ntpath.basename(filePath)

    
    def formatIt(self):
        fileObj=open(self.filePath,'r')
        fileContent=fileObj.read()
        fileObj.close()
        lvlOneFormattedContent1=sqlparse.format(fileContent,reindent=True,keyword_case='upper',identfier_case='lower')
        lvlOneFormattedContent=lvlOneFormattedContent1.replace('DELETE\nFROM','DELETE FROM')
        UtilitiesCC.writeTextToFile("formatted/temp","l1_"+self.fileName,lvlOneFormattedContent)
        lvlTwoContent=open("formatted/temp/l1_"+self.fileName,"r")
        lvlTwoFile=open("formatted/temp/l2_"+self.fileName,'w')
        for line in lvlTwoContent:
            if re.search('SQL SECURITY INVOKER', line):
                new_line=re.sub('SQL SECURITY INVOKER[ ]*', '\nSQL SECURITY INVOKER\n', line)
                lvlTwoFile.write(new_line)
            elif re.search('BEGIN',line):
                new_line=re.sub('BEGIN[ ]*', '\nBEGIN\n', line)
                lvlTwoFile.write(new_line)
            elif re.search('\*/[ ]*[a-zA-Z]',line):
                new_line=re.sub('\*/[ ]*', '*/\n', line)
                lvlTwoFile.write(new_line)
            elif re.search('--.*',line):
                stmt=re.search('--.*',line).group()
                stmt1=re.sub(',',' ',stmt)
                new_line=line.replace(stmt,stmt1)
                lvlTwoFile.write(new_line)
            else:
                lvlTwoFile.write(line)
        lvlTwoContent.close()
        lvlTwoFile.close()
        lvlThreeContent = open("formatted/temp/l2_"+self.fileName,"r")
        lvlThreeFile=open("formatted/temp/l3_"+self.fileName,'w')
        stmt=''
        insert_seen=0
        iq_select_seen=0
        for line in lvlThreeContent:
            words = line.strip().split()
            if len(words) > 0 :
                first_word=words[0]
                if (first_word.lower() == 'insert' or insert_seen==1):
                    insert_seen=1
                    stmt+=line
                    last_word=line.strip().split()[-1].lower()
                    if re.search('\)[ ]*$',last_word):
                        insert_seen=0
                        if stmt != '' :
                            line_stmt=re.sub(r"\s+"," ",stmt)
                            newline1=re.sub('\(','\n(\n\t',line_stmt)
                            newline2=re.sub(',[ ]*','\n\t,',newline1)
                            newline=re.sub('\)[ ]*$','\n)\n',newline2)
                            lvlThreeFile.write(newline)
                        stmt=''
                elif (re.search('\( SELECT',line.strip()) or iq_select_seen==1):
                    iq_select_seen=1
                    stmt+=line
                    last_word=line.strip().split()[-1].lower()
                    if re.search('\w+$',last_word):
                        iq_select_seen=0
                        if stmt != '' :
                            line_stmt=re.sub(r"\s+"," ",stmt)
                            newline=re.sub(',[ ]*','\n          ,',line_stmt)
                            lvlThreeFile.write(newline)
                        stmt=''
                else:
                    lvlThreeFile.write(line)
            else:
                lvlThreeFile.write(line)              
        lvlThreeContent.close()
        lvlThreeFile.close()
        finalContent = open("formatted/temp/l3_"+self.fileName, "r")
        finalFile=open("formatted/"+self.fileName,"w")
        tabCount=0
        for line in finalContent:
            words = line.strip().split()
            line_to_write='\t'*tabCount+line
            if len(words) > 0 :
                first_word=words[0]
                if (first_word.lower() == 'begin' or first_word.lower() == 'if'):
                    tabCount+=1
                elif(re.search('end\s*(if){0,1}\s*;\s*',line.lower())):
                    tabCount-=1
                    line_to_write='\t'*tabCount+line
            finalFile.write(line_to_write)
        finalContent.close()
        finalFile.close()
        return "formatted/"+self.fileName
        
if __name__=="__main__":
    obj=CodeFormatter("/Users/tata.swaroop/Desktop/Desktop/DQ/TAG_CR_26582541_DQ/compile/spl/sp_load_em_carrier_dq_cmptn.sql")
    result=obj.formatIt()
    print "Formatting Done File present at :"+result
    print "done"
    