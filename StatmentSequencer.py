import re
from UtilitiesCC import UtilitiesCC

class StatementSequencer:
    def __init__(self,fileName):
        self.fileName=fileName
        self.workFilePath="work/"+fileName
        
    def sequenceIt(self):
        file_content = open(self.workFilePath, "r")
        insert_seen=0
        select_seen=0
        update_seen=0
        delete_seen=0
        merge_seen=0
        call_seen=0
        declare_seen=0
        set_seen=0
        collect_seen=0
        stmts=[]
        stmt=''
        for line in file_content:
            words = line.strip().split()
            if len(words) > 0 :
                first_word=words[0]
                if (first_word.lower() == 'insert' or insert_seen==1) and select_seen==0 and update_seen==0 and delete_seen==0 and merge_seen==0 and call_seen==0 and declare_seen==0 and set_seen==0 and collect_seen==0 : 
                    insert_seen=1
                    stmt+=line
                    last_word=line.strip().split()[-1].lower()
                    if re.search(';$',last_word):
                        insert_seen=0
                        if stmt != '' :
                            stmts.append(re.sub(r"\s+", " ",stmt).strip())
                        stmt=''
                if (first_word.lower() == 'select' or select_seen==1) and insert_seen==0 and update_seen==0 and delete_seen==0 and merge_seen==0 and call_seen==0 and declare_seen==0 and set_seen==0 and collect_seen==0: 
                    select_seen=1
                    stmt+=line
                    last_word=line.strip().split()[-1].lower()
                    if re.search(';$',last_word):
                        select_seen=0
                        if stmt != '' :
                            stmts.append(re.sub(r"\s+", " ",stmt).strip())
                        stmt=''
                if (first_word.lower() == 'update' or update_seen==1) and insert_seen==0 and select_seen==0 and delete_seen==0 and merge_seen==0 and call_seen==0 and declare_seen==0 and set_seen==0 and collect_seen==0 : 
                    update_seen=1
                    stmt+=line
                    last_word=line.strip().split()[-1].lower()
                    if re.search(';$',last_word):
                        update_seen=0
                        if stmt != '' :
                            stmts.append(re.sub(r"\s+", " ",stmt).strip())
                        stmt=''
                if (first_word.lower() == 'delete' or delete_seen==1) and insert_seen==0 and select_seen==0 and update_seen==0 and merge_seen==0 and call_seen==0 and declare_seen==0 and set_seen==0 and collect_seen==0: 
                    delete_seen=1
                    stmt+=line
                    last_word=line.strip().split()[-1].lower()
                    if re.search(';$',last_word):
                        delete_seen=0
                        if stmt != '' :
                            stmts.append(re.sub(r"\s+", " ",stmt).strip())
                        stmt=''
                if (first_word.lower() == 'merge' or merge_seen==1) and insert_seen==0 and select_seen==0 and update_seen==0 and delete_seen==0 and call_seen==0 and declare_seen==0 and set_seen==0 and collect_seen==0: 
                    merge_seen=1
                    stmt+=line
                    last_word=line.strip().split()[-1].lower()
                    if re.search(';$',last_word):
                        merge_seen=0
                        if stmt != '' :
                            stmts.append(re.sub(r"\s+", " ",stmt).strip())
                        stmt=''
                if (first_word.lower() == 'call' or call_seen==1) and insert_seen==0 and select_seen==0 and update_seen==0 and delete_seen==0 and merge_seen==0 and declare_seen==0 and set_seen==0 and collect_seen==0 : 
                    call_seen=1
                    stmt+=line
                    last_word=line.strip().split()[-1].lower()
                    if re.search(';$',last_word):
                        call_seen=0
                        if stmt != '' :
                            stmts.append(re.sub(r"\s+", " ",stmt).strip())
                        stmt=''
                if (first_word.lower() == 'set' or set_seen==1) and insert_seen==0 and select_seen==0 and update_seen==0 and delete_seen==0 and merge_seen==0 and call_seen==0 and declare_seen==0 and collect_seen==0 : 
                    set_seen=1
                    stmt+=line
                    last_word=line.strip().split()[-1].lower()
                    if re.search(';$',last_word):
                        set_seen=0
                        if stmt != '' :
                            stmts.append(re.sub(r"\s+", " ",stmt).strip())
                        stmt=''
                if (first_word.lower() == 'declare' or declare_seen==1) and insert_seen==0 and select_seen==0 and update_seen==0 and delete_seen==0 and merge_seen==0 and call_seen==0 and set_seen==0 and collect_seen==0: 
                    declare_seen=1
                    stmt+=line
                    last_word=line.strip().split()[-1].lower()
                    if re.search(';$',last_word):
                        declare_seen=0
                        if stmt != '' :
                            stmts.append(re.sub(r"\s+", " ",stmt).strip())
                        stmt=''
                if (first_word.lower() == 'collect' or collect_seen==1) and insert_seen==0 and select_seen==0 and update_seen==0 and delete_seen==0 and merge_seen==0 and call_seen==0 and set_seen==0 and declare_seen==0 : 
                    collect_seen=1
                    stmt+=line
                    last_word=line.strip().split()[-1].lower()
                    if re.search(';$',last_word):
                        collect_seen=0
                        if stmt != '' :
                            stmts.append(re.sub(r"\s+", " ",stmt).strip())
                        stmt=''
        filePath="temp/"+self.fileName
        file_content.close()
        UtilitiesCC.writeToFile(filePath, stmts)
        return filePath;
