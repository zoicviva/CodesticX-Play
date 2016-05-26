# from DirSetup import DirSetup
from ApplicationContainer import ApplicationContainer
from ApplicationHome import ApplicationHome
from ComplexityChart import ComplexityChart
from StatmentSequencer import StatementSequencer
from UtilitiesCC import UtilitiesCC
from CommentHandler import CommentHandler
from Analyser import Analyser
from OperationCount import OperationCount
from TableFlow import TableFlow
from Tkinter import *
import Tkinter
import tkMessageBox
from tkFileDialog import askopenfilename,askdirectory
import webbrowser
import logging,logging.handlers
from os import walk
import os
import time
import re

top = Tkinter.Tk()
# checkVar1 = IntVar()
# checkVar2 = IntVar()
file1=""
file2=""
userHome=os.path.expanduser('~')
resultBoxGlob = top
FORMAT = "%(levelname)s %(asctime)-15s %(module)s %(message)s"

def openHtmlInBrowser():
    webbrowser.open_new(file1)
    webbrowser.open_new_tab(file2)
    resultBoxGlob.destroy()
    return "Success"

def openNavigateHtml():
    webbrowser.open_new("file://"+userHome+"/CodeCompliance/html/index.html")
    resultBoxGlob.destroy()
    return "Success"

# def startButtonCallBack():
#     
#     if checkVar1.get()==0 and checkVar2.get()==0 :
#         tkMessageBox.showinfo( "Code Compliance", "Atleast select one check box")
#     else :
#         filePannel=Tk() # we don't want a full GUI, so keep the root window from appearing
#         filePannel.withdraw()
#         fileNameSelected=askopenfilename() # show an "Open" dialog box and return the path to the selected file
#         filePannel.destroy()
#         
#         fileName=UtilitiesCC.copyFileToOrig(fileNameSelected)
#         logging.info(fileName+" copied to orig folder")
#         fileContent=CommentHandler(fileName).removeComments()
#         UtilitiesCC.writeTextToFile(userHome+"/CodeCompliance/work", fileName, fileContent)
#         fileObj=StatementSequencer(fileName)
#         sequencedfilePath=fileObj.sequenceIt()
#         analyse=Analyser(fileName).startAnalysing(fileName)
#         msg=""
#         if checkVar1.get()==1:
#             global file1
#             file1=OperationCount(fileName).tableWiseCount()
#             msg+="Statement Counts : "+file1+"\n"
#         else:
#             file1=""
#             print "not calling table wise count"
#         if checkVar2.get()==1:
#             global file2
#             file2=TableFlow(fileName).tableFlowGenerator()
#             msg+="Table flow : "+file2+"\n"
#         else:
#             file2=""
#             print "not calling table flow"
#         
#         resultBox=Tkinter.Tk()
#         global resultBoxGlob
#         resultBoxGlob=resultBox
#         resultBox.wm_title("Code Compliance - Success!")
#         w = 590 # width for the Tk root
#         h = 220 # height for the Tk root
#         ws = top.winfo_screenwidth() # width of the screen
#         hs = top.winfo_screenheight() # height of the screen
#         x = (ws/2) - (w/2)
#         y = (hs/2) - (h/2)
#         resultBox.geometry('%dx%d+%d+%d' % (w, h, x, y))
#         resultBox.lift()
#         text=Label(resultBox,bd=0,padx=50,pady=50,text=msg,wraplength=590 )
#         text.pack()
#         openButton = Tkinter.Button(resultBox, text ="Open", command = openHtmlInBrowser)
#         openButton.pack()
    
def startFolderButtonCallBack():
#     filePannel=Tk() # we don't want a full GUI, so keep the root window from appearing
#     filePannel.withdraw()
    top.attributes('-topmost',False)
    folderSelected=askdirectory() # show an "Open" dialog box and return the path to the selected file
#     filePannel.destroy()
    allFiles = []
    for (dirpath, dirnames, filenames) in walk(folderSelected):
        allFiles.extend(filenames)
        break
    files=[]
    for file in allFiles:
        if not re.search(r"^\.", file):
            files.append(file)
    
    
    currentFile=1
    totalFiles=len(files)
    for file in files:
        fileNameSelected=folderSelected+"/"+file
        fileName=UtilitiesCC.copyFileToOrig(fileNameSelected)
        logging.info(fileName+" copied to orig folder")
        fileContent=CommentHandler(fileName).removeComments()
        UtilitiesCC.writeTextToFile(userHome+"/CodeCompliance/work", fileName, fileContent)
        fileObj=StatementSequencer(fileName)
        sequencedfilePath=fileObj.sequenceIt()
        analyse=Analyser(fileName).startAnalysing(fileName)
        OperationCount(fileName).tableWiseCount()
        TableFlow(fileName).tableFlowGenerator()
        currentFile+=1        
    ApplicationHome(files).buildApplication()
    ApplicationContainer().buildContainer(files)
    msg="Navigation Page :"+userHome+"/CodeCompliance/html/index.html"
    
    resultBox=Tkinter.Tk()
    global resultBoxGlob
    resultBoxGlob=resultBox
    resultBox.wm_title("Code Compliance - Success!")
    resultBox.resizable(width=FALSE, height=FALSE)
    w = 590 # width for the Tk root
    h = 220 # height for the Tk root
    ws = top.winfo_screenwidth() # width of the screen
    hs = top.winfo_screenheight() # height of the screen
    x = (ws/2) - (w/2)
    y = (hs/2) - (h/2)
    resultBox.geometry('%dx%d+%d+%d' % (w, h, x, y))
    resultBox.lift()
    
    text=Label(resultBox,bd=0,padx=50,pady=50,text=msg,wraplength=590 )
    text.pack()
    openButton = Tkinter.Button(resultBox, text ="Open", command = openNavigateHtml)
    openButton.pack()
    top.destroy()
 
if __name__=="__main__" :
#     DirSetup.setup()
    logging.basicConfig(filename=userHome+"/CodeCompliance/logs/codecompliance"+time.strftime("_%d%m%Y_%H%M%S")+".log" , level="INFO",filemode="w",datefmt='%Y-%m-%d %H:%M:%S',format=FORMAT)
    top.wm_title("Code Compliance")
    top.update_idletasks()
    top.resizable(width=FALSE, height=FALSE)
    w = 390 # width for the Tk root
    h = 150 # height for the Tk root
    ws = top.winfo_screenwidth() # width of the screen
    hs = top.winfo_screenheight() # height of the screen
    x = (ws/2) - (w/2)
    y = (hs/2) - (h/2)
    top.geometry('%dx%d+%d+%d' % (w, h, x, y))
    top.lift()
    top.attributes('-topmost',True)
    text=Label(top,bd=0,bg="gray",height=1,width=40,padx=50,pady=50,text="Hello, welcome to Code Compliance Tool.")
    text.pack()
    
#     fileButton = Tkinter.Button(top, text ="Select File", command = startButtonCallBack)
#     fileButton.pack({"side": "right"})
    folderButton = Tkinter.Button(top, text ="Select Folder", command = startFolderButtonCallBack)
    folderButton.pack()
#     quitBttn = Tkinter.Button(top, text ="Quit", command = quitMainProgram)
#     quitBttn.pack({"side": "right"})
#     c1 = Checkbutton(top, text = "Statement Count", variable = checkVar1,onvalue = 1, offvalue = 0)
#     c2 = Checkbutton(top, text = "Table Flow", variable = checkVar2,onvalue = 1, offvalue = 0)
#     c1.pack()
#     c2.pack()
    top.mainloop()
    
