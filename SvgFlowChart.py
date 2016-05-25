# -*- coding: utf-8 -*-

import os

class SvgFlowChart:

    def __init__(self,fileName):
        self.userHome=os.path.expanduser('~')
        self.fileName=fileName
        self.leftRectX=0
        self.leftChildLineX=300
        self.leftVtLineX=350
        self.leftCenterLineX=350
        self.leftArrowX=390
        self.centerRectX=400
        self.rightArrowX=710
        self.rightCenterLineX=700
        self.rightVtLineX=750
        self.rightChildLineX=750
        self.rightRectX=800
        self.hzLineWidth=50
        self.rectWidth=300
        self.rectHeight=50
        self.rectYmargin=25
        self.displaceY=0
        self.prevCenterY=0
        self.completeCenter=550
    
    def drawChart(self,jsonObj):
        left=jsonObj["left"]
        center=jsonObj["center"]
        right=jsonObj["right"]
        leftString=""     
        centerString=""
        leftIndex=1
        leftArrLenght=len(left)
        prevLeftRectY=0
        prevLeftChildLineY=(self.rectHeight/2)
        firstLeftChildLineY=0
        lastLeftChildLineY=0
        
        for item in left: 
            if leftIndex==1:
                leftRectY=prevLeftRectY
                leftChildLineY=prevLeftChildLineY
            else : 
                leftRectY=prevLeftRectY+self.rectYmargin+self.rectHeight
                leftChildLineY=prevLeftChildLineY+self.rectYmargin+self.rectHeight
            leftString+="<g><rect class='left' rx='5' ry='5'  width='"+str(self.rectWidth)+"' height='"+str(self.rectHeight)+"' x="+str(self.leftRectX)+" y="+str(leftRectY+self.displaceY)+"  /><text x='"+str(self.leftRectX+(self.rectWidth/2))+"' y='"+str(leftRectY+30+self.displaceY)+"'>"+item+"</text></g>\n"
            leftString+="<line x1='"+str(self.leftChildLineX)+"' y1='"+str(leftChildLineY+self.displaceY)+"' x2='"+str((self.leftChildLineX+self.hzLineWidth))+"' y2='"+str(leftChildLineY+self.displaceY)+"'  />\n"
            if leftIndex==1:
                firstLeftChildLineY=leftChildLineY
            if leftIndex==leftArrLenght:
                lastLeftChildLineY=leftChildLineY
            prevLeftRectY=leftRectY
            prevLeftChildLineY=leftChildLineY
            leftIndex+=1
        if not len(left)==0:
            leftString+="<line x1='"+str(self.leftVtLineX)+"' y1='"+str(firstLeftChildLineY+self.displaceY)+"' x2='"+str(self.leftVtLineX)+"' y2='"+str(lastLeftChildLineY+self.displaceY)+"'  />\n"
        centerY=(firstLeftChildLineY+lastLeftChildLineY)/2
        if centerY==0 :
            centerY=self.rectHeight/2
        
        if not len(left)==0:
            leftString+="<line x1='"+str(self.leftCenterLineX)+"' y1='"+str(centerY+self.displaceY)+"' x2='"+str(self.leftCenterLineX+self.hzLineWidth)+"' y2='"+str(centerY+self.displaceY)+"'  />\n"
            leftString+="<path d='M"+str(self.leftArrowX)+" "+str(centerY-10+self.displaceY)+" L"+str((self.leftArrowX+10))+" "+str(centerY+self.displaceY)+" L"+str(self.leftArrowX)+" "+str(centerY+10+self.displaceY)+" Z' />\n"        
        centerString="<g><rect rx='5' ry='5' class='center'  width='"+str(self.rectWidth)+"' height='"+str(self.rectHeight)+"' x="+str(self.centerRectX)+" y="+str(centerY+self.displaceY-(self.rectHeight/2))+"  /><text x='"+str(self.centerRectX +(self.rectWidth/2))+"' y='"+str(centerY+self.displaceY-(self.rectHeight/2)+30)+"' >"+center+"</text></g>\n"
        finalString=leftString+centerString
        firstRightChildLineY=0
        lastRightChildLineY=0
        vtLineHeight=0
        if len(left)>len(right):
            vtLineHeight=self.rectHeight*(len(right)-1)+self.rectYmargin*(len(right)-1)
            firstRightChildLineY=centerY-(vtLineHeight/2)
            lastRightChildLineY=centerY+(vtLineHeight/2)
        elif len(left)==len(right):
            firstRightChildLineY=firstLeftChildLineY
            lastRightChildLineY=lastLeftChildLineY
            vtLineHeight=self.rectHeight*(len(right)-1)+self.rectYmargin*(len(right)-1)
        elif len(left)==0 and len(right)==1:
            firstRightChildLineY=self.rectHeight/2
            lastRightChildLineY=self.rectHeight/2
        
        rightString=""
        if len(right)>0:
            rightString="<path d='M"+str(self.rightArrowX)+" "+str(centerY+self.displaceY-10)+" L"+str(self.rightArrowX-10)+" "+str(centerY+self.displaceY)+" L"+str(self.rightArrowX)+" "+str(centerY+self.displaceY+10)+" Z' />\n"
            rightString+="<line x1='"+str(self.rightCenterLineX)+"' y1='"+str(centerY+self.displaceY)+"' x2='"+str(self.rightCenterLineX+self.hzLineWidth)+"' y2='"+str(centerY+self.displaceY)+"'  />\n"
            rightString+="<line x1='"+str(self.rightVtLineX)+"' y1='"+str(firstRightChildLineY+self.displaceY)+"' x2='"+str(self.rightVtLineX)+"' y2='"+str(lastRightChildLineY+self.displaceY)+"'  />\n"
        
        rightArrayLength=len(right)        
        rightYdiff=0
        if rightArrayLength==1:
            rightYdiff=0
        else:
            rightYdiff=vtLineHeight/(rightArrayLength-1)
        rightYdiffStart=0
        for item in right:
            rightString+="<line x1='"+str(self.rightChildLineX)+"' y1='"+str(firstRightChildLineY+rightYdiffStart+self.displaceY)+"' x2='"+str(self.rightChildLineX+self.hzLineWidth)+"' y2='"+str(firstRightChildLineY+rightYdiffStart+self.displaceY)+"'  />\n"            
            rightRectY=(firstRightChildLineY+rightYdiffStart-(self.rectHeight/2))
            rightString+="<g><rect class='right' rx='5' ry='5'  width='"+str(self.rectWidth)+"' height='"+str(self.rectHeight)+"' x="+str(self.rightRectX)+" y="+str(rightRectY+self.displaceY)+"  /><text x='"+str(self.rightRectX+(self.rectWidth/2))+"' y='"+str((rightRectY+30+self.displaceY))+"' >"+item+"</text></g>\n"
            rightYdiffStart+=rightYdiff
        finalString+=rightString
        
        currCenterY=centerY+self.displaceY
        
        if not self.displaceY==0:
            finalString+="<line x1='"+str(self.completeCenter)+"' y1='"+str(self.prevCenterY+(self.rectHeight/2))+"' x2='"+str(self.completeCenter)+"' y2='"+str(currCenterY-(self.rectHeight/2))+"'  />\n"  
            finalString+="<path d='M"+str(self.completeCenter-10)+" "+str(currCenterY-(self.rectHeight/2)-10)+" L"+str(self.completeCenter)+" "+str(currCenterY-(self.rectHeight/2))+" L"+str(self.completeCenter+10)+" "+str(currCenterY-(self.rectHeight/2)-10)+" Z' />\n"
        self.prevCenterY=centerY+self.displaceY
        calculatedDisplacement=len(left)*(self.rectHeight+self.rectYmargin)
        if calculatedDisplacement==0:
            calculatedDisplacement=self.rectHeight+self.rectYmargin
        self.displaceY+=calculatedDisplacement
        return finalString   

    def drawFullChart(self,jsonArr):  
        print jsonArr 
        htmlHeadFile=open(self.userHome+"/CodeCompliance/html/template/SVGFlowHead.txt","r")
        htmlHead=htmlHeadFile.read()
        htmlHeadFile.close() 
        body=""
        for jsonObj in jsonArr:
            body+=self.drawChart(jsonObj)
        header="<svg width='1110' height='"+str(self.displaceY)+"'>\n"
        header+=''' 
        <defs>
        <linearGradient id="MyGradient"  x1="0%" y1="0%" x2="0%" y2="100%">
        <stop offset="5%" stop-color="rgb(165, 212, 243)" />
        <stop offset="95%" stop-color="rgb(255,255,255)" />
        </linearGradient>
        </defs>\n'''
        footer="</svg></div></body></html>"
        finalString=htmlHead+header+body+footer
        htmlFile=open(self.userHome+"/CodeCompliance/html/TableFlow_"+self.fileName+".html",'w')
        htmlFile.write(finalString)
        htmlFile.close()
        return htmlFile.name
            
#if __name__ == "__main__":
#    jsonArray=[{'right': ['logistics.em_carrier_dq_raw_file', 'logistics.em_carrier_dq_wk_accuracy'], 'center': 'logistics_app.em_carrier_dq_wk_raw', 'left': ['logistics.fiscal_day', 'logistics.em_carrier_dq_scac_list']}, {'right': [], 'center': 'logistics_app.em_carrier_dq_wk_raw', 'left': ['logistics.fiscal_day', 'logistics.em_carrier_dq_scac_list']}, {'right': ['logistics_app.em_carrier_dq_wk_raw'], 'center': 'logistics_app.em_carrier_dq_wk_raw', 'left': ['logistics.em_carrier_dq_scac_list']},{'right': ['logistics.em_carrier_dq_raw_file', 'logistics.em_carrier_dq_wk_accuracy'], 'center': 'logistics_app.em_carrier_dq_wk_raw', 'left': ['logistics.fiscal_day', 'logistics.em_carrier_dq_scac_list']}]
#    SvgFlowChart("test.sql").drawFullChart(jsonArray)
