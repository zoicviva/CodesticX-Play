import os
class ApplicationContainer:
    
    def __init__(self):
        self.userHome=os.path.expanduser('~')
    
    def buildContainer(self,files):
        htmlHeadFile=open(self.userHome+"/CodeCompliance/html/template/container_head.txt","r")
        htmlHeader=htmlHeadFile.read()
        htmlHeadFile.close()
        htmlTailFile=open(self.userHome+"/CodeCompliance/html/template/container_tail.txt","r")
        htmlTail=htmlTailFile.read()
        htmlTailFile.close()
        body='''
        <ul>
        <li id='homeNavMenu'><a href='AppHome.html' target='myIframe'>Application</a></li>
        <li><a href='#aboutus'>Procedures</a><ul>'''
        
        for fileName in files:
            body+="<li><a href='#about/history'>"+fileName+"</a><ul>\n"
            body+="<li id='mgtNavMenu'><a href='file://"+self.userHome+"/CodeCompliance/html/TableCount_"+fileName+".html' target='myIframe'>Statistics</a></li>\n"
            body+="<li id='salesNavMenu'><a href='file://"+self.userHome+"/CodeCompliance/html/TableFlow_"+fileName+".html' target='myIframe'>Program Flow</a></li>\n"
            body+="<li id='devNavMenu'><a href='#about/team/development'>Complexity & Warning</a></li>\n"
            body+="</ul></li>"
        body+="</ul></li></ul>"
        
        htmlFile=open(self.userHome+"/CodeCompliance/html/index.html",'w')
        htmlFile.write(htmlHeader)
        htmlFile.write(body)
        htmlFile.write(htmlTail)
        htmlFile.close()
        return htmlFile.name
        