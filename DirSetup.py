import os
import logging

class DirSetup:
    
    @staticmethod
    def setup():
        userHome=os.path.expanduser('~')
        if not os.path.exists(userHome+"/CodeCompliance"):
            os.makedirs(userHome+"/CodeCompliance")
        if not os.path.exists(userHome+"/CodeCompliance/temp"):
            os.makedirs(userHome+"/CodeCompliance/temp")
        if not os.path.exists(userHome+"/CodeCompliance/orig"):
            os.makedirs(userHome+"/CodeCompliance/orig")
        if not os.path.exists(userHome+"/CodeCompliance/work"):
            os.makedirs(userHome+"/CodeCompliance/work")
        if not os.path.exists(userHome+"/CodeCompliance/html"):
            os.makedirs(userHome+"/CodeCompliance/html")
        if not os.path.exists(userHome+"/CodeCompliance/html/images"):
            os.makedirs(userHome+"/CodeCompliance/html/images")
        if not os.path.exists(userHome+"/CodeCompliance/html/template"):
            os.makedirs(userHome+"/CodeCompliance/html/template")
        if not os.path.exists(userHome+"/CodeCompliance/html/template/TableFlowHead.txt"):
            headFile=open(userHome+"/CodeCompliance/html/template/TableFlowHead.txt", 'w')
            fileContent='''<html>
<head>
    <style>
    body{ background: #e6e6e6;margin:0px; padding:0px; color: #666; }
    .main{width:1200px; margin:auto;padding:10px;background: #aabcd5;margin-top: 10px;
     background-image:url('images/bg-arrow.png');
     }
    .statment{margin-top: 20px;}
    .left{width:33%;float:left;min-height:30px;}
    .right{width:33%;float:left;}
    .center{width:30%;padding:10px;float:left;}
    .clr{width:100%;clear:both;}
    .left_child{min-height: 35px;text-align: center;}
    span{padding:5px 10px 5px 10px;background: #e6e9f0;border-radius: 6px;cursor:pointer;border: 1px solid #431c5d;}
    .center_child>span{box-shadow: 0px 0px 15px #60739f;}
    .center_child{min-height: 35px;text-align: center;}
    .right_child{min-height: 35px;text-align: center;}

/* CSS talk bubble */
.talk-bubble {
    margin: 4px;
    display: inline-block;
    position: relative;
    height: auto;
    background: linear-gradient(#c2dde6,#c2dde9);
    padding-top:14px;
    box-shadow: 5px 5px 3px #60739f;
}
.tri-right.left-top:after{
    content: ' ';
    position: absolute;
    width: 0;
    height: 0;
    left: -20px;
    right: auto;
    top: 0px;
    bottom: auto;
    border: 22px solid;
    border-color: #c2dde6 transparent transparent transparent;
}
.tri-right.right-top:after{
    content: ' ';
    position: absolute;
    width: 0;
    height: 0;
    left: auto;
    right: -20px;
    top: 0px;
    bottom: auto;
    border: 20px solid;
    border-color: #c2dde6 transparent transparent transparent;
}
#fixDiv{
position: fixed;padding:20px;z-index:100;
right:10px;top:20px;width:200px;height:20px;border:1px solid #666;box-shadow:0px 0px 2px black;background: white;
text-align:center;
}
.divHide{
display:none;
}
</style>
</head>
<body>
<div id="fixDiv" class="divHide"></div>
<div class="main"> '''
            headFile.write(fileContent)
            headFile.close()
        if not os.path.exists(userHome+"/CodeCompliance/html/template/TableFlowTail.txt"):
            tailFile=open(userHome+"/CodeCompliance/html/template/TableFlowTail.txt", 'w')
            fileContent=''' </div>
<script>
var elms = {};
var n = {}, nclasses = classes.length;
function changeColor(classname, color) {
    var curN = n[classname];
    for(var i = 0; i < curN; i ++) {
        elms[classname][i].style.backgroundColor = color;
        if(color=="#60739f"){
            elms[classname][i].style.color = "white";
        }
        else{
            elms[classname][i].style.color = "#666";
        }
    }
}
for(var k = 0; k < nclasses; k ++) {
    var curClass = classes[k];
    elms[curClass] = document.getElementsByClassName(curClass);
    n[curClass] = elms[curClass].length;
    console.log(elms[curClass].length);
    
    var curN = n[curClass];
    for(var i = 0; i < curN; i ++) {
        elms[curClass][i].onmouseover = function() {
            changeColor(this.className, "#60739f");
            document.getElementById("fixDiv").classList.remove("divHide");
            var cnt=document.getElementsByClassName(this.className).length;
            document.getElementById("fixDiv").innerHTML=cnt+" element(s) selected";
        };
        elms[curClass][i].onmouseout = function() {
            changeColor(this.className, "#e6e9f0");
            document.getElementById("fixDiv").classList.add("divHide");
        };
    }
};
</script>

</body>
</head>'''
            tailFile.write(fileContent)
            tailFile.close()
            
# if __name__ == '__main__':
#     DirSetup.setup()