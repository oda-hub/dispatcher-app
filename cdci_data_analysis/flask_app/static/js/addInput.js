var counterAccess = 0;
function HideShow(selectedDiv,divList){
    for (j = 0; j <= divList.length - 1; j++){
        var node = document.getElementById(divList[j]);
        var el =   document.getElementById(divList[j]).children;
        document.getElementById(divList[j]).style.display="none";
        
        for (i = 0; i < el.length; i++){
           el[i].disabled=true;
           
         }
    
    }
     
     
    for (j = 0; j <= divList.length - 1; j++){
    
    
     if (divList[j]==selectedDiv){
        document.getElementById(divList[j]).style.display="block";
        var node = document.getElementById(divList[j]);
        var el   = document.getElementById(divList[j]).children;
        for (i = 0; i < el.length; i++){
            el[i].disabled=false;
            }
        }       
    }
}

    



