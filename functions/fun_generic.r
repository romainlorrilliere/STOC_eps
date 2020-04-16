
vecPackage=c("RODBC","dplyr","data.table","rgdal","lubridate","RPostgreSQL","doBy","reshape2","sf","maptools","maps","animation","dplyr")
ip <- installed.packages()[,1]

for(p in vecPackage){
    if (!(p %in% ip))
        install.packages(pkgs=p,repos = "http://cran.univ-paris1.fr/",dependencies=TRUE)
    require(p,character.only=TRUE)
}





myshell <- function(mycmd,myinvisible=TRUE) {
    is.windows <- Sys.info()["sysname"] == "Windows"

    cat("System command: ",mycmd,"\n",paste="")
    if(is.windows){
        cat("     - OS: Windows \n")
        shell(cmd=mycmd,invisible=myinvisible)
    }else{
        cat("     - OS: Linux alike \n")
        system2(mycmd)
    }
}




  openDB.PSQL <- function(user=NULL,pw=NULL,DBname=NULL){
    ## --- initializing parameters for debugging ----
                                        #DBname=NULL;
                                        #user="romain" # windows
                                        #user = NULL # linux
                                        #  pw=NULL
    ## ---

    library(RPostgreSQL)
    drv <- dbDriver("PostgreSQL")

    if(is.null(DBname)) {
        DBname <- "stoc_eps"
    }

    cat("\n",DBname,user,ifelse(is.null(pw),"","****"),"\n")
                                         # about when I use windows a have to define the user
      if(is.null(user)) {
         con <- dbConnect(drv, dbname=DBname)
      } else {
          con <- dbConnect(drv, dbname=DBname,user=user, password=pw)
    }

    return(con)
}


clean.PSQL <- function(nomDB=NULL) {
    if(is.null(nomDB)) nomDB <- "stoc_eps"
    drv <- dbDriver("PostgreSQL")
    veccon <- dbListConnections(drv)
    for(i in 1:length(veccon)) dbDisconnect(veccon[[i]])
}



  Encoding_utf8 <- function(x) {
            Encoding(x) <- "UTF-8"
            return(x)
        }


getCode_sp <- function(con,champSp,sp) {
    if(is.null(con)) con <- openDB.PSQL()
    querySp <- paste(" select ",champSp,", pk_species as code_sp, french_name from species where ",champSp," in  ",paste("('",paste(sp,collapse="' , '"),"')",";",sep=""),sep="")
    cat("\nRequete recupération des code_sp:\n\n",querySp,"\n\n")

    dsp <- dbGetQuery(con, querySp)

    return(dsp$code_sp)


}


trad_fr2eng <- function(d) {

    tabTrad <- read.csv("Librairie/traduc_fr2eng.csv",stringsAsFactor=FALSE)
    rownames(tabTrad) <- tabTrad$fr

    colnames(d) <- ifelse(colnames(d) %in% tabTrad$fr,tabTrad[colnames(d),"eng"],colnames(d))
    return(d)
}


 get_mode <- function(x) {
        ux <- unique(x)
        ux[which.max(tabulate(match(x, ux)))]
    }
