

source("functions/fun_generic.r")


findDataInVP <- function(d=NULL,nomFile="export_stoc_10012019.txt",id_carre=NULL,num_point=NULL,date=NULL,annee=NULL,espece=NULL,distance_contact=NULL) {
    library(lubridate)
    if(is.null(d)) {
        nomFileVP <- paste("data/",nomFile,sep="")
        d <-  read.csv(nomFileVP,h=TRUE,stringsAsFactors=FALSE,fileEncoding="utf-8",sep="\t")
    }

  #  browser()
    if(!is.null(id_carre)) d <- subset(d,N..Carré.EPS %in% paste("CARRE N°",id_carre,sep=""))
    if(!is.null(num_point))  d <- subset(d, EXPORT_STOC_TEXT_EPS_POINT %in% paste("Point N°",sprintf("%02d",num_point),sep=""))
    if(!is.null(date))  d <- subset(data,Date== format(as.Date(date),"%d.%m.%Y"))
    if(!is.null(annee)) d <- subset(d,year(as.Date(Date,"%d.%m.%Y"))%in% annee)
    if(!is.null(espece)) d <- subset(d,Espèce %in% espece)
    if(!is.null(distance_contact)) d <- subset(d, Distance.de.contact %in% distance_contact)

    return(d)
}


