vecPackage=c("RODBC","dplyr","reshape","data.table","rgdal","lubridate","RPostgreSQL","doBy","reshape2")
ip <- installed.packages()[,1]

for(p in vecPackage)
    if (!(p %in% ip))
        install.packages(pkgs=p,repos = "http://cran.univ-paris1.fr/",dependencies=TRUE)

library(RODBC)
library(reshape)
library(data.table)
library(rgdal)
library(lubridate)
library(RPostgreSQL)
library(doBy)
library(reshape2)




library(foreign)


openDB.PSQL <- function(user=NULL,mp=NULL,nomDB=NULL){

    library(RPostgreSQL)
    drv <- dbDriver("PostgreSQL")
    if(is.null(nomDB)) nomDB <- "stoc_eps"

    if(is.null(user)) {
        con <- dbConnect(drv, dbname=nomDB)
    } else {
        con <- dbConnect(drv, dbname=nomDB,user=user, password=mp)
    }

    return(con)
}


clean.PSQL <- function(nomDB=NULL) {
    if(is.null(nomDB)) nomDB <- "stoc_eps"
    drv <- dbDriver("PostgreSQL")
    veccon <- dbListConnections(drv)
    for(i in 1:length(veccon)) dbDisconnect(veccon[[i]])
}



cti <- function() {


    con <- openDB.PSQL("romain",mp,NULL)

d.sti <- dbReadTable(con,"species_indicateur_fonctionnel")
    d.sti <- d.sti[,c("pk_species","sti_europe")]
    colnames(d.sti) <- c("ESPECE","sti")


d.old <- read.dbf("DB_import/OLD2A.DBF")
d.old$ESPECE <- toupper(d.old$ESPECE)

d.old <- merge(d.old,d.sti,by="ESPECE")
    d.old$nsti <- d.old$TOTAL * d.old$sti

    agg.old <- aggregate(cbind(d.old$TOTAL,d.old$nsti)~ ROUTE + ANNEE,data=d.old,FUN=sum)
    colnames(agg.old)[3:4] <- c("N_sum","nsti_sum")
agg.old$cti <- agg.old$nsti_sum / agg.old$N_sum


}
