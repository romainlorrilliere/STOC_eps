
vecPackage=c("RODBC","reshape","data.table","rgdal","lubridate","RPostgreSQL","doBy","arm","ggplot2","scales","mgcv","visreg","plyr")
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
library(arm)
library(ggplot2)
library(scales)
library(mgcv)
library(visreg)
library(plyr)
openDB.PSQL <- function(nomDB=NULL){
  
  library(RPostgreSQL)
  if(is.null(nomDB)) nomDB <- "stoc_eps"
  
  drv <- dbDriver("PostgreSQL")
  con <- dbConnect(drv, dbname="stoc_eps")
  return(con)
}





historicCarre  <- function(con,anneeMax=2017) {
    require(ggplot2)
    require(reshape2)
    query <-paste("select id_carre, annee from inventaire where annee <= ",anneeMax," group by id_carre, annee order by id_carre, annee;")

                                        # browser()
    d <- dbGetQuery(con, query)
    d$PA <- 1
    
    dan <- aggregate(PA ~ annee, d, sum)

    dd <- cast(d,id_carre~annee,mean)
    rnames <- dd$id_carre
    cnames <- colnames(dd)[-1]
    dd <- as.matrix(dd[,-1])
    rownames(dd) <- rnames
    colnames(dd) <- cnames
    dd[is.nan(dd)] <- 0


    nb_carre_diff <- nrow(dd)

    colnames(dan)[2] <- "nbCarre"
    dan$Nouveaux <- NA
    dan$NonAjour <- NA
    dan$Arrete <- NA
    dan$Nouveaux[1] <- sum(dd[,1])
    dan$NonAjour[1] <- 0
    dan$Arrete[1] <- 0
    dan$Arrete[ncol(dd)] <- NA

    for(j in 2:ncol(dd)) {
        if(j==2) 
            dan$Nouveaux[j] <- sum(dd[dd[,j]==1 & dd[,j-1]==0,j]) else dan$Nouveaux[j] <- sum((dd[dd[,j]==1 & rowSums(dd[,1:j-1])==0,j]))
    }
                                        #-as.numeric(rowSums(dd[dd[,j]==1 & dd[,j-1]==0,1:j-1])>0)))
    
    
    for(j in 2:ncol(dd)) 
        if(j<ncol(dd))
            dan$NonAjour[j] <- sum(as.numeric(rowSums(dd[(dd[,j]==0 & dd[,j-1]==1),j:ncol(dd)])>0)) else  dan$NonAjour[j] <- sum(dd[(dd[,j]==0 & dd[,j-1]==1),j-1])
    
    for(j in 2:(ncol(dd)-1)) 
        dan$Arrete[j] <- sum(as.numeric(rowSums(dd[(dd[,j]==0 & dd[,j-1]==1),j:ncol(dd)])==0))


    write.csv2(dan,"Output/carreSTOCactif.csv")

    ggAnnee <- melt(dan,"annee")

    gg <- ggplot(subset(ggAnnee,variable != "NonAjour"),aes(x=annee,y=value,colour=variable))+geom_line(size=1.5)+geom_point(size=2)
    gg <- gg + scale_colour_manual(values=c("nbCarre" = "#0d259f","Nouveaux"="#0d9f1b","Arrete" = "#9f0d0d" ),
                                   labels=c("nbCarre" = "Carrés actif","Nouveaux"="Nouveaux carrés","Arrete" = "Carrés arrêtés"),name="" )
    gg <- gg + labs(title="",x="",y="")
    ggsave("Output/carreSTOC.png",gg)
    


    birth <- apply(dd,1,FUN = function(x) min(which(x==1)))
    dage <- dd
    for(i in 1:nrow(dd)) {
        cc <- names(birth[i])
        b <- birth[i]
        dage[cc,b:ncol(dage)] <- 1:(ncol(dage)-b+1)
    }

    dage <- dage*dd
    dage <- as.data.frame(as.matrix(dage))
    dage2 <- data.frame(id_carre=row.names(dage),dage)

    dage2 <- melt(dage2,"id_carre")
    colnames(dage2) <- c("id_carre","annee","age")
    dage2$annee <- as.numeric(substring(as.character(dage2$annee),2,5))

    dage2 <- subset(dage2,age>0)
    ggAge <- aggregate(id_carre~annee+age,dage2,length)


    gg <- ggplot(ggAge,aes(age,id_carre))+ geom_col() + facet_wrap(~annee)
    gg <- gg + labs(title="Pyramide des ages des stations STOC EPS",x="Age",y="Nombre de carré STOC actifs")
    
    ggsave("Output/carreSTOC_pyramideAge.png",gg)

    write.csv2(dan,"Output/carreSTOCage.csv")


}



verificationCarre <- function() {
    ddiane <- read.table("carre2001-2015sp135max50_2.txt",header=TRUE,stringsAsFactors=FALSE)
    dd2 <- ddiane[,1:4]
    ddan <- data.frame(table(dd2$annee))
    colnames(ddan) <- c("annee","nb_diane")
    #plot(table(dd2$annee))
    #plot(dd2$x,dd2$y)

    con <- openDB.PSQL()
    query <- "select id_carre, annee from carre_annee;"
    dr <- dbGetQuery(con, query)
    dran <- data.frame(table(dr$annee))
     colnames(dran) <- c("annee","nb_romain")

    dan <- merge(dran,ddan,by="annee")
    dan$diff <- dan$nb_diane-dan$nb_romain


    dd2$carreannee <- paste(dd2$annee,dd2$carre,sep="_")
    dr$carreannee <- paste(dr$annee,dr$id_carre,sep="_")

    dcarre1 <- setdiff(dd2$carreannee,dr$carreannee)

    dd3 <- subset(dd2,carre %in% dcarre1)


   dcarre2 <- setdiff(unique(dr$id_carre),unique(dd2$carre))


    dr3 <- subset(dr,id_carre %in% dcarre2 & annee <= max(dd2$annee))


    dd2bis <- data.frame(carreannee=dd2$carreannee,diane=1)
    drbis <- data.frame(carreannee=dr$carreannee,romain=1)

    d <- merge(dd2bis,drbis,by="carreannee",all=TRUE)
    d$diane[is.na(d$diane)] <- 0
    d$romain[is.na(d$romain)] <- 0
    d$all <- (d$diane + d$romain) == 2


    d2 <- subset(d,!all)
    d2$carre <- substr(d2$carreannee,6,nchar(as.character(d2$carreannee)))
    d2$annee <- substr(d2$carreannee,1,4)

    d2 <- d2[order(d2$carre,d2$annee),]
    d2 <- subset(d2,annee <= max(ddiane$annee))


    d3 <- unique(d2[,c("carre","diane","romain")])

    ddiane <- subset(d,diane==1)
    ddiane$carre <- substr(ddiane$carreannee,6,nchar(as.character(ddiane$carreannee)))
    ddiane$annee <- substr(ddiane$carreannee,1,4)

    dromain <- subset(d,romain==1)
    dromain$carre <- substr(dromain$carreannee,6,nchar(as.character(dromain$carreannee)))
    dromain$annee <- substr(dromain$carreannee,1,4)


    d3diane <- subset(d3,diane==1)
    d3romain <- subset(d3,romain==1)

    d3diane$jamais <- !(d3diane$carre %in% dromain$carre)
    d3romain$jamais <- !(d3romain$carre %in% ddiane$carre)

    d3dianeJ <- subset(d3diane,jamais)
    d3romainJ <- subset(d3romain,jamais)


    
    nomFileFNat="FNat_plat_2017-01-04.csv"
     nomFileFNat=paste("Data/",nomFileFNat,sep="")
    
    dFNat <- read.csv(nomFileFNat,fileEncoding="iso-8859-1",stringsAsFactors=FALSE)
       dFNat$insee <- sprintf("%05d", dFNat$insee)
        dFNat$heure <- sprintf("%04d", dFNat$heure)
        dFNat$numobs <- as.character(dFNat$numobs)
        dFNat$lieudit <- as.character(dFNat$lieudit)
    dFNat$section_cadastrale <- as.character(dFNat$section_cadastrale)
       dFNat <- subset(dFNat, etude %in% c("STOC-EPS","STOC-SITES ONF","STOC-EPS MAMMIFERES"))
          dFNat$etude[dFNat$etude=="STOC-EPS MAMMIFERES"] <- "STOC_EPS"
    dFNat$etude[dFNat$etude=="STOC-EPS"] <- "STOC_EPS"
    
        dFNat$etude[dFNat$etude=="STOC-SITES ONF"] <- "STOC_ONF"
        dFNat$etude[dFNat$id_carre %in% unique(subset(dFNat,etude=="STOC_ONF")$id_carre)] <- "STOC_ONF"
       dCarreONF <- read.csv("Data/siteONF.csv")
        dCarreONF <- dCarreONF[,c(1,2,5)]
        dCarreONF$nouveau_lieudit <- sprintf("%06d",dCarreONF$nouveau_lieudit)
        colnames(dCarreONF) <- c("lieudit","dept","id_carre")
        
        ddFNat <- merge(dFNat,dCarreONF,by=c("lieudit","dept"),all=TRUE)
        ## pour un bug incomprehenssible dans le merge
        ddFNat[grep("Messarges",ddFNat$lieudit),"id_carre"] <- "030523"
        ddFNat[grep("Tronçais",ddFNat$lieudit),"id_carre"] <- "030143"
        ddFNat <- subset(ddFNat,!is.na(unique_citation))
        dFNat <- ddFNat
        dFNat$id_carre <- ifelse(is.na(dFNat$id_carre),paste(dFNat$dept,substring(dFNat$lieudit,13,nchar(dFNat$lieudit)),sep=""),dFNat$id_carre)
      
    dFNatr <- subset(dFNat,id_carre %in% d3romainJ$carre)
    dFNatd <- subset(dFNat,id_carre %in% d3dianeJ$carre)


    dFNatr2 <- unique(subset(dFNatr,select=c("id_carre","unique_inventaire","lieudit","dateobs","heure")))
    dFNatr2$annee <- as.numeric(substr(dFNatr2$dateobs,1,4))
    
    dFNatr3 <- unique(subset(dFNatr2,select=c("id_carre","annee")))
    aggregate(annee~id_carre,data=dFNatr3,length)

 aggregate(annee~carre,data=dd2,length)

    
    }

