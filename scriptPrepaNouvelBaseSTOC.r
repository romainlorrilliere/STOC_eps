###################################################################################################################
###
###                       Script de preparation de la base STOC-eps reuniant les deux bases
###                                              FNat   et    VigiePlume
###
###################################################################################################################


### mon compte windows romain crex
### mon compte windows admin postgres crexCREX44!
### mon compte linux romain




### Beta 1.0

### Selection dans la base FNat seulement des carré STOC EPS


### requete dans base access FNat (seulement sous windows et sous R 32bit)

###  exportation de table a plat

### Importation de table FNat a plat
### Importation de table VigiePlume a plat



### 2017-11-14 modification de l'entete de colonne des nom de point de la table vigieplume


vecPackage=c("RODBC","reshape","reshape2","data.table","rgdal","lubridate","RPostgreSQL","doBy","ggplot2","maptools","maps","animation")
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
library(ggplot2)
library(reshape2)
library(maptools)
library(maps)
library(animation)

openDB.PSQL <- function(nomDB=NULL){

    library(RPostgreSQL)
    if(is.null(nomDB)) nomDB <- "stoc_eps"

    drv <- dbDriver("PostgreSQL")
    con <- dbConnect(drv, dbname="stoc_eps")
    return(con)
}



prepaData <- function(dateExportVP="2018-02-14",nomFileVP="export_stoc_14022018.txt",nomFileVP_ONF="export_stoc_ONF_14022018.txt",
                      dateExportFNat="2017-01-04", importACCESS=FALSE,
                      nomFileFNat="FNat_plat_2017-01-04.csv",nomDBFNat="Base FNat2000.MDB",importationDataBrut=FALSE,
                      constructionPoint=FALSE,constructionCarre=FALSE,constructionInventaire=FALSE,
                      constructionObservation = FALSE, constructionHabitat = FALSE,
                      dateConstruction=NULL,postgresql_import=FALSE,nomDBpostgresql=NULL,
                      postgresql_createAll=FALSE,postgresUser="romain",
                      postGIS_initiation=FALSE,repertoire=NULL,postgresql_abondanceSeuil=FALSE,seuilAbondance = .99,historiqueCarre=TRUE,
                      pointCarreAnnee=TRUE,importPointCarreAnnee=TRUE,fileTemp=FALSE)
{
    library(RODBC)
    library(reshape)
    library(data.table)
    library(rgdal)



    cat("               ===========================================================================\n")
    cat("               |                                                                         |\n")
    cat("               |            Importation des Donnees STOC-eps des 2 base de donnnes       |\n")
    cat("               |                              FNat et VigiePlume                         |\n")
    cat("               |                                                                         |\n")
    cat("               ===========================================================================\n")

    version <- "V.1"

    cat(paste("\n\n Version",version,"\n"))
    cat(" --------------\n\n")



##   dateExportVP="2017-11-14";nomFileVP="export_stoc_14112017.txt";nomFileVP_ONF="export_stoc-onf_14112017.txt";
##   dateExportFNat="2017-01-04"; importACCESS=FALSE;
##   nomFileFNat="FNat_plat_2017-01-04.csv";nomDBFNat="Base FNat2000.MDB";importationDataBrut=TRUE;
##   constructionPoint=TRUE;constructionCarre=TRUE;constructionInventaire=TRUE;
##   constructionObservation = TRUE; constructionHabitat = TRUE;
##   dateConstruction=NULL;postgresql_import=TRUE;nomDBpostgresql=NULL;
##   postgresql_createAll=TRUE;postgresUser="romain";
##   postGIS_initiation=TRUE;repertoire=NULL;postgresql_abondanceSeuil=TRUE;historiqueCarre=TRUE;
##   pointCarreAnnee=TRUE;importPointCarreAnnee=TRUE;fileTemp=FALSE


    start <- Sys.time() ## heure de demarage est utiliser comme identifiant par defaut
    if(is.null(dateConstruction)) dateConstruction = format(start, "%Y-%m-%d")


    cat("\n     # Debut du process:",format(start, "%Y-%m-%d %H:%M\n"))

    nomFileVP=paste("Data/",nomFileVP,sep="")
    nomFileVPonf=paste("Data/",nomFileVP_ONF,sep="")

    nomDBFNat=paste("Data/",nomDBFNat,sep="")
    nomFileFNat=paste("Data/",nomFileFNat,sep="")


    cat("\n\n 1) Importation\n----------------------\n")
    if(importationDataBrut) {

        cat("\n - VigiePlume\n <-- Fichier a plat:",nomFileVP,"\n")
        flush.console()
        dVP <- read.csv(nomFileVP,h=TRUE,stringsAsFactors=FALSE,fileEncoding="utf-8",sep="\t")
        cat("\n    !!! suppression des ligne issue d'une etude LPO ahiqutaine hors echantillonage STOC-EPS 'PRANAT','Frolet'\n")


        ligneExclu <- union(grep("NAT",dVP$N..Carré.EPS),grep("Frolet",dVP$N..Carré.EPS))
        cat(length(ligneExclu)," lignes exclues\n\n")
        ligneConserv <- setdiff(1:nrow(dVP),ligneExclu)
        dVP <- dVP[ligneConserv,]
        cat(" <-- Fichier a plat ONF:",nomFileVPonf,"\n")
        flush.console()
        dVPonf <- read.csv(nomFileVPonf,h=TRUE,stringsAsFactors=FALSE,fileEncoding="utf-8",sep="\t")
        dVP <- rbind(dVP,dVPonf)

        dVP <-subset(dVP,!is.na(Nombre) & !is.na(Espèce) & Espèce != "")
        dVP$INSEE <-sprintf("%05d", dVP$INSEE)
        dVP$Département <- sprintf("%02d", dVP$Département)

        cat("\n - FNat\n")
        if(importACCESS) {
            cat(" <-- Base de données ACCESS:",nomDBFNat,"\n")
            cat(" !! ATTENTION cette fonctionne uniquement sous windows avec une version 32bit de R (.../bin/i386/Rgui.exe)\n")
            cat(" Cette importation peut prendre prendre plusieurs minutes,\nmerci de patienter...\n")
            flush.console()
            dFNat <- import_FNat(file=nomDBFNat,fichierAPlat=nomFileFNat,output=TRUE)
            cat("   --> OK \n")
            flush.console()

        } else {
            cat(" <-- Fichier à plat:",nomFileFNat,"\n")
            cat(" Cette importation peut prendre un peu de temps,\nmerci de patienter .... \n")
            flush.console()
            dFNat <- read.csv(nomFileFNat,fileEncoding="iso-8859-1",stringsAsFactors=FALSE)
            cat("   --> OK \n")
            flush.console()
        }

        dFNat$insee <- sprintf("%05d", dFNat$insee)
        dFNat$heure <- sprintf("%04d", dFNat$heure)
        dFNat$numobs <- as.character(dFNat$numobs)
        dFNat$lieudit <- as.character(dFNat$lieudit)
        dFNat$section_cadastrale <- as.character(dFNat$section_cadastrale)
        dFNat <- subset(dFNat, etude %in% c("STOC-EPS","STOC-SITES ONF","STOC-EPS MAMMIFERES"))
        dFNat$etude[dFNat$etude=="STOC-EPS"] <- "STOC_EPS"
        dFNat$etude[dFNat$etude=="STOC-EPS MAMMIFERES"] <- "STOC_EPS"
        dFNat$etude[dFNat$etude=="STOC-SITES ONF"] <- "STOC_ONF"

      #  dFNat$etude[dFNat$id_carre == "761436"] <- "STOC_ONF"

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
        dFNat$etude[dFNat$id_carre %in% unique(subset(dFNat,etude=="STOC_ONF")$id_carre)] <- "STOC_ONF"
    } else { cat(" ---> SKIP\n") }


    cat("\n\n 2) Table point\n----------------------\n")
    flush.console()

    if(constructionPoint) {
        cat("\n - VigiePlume: ")
        flush.console()
        ddVP.point <- vp2point(dVP,dateExportVP,TRUE)
        cat(nrow(ddVP.point)," lignes \n")
        flush.console()

        cat("\n - FNat: ")
        flush.console()
        ddFNat.point <- FNat2point(dFNat,dateExportFNat,TRUE)
        cat(nrow(ddFNat.point)," lignes \n")

        cat("\n  -> union: ")
        dd.point <- union.point(ddVP.point,ddFNat.point)
        cat(nrow(dd.point)," lignes \n")

        file <- paste("DB_import/point_",dateConstruction,".csv",sep="")
        cat("\n  -->",file,"\n")
        write.table(dd.point,file,row.names=FALSE,na = "",quote=TRUE, sep=";",dec=".")

    } else { cat(" ---> SKIP\n") }


    cat("\n\n 3) Table carre\n----------------------\n")
    flush.console()

    if(constructionCarre) {
        cat("\n - VigiePlume: ")
        flush.console()
        ddVP.carre <- vp2carre(dVP,dateExportVP,TRUE)
        cat(nrow(ddVP.carre)," lignes \n")

        cat("\n - FNat: ")
        flush.console()
        ddFNat.carre <- FNat2carre(dFNat,dateExportFNat,TRUE)
        cat(nrow(ddFNat.carre)," lignes \n")

        cat("\n  -> union: ")
        dd.carre <- union.carre(ddVP.carre,ddFNat.carre)
        cat(nrow(dd.carre)," lignes \n")

        file <- paste("DB_import/carre_",dateConstruction,".csv",sep="")
        cat("\n  -->",file,"\n")
        write.table(dd.carre,file,row.names=FALSE,na = "",quote=TRUE, sep=";",dec=".")

    } else { cat(" ---> SKIP\n") }


    cat("\n\n4) Table inventaire\n----------------------\n")
    flush.console()

    if(constructionInventaire | constructionObservation) {
        cat("\n - VigiePlume: ")
        flush.console()
        ddVP.inv <- vp2inventaire(dVP,dateExportVP,version,TRUE)
        cat(nrow(ddVP.inv)," lignes \n")

        cat("\n - FNat: ")
        flush.console()
        ddFNat.inv <- FNat2inventaire(dFNat,dateExportFNat,version,TRUE)
        cat(nrow(ddFNat.inv)," lignes \n")

        cat("\n  -> union: ")
        dd.inv <- union.inventaire(ddFNat.inv,ddVP.inv,dateConstruction)
        cat(nrow(dd.inv)," lignes \n")

        file <- paste("DB_import/inventaire_",dateConstruction,".csv",sep="")
        cat("\n  -->",file,"\n")
        write.table(dd.inv,file,row.names=FALSE,na = "",quote=TRUE, sep=";",dec=".")

    } else { cat(" ---> SKIP\n") }


    cat("\n\n 5) Table observation\n----------------------\n")
    flush.console()

    if(constructionObservation) {
        cat("\n - VigiePlume: ")
        flush.console()
        ddVP.obs <- vp2observation(dVP,dateExportVP,TRUE)
        cat(nrow(ddVP.obs)," lignes \n")

        cat("\n - FNat: ")
        flush.console()
        cat("\n",  nrow(subset(dFNat,!(unique_inventaire %in% ddFNat.inv$unique_inventaire_fnat)))," lignes exclues !!\n")
        dFNat <- subset(dFNat,unique_inventaire %in% ddFNat.inv$unique_inventaire_fnat)

        ddFNat.obs <- FNat2observation(dFNat,dateExportFNat,TRUE)

        ddFNat.obs <- subset(ddFNat.obs,!(id_inventaire %in% ddVP.inv$pk_inventaire))
        cat(nrow(ddFNat.obs)," lignes \n")

        cat("\n  -> union: ")
        dd.obs <- union.observation(ddFNat.obs,ddVP.obs,dateConstruction)
        cat(nrow(dd.obs)," lignes \n")

        file <- paste("DB_import/observation_",dateConstruction,".csv",sep="")
        cat("\n  -->",file,"\n")
        write.table(dd.obs,file,row.names=FALSE,na = "",quote=TRUE, sep=";",dec=".")
    } else { cat(" ---> SKIP\n") }


    cat("\n\n 6) Table habitat\n----------------------\n")
    flush.console()

    if(constructionHabitat) {
        cat("\n - VigiePlume: ")
        ddVP.hab <- vp2habitat(dVP,dateExportVP,TRUE)
        cat(nrow(ddVP.hab)," lignes \n")

        cat("\n - FNat: ")
        flush.console()
        ddFNat.hab <- FNat2habitat(dFNat,dateExportFNat,TRUE)
        cat(nrow(ddFNat.hab)," lignes \n")

        cat("\n  -> union: ")
        dd.hab <- union.habitat(ddFNat.hab,ddVP.hab,dateConstruction)
        cat(nrow(dd.hab)," lignes \n")


        file <- paste("DB_import/habitat_",dateConstruction,".csv",sep="")
        cat("\n  -->",file,"\n")
        write.table(dd.hab,file,row.names=FALSE,na = "",quote=TRUE, sep=";",dec=".")
    } else { cat(" ---> SKIP\n") }


    cat("\n\n 7) Base de Donnees\n----------------------\n")
    flush.console()

    if(postgresql_import) {
        cat("\n - Creation de la base de donnees PostGreSQL :\n")
        createDB_postgres(dateConstruction,nomDBpostgresql,postgresUser,postgresql_createAll,postGIS_initiation,postgresql_abondanceSeuil,seuilAbondance,repertoire,fileTemp)
        cat("\n    -> OK\n")
    } else { cat(" ---> SKIP\n") }


    cat("\n\n 9) Point et Carrees Annee\n----------------------\n")
    flush.console()
    if(pointCarreAnnee) {
        point_carre_annee(dateConstruction=dateConstruction,version=version,con=openDB.PSQL(),importation=importPointCarreAnnee,repertoire=repertoire,nomDBpostgresql=nomDBpostgresql,postgresUser="romain")
    } else { cat(" ---> SKIP\n") }



    cat("\n\n 10) Historic des Carrees\n----------------------\n")
    flush.console()
    if(historiqueCarre & postgresql_import) {
        historicCarre()
    } else { cat(" ---> SKIP\n") }





    end <- Sys.time() ## heure de fin

    cat("\n\n\n     # Fin du process:",format(end, "%Y-%m-%d %H:%M\n"))
    cat("     #      ==> Duree:",round(as.difftime(end - start,units="mins")),"minutes\n")





}


read.data <-  function(file=NULL,decimalSigne=".",encode="utf-8") {
    cat("1) IMPORTATION \n--------------\n")
    cat("<--",file,"\n")
    data <- read.table(file,sep="\t",stringsAsFactors=FALSE,header=TRUE,dec=decimalSigne,fileEncoding=encode)
    ## verification qu'il y a plusieur colonnes et essaye different separateur
    if(ncol(data)==1) {
        data <- read.table(file,sep=";",stringsAsFactors=FALSE,header=TRUE,dec=decimalSigne,fileEncoding=encode)
        if(ncol(data)==1) {
            data <- read.table(file,sep=",",stringsAsFactors=FALSE,header=TRUE,dec=decimalSigne,fileEncoding=encode)
            if(ncol(data)==1) {
                data <- read.table(file,sep=" ",stringsAsFactors=FALSE,header=TRUE,dec=decimalSigne,fileEncoding=encode)
                if(ncol(data)==1) {
                    stop("!!!! L'importation a echoue\n   les seperatateurs de colonne utilise ne sont pas parmi ([tabulation], ';' ',' [espace])\n   -> veuillez verifier votre fichier de donnees\n")
                }
            }
        }
    }

    return(data)
}




#####################################################
## Requete sur la base access de FNat
#####################################################



import_FNat <- function(file="Base FNat2000.MDB",fichierAPlat="FNat_plat_2017-01-04.csv ") {
### Fonction fonctionnant seulement sous windows avec une version 32 bit de R
### ...\bin\i386\Rgui.exe
    library(RODBC)
    library(reshape)
    library(data.table)

    cat("\nConnexion à la base de données ACCESS\n")
    flush.console()
    con <- odbcConnectAccess(file)

    cat("\nRequete principale\nC'est l'heure d'un petit café :-o\n")
    flush.console()
    query <- "SELECT TCCitations.Unique_Inventaire, TCCitations.Unique_Citation,TCInventaires.NumObs, TCInventaires.Etude, TCInventaires.Unique_Localité, TCInventaires.section_cadastrale, TCInventaires.Lieudit, TCInventaires.Pays, TCInventaires.Dept, TCInventaires.INSEE, TCInventaires.DateObs, TCInventaires.Heure, TCInventaires.Durée, TCInventaires.NbrEchantillon, TOObservateurs.Nom, TOObservateurs.EMail, TLCoord.Altitude, TCCitations.Classe, TCCitations.Espèce, TCCitations.NbrInd, TPDistance_Contact.Libellé, TLCoord.Longitude_Lambert_93, TLCoord.Latitude_Lambert_93
FROM TLCoord INNER JOIN (TPDistance_Contact INNER JOIN (TOObservateurs INNER JOIN (TCInventaires INNER JOIN TCCitations ON TCInventaires.Unique_Inventaire = TCCitations.Unique_Inventaire) ON (TOObservateurs.Observateur = TCInventaires.Observateur) AND (TOObservateurs.Organisme = TCInventaires.Organisme)) ON TPDistance_Contact.Code = TCCitations.Distance_Contact) ON TLCoord.Unique_Localité = TCInventaires.Unique_Localité;"
    d1 <- sqlQuery(con,query)

    cat("\nRequete météo\n")
    flush.console()
    query <- "SELECT TCMétéo.Unique_Inventaire, TCMétéo.Code, TCMétéo.Valeur_Discrète
FROM TCMétéo;"

    d_meteo <- sqlQuery(con,query)
    d_meteo <- cast(d_meteo,Unique_Inventaire~Code)[,1:5]

    cat("\nRequetes habitats\n il va encore falloir être un peu patient :-/")
    flush.console()
    query <- "SELECT TLDescEPS.Unique_Localité, TLDescEPS.DateEPS, TLDescEPS.Milieu, TLDescEPS.Type_Milieu, TLDescEPS.Catégorie_1, TLDescEPS.Catégorie_2, TLDescEPS.Sous_Catégorie_1, TLDescEPS.Sous_Catégorie_2
FROM TLDescEPS
WHERE (((TLDescEPS.Habitat)='P'));"
    d_habP <- sqlQuery(con,query)
    colnames(d_habP)[3:ncol(d_habP)]<- paste("hab_p_",colnames(d_habP)[3:ncol(d_habP)],sep="")



    query <- "SELECT TLDescEPS.Unique_Localité, TLDescEPS.DateEPS, TLDescEPS.Milieu, TLDescEPS.Type_Milieu, TLDescEPS.Catégorie_1, TLDescEPS.Catégorie_2, TLDescEPS.Sous_Catégorie_1, TLDescEPS.Sous_Catégorie_2
FROM TLDescEPS
WHERE (((TLDescEPS.Habitat)='S'));"
    d_habS <- sqlQuery(con,query)
    colnames(d_habS)[3:ncol(d_habS)]<- paste("hab_s_",colnames(d_habS)[3:ncol(d_habS)],sep="")

    d_hab <- merge(d_habP,d_habS,by=c("Unique_Localité","DateEPS"),all=TRUE)
    d_hab <- d_hab[order(d_hab$Unique_Localité,d_hab$DateEPS),]



    d2 <- subset(d1,!is.na(NbrInd)& NbrEchantillon < 7)
    d3 <- d2[grep("Point",d2$section_cadastrale),]
    rm(list=c("d1","d2"))




    t3 <- data.table(d3,id="Unique_Citation")
    t_meteo <- data.table(d_meteo, id="Unique_Inventaire")
    t_hab <- data.table(d_hab)
    t_hab$id_hab <- paste(t_hab$Unique_Localité,t_hab$DateEPS,sep="_")
    t3$id_hab <- paste(t3$Unique_Localité,t3$DateObs,sep="_")
    tt <- merge(t3, t_meteo,by="Unique_Inventaire",all.x = TRUE, all.y=FALSE, allow.cartesian=TRUE)

    tt <- merge(tt, t_hab, ,by="id_hab",all.x = TRUE, all.y=FALSE, allow.cartesian=TRUE)


    tt <- subset(tt, select=c("Unique_Inventaire","Unique_Citation","NumObs","Etude","Unique_Localité.x","section_cadastrale","Lieudit","Pays","Dept","INSEE","DateObs","Heure","Durée","NbrEchantillon","Nom","EMail","Altitude","Classe","Espèce","NbrInd","Libellé","Longitude_Lambert_93","Latitude_Lambert_93","EPS-Nuage","EPS-Pluie","EPS-Vent","EPS-Visibilité","hab_p_Milieu","hab_p_Type_Milieu","hab_p_Catégorie_1","hab_p_Catégorie_2","hab_p_Sous_Catégorie_1","hab_p_Sous_Catégorie_2","hab_s_Milieu","hab_s_Type_Milieu","hab_s_Catégorie_1","hab_s_Catégorie_2","hab_s_Sous_Catégorie_1","hab_s_Sous_Catégorie_2"))

    colnames(tt) <- c("unique_inventaire","unique_citation","numobs","etude","unique_localite","section_cadastrale","lieudit","pays","dept","insee","dateobs","heure","duree","nbr_echantillon","nom","email","altitude","classe","espece","nbr_ind","classe_dist","longitude_lambert_93","latitude_lambert_93","nuage","pluie","vent","visibilite","hab_p_milieu","hab_p_type_milieu","hab_p_cat_1","hab_p_cat_2","hab_p_sous_cat_1","hab_p_sous_cat_2","hab_s_milieu","hab_s_type_milieu","hab_s_cat_1","hab_s_cat_2","hab_s_sous_cat_1","hab_s_sous_cat_2"
                      )


    cat("\nEcriture du fichier:", fichierAPlat,"\n Tu peux aller boire un second café\n")
    write.csv(tt,fichierAPlat,row.names=FALSE)

    cat("Exportation OK !!\n")

    odbcClose(con)

}






#####################################################
##  Les fonctions pour les tables issue de VigiePlume
#####################################################




vp2point <- function(d,dateExport,output=FALSE) {
#browser()
    dd <- data.frame(pk_point=paste(substring(d$N..Carré.EPS,9),"P",substring(d$EXPORT_STOC_TEXT_EPS_POINT,nchar(d$EXPORT_STOC_TEXT_EPS_POINT)-1,nchar(d$EXPORT_STOC_TEXT_EPS_POINT)),sep=""),
                     id_carre = substring(d$N..Carré.EPS,9,nchar(d$N..Carré.EPS)),
                     commune = d$Commune,
                     site = d$Site,
                     insee = d$INSEE,
                     departement = d$Département,
                     nom_point = d$EXPORT_STOC_TEXT_EPS_POINT,
                     num_point = as.numeric(substring(d$EXPORT_STOC_TEXT_EPS_POINT,nchar(d$EXPORT_STOC_TEXT_EPS_POINT)-1,nchar(d$EXPORT_STOC_TEXT_EPS_POINT))),
                     altitude = d$Altitude,
                     longitude_wgs84 = d$Longitude,
                     latitude_wgs84 = d$Latitude,
                     db = "vigieplume",date_export=dateExport)

    dd_maj <- aggregate(subset(dd,select=c("commune","site","insee","departement")), list(pk_point=dd$pk_point), function(x) levels(x)[which.max(table(x))])
    dd_med <- aggregate(subset(dd,select=c("altitude","latitude_wgs84","longitude_wgs84")), list(pk_point=dd$pk_point), function(x) median(x) )
    dd_med$altitude <- round(dd_med$altitude)
    dd_point <- unique(subset(dd,select=c("pk_point","id_carre","nom_point","num_point")))

    dd <- merge(dd_point,dd_maj,by="pk_point")
    dd <- merge(dd,dd_med,by="pk_point")

    dd <- data.frame(dd[,c("pk_point","id_carre","commune","site","insee","departement",
                           "nom_point","num_point","altitude",
                           "longitude_wgs84","latitude_wgs84")],
                     db="vigieplume",date_export=dateExport)

    write.csv(dd,paste("DB_import/point_VP_",dateExport,".csv",sep=""),row.names=FALSE)
    if(output) return(dd)
}

vp2carre <- function(d,dateExport,output=FALSE) {
    ## d <- dVP; dateExport = dateExportVP; output=FALSE

    dcarrenat <- read.csv("DB_import/tablesGeneriques/carrenat.csv")
    dcarrenat$pk_carre <- sprintf("%06d",    dcarrenat$pk_carre)


    dd <-  data.frame(pk_carre = substring(d$N..Carré.EPS,9,nchar(d$N..Carré.EPS)),
                      commune = d$Commune,# Commune majoritaire
                      site  = d$Site, # site majoritaire
                      etude = d$Etude,
                      insee = d$INSEE,# Commune majoritaire
                      departement = d$Département, # Departement majoritaire
                      nom_carre = d$N..Carré.EPS,
                      nom_carre_fnat = NA,
                      altitude = d$Altitude,
                      latitude_wgs84 = d$Latitude,#moyenne des points
                      longitude_wgs84 = d$Longitude, #moyenne des points
                      db = "vigieplume",date_export=dateExport)
    dd_etude <- unique(data.frame(pk_carre=dd$pk_carre,etude=as.character("STOC_EPS"),stringsAsFactors=FALSE))
    dd_etude$etude[which(dd_etude$pk_carre %in% as.character(unique(subset(dd,etude=="STOC_ONF")$pk_carre)))] <- "STOC_ONF"

    dd_maj <- aggregate(subset(dd,select=c("commune","site","insee","departement")), list(pk_carre=dd$pk_carre,nom_carre=dd$nom_carre), function(x) levels(x)[which.max(table(x))])
    dd_mean <- aggregate(subset(dd,select=c("altitude","latitude_wgs84","longitude_wgs84")), list(pk_carre=dd$pk_carre), function(x) mean(x) )
    dd <- merge(dd_maj,dd_mean,by="pk_carre")
    dd <- merge(dd,dd_etude,by="pk_carre")


    dd <- merge(dd,dcarrenat,by="pk_carre",all.x=TRUE)


    dd <- data.frame(pk_carre=dd$pk_carre,nom_carre=dd$nom_carre,
                     nom_carre_fnat=NA,commune=dd$commune,site = dd$site,
                     etude = dd$etude,
                     insee=dd$insee,departement=dd$departement,altitude=round(dd$altitude),
                     area = dd$area,perimeter=dd$perimeter,
                     longitude_points_wgs84=dd$longitude_wgs84,latitude_points_wgs84=dd$latitude_wgs84,
                     longitude_grid_wgs84=dd$lon_WGS84, latitude_grid_wgs84=dd$lat_WGS84,
                     db="vigieplume",date_export=dateExport)


    write.csv(dd,paste("DB_import/carre_VP_",dateExport,".csv",sep=""),row.names=FALSE)

    if(output) return(dd)

}

vp2inventaire <- function(d,dateExport,version = "V.1",output=FALSE) {
    dd <- unique(data.frame(pk_inventaire = paste(format(as.Date(d$Date,format="%d.%m.%Y"),"%Y%m%d"),substring(d$N..Carré.EPS,9),"P",substring(d$EXPORT_STOC_TEXT_EPS_POINT,nchar(d$EXPORT_STOC_TEXT_EPS_POINT)-1),sep=""),
                            unique_inventaire_fnat=NA,
                            etude = d$Etude,
                            id_point = paste(substring(d$N..Carré.EPS,9),"P",substring(d$EXPORT_STOC_TEXT_EPS_POINT,nchar(d$EXPORT_STOC_TEXT_EPS_POINT)-1),sep=""),
                            id_carre =  substring(d$N..Carré.EPS,9,nchar(d$N..Carré.EPS)),
                            num_point = as.numeric(substring(d$EXPORT_STOC_TEXT_EPS_POINT,nchar(d$EXPORT_STOC_TEXT_EPS_POINT)-1,nchar(d$EXPORT_STOC_TEXT_EPS_POINT))),
                            date = as.character(as.Date(d$Date,format="%d.%m.%Y")),
                            jour_julien = as.numeric(as.character(format(as.Date(d$Date,format="%d.%m.%Y"),"%j"))),
                            annee=as.numeric(substring(d$Date,7,10)),
                            passage = d$N..Passage,
                            info_passage=NA,
                            passage_stoc=NA,
                            heure_debut = d$Heure,
                            heure_fin = d$Heure.fin,
                            duree_minute = as.numeric(difftime(strptime(d$Heure.fin,"%H:%M"),strptime(d$Heure,"%H:%M"))),
                            observateur = d$Observateur,
                            email = d$Email,
                            nuage = d$EPS.Nuage,
                            pluie = d$EPS.Pluie,
                            vent = d$EPS.Vent,
                            visibilite = d$EPS.Visibilité,
                            neige = d$EPS.Neige,
                            db = "vigieplume",date_export=dateExport,altitude=d$Altitude,version=version))



    aa <- table(dd$pk_inventaire)

    aa_unique <- names(aa)[aa==1]
    aa_doublon <- names(aa)[aa>1]

    if(length(aa_doublon)>0) {


        dd_unique <- subset(dd,pk_inventaire %in% aa_unique)

        ddbrut <- data.frame(pk_inventaire = paste(format(as.Date(d$Date,format="%d.%m.%Y"),"%Y%m%d"),substring(d$N..Carré.EPS,9),"P",substring(d$EXPORT_STOC_TEXT_EPS_POINT,nchar(d$EXPORT_STOC_TEXT_EPS_POINT)-1),sep=""),
                             unique_inventaire_fnat=NA,
                             etude = d$Etude,
                             id_point = paste(substring(d$N..Carré.EPS,9),"P",substring(d$EXPORT_STOC_TEXT_EPS_POINT,nchar(d$EXPORT_STOC_TEXT_EPS_POINT)-1),sep=""),
                             id_carre =  substring(d$N..Carré.EPS,9,nchar(d$N..Carré.EPS)),
                             num_point = as.numeric(substring(d$EXPORT_STOC_TEXT_EPS_POINT,nchar(d$EXPORT_STOC_TEXT_EPS_POINT)-1,nchar(d$EXPORT_STOC_TEXT_EPS_POINT))),
                             date = as.character(as.Date(d$Date,format="%d.%m.%Y")),
                             jour_julien = as.numeric(as.character(format(as.Date(d$Date,format="%d.%m.%Y"),"%j"))),
                             annee=as.numeric(substring(d$Date,7,10)),
                             passage = d$N..Passage,
                             info_passage=NA,
                             passage_stoc=NA,
                             heure_debut = d$Heure,
                             heure_fin = d$Heure.fin,
                             duree_minute = as.numeric(difftime(strptime(d$Heure.fin,"%H:%M"),strptime(d$Heure,"%H:%M"))),
                             observateur = d$Observateur,
                             email = d$Email,
                             nuage = d$EPS.Nuage,
                             pluie = d$EPS.Pluie,
                             vent = d$EPS.Vent,
                             visibilite = d$EPS.Visibilité,
                             neige = d$EPS.Neige,
                             db = "vigieplume",date_export=dateExport,altitude=d$Altitude,version=version)

        dd_doub <- subset(ddbrut,pk_inventaire %in% aa_doublon)



        col_num2char <- c("unique_inventaire_fnat","passage",
                          "duree_minute","nuage","pluie","vent","visibilite","neige","altitude")
        col_as.factor <- c("unique_inventaire_fnat","passage",
                           "heure_debut","heure_fin","duree_minute",
                           "observateur","email",
                           "nuage","pluie","vent","visibilite","neige","altitude")

        for(col in col_num2char){
            dd_doub[,col] <- as.character(dd_doub[,col])
            dd_doub[,col] <- ifelse(is.na(dd_doub[,col]),"-99999",dd_doub[,col])

        }



        for(col in col_as.factor){
            dd_doub[,col] <- as.factor(dd_doub[,col])
        }


        dd_inv <- unique(subset(dd_doub,select =c("pk_inventaire","id_point",
                                                  "id_carre","num_point","date","jour_julien",
                                                  "annee")))

        dd_maj <- aggregate(subset(dd_doub,select=c("passage",
                                                    "heure_debut","heure_fin","etude",
                                                    "observateur","email",
                                                    "nuage","pluie","vent","visibilite","neige","altitude")),
                            list(pk_inventaire=dd_doub$pk_inventaire),
                            function(x) levels(x)[which.max(table(x))])


        ddd <- merge(dd_inv,dd_maj,by="pk_inventaire")

        ddd <- data.frame(pk_inventaire=ddd$pk_inventaire,unique_inventaire_fnat=NA,etude=ddd$etude,
                          id_point=ddd$id_point,id_carre=ddd$id_carre,num_point=ddd$num_point,
                          date=ddd$date,jour_julien=ddd$jour_julien,annee=ddd$annee,
                          passage=ddd$passage,info_passage=NA,passage_stoc = NA,
                          heure_debut=ddd$heure_debut,heure_fin=ddd$heure_fin,duree_minute=NA,
                          observateur=ddd$observateur,email=ddd$email,
                          nuage=ddd$nuage,pluie=ddd$pluie,vent=ddd$vent,visibilite=ddd$visibilite,neige=ddd$neige,
                          db="FNat",date_export=dateExport,altitude=ddd$altitude,version=version)


        for(col in col_num2char){
            ddd[,col] <-  as.numeric(as.character(ddd[,col]))
            ddd[,col] <- ifelse(ddd[,col] == -99999,NA,ddd[,col])
        }


        dd <- rbind(dd_unique,ddd)
    }
    dd <- dd[order(as.character(dd$pk_inventaire)),]

    dd$duree_minute <- as.numeric(difftime(strptime(dd$heure_fin,"%H:%M"),strptime(dd$heure_debut,"%H:%M")))




    dd$info_passage[dd$jour_julien<90] <- "precoce"
    dd$passage_stoc[dd$jour_julien>=90 & dd$jour_julien<=130 & dd$altitude < 800] <- 1
    dd$passage_stoc[dd$jour_julien>=90 & dd$jour_julien<=137 & dd$altitude >= 800] <- 1 ## pour les STOC en altitude
    dd$passage_stoc[dd$jour_julien>=131 & dd$jour_julien<=175 & dd$altitude < 800] <- 2
    dd$passage_stoc[dd$jour_julien>=138 & dd$jour_julien<=175 & dd$altitude >= 800] <- 2 ## pour les STOC en altitude
    dd$info_passage[dd$passage_stoc==2 | dd$passage_stoc==1] <- "normal"
    dd$info_passage[dd$jour_julien>175] <- "tardif"

    dd$id_ancar <- paste(dd$id_point,dd$annee,sep="_")

    ddNB <- unique(subset(dd, select=c("id_ancar","jour_julien")))
    tcont <-  data.frame(table(ddNB$id_ancar))
    colnames(tcont) <- c("id_ancar","nombre_de_passage")
    dd <- merge(dd,tcont,by="id_ancar",all=TRUE)



    ddunique <- unique(subset(dd, dd$info_passage == "normal", select=c("id_ancar","jour_julien")))
    tcont <-  table(ddunique$id_ancar)
    passageUnique <-  names(tcont)[tcont==1]
    dd$info_passage[dd$id_ancar %in% passageUnique] <- "passage_unique"

    dd1 <- unique(subset(dd,passage_stoc==1,select=c("id_ancar","jour_julien","passage_stoc")))
    tdd1 <- table(dd1$id_ancar)
    passageEnTrop <-  names(tdd1)[tdd1>1]
    dd$info_passage[dd$id_ancar %in% passageEnTrop & dd$passage_stoc == 1] <- "plusieurs_passage_1"

    ddpp1 <- subset(dd,info_passage=="plusieurs_passage_1")
    maxP1 <- aggregate(jour_julien~id_ancar,data=ddpp1,max)
    maxP1$id_ancar_valide <- paste(maxP1$id_ancar,maxP1$jour_julien,sep="_")
    dd$info_passage[paste(dd$id_ancar,dd$jour_julien,sep="_") %in% maxP1$id_ancar_valide & dd$passage_stoc == 1] <- "dernier_passage_1"
    dd$passage_stoc[!(paste(dd$id_ancar,dd$jour_julien,sep="_") %in% maxP1$id_ancar_valide) & dd$info_passage == "plusieurs_passage_1" & dd$passage_stoc == 1] <- NA



    dd2 <- unique(subset(dd,passage_stoc==2,select=c("id_ancar","jour_julien","passage_stoc")))
    tdd2 <- table(dd2$id_ancar)
    passageEnTrop <-  names(tdd2)[tdd2>1]
    dd$info_passage[dd$id_ancar %in% passageEnTrop  & dd$passage_stoc == 2] <- "plusieurs_passage_2"

    ddpp2 <- subset(dd,info_passage=="plusieurs_passage_2")
    maxP2 <- aggregate(jour_julien~id_ancar,data=ddpp2,max)
    maxP2$id_ancar_valide <- paste(maxP2$id_ancar,maxP2$jour_julien,sep="_")
    dd$info_passage[paste(dd$id_ancar,dd$jour_julien,sep="_") %in% maxP2$id_ancar_valide & dd$passage_stoc == 2] <- "dernier_passage_2"
    dd$passage_stoc[!(paste(dd$id_ancar,dd$jour_julien,sep="_") %in% maxP2$id_ancar_valide) & dd$info_passage == "plusieurs_passage_2" & dd$passage_stoc == 2] <- NA


    ddNorm <- unique(subset(dd,info_passage=="normal" & (passage_stoc==1 | passage_stoc==2),select=c("id_ancar","jour_julien","passage_stoc")))
    ddNormW <- reshape(ddNorm
                      ,v.names="jour_julien"
                      ,idvar=c("id_ancar")
                      ,timevar="passage_stoc"
                      ,direction="wide")

    ddNormW$temps_entre_passage = abs(ddNormW[,3] - ddNormW[,2])

    ddNormW <- subset(ddNormW,select=c("id_ancar","temps_entre_passage"))

    dd <- merge(dd,ddNormW,by="id_ancar",all=TRUE)
    dd$info_entre_passage<- "normal"
    dd$info_entre_passage[dd$temps_entre_passage<28] <- "passages_rapproche"
    dd$info_entre_passage[dd$temps_entre_passage>42] <- "passages_eloigne"


    dd<-dd[,c("pk_inventaire","unique_inventaire_fnat","id_carre","id_point","num_point","etude","annee","date","jour_julien","passage","info_passage","passage_stoc","nombre_de_passage","temps_entre_passage","info_entre_passage","heure_debut","heure_fin","duree_minute","observateur","email","nuage","pluie","vent","visibilite","neige","db","date_export","version")]


    write.csv(dd,paste("DB_import/inventaire_VP_",dateExport,".csv",sep=""),row.names=FALSE)

    if(output) return(dd)

}



vp2observation <- function(d,dateExport,output=FALSE) {
    dd <- data.frame(pk_observation= paste(format(as.Date(d$Date,format="%d.%m.%Y"),"%Y%m%d"),substring(d$N..Carré.EPS,9),"P",substring(d$EXPORT_STOC_TEXT_EPS_POINT,nchar(d$EXPORT_STOC_TEXT_EPS_POINT)-1),sep=""),
                     id_fnat_unique_citation = NA,
                     id_inventaire =paste(format(as.Date(d$Date,format="%d.%m.%Y"),"%Y%m%d"),substring(d$N..Carré.EPS,9),"P",substring(d$EXPORT_STOC_TEXT_EPS_POINT,nchar(d$EXPORT_STOC_TEXT_EPS_POINT)-1),sep=""),
                     id_point = paste(substring(d$N..Carré.EPS,9,nchar(d$N..Carré.EPS)),"P",substring(d$EXPORT_STOC_TEXT_EPS_POINT,nchar(d$EXPORT_STOC_TEXT_EPS_POINT)-1,nchar(d$EXPORT_STOC_TEXT_EPS_POINT)),sep=""),
                     id_carre =  substring(d$N..Carré.EPS,9,nchar(d$N..Carré.EPS)),
                     num_point = as.numeric(substring(d$EXPORT_STOC_TEXT_EPS_POINT,nchar(d$EXPORT_STOC_TEXT_EPS_POINT)-1,nchar(d$EXPORT_STOC_TEXT_EPS_POINT))),
                     passage = d$N..Passage,
                     date = as.character(as.Date(d$Date,format="%d.%m.%Y")),
                     annee=as.numeric(substring(d$Date,7,10)),
                     classe=d$Classe,
                     espece = toupper(d$Espèce),
                     code_sp = substring(toupper(d$Espèce),1,6),
                     abondance = as.numeric(as.character(d$Nombre)),
                     distance_contact=d$Distance.de.contact,
                     db = "vigieplume",date_export=dateExport,
                     id_data=paste(substring(d$Date,7,10),d$N..Passage,substring(d$N..Carré.EPS,9,nchar(d$N..Carré.EPS)),"P",substring(d$EXPORT_STOC_TEXT_EPS_POINT,nchar(d$EXPORT_STOC_TEXT_EPS_POINT)-1,nchar(d$EXPORT_STOC_TEXT_EPS_POINT)),substring(d$Espèce,1,6),d$Distance.de.contact,sep="")
                     )

    dd <- dd[order(dd$id_inventaire,dd$espece,dd$distance_contact),]
    iFirst_id_inventaire <- setNames(match(unique(dd$id_inventaire),dd$id_inventaire),unique(dd$id_inventaire))
    dd$pk_observation <- paste(dd$pk_observation,sprintf("%02d",(1:nrow(dd))-iFirst_id_inventaire[as.character(dd$id_inventaire)]+1),sep="")

    contDoublon <- table(dd$id_data)
    doublon <- names(contDoublon)[contDoublon>1]
    if(length(doublon)>0) {
        tDoublon <- subset(dd,id_data %in% doublon)
        file <- paste("OutputImport/_doublonInventaireVigiePlume_",dateExport,".csv",sep="")
        cat("  \n !!! Doublon saisies \n c est a dire plusieurs lignes pour un inventaire une espece et une distance\n Ceci concerne ", nrow(tDoublon)," lignes\n")
        cat("  Toutes lignes sont présenté dans le fichier: \n  --> ",file,"\n")
        write.csv(tDoublon[,-ncol(tDoublon)],file,row.names=FALSE)

        cat(" \n Pour chaque doublon nous conservons la valeur maximum\n")



        tAbMax <- aggregate(subset(tDoublon,select=c("abondance")),list(id_data=tDoublon$id_data),max)
        tAbMax$conservee <- TRUE

        tDoublon <- merge(tDoublon,tAbMax,by=c("id_data","abondance"),all=TRUE)
        tDoublon$conservee[is.na(tDoublon$conservee)] <- FALSE

        dataExclues <- subset(tDoublon,!conservee)
        dataEclues <-subset(dataExclues,select=c("pk_observation","id_fnat_unique_citation","id_inventaire","id_point","id_carre","num_point","passage","date","annee","classe","espece","code_sp","distance_contact","abondance","db","date_export"))

        file <- paste("OutputImport/_doublonInventaireVigiePlume_Exclu_",dateExport,".csv",sep="")
        cat("  \n !!! Doublon exclue enregistre dans le fichier \n --> ",file,"\n")
        write.csv(tDoublon[,-ncol(tDoublon)],file,row.names=FALSE)

        dd <- subset(dd,!(pk_observation %in% tDoublon$pk_observation[!tDoublon$conservee]))

        dd <- dd[order(dd$id_inventaire,dd$espece,dd$distance_contact),]
    }

    dd <- dd[,-ncol(dd)]
    write.csv(dd,paste("DB_import/observation_VP_",dateExport,".csv",sep=""),row.names=FALSE)

    if(output) return(dd)

}


vp2habitat <- function(d,dateExport,output=FALSE) {

    dd <- unique(data.frame(pk_habitat=paste(format(as.Date(d$Date,format="%d.%m.%Y"),"%Y%m%d"),
                                             substring(d$N..Carré.EPS,9,nchar(d$N..Carré.EPS)),"P",substring(d$EXPORT_STOC_TEXT_EPS_POINT,nchar(d$EXPORT_STOC_TEXT_EPS_POINT)-1,nchar(d$EXPORT_STOC_TEXT_EPS_POINT)),sep=""),
                            id_point = paste(substring(d$N..Carré.EPS,9,nchar(d$N..Carré.EPS)),"P",substring(d$EXPORT_STOC_TEXT_EPS_POINT,nchar(d$EXPORT_STOC_TEXT_EPS_POINT)-1,nchar(d$EXPORT_STOC_TEXT_EPS_POINT)),sep=""),
                            passage = d$N..Passage,
                            date = as.character(as.Date(d$Date,format="%d.%m.%Y")),
                            annee=as.numeric(substring(d$Date,7,10)),
                            p_milieu=toupper(ifelse(d$EPS.P.Milieu=="",NA,d$EPS.P.Milieu)),
                            p_type=ifelse(d$EPS.P.Type=="",NA,d$EPS.P.Type),
                            p_cat1=ifelse(d$EPS.P.Cat1=="",NA,d$EPS.P.Cat1),
                            p_cat2 = ifelse(d$EPS.P.Cat2=="",NA,d$EPS.P.Cat2),
                            s_milieu=toupper(ifelse(d$EPS.S.Milieu=="",NA,d$EPS.S.Milieu)),
                            s_type=ifelse(d$EPS.S.Type=="",NA,d$EPS.S.Type),
                            s_cat1=ifelse(d$EPS.S.Cat1=="",NA,d$EPS.S.Cat1),
                            s_cat2 = ifelse(d$EPS.S.Cat2=="",NA,d$EPS.S.Cat2),
                            db = "vigieplume",date_export=dateExport ))

    aa <- table(dd$pk_habitat)

    aa_unique <- names(aa)[aa==1]
    aa_doublon <- names(aa)[aa>1]

    if(length(aa_doublon)>0) {


        dd_unique <- subset(dd,pk_habitat %in% aa_unique)

        ddbrut <- data.frame(pk_habitat=paste(format(as.Date(d$Date,format="%d.%m.%Y"),"%Y%m%d"),
                                              substring(d$N..Carré.EPS,9,nchar(d$N..Carré.EPS)),"P",substring(d$EXPORT_STOC_TEXT_EPS_POINT,nchar(d$EXPORT_STOC_TEXT_EPS_POINT)-1,nchar(d$EXPORT_STOC_TEXT_EPS_POINT)),sep=""),
                             id_point = paste(substring(d$N..Carré.EPS,9,nchar(d$N..Carré.EPS)),"P",substring(d$EXPORT_STOC_TEXT_EPS_POINT,nchar(d$EXPORT_STOC_TEXT_EPS_POINT)-1,nchar(d$EXPORT_STOC_TEXT_EPS_POINT)),sep=""),
                             passage = d$N..Passage,
                             date = as.character(as.Date(d$Date,format="%d.%m.%Y")),
                             annee=as.numeric(substring(d$Date,7,10)),
                             p_milieu=toupper(ifelse(d$EPS.P.Milieu=="",NA,d$EPS.P.Milieu)),
                             p_type=ifelse(d$EPS.P.Type=="",NA,d$EPS.P.Type),
                             p_cat1=ifelse(d$EPS.P.Cat1=="",NA,d$EPS.P.Cat1),
                             p_cat2 = ifelse(d$EPS.P.Cat2=="",NA,d$EPS.P.Cat2),
                             s_milieu=toupper(ifelse(d$EPS.S.Milieu=="",NA,d$EPS.S.Milieu)),
                             s_type=ifelse(d$EPS.S.Type=="",NA,d$EPS.S.Type),
                             s_cat1=ifelse(d$EPS.S.Cat1=="",NA,d$EPS.S.Cat1),
                             s_cat2 = ifelse(d$EPS.S.Cat2=="",NA,d$EPS.S.Cat2),
                             db = "vigieplume",date_export=dateExport )
        dd_doub <- subset(ddbrut,pk_habitat %in% aa_doublon)



        col_num2char <- c("passage","p_type","p_cat1","p_cat2",
                          "s_type","s_cat1","s_cat2")
        col_as.factor <- c("passage",
                           "p_type","p_cat1","p_cat2",
                           "s_type","s_cat1","s_cat2",
                           "p_milieu","s_milieu")

        for(col in col_num2char){
            dd_doub[,col] <- as.character(dd_doub[,col])
            dd_doub[,col] <- ifelse(is.na(dd_doub[,col]),"-99999",dd_doub[,col])

        }



        for(col in col_as.factor){
            dd_doub[,col] <- as.factor(dd_doub[,col])
        }


        dd_inv <- unique(subset(dd_doub,select =c("pk_habitat","id_point","date","annee")))

        dd_maj <- aggregate(subset(dd_doub,select=c("passage","p_type","p_cat1","p_cat2",
                                                    "s_type","s_cat1","s_cat2",
                                                    "p_milieu","s_milieu")),
                            list(pk_habitat=dd_doub$pk_habitat),
                            function(x) levels(x)[which.max(table(x))])


        ddd <- merge(dd_inv,dd_maj,by="pk_habitat")

        ddd <- data.frame(pk_habitat= ddd$pk_habitat,
                          id_point = ddd$id_point,
                          passage = ddd$passage,
                          date = ddd$date,
                          annee= ddd$annee,
                          p_milieu=ddd$p_milieu,
                          p_type=ddd$p_type,
                          p_cat1=ddd$p_cat1,
                          p_cat2 = ddd$p_cat2,
                          s_milieu=ddd$s_milieu,
                          s_type=ddd$s_type,
                          s_cat1=ddd$s_cat1,
                          s_cat2 = ddd$s_cat2,
                          db = "vigieplume",date_export=dateExport)


        for(col in col_num2char){
            ddd[,col] <-  as.numeric(as.character(ddd[,col]))
            ddd[,col] <- ifelse(ddd[,col] == -99999,NA,ddd[,col])
        }


        dd <- rbind(dd_unique,ddd)
    }
    dd <- dd[order(as.character(dd$pk_habitat)),]



    write.csv(dd,paste("DB_import/habitat_VP_",dateExport,".csv",sep=""),row.names=FALSE)

    if(output) return(dd)

}





#####################################################
##  Les fonctions pour les tables issue de FNat
#####################################################




FNat2point <- function(d,dateExport,output=TRUE) {
                                        #   d <- dFNat
                                        #   dateExport <- dateExportFNat
                                        #    output=TRUE

    library(rgdal)
    dcoord <- data.frame(lon=d$longitude_lambert_93, lat=d$latitude_lambert_93)
    coordinates(dcoord) <- c("lon", "lat")
    proj4string(dcoord) <- CRS("+init=epsg:2154") # Lambert 93  27582
    dcoord_WGS84 <- spTransform(dcoord, CRS("+init=epsg:4326"))


    dd <- data.frame(pk_point=paste(d$id_carre,"P",substring(d$section_cadastrale,nchar(d$section_cadastrale)-1),sep=""),
                     id_carre =d$id_carre,
                     commune = NA,
                     insee = as.factor(ifelse(d$insee != 0,d$insee,NA)),
                     site = NA,
                     departement = as.factor(d$dept),
                     nom_point = d$section_cadastrale,
                     num_point = as.numeric(substring(d$section_cadastrale,nchar(d$section_cadastrale)-1)),
                     altitude = d$altitude,
                     latitude_wgs84 = dcoord_WGS84$lat,
                     longitude_wgs84 = dcoord_WGS84$lon,
                     db = "FNat",date_export=dateExport)


    dd_maj <- aggregate(subset(dd,select=c("insee","departement")), list(pk_point=dd$pk_point), function(x) levels(x)[which.max(table(x))])
    dd_maj$insee <- as.numeric(as.character(dd_maj$insee))
    dd_maj$insee <- ifelse(dd_maj$insee != 0,dd_maj$insee,NA)
    dd$maj$departement <- as.character(dd_maj$departement)
    ## suppression des coordonnee trop au sud
    dd$longitude_wgs84[dd$latitude_wgs84<38] <- NA
    dd$latitude_wgs84[dd$latitude_wgs84<38] <- NA
    dd$longitude_wgs84[dd$longitude_wgs84>11] <- NA
    dd$latitude_wgs84[dd$longitude_wgs84>11] <- NA

    dd_med <- aggregate(subset(dd,select=c("altitude","latitude_wgs84","longitude_wgs84")), list(pk_point=dd$pk_point), function(x) median(x) )
    dd_point <- unique(subset(dd,select=c("pk_point","id_carre","nom_point","num_point")))

    dd <- merge(dd_point,dd_maj,by="pk_point")
    dd <- merge(dd,dd_med,by="pk_point")



    dd <- data.frame(pk_point=dd$pk_point,
                     id_carre = dd$id_carre,
                     commune = NA,
                     insee = dd$insee,
                     site = NA,
                     departement = dd$departement,
                     nom_point = dd$nom_point,
                     num_point = dd$num_point,
                     altitude = round(dd$altitude),
                     longitude_wgs84 = dd$longitude_wgs84,
                     latitude_wgs84 = dd$latitude_wgs84,
                     db = "FNat",date_export=dateExport)
    write.csv(dd,paste("DB_import/point_FNat_",dateExport,".csv",sep=""),row.names=FALSE)

    if(output) return(dd)
}



FNat2carre <- function(d,dateExport,output=FALSE) {
    library(rgdal)
    ## d = dFNat;dateExport=dateExportFNat;output=FALSE
    dcarrenat <- read.csv("DB_import/tablesGeneriques/carrenat.csv")
    dcarrenat$pk_carre <- sprintf("%06d",    dcarrenat$pk_carre)


    dcoord <- data.frame(lon=d$longitude_lambert_93, lat=d$latitude_lambert_93)
    coordinates(dcoord) <- c("lon", "lat")
    proj4string(dcoord) <- CRS("+init=epsg:2154") # Lambert 93
    dcoord_WGS84 <- spTransform(dcoord, CRS("+init=epsg:4326"))

    dd <- data.frame(pk_carre =d$id_carre,
                     nom_carre = paste("Carré EPS N°",d$id_carre,sep=""),
                     nom_carre_fnat = d$lieudit,
                     etude = d$etude,
                     commune = "",
                     insee = ifelse(d$insee != 0,d$insee,""),
                     departement = d$dept,
                     altitude = d$altitude,
                     latitude_wgs84 = dcoord_WGS84$lat,
                     longitude_wgs84 = dcoord_WGS84$lon)

                                        #  dd$nom_carre_fnat <- gsub("n", "N", dd$nom_carre_fnat) # uniformisation des lieudit de FNat



    dd <- merge(dd,dcarrenat,by="pk_carre",all.x=TRUE)

     dd_etude <- unique(data.frame(pk_carre=dd$pk_carre,etude=as.character("STOC_EPS"),stringsAsFactors=FALSE))
    dd_etude$etude[which(dd_etude$pk_carre %in% as.character(unique(subset(dd,etude=="STOC_ONF")$pk_carre)))] <- "STOC_ONF"

    dd_maj <- aggregate(subset(dd,select=c("commune","insee","departement")), list(pk_carre=dd$pk_carre), function(x) levels(x)[which.max(table(x))])
    dd_nomcarre <- aggregate(dd$nom_carre_fnat, list(pk_carre=dd$pk_carre), function(x) levels(x)[which.max(table(x))])
    colnames(dd_nomcarre)[2] <- "nom_carre_fnat"
    dd_maj <- merge(dd_maj,dd_nomcarre,by="pk_carre",all=TRUE)

    dd$longitude_wgs84[dd$latitude_wgs84<38] <- NA
    dd$latitude_wgs84[dd$latitude_wgs84<38] <- NA
    dd$longitude_wgs84[dd$longitude_wgs84>11] <- NA
    dd$latitude_wgs84[dd$longitude_wgs84>11] <- NA

    dd_mean <- aggregate(subset(dd,select=c("altitude","latitude_wgs84","longitude_wgs84")), list(pk_carre=dd$pk_carre), function(x) mean(x) )
    dd_nom <- unique(subset(dd,select = c("pk_carre","nom_carre")))

    dd <- merge(dd_maj,dd_mean,by="pk_carre")
    dd <- merge(dd,dd_etude,by="pk_carre")
    dd <- merge(dd,dd_nom,by="pk_carre")

    ##   de <- unique(data.frame(pk_carre=id_carre))
    ##    iMammif <- grep("MAMMIF",de$etude)
    ##    iEPS <- grep("EPS",de$etude)
    ##    iEPS <- setdiff(iEPS,iMammif)
    ##    de <- de[iEPS,]

    ##    de <- data.frame(pk_carre=de$pk_carre,carre_stoc=TRUE)
    ##   dd <- merge(dd,de,by="pk_carre",all=TRUE)

    ##   dd$carre_stoc[is.na(dd$carre_stoc)] <- FALSE
    dd$commune[dd$commune==""] <- NA
    dd$insee[dd$insee==""] <- NA


    dd <- merge(dd,dcarrenat,by="pk_carre",all.x=TRUE)


    dd <- data.frame(pk_carre=dd$pk_carre,nom_carre=dd$nom_carre,
                     nom_carre_fnat=dd$nom_carre_fnat,commune=dd$commune,site = NA,etude=dd$etude,
                     insee=dd$insee,departement=dd$departement,altitude=round(dd$altitude),
                     area = dd$area,perimeter=dd$perimeter,
                     longitude_points_wgs84=dd$longitude_wgs84,latitude_points_wgs84=dd$latitude_wgs84,
                     longitude_grid_wgs84=dd$lon_WGS84,latitude_grid_wgs84=dd$lat_WGS84,
                     db="FNat",date_export=dateExport)


    write.csv(dd,paste("DB_import/carre_FNat_",dateExport,".csv",sep=""),row.names=FALSE)

    if(output) return(dd)



}




FNat2inventaire <-  function(d,dateExport,version = "V.1",output=FALSE) {
    library(lubridate)

    ##d = dFNat; dateExport = dateExportFNat ;verion = "VV";output=FALSE

    dd <- unique(data.frame(pk_inventaire = paste(d$dateobs,d$id_carre,"P",substring(d$section_cadastrale,nchar(d$section_cadastrale)-1),sep=""),
                            unique_inventaire_fnat=d$unique_inventaire,
                            etude = d$etude,
                            id_point = paste(d$id_carre,"P",substring(d$section_cadastrale,nchar(d$section_cadastrale)-1),sep=""),
                            id_carre =  d$id_carre,
                            num_point = as.numeric(substring(d$section_cadastrale,nchar(d$section_cadastrale)-1)),
                            date = as.character(as.Date(as.character(d$dateobs),format="%Y%m%j")),
                            jour_julien = as.numeric(as.character(format(as.Date(as.character(d$dateobs),format="%Y%m%j"),"%j"))),
                            annee=as.numeric(substring(as.character(d$dateobs),1,4)),
                            passage = d$nbr_echantillon,
                            info_passage=NA,
                            heure_debut = format(strptime(d$heure,"%H%M"),"%H:%M"),
                            heure_fin = format(strptime(d$heure,"%H%M")+as.difftime(paste("00:",sprintf("%02d", d$duree),sep=""),"%H:%M"),"%H:%M"),
                            duree_minute = d$duree,
                            observateur = d$nom,
                            email = d$email,
                            nuage = d$nuage,
                            pluie = d$pluie,
                            vent = d$vent,
                            visibilite = d$visibilite,
                            neige = NA,
                            db = "FNat",date_export=dateExport,altitude = d$altitude,version=version))


    dd$altitude <-ifelse(is.na(dd$altitude),0,dd$altitude)


    aa <- table(dd$pk_inventaire)

    aa_unique <- names(aa)[aa==1]
    aa_doublon <- names(aa)[aa>1]

    if(length(aa_doublon)>0) {

        dd_unique <- subset(dd,pk_inventaire %in% aa_unique)

        ddbrut <- data.frame(pk_inventaire = paste(d$dateobs,d$id_carre,"P",substring(d$section_cadastrale,nchar(d$section_cadastrale)-1),sep=""),
                             unique_inventaire_fnat=d$unique_inventaire,
                             etude = d$etude,
                             id_point = paste(d$id_carre,"P",substring(d$section_cadastrale,nchar(d$section_cadastrale)-1),sep=""),
                             id_carre =  d$id_carre,
                             num_point = as.numeric(substring(d$section_cadastrale,nchar(d$section_cadastrale)-1)),
                             date = as.character(as.Date(as.character(d$dateobs),format="%Y%m%j")),
                             jour_julien = as.numeric(as.character(format(as.Date(as.character(d$dateobs),format="%Y%m%j"),"%j"))),
                             annee=as.numeric(substring(as.character(d$dateobs),1,4)),
                             passage = d$nbr_echantillon,
                             info_passage=NA,
                             heure_debut = format(strptime(d$heure,"%H%M"),"%H:%M"),
                             heure_fin = format(strptime(d$heure,"%H%M")+as.difftime(paste("00:",sprintf("%02d", d$duree),sep=""),"%H:%M"),"%H:%M"),
                             duree_minute = d$duree,
                             observateur = d$nom,
                             email = d$email,
                             nuage = d$nuage,
                             pluie = d$pluie,
                             vent = d$vent,
                             visibilite = d$visibilite,
                             neige = NA,
                             db = "FNat",date_export=dateExport,altitude = d$altitude,version=version)


        dd_doub <- subset(ddbrut,pk_inventaire %in% aa_doublon)



        col_num2char <- c("unique_inventaire_fnat","passage",
                          "duree_minute","nuage","pluie","vent","visibilite","altitude")
        col_as.factor <- c("unique_inventaire_fnat","passage",
                           "heure_debut","duree_minute",
                           "observateur","email",
                           "nuage","pluie","vent","visibilite","altitude")

        for(col in col_num2char){
            dd_doub[,col] <- as.character(dd_doub[,col])
            dd_doub[,col] <- ifelse(is.na(dd_doub[,col]),"-99999",dd_doub[,col])

        }



        for(col in col_as.factor){
            dd_doub[,col] <- as.factor(dd_doub[,col])
        }


        dd_inv <- unique(subset(dd_doub,select =c("pk_inventaire","id_point",
                                                  "id_carre","num_point","date","jour_julien",
                                                  "annee")))

        dd_maj <- aggregate(subset(dd_doub,select=c("unique_inventaire_fnat","passage","etude",
                                                    "heure_debut","duree_minute",
                                                    "observateur","email",
                                                    "nuage","pluie","vent","visibilite","altitude")),
                            list(pk_inventaire=dd_doub$pk_inventaire),
                            function(x) levels(x)[which.max(table(x))])


        ddd <- merge(dd_inv,dd_maj,by="pk_inventaire")

        ddd <- data.frame(pk_inventaire=ddd$pk_inventaire,unique_inventaire_fnat=ddd$unique_inventaire_fnat,etude=ddd$etude,
                          id_point=ddd$id_point,id_carre=ddd$id_carre,num_point=ddd$num_point,
                          date=ddd$date,jour_julien=ddd$jour_julien,annee=ddd$annee,
                          passage=ddd$passage,info_passage=NA,
                          heure_debut=ddd$heure_debut,heure_fin=NA,duree_minute=ddd$duree_minute,
                          observateur=ddd$observateur,email=ddd$email,
                          nuage=ddd$nuage,pluie=ddd$pluie,vent=ddd$vent,visibilite=ddd$visibilite,neige= NA,
                          db="FNat",date_export=dateExport,altitude=ddd$altitude,version=version)


        for(col in col_num2char){
            ddd[,col] <-  as.numeric(as.character(ddd[,col]))
            ddd[,col] <- ifelse(ddd[,col] == -99999,NA,ddd[,col])
        }


        dd <- rbind(dd_unique,ddd)
    }
    dd <- dd[order(as.character(dd$pk_inventaire)),]

    dd$heure_fin <- format(strptime(dd$heure_debut,"%H:%M")+minutes(dd$duree_minute),"%H:%M")


    dd$info_passage[dd$jour_julien<90] <- "precoce"
    dd$passage_stoc[dd$jour_julien>=90 & dd$jour_julien<=130 & dd$altitude < 800] <- 1
    dd$passage_stoc[dd$jour_julien>=90 & dd$jour_julien<=137 & dd$altitude >= 800] <- 1 ## pour les STOC en altitude
    dd$passage_stoc[dd$jour_julien>=131 & dd$jour_julien<=170 & dd$altitude   < 800] <- 2
    dd$passage_stoc[dd$jour_julien>=138 & dd$jour_julien<=170 & dd$altitude  >= 800] <- 2 ## pour les STOC en altitude
    dd$info_passage[dd$passage_stoc==2 | dd$passage_stoc==1] <- "normal"
    dd$info_passage[dd$jour_julien>170] <- "tardif"

    dd$id_ancar <- paste(dd$id_point,dd$annee,sep="_")

    ddNB <- unique(subset(dd, select=c("id_ancar","jour_julien")))
    tcont <-  data.frame(table(ddNB$id_ancar))
    colnames(tcont) <- c("id_ancar","nombre_de_passage")
    dd <- merge(dd,tcont,by="id_ancar",all=TRUE)



    ddunique <- unique(subset(dd, dd$info_passage == "normal", select=c("id_ancar","jour_julien")))
    tcont <-  table(ddunique$id_ancar)
    passageUnique <-  names(tcont)[tcont==1]
    dd$info_passage[dd$id_ancar %in% passageUnique] <- "passage_unique"

    dd1 <- unique(subset(dd,passage_stoc==1,select=c("id_ancar","jour_julien","passage_stoc")))
    tdd1 <- table(dd1$id_ancar)
    passageEnTrop <-  names(tdd1)[tdd1>1]
    dd$info_passage[dd$id_ancar %in% passageEnTrop & dd$passage_stoc == 1] <- "plusieurs_passage_1"

    ddpp1 <- subset(dd,info_passage=="plusieurs_passage_1")
    maxP1 <- aggregate(jour_julien~id_ancar,data=ddpp1,max)
    maxP1$id_ancar_valide <- paste(maxP1$id_ancar,maxP1$jour_julien,sep="_")
    dd$info_passage[paste(dd$id_ancar,dd$jour_julien,sep="_") %in% maxP1$id_ancar_valide & dd$passage_stoc == 1] <- "dernier_passage_1"
    dd$passage_stoc[!(paste(dd$id_ancar,dd$jour_julien,sep="_") %in% maxP1$id_ancar_valide) & dd$info_passage == "plusieurs_passage_1" & dd$passage_stoc == 1] <- NA



    dd2 <- unique(subset(dd,passage_stoc==2,select=c("id_ancar","jour_julien","passage_stoc")))
    tdd2 <- table(dd2$id_ancar)
    passageEnTrop <-  names(tdd2)[tdd2>1]
    dd$info_passage[dd$id_ancar %in% passageEnTrop  & dd$passage_stoc == 2] <- "plusieurs_passage_2"

    ddpp2 <- subset(dd,info_passage=="plusieurs_passage_2")
    maxP2 <- aggregate(jour_julien~id_ancar,data=ddpp2,max)
    maxP2$id_ancar_valide <- paste(maxP2$id_ancar,maxP2$jour_julien,sep="_")
    dd$info_passage[paste(dd$id_ancar,dd$jour_julien,sep="_") %in% maxP2$id_ancar_valide & dd$passage_stoc == 2] <- "dernier_passage_2"
    dd$passage_stoc[!(paste(dd$id_ancar,dd$jour_julien,sep="_") %in% maxP2$id_ancar_valide) & dd$info_passage == "plusieurs_passage_2" & dd$passage_stoc == 2] <- NA


    ddNorm <- unique(subset(dd,info_passage=="normal" & (passage_stoc==1 | passage_stoc==2),select=c("id_ancar","jour_julien","passage_stoc")))
    ddNormW <- reshape(ddNorm
                      ,v.names="jour_julien"
                      ,idvar=c("id_ancar")
                      ,timevar="passage_stoc"
                      ,direction="wide")

    ddNormW$temps_entre_passage = abs(ddNormW[,3] - ddNormW[,2])

    ddNormW <- subset(ddNormW,select=c("id_ancar","temps_entre_passage"))

    dd <- merge(dd,ddNormW,by="id_ancar",all=TRUE)

    dd$info_entre_passage<- "normal"
    dd$info_entre_passage[dd$temps_entre_passage<28] <- "passages_rapproche"
    dd$info_entre_passage[dd$temps_entre_passage>42] <- "passages_eloigne"


    dd<-dd[,c("pk_inventaire","unique_inventaire_fnat","id_carre","id_point","num_point","etude","annee","date","jour_julien","passage","info_passage","passage_stoc","nombre_de_passage","temps_entre_passage","info_entre_passage","heure_debut","heure_fin","duree_minute","observateur","email","nuage","pluie","vent","visibilite","neige","db","date_export","version")]


    dd <- dd[order(as.character(dd$pk_inventaire)),]





    write.csv(dd,paste("DB_import/inventaire_FNat_",dateExport,".csv",sep=""),row.names=FALSE)

    if(output) return(dd)

}





FNat2observation <- function(d,dateExport,output=FALSE) {
    library(doBy)
  ##  d = dFNat; dateExport = dateExportFNat ;verion = "VV";output=FALSE
    dd <- data.frame(pk_observation= paste(d$dateobs,d$id_carre,"P",substring(d$section_cadastrale,nchar(d$section_cadastrale)-1),sep=""),
                     id_fnat_unique_citation = d$unique_citation,
                     id_inventaire =  paste(d$dateobs,d$id_carre,"P",substring(d$section_cadastrale,nchar(d$section_cadastrale)-1),sep=""),
                     id_point =  paste(d$id_carre,"P",substring(d$section_cadastrale,nchar(d$section_cadastrale)-1),sep=""),
                     id_carre = d$id_carre,
                     num_point = as.numeric(substring(d$section_cadastrale,nchar(d$section_cadastrale)-1)),
                     passage = d$nbr_echantillon,
                     date = as.character(as.Date(as.character(d$dateobs),format="%Y%m%j")),
                     annee=as.numeric(substring(as.character(d$dateobs),1,4)),
                     classe=d$classe,
                     espece = toupper(d$espece),
                     code_sp = substring(toupper(d$espece),1,6),
                     abondance = d$nbr_ind,
                     distance_contact=d$classe_dist,
                     db = "FNat",date_export=dateExport
                     )


    dd$distance_contact <- recodeVar(dd$distance_contact , c("25-100m","En vol","< 25m","> 100m","Non indiquée",""), c("LESS100", "TRANSIT","LESS25","MORE100","U","U"))

    dd <- dd[order(dd$id_inventaire,dd$espece,dd$distance_contact),]
    iFirst_id_inventaire <- setNames(match(unique(dd$id_inventaire),dd$id_inventaire),unique(dd$id_inventaire))

    dd$pk_observation <- paste(dd$pk_observation,sprintf("%02d",(1:nrow(dd))-iFirst_id_inventaire[as.character(dd$id_inventaire)]+1),sep="")

    dd$id_data=paste(dd$id_inventaire,dd$code_sp,dd$distance_contact,sep="")


    contDoublon <- table(dd$id_data)
    doublon <- names(contDoublon)[contDoublon>1]
    if(length(doublon)>0) {
        tDoublon <- subset(dd,id_data %in% doublon)
        file <- paste("OutputImport/_doublonInventaireFNat_",dateExport,".csv",sep="")
        cat("  \n !!! Doublon saisies \n c est a dire plusieurs lignes pour un inventaire une espece et une distance\n Ceci concerne ", nrow(tDoublon)," lignes\n")
        cat("  Toutes lignes sont présenté dans le fichier: \n  --> ",file,"\n")
        write.csv(tDoublon[,-ncol(tDoublon)],file,row.names=FALSE)

        cat(" \n Pour chaque doublon nous conservons la valeur maximum\n")



        tAbMax <- aggregate(subset(tDoublon,select=c("abondance")),list(id_data=tDoublon$id_data),max)
        tAbMax$conservee <- TRUE

        tDoublon <- merge(tDoublon,tAbMax,by=c("id_data","abondance"),all=TRUE)
        tDoublon$conservee[is.na(tDoublon$conservee)] <- FALSE

        dataExclues <- subset(tDoublon,!conservee)
        dataEclues <-subset(dataExclues,select=c("pk_observation","id_fnat_unique_citation","id_inventaire","id_point","id_carre","num_point","passage","date","annee","classe","espece","code_sp","distance_contact","abondance","db","date_export"))

        file <- paste("OutputImport/_doublonInventaireVigiePlume_FNat_",dateExport,".csv",sep="")
        cat("  \n !!! Doublon exclue enregistre dans le fichier \n --> ",file,"\n")
        write.csv(tDoublon[,-ncol(tDoublon)],file,row.names=FALSE)

        dd <- subset(dd,!(pk_observation %in% tDoublon$pk_observation[!tDoublon$conservee]))

        dd <- dd[order(dd$id_inventaire,dd$espece,dd$distance_contact),]
    }


    dd <- dd[,-ncol(dd)]

    write.csv(dd,paste("DB_import/observation_FNat_",dateExport,".csv",sep=""),row.names=FALSE)

    if(output) return(dd)

}





FNat2habitat <- function(d,dateExport,output=FALSE) {

    dd <- unique(data.frame(pk_habitat= paste(d$dateobs,d$dept,substring(d$lieudit,13),"P",substring(d$section_cadastrale,nchar(d$section_cadastrale)-1),sep=""),


                            id_point = paste(d$dept,substring(d$lieudit,13),"P",substring(d$section_cadastrale,nchar(d$section_cadastrale)-1),sep=""),
                            passage = d$nbr_echantillon,
                            date = as.character(as.Date(as.character(d$dateobs),format="%Y%m%j")),
                            annee=as.numeric(substring(as.character(d$dateobs),1,4)),
                            p_milieu=toupper(d$hab_p_milieu),
                            p_type=d$hab_p_type_milieu,
                            p_cat1=d$hab_p_cat_1,
                            p_cat2 = d$hab_p_cat_2,
                            s_milieu=toupper(d$hab_s_milieu),
                            s_type=d$hab_s_type_milieu,
                            s_cat1=d$hab_s_cat_1,
                            s_cat2 = d$hab_s_cat_2,
                            db = "FNat",date_export=dateExport ))




    aa <- table(dd$pk_habitat)

    aa_unique <- names(aa)[aa==1]
    aa_doublon <- names(aa)[aa>1]

    if(length(aa_doublon)>0) {

        dd_unique <- subset(dd,pk_habitat %in% aa_unique)


        ddbrut <- data.frame(pk_habitat= paste(d$dateobs,d$dept,substring(d$lieudit,13),"P",substring(d$section_cadastrale,nchar(d$section_cadastrale)-1),sep=""),


                             id_point = paste(d$dept,substring(d$lieudit,13),"P",substring(d$section_cadastrale,nchar(d$section_cadastrale)-1),sep=""),
                             passage = d$nbr_echantillon,
                             date = as.character(as.Date(as.character(d$dateobs),format="%Y%m%j")),
                             annee=as.numeric(substring(as.character(d$dateobs),1,4)),
                             p_milieu=toupper(d$hab_p_milieu),
                             p_type=d$hab_p_type_milieu,
                             p_cat1=d$hab_p_cat_1,
                             p_cat2 = d$hab_p_cat_2,
                             s_milieu=toupper(d$hab_s_milieu),
                             s_type=d$hab_s_type_milieu,
                             s_cat1=d$hab_s_cat_1,
                             s_cat2 = d$hab_s_cat_2,
                             db = "FNat",date_export=dateExport )



        dd_doub <- subset(ddbrut,pk_habitat %in% aa_doublon)



        col_num2char <- c("passage","p_type","p_cat1","p_cat2",
                          "s_type","s_cat1","s_cat2")
        col_as.factor <- c("passage","p_milieu","p_type","p_cat1","p_cat2",
                           "s_milieu","s_type","s_cat1","s_cat2")

        for(col in col_num2char){
            dd_doub[,col] <- as.character(dd_doub[,col])
            dd_doub[,col] <- ifelse(is.na(dd_doub[,col]),"-99999",dd_doub[,col])

        }



        for(col in col_as.factor){
            dd_doub[,col] <- as.factor(dd_doub[,col])
        }





        dd_inv <- unique(subset(dd_doub,select =c("pk_habitat","id_point",
                                                  "date", "annee")))

        dd_maj1 <- aggregate(subset(dd_doub,select=c("passage")),
                             list(pk_habitat=dd_doub$pk_habitat),
                             function(x) levels(x)[which.max(table(x))])

        dd_maj2 <- aggregate(subset(dd_doub,select=c("p_milieu","p_type","p_cat1","p_cat2",
                                                     "s_milieu","s_type","s_cat1","s_cat2")),
                             list(pk_habitat=dd_doub$pk_habitat),
                             function(x) levels(x)[which.max(table(na.omit(x)))])



        ddd <- merge(merge(dd_inv,dd_maj1,by="pk_habitat"),dd_maj2,by="pk_habitat")


        ddd <- data.frame(pk_habitat= ddd$pk_habitat, id_point =  ddd$id_point, passage =  ddd$passage, date = ddd$date,  annee= ddd$annee,
                          p_milieu= ddd$p_milieu, p_type=  ddd$p_type, p_cat1=  ddd$p_cat1,p_cat2 =  ddd$p_cat2,
                          s_milieu=  ddd$s_milieu, s_type=  ddd$s_type, s_cat1= ddd$s_cat1, s_cat2 =  ddd$s_cat2,
                          db = "FNat",date_export=dateExport)





        for(col in col_num2char){
            ddd[,col] <-  as.numeric(as.character(ddd[,col]))
            ddd[,col] <- ifelse(ddd[,col] == -99999,NA,ddd[,col])
        }


        dd <- rbind(dd_unique,ddd)
    }
    dd <- dd[order(as.character(dd$pk_habitat)),]









    write.csv(dd,paste("DB_import/habitat_FNat_",dateExport,".csv",sep=""),row.names=FALSE)


    if(output) return(dd)

}





###########################################################################################
##   Fonctions qui font les unions des table VigiePlume et FNat et qui gerent les doublons
###########################################################################################


union.point <- function(ddVP.point,ddFNat.point) {
    dd.point <- rbind(ddVP.point,subset(ddFNat.point,!(pk_point %in% unique(ddVP.point$pk_point))))
    dd.point <- dd.point[order(dd.point$pk_point),]
    ##  ulf <- data.frame(pk_point=ddFNat.point$pk_point,ulf = ddFNat.point$unique_localite_fnat)

    ##  dd.point <- merge(dd.point,ulf,by="pk_point",all = TRUE)
    ##  dd.point$unique_localite_fnat <- ifelse(is.na(dd.point$unique_localite_fnat),dd.point$ulf)
    ##  dd.point <- dd.point[,-15]
    return(dd.point)

}


union.carre <- function(ddVP.carre,ddFNat.carre) {
    dd.carre <- rbind(ddVP.carre,subset(ddFNat.carre,!(pk_carre %in% unique(ddVP.carre$pk_carre))))
    dd.carre <- dd.carre[order(as.character(dd.carre$pk_carre)),]

    ncf <- data.frame(pk_carre=ddFNat.carre$pk_carre,ncf = ddFNat.carre$nom_carre_fnat)
    dd.carre <- merge(dd.carre,ncf,by="pk_carre",all = TRUE)
    dd.carre$nom_carre_fnat <- ifelse(is.na(dd.carre$nom_carre_fnat),as.character(dd.carre$ncf),dd.carre$nom_carre_fnat)
    dd.carre <- dd.carre[,-ncol(dd.carre)]


    return(dd.carre)

}



union.inventaire <- function(ddFNat.inv,ddVP.inv,dateConstruction) {
    dd.inv <- rbind(ddVP.inv,subset(ddFNat.inv,!(pk_inventaire %in% unique(ddVP.inv$pk_inventaire))))
    dd.inv <- dd.inv[order(as.character(dd.inv$pk_inventaire)),]
    dd.invDoublon <- subset(ddFNat.inv,(pk_inventaire %in% unique(ddVP.inv$pk_inventaire)))
    dd.inv$etude[dd.inv$id_carre %in% unique(subset(dd.inv,etude == "STOC_ONF")$id_carre)] <- "STOC_ONF"
    if(nrow(dd.invDoublon)>0) {
        file <- paste("OutputImport/_doublonInventaireExclu_",dateConstruction,".csv",sep="")
        cat("\n !!! ",nrow(dd.invDoublon) ," inventaires saisies dans les deux bases de données (FNat et VigiePlume) \n Les donnees de FNat sont exclu et les donnees de VigiePlume conservees\n Retrouvees les donnees exclu dans le fichier:\n --> ",file,"\n")
        write.csv(dd.invDoublon,file,row.names=FALSE)

    }
    return(dd.inv)
}

union.observation <- function(ddFNat.obs,ddVP.obs,dateConstruction) {
    ddFNat.obs$id_data=paste(ddFNat.obs$id_inventaire,ddFNat.obs$code_sp,ddFNat.obs$distance_contact,sep="")
    ddVP.obs$id_data=paste(ddVP.obs$id_inventaire,ddVP.obs$code_sp,ddVP.obs$distance_contact,sep="")

    dd.obs <- rbind(ddVP.obs,subset(ddFNat.obs,!(id_data%in% unique(ddVP.obs$id_data))))
    dd.obs <- dd.obs[order(as.character(dd.obs$pk_observation)),]
    dd.obs <- dd.obs[,-ncol(dd.obs)]
    dd.obsDoublon <-subset(ddFNat.obs,(id_data %in% unique(ddVP.obs$id_data)))
    if(nrow(dd.obsDoublon)>0) {
        file <- paste("OutputImport/_doublonObsExclu_",dateConstruction,".csv",sep="")
        cat("\n !!! ",nrow(dd.obsDoublon) ," observation saisies dans les deux bases de données (FNat et VigiePlume) \n Les donnees de FNat sont exclu et les donnees de VigiePlume conservees\n Retrouvees les donnees exclu dans le fichier:\n --> ",file,"\n")
        write.csv(dd.obsDoublon,file,row.names=FALSE)

    }
    return(dd.obs)
}


union.habitat <- function(ddFNat.hab,ddVP.hab,dateConstruction) {
    dd.hab <- rbind(ddVP.hab,subset(ddFNat.hab,!(pk_habitat %in% unique(ddVP.hab$pk_habitat))))
    dd.hab <- dd.hab[order(as.character(dd.hab$pk_habitat)),]
    dd.habDoublon <- subset(ddFNat.hab,(pk_habitat %in% unique(ddVP.hab$pk_habitat)))
    if(nrow(dd.habDoublon)>0) {
        file <- paste("OutputImport/_doublonHabitatExclu_",dateConstruction,".csv",sep="")
        cat("\n !!! ",nrow(dd.habDoublon) ," habitats saisies dans les deux bases de données (FNat et VigiePlume) \n Les donnees de FNat sont exclu et les donnees de VigiePlume conservees\n Retrouvees les donnees exclu dans le fichier:\n --> ",file,"\n")
        write.csv(dd.habDoublon,file,row.names=FALSE)

    }
    return(dd.hab)
}


##########################################################################################


createDB_postgres <- function(dateConstruction,nomDBpostgresql=NULL,postgresUser="romain",postgresql_createAll = TRUE,postGIS_initiation=TRUE,postgresql_abondanceSeuil=TRUE,seuilAbondance = .99,repertoire=NULL,fileTemp=FALSE) {

    if(is.null(dateConstruction)) dateConstruction = format(start, "%Y-%m-%d")
    if(is.null(repertoire)) repertoire <- paste(getwd(),"/",sep="")
    if(is.null(nomDBpostgresql)) nomDBpostgresql <- "stoc_eps"

########  linux <- Sys.info()[1]=="Linux"

    cat("\n  Importation des tables des données STOC-eps et creation des index\n   ----------------------------------\n")
    cat("     1- point\n")
    cat("     2- carre\n")
    cat("     3- inventaire\n")
    cat("     4- observation\n")
    cat("     5- habitat\n")
    commande <- paste("psql -U ",postgresUser," ",nomDBpostgresql," -f ",repertoire,"sql/postgres_createTableBBS.sql",sep="")
    shell(commande)

    ## importation data
    cat(" \\copy point FROM ",repertoire,
        "DB_import/point_",dateConstruction,".csv",
        " with (format csv, header, delimiter ';')\n",sep="",
        file=paste(repertoire,"sql/_sqlR_importationData.sql",sep=""),append=FALSE)
    cat(" \\copy carre FROM ",repertoire,
        "DB_import/carre_",dateConstruction,".csv",
        " with (format csv, header, delimiter ';')\n",sep="",
        file=paste(repertoire,"sql/_sqlR_importationData.sql",sep=""),append=TRUE)
    cat(" \\copy inventaire FROM ",repertoire,
        "DB_import/inventaire_",dateConstruction,".csv",
        " with (format csv, header, delimiter ';')\n",sep="",
        file=paste(repertoire,"sql/_sqlR_importationData.sql",sep=""),append=TRUE)
    cat(" \\copy observation FROM ",repertoire,
        "DB_import/observation_",dateConstruction,".csv",
        " with (format csv, header, delimiter ';')\n",sep="",
        file=paste(repertoire,"sql/_sqlR_importationData.sql",sep=""),append=TRUE)
    cat(" \\copy habitat (pk_habitat, id_point, passage, date, annee, p_milieu, p_type, p_cat1, p_cat2,s_milieu, s_type, s_cat1, s_cat2, db, date_export) FROM ",repertoire,
        "DB_import/habitat_",dateConstruction,".csv",
        " with (format csv, header, delimiter ';')\n",sep="",
        file=paste(repertoire,"sql/_sqlR_importationData.sql",sep=""),append=TRUE)

    commande <- paste("psql -U ",postgresUser," ",nomDBpostgresql," -f ",repertoire,"sql/_sqlR_importationData.sql",sep="")
    shell(commande)


    commande <- paste("psql -U ",postgresUser," ",nomDBpostgresql," -f ",repertoire,"sql/postgres_createIndexBBS.sql",sep="")
    shell(commande)

    if(postgresql_abondanceSeuil) makeAbondanceTrunc(seuilAbondance,repertoire,nomDBpostgresql,postgresUser,fileTemp)

    if(postgresql_createAll)  maketableGenerique(repertoire,nomDBpostgresql,postgresUser, fileTemp)


    if(postGIS_initiation) {
        cat("\n  Creation des champs postGIS\n   ----------------------\n")
        cat("     1- point.geom93\n")
        cat("     2- carre.grid_geom93\n")
        cat("     3- carre.points_geom93\n")


        commande <- paste("psql -U ",postgresUser," ",nomDBpostgresql," -f ",repertoire,"sql/postgis_initialImport.sql",sep="")
        shell(commande)
    }

    if(postgresql_createAll & postGIS_initiation){
        cat("\n  Creation des champs postGIS\n   ----------------------\n")
        cat("     1- carrenat.geom93\n")


        commande <- paste("psql -U ",postgresUser," ",nomDBpostgresql," -f ",repertoire,"sql/postgis_tableGenerique.sql",sep="")
        shell(commande)
    }

}

openDB.PSQL <- function(nomDB=NULL){

    library(RPostgreSQL)
    if(is.null(nomDB)) nomDB <- "stoc_eps"

    drv <- dbDriver("PostgreSQL")
    con <- dbConnect(drv, dbname="stoc_eps")
    return(con)
}


maketableGenerique <- function(repertoire=NULL,nomDBpostgresql=NULL,postgresUser="romain", fileTemp=TRUE) {

                                        # require(ggplot2)
                                          repertoire=NULL;savePostgres=TRUE;nomDBpostgresql=NULL;postgresUser="romain"; fileTemp=FALSE;
    cat("\n  Importation des tables generiques et creation des index\n   ------------------------------------\n")
    cat("     1- carrenat\n")
    cat("     2- espece\n")
    cat("     3- espece_list_indicateur\n")
    cat("     4- espece_indicateur_fonctionnel\n")

    if(is.null(repertoire)) repertoire <- paste(getwd(),"/",sep="")
    if(is.null(nomDBpostgresql)) nomDBpostgresql <- "stoc_eps"

    commande <- paste("psql -U ",postgresUser," ",nomDBpostgresql," -f ",repertoire,"sql/postgres_createTableGenerique.sql",sep="")
    shell(commande)

    cat(" \\copy carrenat FROM ",repertoire,
        "DB_import/tablesGeneriques/carrenat.csv",
        " with (format csv, header, delimiter ',')\n",sep="",
        file=paste(repertoire,"sql/_sqlR_importationDataTableGenerique.sql",sep=""),append=FALSE)

    cat(" \\copy species FROM ",repertoire,
        "DB_import/tablesGeneriques/espece.csv",
        " with (format csv, header, delimiter ',')\n",sep="",
        file=paste(repertoire,"sql/_sqlR_importationDataTableGenerique.sql",sep=""),append=TRUE)

    cat(" \\copy species_list_indicateur FROM ",repertoire,
        "DB_import/tablesGeneriques/espece_list_indicateur.csv",
        " with (format csv, header, delimiter ',')\n",sep="",
        file=paste(repertoire,"sql/_sqlR_importationDataTableGenerique.sql",sep=""),append=TRUE)

    cat(" \\copy species_indicateur_fonctionnel FROM ",repertoire,
        "DB_import/tablesGeneriques/espece_indicateur_fonctionel.csv",
        " with (format csv, header, delimiter ',')\n",sep="",
        file=paste(repertoire,"sql/_sqlR_importationDataTableGenerique.sql",sep=""),append=TRUE)

    commande <- paste("psql -U ",postgresUser," ",nomDBpostgresql," -f ",repertoire,"sql/_sqlR_importationDataTableGenerique.sql",sep="")
    shell(commande)


    commande <- paste("psql -U ",postgresUser," ",nomDBpostgresql," -f ",repertoire,"sql/postgres_createIndexGenerique.sql",sep="")
    shell(commande)

    if (!fileTemp){
        cat("REMOVE FILE TEMP\n")
        commande <- paste("rm  ",repertoire,"sql/_sqlR_importationDataTableGenerique.sql",sep="")
        shell(commande)
    }


}


makeAbondanceTrunc <- function( seuilAbondance = .99,repertoire=NULL,nomDBpostgresql=NULL,postgresUser="romain", fileTemp=TRUE,graphe=TRUE) {

    require(ggplot2)
                                        # repertoire=NULL;savePostgres=TRUE;nomDBpostgresql=NULL;postgresUser="romain"; fileTemp=TRUE; seuilAbondance = .99


    seuilAbondanceTxt <- as.character(seuilAbondance*100)

    cat("\n  Creation de la table des seuil d abondance\n   ----------------------------------\n")
    cat("     1- query table abondance\n")


    if(is.null(repertoire)) repertoire <- paste(getwd(),"/",sep="")
    if(is.null(nomDBpostgresql)) nomDBpostgresql <- "stoc_eps"

    con <- openDB.PSQL(nomDBpostgresql)

    query <- "
select o.id_inventaire,o.id_carre,o.id_point,o.date, code_sp,abondance,distance_contact
from inventaire as i, observation as o
where i.pk_inventaire = o.id_inventaire and (i.passage_stoc = 1 or i.passage_stoc = 2);
"
    tAbond <- dbGetQuery(con,query)

                                        #    tAbond <- dbReadTable(con,"observation")

    cat("     2- calcul seuil\n")
    cat("           * 100m\n")
    tAbond100 <- subset(tAbond,distance_contact %in% c("LESS25","LESS100"))
    tAbondSum100 <- aggregate(abondance~code_sp+id_inventaire,data=tAbond100,sum)
    tseuil100 <- aggregate(abondance~code_sp,data=tAbondSum100,FUN = function(X) ceiling(quantile(X,seuilAbondance))+1)
    ##    tseuil100 <- data.frame(as.character(aggAbond[,1]),aggAbond[,2][,1],stringsAsFactors=FALSE)
    colnames(tseuil100) <- c("pk_species",paste("abond100_seuil",seuilAbondanceTxt,sep=""))

    cat("           * 200m\n")
    tAbond200 <- subset(tAbond,distance_contact %in% c("LESS25","LESS100","MORE100","LESS200"))
    tAbondSum200 <- aggregate(abondance~code_sp+id_inventaire,data=tAbond200,sum)
    tseuil200 <- aggregate(abondance~code_sp,data=tAbondSum200,FUN = function(X) ceiling(quantile(X,seuilAbondance))+1)

    ##tseuil200 <- data.frame(as.character(aggAbond[,1]),aggAbond[,2][,1],stringsAsFactors=FALSE)
    colnames(tseuil200) <- c("pk_species",paste("abond200_seuil",seuilAbondanceTxt,sep=""))

cat("           * All\n")
    tAbondSumAll <- aggregate(abondance~code_sp+id_inventaire,data=tAbond,sum)
    tseuilAll <- aggregate(abondance~code_sp,data=tAbondSumAll,FUN = function(X) ceiling(quantile(X,seuilAbondance))+1)

    ##tseuilAll <- data.frame(as.character(aggAbond[,1]),aggAbond[,2][,1],stringsAsFactors=FALSE)
    colnames(tseuilAll) <- c("pk_species",paste("abondAll_seuil",seuilAbondanceTxt,sep=""))


    tseuil <- merge(tseuil100,tseuil200,by="pk_species",merge=ALL)
    tseuil <- merge(tseuil,tseuilAll,by="pk_species",merge=ALL)

    write.csv(tseuil,"DB_import/espece_abondance_point_seuil.csv",row.names=FALSE)

    cat("     3- importation de la table\n")
    cat("
DROP table if exists espece_abondance_point_seuil;
CREATE TABLE espece_abondance_point_seuil
	(pk_species varchar(6),
	abond100_seuil",seuilAbondanceTxt," real,
	abond200_seuil",seuilAbondanceTxt," real,
	abondAll_seuil",seuilAbondanceTxt," real);

 \\copy espece_abondance_point_seuil FROM ",repertoire,
"DB_import/espece_abondance_point_seuil.csv",
" with (format csv, header, delimiter ',')\n", sep="",file=paste(repertoire,"sql/_sqlR_espece_abondance_point_seuil.sql",sep=""))


    cat("\n -- RUN SQL --\n")
    commande <- paste("psql -U ",postgresUser," ",nomDBpostgresql," -f ",repertoire,"sql/_sqlR_espece_abondance_point_seuil.sql",sep="")
    shell(commande)


    if(graphe){
        cat("     4- Graphes\n")

        tsp <- dbReadTable(con,"species")

        colnames(tseuil100) <- c("pk_species","seuil")
        tseuil100 <- data.frame(tseuil100,groupe="100")

        colnames(tseuil200) <- c("pk_species","seuil")
        tseuil200 <- data.frame(tseuil200,groupe="200")

        colnames(tseuilAll) <- c("pk_species","seuil")
        tseuilAll <- data.frame(tseuilAll,groupe="All")


        ggseuil <- rbind(tseuil100,tseuil200,tseuilAll)

        ggseuil <- merge(ggseuil,tsp,by="pk_species")


        tAbondSum100 <- data.frame(tAbondSum100,groupe="100")
        tAbondSum200 <- data.frame(tAbondSum200,groupe="200")
        tAbondSumAll <- data.frame(tAbondSumAll,groupe="All")


        tAbondSum <- rbind(tAbondSum100,tAbondSum200,tAbondSumAll)
        colnames(tAbondSum)[1] <- "pk_species"
        tAbondSum <- merge(tAbondSum,tsp,by="pk_species")

        tAbondSum$code.nomfr <- paste(tAbondSum$pk_species,tAbondSum$french_name,sep=":")
        ggseuil$code.nomfr <- paste(ggseuil$pk_species,ggseuil$french_name,sep=":")

        tAbondSum <- merge(tAbondSum,ggseuil[,c("pk_species","seuil","groupe")],
                           by=c("pk_species","groupe"))

        tAbondSum$abondanceTrunc <- ifelse(tAbondSum$abondance > tAbondSum$seuil,
                                           tAbondSum$seuil,tAbondSum$abondance)
        vecSp <- sort(as.character(unique(tAbondSum$pk_species)))

        veci <- seq(1,length(vecSp),25)
        for(I in 1:length(veci)) {
            i1 <- veci[I]
            i2 <- i1 + 24
            if(i2 > length(vecSp)) i2 <-  length(vecSp)
            vecSp.i <- vecSp[i1:i2]

            ggseuil.i <- subset(ggseuil,pk_species %in% vecSp.i)
            tAbondSum.i <- subset(tAbondSum,pk_species %in% vecSp.i)

##### graphe de densite + seuil group et espece

            gg <- ggplot(tAbondSum.i,aes(x=abondance,fill=groupe,colour=groupe,group=groupe))
            gg <- gg + geom_density(alpha=.2)
            gg <- gg + geom_vline(data=ggseuil.i,aes(xintercept =seuil,colour=groupe),size = 1.5,alpha=.5)
            gg <- gg + facet_wrap(~code.nomfr,scales="free")

            ggfile <- paste("OutputImport/AbondanceSeuil_",vecSp[i1],"-",vecSp[i2],".png")
            ggsave(ggfile,gg,width = 12,height = 9)


            gg <- ggplot(tAbondSum.i,aes(x=abondanceTrunc,fill=groupe,colour=groupe,group=groupe))
            gg <- gg + geom_density(alpha=.2)
            gg <- gg + facet_wrap(~code.nomfr,scales="free")

            ggfile <- paste("OutputImport/AbondanceTruncSeuil_",vecSp[i1],"-",vecSp[i2],".png")
            ggsave(ggfile,gg,width = 12,height = 9)



        }


        if (!fileTemp){
            cat("REMOVE FILE TEMP\n")
            commande <- paste("rm  ",repertoire,"sql/_sqlR_espece_abondance_point_seuil.sql",sep="")
            shell(commande)
        }
    }

}



###############################################################################################



point_carre_annee <- function(dateConstruction=NULL,version="V.1",con=NULL,importation=TRUE,repertoire=NULL,nomDBpostgresql=NULL,postgresUser="romain") {

   # dateConstruction=NULL;version="V.1";con=NULL;importation=TRUE;repertoire=NULL;nomDBpostgresql=NULL;postgresUser="romain"
    start <- Sys.time()
    dateExport <- format(start,"%Y-%m-%d")

    if(is.null(dateConstruction)) dateConstruction = format(start, "%Y-%m-%d")
    if(is.null(repertoire)) repertoire <- paste(getwd(),"/",sep="")
    if(is.null(nomDBpostgresql)) nomDBpostgresql <- "stoc_eps"

    cat("\n  Creation des tables point_annee et carre_annee\n   ----------------------------------\n")

    cat("     1- construction table point_annee\n")





        if(is.null(con)) con <- openDB.PSQL()
        query <-paste("select i.annee::varchar(4)||i.id_point as pk_point_annee, i.id_point,i.annee,i.date,passage_stoc,info_passage,nombre_de_passage,h.p_milieu,h.s_milieu
from inventaire as i
left join habitat as h on i.pk_inventaire = h.pk_habitat
order by id_point, annee;")
        d <- dbGetQuery(con,query)
        did <- unique(subset(d,select=c("pk_point_annee","id_point","annee")))
        did <- did[order(did$id_point,did$annee),]
        rownames(did) <- did$pk_point_annee
        did$p_milieu <- NA
        did$s_milieu <- NA
        did$p_dernier_descri <- 0

        ## suppression des valeur d habitat abberante
        habitatPossible <- LETTERS[1:7]
        d$p_milieu[!(d$p_milieu %in% habitatPossible)] <- NA
        d$s_milieu[!(d$s_milieu %in% habitatPossible)] <- NA

        ## ## Habitat principal
        dp<- subset(d,select=c("pk_point_annee","id_point","annee","passage_stoc","p_milieu"))

        ## on ne garde que les deux passages STOC donc on vire passage_stoc == NA et on vire aussi les habitat non renseigne
        dp <- na.omit(dp)
        dp<- dp[,-grep("passage_stoc", colnames(dp))]
        udp <- unique(dp)
        ## recherche les lignes dubliquees
        i.dup.dp <- which(duplicated(subset(udp,select=c("pk_point_annee","id_point","annee"))))
        ## table des duplications
        dup.dp <- udp[i.dup.dp,c("pk_point_annee","id_point","annee")]
        ## les habitats non dupliques
        uniq.dp <- udp[!(udp$pk_point_annee %in% dup.dp$pk_point_annee),]
        did[uniq.dp$pk_point_annee,"p_milieu"] <- uniq.dp$p_milieu


        ## gestion des conflits
        for(i in 1:length(dup.dp)) {
            dpi <- subset(dp,id_point == dup.dp$id_point[i] & annee >= (dup.dp$annee[i]-1) & annee <= (dup.dp$annee[i]+2) )
            ## ic indice du conflit
            ics <- sort(which(dpi$annee == dup.dp$annee[i]))
                                        # print(dpi)
            ihab <- NA
            if(nrow(dpi) %in% ics) {
                ## si derniere annee on choisi le 2eme passage
                ihab <- dpi$p_milieu[nrow(dpi)]
            } else {
                if(1 %in% ics) {
                    ## si 1er annee on choisi 1er passage si hab[1] == hab[3] sinon 2eme passage
                    if(dpi$p_milieu[1] == dpi$p_milieu[3]) ihab <- dpi$p_milieu[3] else ihab <- dpi$p_milieu[2]
                } else  {
                    ## 1er passage si hab[1] == hab[3] et hab[2] != hab[-1] sion 2eme passage
                    if(dpi$p_milieu[ics[1]] == dpi$p_milieu[ics[1]+2])
                        ihab <- dpi$p_milieu[ics[1]]
                    if(dpi$p_milieu[ics[2]] == dpi$p_milieu[ics[2]-2])
                        ihab <-dpi$p_milieu[ics[2]]
                    if(is.na(ihab))
                        ihab <-dpi$p_milieu[ics[2]]
                }
            }

            did[dup.dp$pk_point_annee[i],"p_milieu"] <- ihab
        }


        ## on rempli les NA en prolongeant les habitats


        idpointNA <- unique(subset(did,is.na(p_milieu))$id_point)
        for(ip in idpointNA) {
            dpi <- subset(did,id_point==ip)
            iips <- which(did$id_point == ip)
            iNAs <- which(is.na(did$"p_milieu") & did$id_point == ip)
            for(iNA in iNAs) {
                if(iNA == iips[1]) {
                    ihabPossible <- which(!(is.na(did$"p_milieu")) & did$id_point == ip)
                    if(length(ihabPossible)>0) {
                        did$p_milieu[iNA] <- did$p_milieu[min(which(!(is.na(did$"p_milieu")) & did$id_point == ip))]
                        did$p_dernier_descri[iNA] <- 1000
                    } else {
                        did$p_milieu[iNA] <- NA
                        did$p_dernier_descri[iNA] <- 1000
                    }

                } else {
                    did$p_milieu[iNA] <- did$p_milieu[iNA-1]
                    did$p_dernier_descri[iNA] <- did$p_dernier_descri[iNA-1]+1
                }

            }

        }



        ## ## Habitat secondaire
        dp<- subset(d,select=c("pk_point_annee","id_point","annee","passage_stoc","s_milieu"))
        ## on ne garde que les deux passages STOC donc on vire passage_stoc == NA et on vire aussi les habitat non renseigne
        dp <- na.omit(dp)
        dp<- dp[,-grep("passage_stoc", colnames(dp))]
        udp <- unique(dp)
        ## recherche les lignes dubliquees
        i.dup.dp <- which(duplicated(subset(udp,select=c("pk_point_annee","id_point","annee"))))
        ## table des duplications
        dup.dp <- udp[i.dup.dp,c("pk_point_annee","id_point","annee")]
        ## les habitats non dupliques
        uniq.dp <- udp[!(udp$pk_point_annee %in% dup.dp$pk_point_annee),]
        did[uniq.dp$pk_point_annee,"s_milieu"] <- uniq.dp$s_milieu


        ## gestion des conflits
        for(i in 1:length(dup.dp)) {
            dpi <- subset(dp,id_point == dup.dp$id_point[i] & annee >= (dup.dp$annee[i]-1) & annee <= (dup.dp$annee[i]+2) )
            ## ic indice du conflit
            ics <- sort(which(dpi$annee == dup.dp$annee[i]))
                                        # print(dpi)
            ihab <- NA
            if(nrow(dpi) %in% ics) {
                ## si derniere annee on choisi le 2eme passage
                ihab <- dpi$s_milieu[nrow(dpi)]
            } else {
                if(1 %in% ics) {
                    ## si 1er annee on choisi 1er passage si hab[1] == hab[3] sinon 2eme passage
                    if(dpi$s_milieu[1] == dpi$s_milieu[3]) ihab <- dpi$s_milieu[3] else ihab <- dpi$s_milieu[2]
                } else  {
                    ## 1er passage si hab[1] == hab[3] et hab[2] != hab[-1] sion 2eme passage
                    if(dpi$s_milieu[ics[1]] == dpi$s_milieu[ics[1]+2])
                        ihab <- dpi$s_milieu[ics[1]]
                    if(dpi$s_milieu[ics[2]] == dpi$s_milieu[ics[2]-2])
                        ihab <-dpi$s_milieu[ics[2]]
                    if(is.na(ihab))
                        ihab <-dpi$s_milieu[ics[2]]
                }
            }

            did[dup.dp$pk_point_annee[i],"s_milieu"] <- ihab
        }



           query <- "select pk_point_annee, count(passage_stoc)::real/2 as qualite_inventaire_stoc
from (select annee::varchar(4)||id_point as pk_point_annee, passage_stoc
from inventaire where passage_stoc in (1,2)) as p
group by pk_point_annee;"

        dqual <-  dbGetQuery(con,query)
    did <- merge(did,dqual,by="pk_point_annee",all=TRUE)
    did$qualite_inventaire_stoc[is.na(did$qualite_inventaire_stoc)] <- 0

        did2 <- subset(did,annee>2000 & qualite_inventaire_stoc > 0)
        did2$p_milieu[is.na(did2$p_milieu)] <- "X"
        ggdid <- aggregate(pk_point_annee~p_milieu+annee,did2,length)

    ggdid <- subset(ggdid,annee > 2000)
    tminmax <- c(min(ggdid$annee),max(ggdid$annee)-1)

        vecLegend <- c(A="Forêts",B="Buissons",C="Pelouses, landes et marais",D="Milieux agricoles",E="Milieux urbains",F="Milieux aquatiques",G="Rochers",X="Non renseigné")
        vecCouleur <- c(A="#276325",B="#44b441",C="#69e566",D="#f0db30",E="#c0122a",F="#58afe1",G="#878f93",X="#000000")

        gg <- ggplot(ggdid,aes(x = annee, y = pk_point_annee,fill = p_milieu)) +
            geom_bar(position = "fill",stat = "identity")
    gg <- gg + scale_x_continuous(limits=tminmax)
        gg <- gg + scale_fill_manual(values=vecCouleur,labels=vecLegend,name="Habitat principale")
        gg <- gg + labs(title="Distibution de l'habitat principale des points STOC-EPS",x="",y="")
        ggsave("OutputImport/habitatPoint.png",gg)


        did$foret_p <- ifelse(did$p_milieu %in% c("A","B"),TRUE,FALSE)
        did$ouvert_p <- ifelse(did$p_milieu %in% c("C","G"),TRUE,FALSE)
        did$agri_p <- ifelse(did$p_milieu %in% c("C","D") ,TRUE,FALSE)
        did$urbain_p <- ifelse(did$p_milieu %in% "E" ,TRUE,FALSE)

        did$foret_ps <- ifelse(did$p_milieu %in% c("A","B") | did$s_milieu %in% c("A","B"),TRUE,FALSE)
        did$ouvert_ps <- ifelse(did$p_milieu %in% c("C","G") | did$s_milieu %in% c("C","G"),TRUE,FALSE)
        did$agri_ps <- ifelse(did$p_milieu %in% c("C","D") | did$s_milieu %in% c("C","D") ,TRUE,FALSE)
        did$urbain_ps <- ifelse(did$p_milieu %in% "E" | did$s_milieu %in% "E" ,TRUE,FALSE)




        ggdid <- melt(subset(did,select=c("id_point","annee","foret_p","ouvert_p","agri_p","urbain_p","foret_ps","ouvert_ps","agri_ps","urbain_ps")),c("id_point","annee"))
        colnames(ggdid) <- c("id_point","annee","habitat_cat","value")
        ggdid$value <- as.numeric(ggdid$value)

        tradHabitat1 <- c(foret_p = "foret",ouvert_p = "ouvert",agri_p = "agri",urbain_p = "urbain",
                          foret_ps = "foret",ouvert_ps = "ouvert",agri_ps = "agri",urbain_ps = "urbain")

        tradHabitat2 <- c(foret_p = "principal",ouvert_p = "principal",agri_p = "principal",urbain_p = "principal",
                          foret_ps = "secondaire",ouvert_ps = "secondaire",agri_ps = "secondaire",urbain_ps = "secondaire")



        ggdid$habitat <- tradHabitat1[ggdid$habitat_cat]
    ggdid$cat <- tradHabitat2[ggdid$habitat_cat]

    ggdid <- subset(ggdid,annee > 2000)

        ggdid <- aggregate(value ~ annee + habitat + cat, ggdid, sum)

        tminmax <- c(min(ggdid$annee),max(ggdid$annee))
        vecLegend <- c(foret="Milieux forestiers",ouvert="Milieux ouverts", agri="Milieux agricoles",urbain="Milieux urbains")
        vecCouleur <- c(foret="#276325",ouvert="#69e566", agri="#f0db30",urbain="#c0122a")

        vecLine <- c(principal=1,secondaire=3)
        vecLineLegend <- c(principal="Habitat principale",secondaire="Habitat principale\nou secondaire")

        gg <- ggplot(ggdid,aes(x=annee,y=value,colour=habitat,linetype=cat)) + geom_line(size=1.1,alpha = .8) + geom_point(size=1.5,alpha=.8)
        gg <- gg + scale_x_continuous(limits=tminmax)
        gg <- gg + scale_colour_manual(values=vecCouleur,labels=vecLegend,name="Habitat")
        gg <- gg + scale_linetype_manual(values=vecLine,labels=vecLineLegend,name="")
        gg <- gg + labs(title="Nombre de points STOC-EPS par grand type d'habitat",x="",y="")
        ggsave("OutputImport/habitatNbPoint.png",gg)

        did$id_carre <- substr(did$id_point,1,6)




        did <- did[,c("pk_point_annee","id_point","id_carre","annee","qualite_inventaire_stoc","p_milieu","s_milieu","p_dernier_descri","foret_p","ouvert_p","agri_p","urbain_p","foret_ps","ouvert_ps","agri_ps","urbain_ps")]

        did$date_export <- dateExport
        did$version <- version
        write.table(did,paste("DB_import/point_annee.csv",sep=""),row.names=FALSE,na = "",quote=TRUE, sep=";",dec=".")


        ## table carre
        cat("     2- construction table carre_annee\n")


        did$id_carre <- substr(did$id_point,1,6)

        dcarre <- aggregate(did[,c("qualite_inventaire_stoc","foret_p","ouvert_p","agri_p","urbain_p","foret_ps","ouvert_ps","agri_ps","urbain_ps")],list(did$id_carre ,did$annee), FUN = function(x) sum(as.numeric(x)))

        colnames(dcarre) <-c("id_carre","annee","qualite_inventaire_stoc",paste("nbp_",c("foret_p","ouvert_p","agri_p","urbain_p","foret_ps","ouvert_ps","agri_ps","urbain_ps"),sep=""))

        dcarre$qualite_inventaire_stoc <- dcarre$qualite_inventaire_stoc/10
        dcarre$foret_p <- dcarre$nbp_foret_p >= 5
        dcarre$foret_ps <- dcarre$nbp_foret_ps >= 5
        dcarre$ouvert_p <- dcarre$nbp_ouvert_p >= 5
        dcarre$ouvert_ps <- dcarre$nbp_ouvert_ps >= 5
        dcarre$agri_p <- dcarre$nbp_agri_p >= 5
        dcarre$agri_ps <- dcarre$nbp_agri_ps >= 5
        dcarre$urbain_p <- dcarre$nbp_urbain_p >= 5
        dcarre$urbain_ps <- dcarre$nbp_urbain_ps >= 5


    dcarre2 <- subset(dcarre,annee>2000 & qualite_inventaire_stoc > 0)

        ggdid <- melt(subset(dcarre2,select=c("id_carre","annee","foret_p","ouvert_p","agri_p","urbain_p","foret_ps","ouvert_ps","agri_ps","urbain_ps")),c("id_carre","annee"))
        colnames(ggdid) <- c("id_point","annee","habitat_cat","value")
        ggdid$value <- as.numeric(ggdid$value)

        tradHabitat1 <- c(foret_p = "foret",ouvert_p = "ouvert",agri_p = "agri",urbain_p = "urbain",
                          foret_ps = "foret",ouvert_ps = "ouvert",agri_ps = "agri",urbain_ps = "urbain")

        tradHabitat2 <- c(foret_p = "principal",ouvert_p = "principal",agri_p = "principal",urbain_p = "principal",
                          foret_ps = "secondaire",ouvert_ps = "secondaire",agri_ps = "secondaire",urbain_ps = "secondaire")



        ggdid$habitat <- tradHabitat1[ggdid$habitat_cat]
        ggdid$cat <- tradHabitat2[ggdid$habitat_cat]


        ggdid <- aggregate(value ~ annee + habitat + cat, ggdid, sum)

        tminmax <- c(min(ggdid$annee),max(ggdid$annee)-1)
        vecLegend <- c(foret="Milieux forestiers",ouvert="Milieux ouverts", agri="Milieux agricoles",urbain="Milieux urbains")
        vecCouleur <- c(foret="#276325",ouvert="#69e566", agri="#f0db30",urbain="#c0122a")

        vecLine <- c(principal=1,secondaire=3)
        vecLineLegend <- c(principal="Habitat principale",secondaire="Habitat principale\nou secondaire")

        gg <- ggplot(ggdid,aes(x=annee,y=value,colour=habitat,linetype=cat)) + geom_line(size=1.1,alpha=.8)+ geom_point(size=1.5,alpha=.8)
        gg <- gg + scale_x_continuous(limits=tminmax)
        gg <- gg + scale_colour_manual(values=vecCouleur,labels=vecLegend,name="Habitat")
        gg <- gg + scale_linetype_manual(values=vecLine,labels=vecLineLegend,name="")
        gg <- gg + labs(title="Nombre de carrés STOC-EPS par grand type d'habitat",subtitle="au moins 5 points de l'habitat",x="",y="")
        ggsave("OutputImport/habitatNbCarre.png",gg)



        dcarre$pk_carre_annee <- paste(dcarre$annee,dcarre$id_carre,sep="")

        dcarre <- dcarre[,c("pk_carre_annee","id_carre","annee","qualite_inventaire_stoc","foret_p","ouvert_p","agri_p","urbain_p","foret_ps","ouvert_ps","agri_ps","urbain_ps",paste("nbp_",c("foret_p","ouvert_p","agri_p","urbain_p","foret_ps","ouvert_ps","agri_ps","urbain_ps"),sep=""))]

        dcarre$date_export <- dateExport
        dcarre$version <- version

        write.table(dcarre,paste("DB_import/carre_annee.csv",sep=""),row.names=FALSE,na = "",quote=TRUE, sep=";",dec=".")


        if(importation) {
            cat("     3- Importation des tables\n")

            cat("\n -- RUN SQL --\n")
            commande <- paste("psql -U ",postgresUser," ",nomDBpostgresql," -f ",repertoire,"sql/postgres_point_carre_annee.sql",sep="")
            shell(commande)


        }



    }



###############################################################################################


    historicCarre  <- function(con=NULL,anneeMax=NULL) {

                              ##  con=NULL;anneeMax=2017 ##

        require(ggplot2)
        require(reshape2)
        require(maps)
        require(maptools)
        require(animation)

        if(is.null(con)) con <- openDB.PSQL()

        if(is.null(anneeMax)) anneeTxt <- NULL else anneeTxt <- anneeMax
        if(is.null(anneeMax)) anneeMax = 9999

        query <-paste("select id_carre, annee from carre_annee where annee <= ",anneeMax," and annee > 2000 and qualite_inventaire_stoc > 0 group by id_carre, annee order by id_carre, annee;")

      #  browser()
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
        dan$NonRealise <- NA
        dan$Arrete <- NA
        dan$Nouveaux[1] <- sum(dd[,1])
        dan$NonRealise[1] <- 0
        dan$Arrete[1] <- 0
        dan$Arrete[ncol(dd)] <- NA

        for(j in 2:ncol(dd)) {
            if(j==2)
                dan$Nouveaux[j] <- sum(dd[dd[,j]==1 & dd[,j-1]==0,j]) else dan$Nouveaux[j] <- sum(dd[dd[,j]==1 & rowSums(dd[,1:j-1])==0,j])
        }
                                        #-as.numeric(rowSums(dd[dd[,j]==1 & dd[,j-1]==0,1:j-1])>0)))


        for(j in 2:ncol(dd))
            if(j<ncol(dd))
                dan$NonRealise[j] <- sum(dd[dd[,j]==0 & rowSums(dd[,j:ncol(dd)])>0 & dd[,j-1]==1,(j-1)])  else  dan$NonRealise[j] <- sum(dd[(dd[,j]==0 & dd[,j-1]==1),j-1])

        for(j in 2:(ncol(dd)-1))
            dan$Arrete[j] <- sum(dd[rowSums(dd[,j:ncol(dd)])==0 & dd[,j-1]==1,(j-1)])

        write.csv2(dan,paste("OutputImport/carreSTOCactif_",anneeTxt,".csv",sep=""))

        ggAnnee <- melt(dan,"annee")

        gg <- ggplot(ggAnnee,aes(x=annee,y=value,colour=variable))+geom_line(size=1.1)+geom_point(size=1.3)
        gg <- gg + scale_colour_manual(values=c("nbCarre" = "#0d259f","Nouveaux"="#0d9f1b","Arrete" = "#9f0d0d" ,"NonRealise" = "#ff9d00"),
                                       labels=c("nbCarre" = "Carrés actif","Nouveaux"="Nouveaux carrés","Arrete" = "Carrés arrêtés","NonRealise" = "Carrés non réalisés"),name="" )
        gg <- gg + labs(title="",x="",y="")
        ggsave(paste("OutputImport/carreSTOC_",anneeTxt,".png",sep=""),gg)



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

        ggsave(paste("OutputImport/carreSTOC_pyramideAge_",anneeTxt,".png",sep=""),gg)

        write.csv2(dan,paste("OutputImport/carreSTOCage_",anneeTxt,".csv",sep=""))


        query <-paste("select id_carre, longitude_grid_wgs84, latitude_grid_wgs84
from carre_annee as i, carre as c
where i.id_carre = c.pk_carre and annee <= 9999 and annee > 2000 and qualite_inventaire_stoc > 0
group by id_carre, longitude_grid_wgs84, latitude_grid_wgs84
order by id_carre;")

                                        # browser()
        dcoord <- dbGetQuery(con, query)
        write.csv2(dcoord,paste("OutputImport/coord_carreSTOC_",anneeTxt,".csv",sep=""))

        france <- map_data("france")

        gg <- ggplot(dcoord,aes(longitude_grid_wgs84,latitude_grid_wgs84))+
            geom_polygon( data=france, aes(x=long, y=lat, group = group),colour="gray", fill="white",size=0.3 )+
            geom_point(size=.8,alpha=.8,colour="black")
        gg <- gg + theme(axis.ticks = element_blank(), axis.text = element_blank()) + labs(title="Localisation des carré STOC-EPS suivis au moins 1 année depuis 2001",x="",y="")
        gg <- gg + coord_fixed(ratio=1.2)
        ggsave(paste("OutputImport/carreSTOC_map_simple_",anneeTxt,".png",sep=""),gg)


        dage2 <- merge(dage2,dcoord,by="id_carre")
        dage2$creation <- dage2$annee-dage2$age + 1

        ageMax <- max(dage2$age)
       # dage2 <- subset(dage2,annee<=2016)

        gg <- ggplot(dage2,aes(longitude_grid_wgs84,latitude_grid_wgs84,colour=creation))+
            geom_polygon( data=france, aes(x=long, y=lat, group = group),colour="gray", fill="white",size=0.3 )+
            geom_point(size=.8,alpha=.8,colour="black")+ geom_point(size=0.6,alpha=.8) + facet_wrap(~annee)
        gg <- gg + scale_colour_gradientn(colours=c("#4d004b","#8c6bb1","#bfd3e6"),name="Date de\ncréation")
        gg <- gg + theme(axis.ticks = element_blank(), axis.text = element_blank()) + labs(title="Localisation et age des suivis STOC-EPS",x="",y="")
        gg <- gg + coord_fixed(ratio=1.2)
        ggsave(paste("OutputImport/carreSTOC_map_age_",anneeTxt,".png",sep=""),gg,height = 10.5,width = 13)

        saveGIF({
            for(a in sort(unique(dage2$annee))) {
                dage2a <- subset(dage2,annee==a)
                gg <- ggplot(dage2a,aes(longitude_grid_wgs84,latitude_grid_wgs84,colour=creation))+
                    geom_polygon( data=france, aes(x=long, y=lat, group = group),colour="gray", fill="white",size=0.3 )+
                    geom_point(size=1.5,alpha=.8,colour="black")+geom_point(size=1.3,alpha=.8) + facet_wrap(~annee)
                gg <- gg + scale_colour_gradientn(colours=c("#4d004b","#8c6bb1","#bfd3e6"),name="Date de\ncréation",limits=c(min(dage2$annee),max(dage2$annee)))
                gg <- gg + coord_fixed(ratio=1.2)
                gg <- gg + theme(axis.ticks = element_blank(), axis.text = element_blank()) + labs(title="Localisation et age des suivis STOC-EPS",x="",y="")
                print(gg)
                ani.pause()
            }
        },
        interval = 1.3,
        movie.name = paste("OutputImport/carreSTOC_mapGIF_",anneeTxt,".gif",sep=""),ani.width = 500, ani.height = 500)





    }




