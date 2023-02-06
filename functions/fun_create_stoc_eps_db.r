###################################################################################################################
###
###                       Script de preparation de la base STOC-eps reuniant les deux bases
###                                              FNat   et    VigiePlume
###
###################################################################################################################



### Beta 1.0

### Selection dans la base FNat seulement des carré STOC EPS


### requete dans base access FNat (seulement sous windows et sous R 32bit)

###  exportation de table a plat

### Importation de table FNat a plat
### Importation de table VigiePlume a plat



### 2018-10-16 ajout des couches shape PRA et maille_atlas

### 2017-11-14 modification de l'entete de colonne des nom de point de la table vigieplume


source("functions/fun_generic.r")


if("STOC_eps_database" %in% dir()) setwd("STOC_eps_database")



f_prepaData <- function(dateExportVP="2019-01-10",nomFileVP="export_stoc_10012019.txt",
                        nomFileVP_ONF="export_stoc_ONF_10012019.txt",
                        dateExportFNat="2017-01-04", importACCESS=FALSE,
                        nomFileFNat="FNat_plat_2017-01-04.csv",nomDBFNat="Base FNat2000.MDB",
                        importationDataBrut=TRUE,
                        constructionPoint=TRUE,constructionCarre=TRUE,
                        constructionInventaire=TRUE,
                        constructionObservation = TRUE, constructionHabitat = TRUE,
                        dateConstruction=NULL,postgresql_import=TRUE,nomDBpostgresql=NULL,
                        postgresql_createAll=TRUE,postgresUser="postgres",
                        postgresPassword="postgres",
                        postGIS_initiation=TRUE,import_shape=FALSE,repertoire=NULL,
                        postgresql_abondanceSeuil=TRUE,seuilAbondance = .99,
                        historiqueCarre=TRUE,
                        pointCarreAnnee=TRUE,importPointCarreAnnee=TRUE,fileTemp=FALSE)
{
    library(RODBC)
    library(reshape)
    library(data.table)
    library(rgdal)
    library(sf)
    library(dplyr)
    library(qdap) #multigsub


    cat("               ===========================================================================\n")
    cat("               |                                                                         |\n")
    cat("               |            Importation des Donnees STOC-eps des 2 base de donnnes       |\n")
    cat("               |                              FNat et VigiePlume                         |\n")
    cat("               |                                                                         |\n")
    cat("               ===========================================================================\n")

    version <- "V.1.1_2020-04-07"

    cat(paste("\n\n Version",version,"\n"))
    cat(" ----------------------------\n\n")



    dateExportVP=c("2019-11-25","2019-12-03");nomFileVP=c("export_stoc_25112019.txt","export_stoc_onf_03122019.txt");
    dateExportFNat="2017-01-04"; importACCESS=FALSE;
    nomFileFNat="FNat_plat_2017-01-04.csv";nomDBFNat="Base FNat2000.MDB";importationDataBrut_FNat=TRUE;importationDataBrut_VP=TRUE;
    constructionPoint=FALSE;constructionCarre=FALSE;constructionInventaire=FALSE;
    constructionObservation = FALSE; constructionHabitat = TRUE;
    dateConstruction="2022-01-19";postgresql_import=TRUE;nomDBpostgresql=NULL;seuilAbondance = .99
    nomDBpostgresql="stoc_eps";
    postgresql_createAll=TRUE;postgresUser="romain";
    postGIS_initiation=TRUE;import_shape=FALSE;repertoire=NULL;postgresql_abondanceSeuil=TRUE;historiqueCarre=TRUE;
    pointCarreAnnee=TRUE;importPointCarreAnnee=TRUE;fileTemp=FALSE
    ##
    ##    postgresPassword = "xxxxxx"


    start <- Sys.time() ## heure de demarage est utiliser comme identifiant par defaut
    if(is.null(dateConstruction)) dateConstruction = format(start, "%Y-%m-%d")

    repOut <- paste0("output_import/",dateConstruction,"/")
    dir.create(repOut,showWarnings=FALSE)

    cat("\n     # Debut du process:",format(start, "%Y-%m-%d %H:%M\n"))

    ##    nomFileVP=paste0("data_raw/",nomFileVP)
    ##    nomFileVPonf=paste0("data_raw/",nomFileVP_ONF)
    ##
    ##    nomDBFNat=paste0("data_raw/",nomDBFNat)
    ##    nomFileFNat=paste0("data_raw/",nomFileFNat)
    ##


    cat("\n\n   Les paramètres:\n---------------------\n\n")

    cat("dateExportVP: ",dateExportVP,"\n")
    cat("nomFileVP: ",nomFileVP,"\n")
    cat("dateExportFNat: ",dateExportFNat,"\n")
    cat("importACCESS: ",importACCESS,"\n")
    cat("nomFileFNat: ",nomFileFNat,"\n")
    cat("nomDBFNat: ",nomDBFNat,"\n")
    cat("importationDataBrut_FNat: ",importationDataBrut_FNat,"\n")
    cat("importationDataBrut_VP: ",importationDataBrut_VP,"\n")
    cat("constructionPoint: ",constructionPoint,"\n")
    cat("constructionCarre: ",constructionCarre,"\n")
    cat("constructionInventaire: ",constructionInventaire,"\n")
    cat("constructionObservation: ",constructionObservation ,"\n")
    cat("constructionHabitat: ", constructionHabitat ,"\n")
    cat("postgresql_import: ",postgresql_import,"\n")
    cat("nomDBpostgresql: ",nomDBpostgresql,"\n")
    cat("postgresql_createAll: ",postgresql_createAll,"\n")
    cat("postgresUser: ",postgresUser,"\n")
    cat("postgresPassword: ",ifelse(postgresPassword=="postgres",postgresPassword,"********"),"\n")
    cat("postGIS_initiation: ",postGIS_initiation,"\n")
    cat("import_shape: ",import_shape,"\n")
    cat("repertoire: ",repertoire,"\n")
    cat("postgresql_abondanceSeuil: ",postgresql_abondanceSeuil,"\n")
    cat("seuilAbondance: ",seuilAbondance ,"\n")
    cat("historiqueCarre: ",historiqueCarre,"\n")
    cat("pointCarreAnnee: ",pointCarreAnnee,"\n")
    cat("importPointCarreAnnee: ",importPointCarreAnnee,"\n")
    cat("fileTemp: ",fileTemp,"\n")

    cat("\n\ndateConstruction: ",dateConstruction,"\n")

    cat("\n\n repertoire output import:",repOut,"\n")


### I) Importation et preparation des données récente Vigie-Plume
    cat("\n\n I) Importation et preparation des données récente Vigie-Plume \n=================================\n")


    ## 1) Importation et preparation
    cat("\n\n 1) Importation et preparation \n----------------------\n")
    if(importationDataBrut_VP) {

        cat("\n - VigiePlume\n <-- Fichier a plat:",nomFileVP,"\n")
        flush.console()

        dVP <- vp_importation(nomFileVP,dateExportVP,repImport="data_raw/",repOut=repOut)

    } else { cat(" ---> SKIP\n") }

    ## 2) Table points
    cat("\n\n 2) Table points\n----------------------\n")
    flush.console()

    if(constructionPoint) {
        cat("\n - VigiePlume: ")
        flush.console()

        if(importationDataBrut_VP) {
            cat(" construction -> ")
            ddVP.point <- vp2point(dVP,dateConstruction,repOut,TRUE)
        } else {
            filename <- paste0(repOut,"point_VP_",dateConstruction,".csv")
            cat(" importation \n",filename," -> ")
            ddVP.point <- fread(filename)
        }
        cat(nrow(ddVP.point)," lignes \n")
        flush.console()


    } else { cat(" ---> SKIP\n") }

    ## 3) Table carres
    cat("\n\n 3) Table carres\n----------------------\n")
    flush.console()

    if(constructionCarre) {
        cat("\n - VigiePlume: ")
        flush.console()
        if(importationDataBrut_VP) {
            cat(" construction -> ")
            ddVP.carre <- vp2carre(dVP,dateConstruction,repOut,TRUE)
        } else {
            filename <- paste0(repOut,"carre_VP_",dateExport,".csv")
            cat(" importation \n",filename," -> ")
            ddVP.carre <- fread(filename)
        }
        cat(nrow(ddVP.carre)," lignes \n")


    } else { cat(" ---> SKIP\n") }

    ## 4) Table inventaires
    cat("\n\n4) Table inventaires\n----------------------\n")
    flush.console()

    if(constructionInventaire | constructionObservation) {
        cat("\n - VigiePlume: ")
        flush.console()

        if(importationDataBrut_VP) {
            cat(" construction -> ")
            ddVP.inv <- vp2inventaire(dVP,dateExportVP,version,dateConstruction,repOut,TRUE)
        } else {
            filename <- paste0(repOut,"inventaire_VP_",dateConstruction,".csv")
            cat(" importation  \n",filename," -> ")
            ddVP.inv <- fread(filename)
        }
        cat(nrow(ddVP.inv)," lignes \n")

    } else { cat(" ---> SKIP\n") }

    ## 5) Table observations
    cat("\n\n 5) Table observations\n----------------------\n")
    flush.console()

    if(constructionObservation) {
        cat("\n - VigiePlume: ")
        flush.console()

        if(importationDataBrut_VP) {
            cat(" construction -> ")
            ddVP.obs <- vp2observation(dVP,dateExportVP,dateConstruction,repOut,TRUE)
        } else {
            filename <- paste0(repOut,"observation_VP_",dateConstruction,".csv")
            cat(" importation \n",filename," -> ")
            ddVP.obs <- fread(filename)
        }
        cat(nrow(ddVP.obs)," lignes \n")

    } else { cat(" ---> SKIP\n") }

    ##  6) Table habitats
    cat("\n\n 6) Table habitats\n----------------------\n")
    flush.console()

    if(constructionHabitat) {
        cat("\n - VigiePlume: ")

        if(importationDataBrut_VP) {
            cat(" construction -> ")
            ddVP.hab <- vp2habitat(dVP,dateExportVP,dateConstruction,repOut,TRUE)
        } else {
            filename <- paste0(repOut,"habitat_VP_",dateConstruction,".csv")
            cat(" importation \n",filename," -> ")
            ddVP.inv <- fread(filename)
        }
        cat(nrow(ddVP.hab)," lignes \n")

    } else { cat(" ---> SKIP\n") }





###  II) Importation et preparation des données historique FNat
    cat("\n\n II) Importation et preparation des données historique FNat \n=================================\n")


    ## 1) Importation et preparation
    cat("\n\n 1) Importation et preparation \n----------------------\n")
    if(importationDataBrut_FNat) {

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
            dFNat <-  FNat_importation(nomFileFNat,dateExportFNat,repImport="data_raw/",repOut=repOut)
            cat("   --> OK \n")
            flush.console()
        }

    } else { cat(" ---> SKIP\n") }

    ## 2) Table points
    cat("\n\n 2) Table points\n----------------------\n")
    flush.console()

    if(constructionPoint) {

        cat("\n - FNat: ")
        flush.console()
        if(importationDataBrut_FNat) {
            ddFNat.point <- FNat2point(dFNat,dateExportFNat,repOut,TRUE)
        } else {
            filename <- paste0(repOut,"point_FNat_",dateConstruction,".csv")
            cat(" importation \n",filename," -> ")
            ddFNat.point <- fread(filename)
        }
        cat(nrow(ddFNat.point)," lignes \n")

    } else { cat(" ---> SKIP\n") }

    ## 3) Table carres
    cat("\n\n 3) Table carres\n----------------------\n")
    flush.console()

    if(constructionCarre) {

        cat("\n - FNat: ")
        flush.console()
        if(importationDataBrut_FNat) {
            ddFNat.carre <- FNat2carre(dFNat,dateExportFNat,repOut,TRUE)

        } else {
            filename <- paste0(repOut,"carre_FNat_",dateConstruction,".csv")
            cat(" importation \n",filename," -> ")
            ddFNat.carre <- fread(filename)
        }
        cat(nrow(ddFNat.carre)," lignes \n")

    } else { cat(" ---> SKIP\n") }

    ## 4) Table inventaires
    cat("\n\n4) Table inventaires\n----------------------\n")
    flush.console()

    if(constructionInventaire | constructionObservation) {
        cat("\n - FNat: ")
        flush.console()
        if(importationDataBrut_FNat) {
            ddFNat.inv <- FNat2inventaire(dFNat,dateExportFNat,version,dateConstruction,repOut,TRUE)

        } else {
            filename <- paste0(repOut,"inventaire_FNat_",dateConstruction,".csv")
            cat(" importation \n",filename," -> ")
            ddFNat.inv <- fread(filename)
        }
        cat(nrow(ddFNat.inv)," lignes \n")

    } else { cat(" ---> SKIP\n") }

    ## 5) Table observations
    cat("\n\n 5) Table observations\n----------------------\n")
    flush.console()

    if(constructionObservation) {


        cat("\n - FNat: ")
        flush.console()
        if(importationDataBrut_FNat) {
            cat("\n",  nrow(subset(dFNat,!(unique_inventaire %in% ddFNat.inv$unique_inventaire_fnat)))," lignes exclues !!\n")
            dFNat <- subset(dFNat,unique_inventaire %in% ddFNat.inv$unique_inventaire_fnat)

            ddFNat.obs <- FNat2observation(dFNat,dateExportFNat,dateConstruction,repOut,TRUE)

            ddFNat.obs <- subset(ddFNat.obs,!(id_inventaire %in% ddFNat.inv$pk_inventaire))
        } else {
            filename <- paste0(repOut,"observation_FNat_",dateConstruction,".csv")
            cat(" importation \n",filename," -> ")
            ddFNat.obs <- fread(filename)
        }
        cat(nrow(ddFNat.obs)," lignes \n")

    } else { cat(" ---> SKIP\n") }

    ##  6) Table habitats
    cat("\n\n 6) Table habitats\n----------------------\n")
    flush.console()

    if(constructionHabitat) {
        cat("\n - FNat: ")
        flush.console()
        if(importationDataBrut_FNat) {
            ddFNat.hab <- FNat2habitat(dFNat,dateExportFNat,repOut,TRUE)
        } else {
            filename <- paste0(repOut,"habitat_FNat_",dateConstruction,".csv")
            cat(" importation \n",filename," -> ")
            ddFNat.hab <- fread(filename)
        }
        cat(nrow(ddFNat.hab)," lignes \n")

    } else { cat(" ---> SKIP\n") }




### III) Union des deux jeux de données
    cat("\n\n III) Union des deux jeux de données \n=================================\n")


    ## 1) Importation et preparation
    cat("\n\n 1) Importation et preparation \n DONE !\n")

    ## 2) Table points
    cat("\n\n 2) Table points\n----------------------\n")
    flush.console()

    if(constructionPoint) {
        cat("\n  -> union: ")
        dd.point <- union.point(ddVP.point,ddFNat.point)
        cat(nrow(dd.point)," lignes \n")

        file <- paste0("data_DB_import/point_",dateConstruction,".csv")
        cat("\n  -->",file,"\n")
        write.table(dd.point,file,row.names=FALSE,na = "",quote=TRUE, sep=";",dec=".",fileEncoding ="UTF-8")

    } else { cat(" ---> SKIP\n") }

    ## 3) Table carres
    cat("\n\n 3) Table carres\n----------------------\n")
    flush.console()

    if(constructionCarre) {
        cat("\n  -> union: ")
        dd.carre <- union.carre(ddVP.carre,ddFNat.carre)
        cat(nrow(dd.carre)," lignes \n")

        file <- paste0("data_DB_import/carre_",dateConstruction,".csv")
        cat("\n  -->",file,"\n")
        write.table(dd.carre,file,row.names=FALSE,na = "",quote=TRUE, sep=";",dec=".",fileEncoding ="UTF-8")

    } else { cat(" ---> SKIP\n") }

    ## 4) Table inventaires
    cat("\n\n4) Table inventaires\n----------------------\n")
    flush.console()

    if(constructionInventaire | constructionObservation) {
        cat("\n  -> union: ")
        dd.inv <- union.inventaire(ddFNat.inv,ddVP.inv,dateConstruction,repOut)
        cat(nrow(dd.inv)," lignes \n")

        file <- paste0("data_DB_import/inventaire_",dateConstruction,".csv")
        cat("\n  -->",file,"\n")
        write.table(dd.inv,file,row.names=FALSE,na = "",quote=TRUE, sep=";",dec=".",fileEncoding ="UTF-8")

    } else { cat(" ---> SKIP\n") }

    ## 5) Table observations
    cat("\n\n 5) Table observations\n----------------------\n")
    flush.console()

    if(constructionObservation) {
        cat("\n  -> union: ")
        dd.obs <- union.observation(ddFNat.obs,ddVP.obs,dateConstruction,repOut)
        cat(nrow(dd.obs)," lignes \n")

        file <- paste0("data_DB_import/observation_",dateConstruction,".csv")
        cat("\n  -->",file,"\n")
        write.table(dd.obs,file,row.names=FALSE,na = "",quote=TRUE, sep=";",dec=".",fileEncoding ="UTF-8")
    } else { cat(" ---> SKIP\n") }

    ##  6) Table habitats
    cat("\n\n 6) Table habitats\n----------------------\n")
    flush.console()

    if(constructionHabitat) {
        cat("\n  -> union: ")
        dd.hab <- union.habitat(ddFNat.hab,ddVP.hab,dateConstruction,repOut)
        cat(nrow(dd.hab)," lignes \n")


        file <- paste0("data_DB_import/habitat_",dateConstruction,".csv")
        cat("\n  -->",file,"\n")
        write.table(dd.hab,file,row.names=FALSE,na = "",quote=TRUE, sep=";",dec=".",fileEncoding ="UTF-8")
    } else { cat(" ---> SKIP\n") }






### IV) Base de Donnees

    cat("\n\n IV) Base de Donnees\n=================================\n")
    flush.console()

    ## Creation de la base de donnees PostGreSQL
    if(postgresql_import) {
        cat("\n 1) Creation de la base de donnees PostGreSQL :\n")
        createDB_postgres(dateConstruction,nomDBpostgresql,postgresUser,postgresPassword,postgresql_createAll,postGIS_initiation,postgresql_abondanceSeuil,seuilAbondance,repertoire,repOut=repOut,fileTemp)
        cat("\n    -> OK\n")
    } else { cat(" ---> SKIP\n") }

    ## Point et Carrees Annee
    cat("\n\n 2) Point et Carrees Annee\n----------------------\n")
    flush.console()
    if(pointCarreAnnee) {
        point_carre_annee(dateConstruction=dateConstruction,version=version,con=NULL,importation=importPointCarreAnnee,repertoire=repertoire,nomDBpostgresql=nomDBpostgresql,postgresUser=postgresUser,postgresPassword=postgresPassword)
    } else { cat(" ---> SKIP\n") }


    ## Historic des Carrees
    cat("\n\n 3) Historic des Carrees\n----------------------\n")
    flush.console()
    if(historiqueCarre & postgresql_import) {
        historicCarre(nomDBpostgresql=nomDBpostgresql,postgresUser=postgresUser,postgresPassword=postgresPassword)
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
    colnames(d_habP)[3:ncol(d_habP)]<- paste0("hab_p_",colnames(d_habP)[3:ncol(d_habP)])



    query <- "SELECT TLDescEPS.Unique_Localité, TLDescEPS.DateEPS, TLDescEPS.Milieu, TLDescEPS.Type_Milieu, TLDescEPS.Catégorie_1, TLDescEPS.Catégorie_2, TLDescEPS.Sous_Catégorie_1, TLDescEPS.Sous_Catégorie_2
FROM TLDescEPS
WHERE (((TLDescEPS.Habitat)='S'));"
    d_habS <- sqlQuery(con,query)
    colnames(d_habS)[3:ncol(d_habS)]<- paste0("hab_s_",colnames(d_habS)[3:ncol(d_habS)])

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



vp_importation <- function(nomFileVP,dateExportVP,repImport="data_raw/",repOut) {
    require(data.table)

    d_nom <- data.table(old=c("Code.inventaire","Etude","Site","Pays","Département","INSEE","Commune","N..Carré.EPS","Date","Heure","Heure.fin","N..Passage","Observateur","Email","EXPORT_STOC_TEXT_EPS_POINT","Altitude","Classe","Espèce","Nombre","Distance.de.contact","Longitude","Latitude","Type.de.coordonnées","Type.de.coordonnées.lambert","EPS.Nuage","EPS.Pluie","EPS.Vent","EPS.Visibilité","EPS.Neige","EPS.Transport","EPS.P.Milieu","EPS.P.Type","EPS.P.Cat1","EPS.P.Cat2","EPS.P.Sous.Cat1","EPS.P.Sous.Cat2","EPS.S.Milieu","EPS.S.Type","EPS.S.Cat1","EPS.S.Cat2","EPS.S.Sous.Cat1","EPS.S.Sous.Cat2"),new=c("Code.inventaire","etude","site","pays","departement","insee","commune","id_carre","date","heure","heure_fin","passage_observateur","observateur","email","nom_point","altitude","classe","espece","abondance","distance_contact","longitude_wgs84","latitude_wgs84","type_coord","type_coord_lambert","nuage","pluie","vent","visibilite","neige","transport","p_milieu","p_type","p_cat1","p_cat2","p_sous_cat1","p_sous_cat2","s_milieu","s_type","s_cat1","s_cat2","s_sous_cat1","s_sous_cat2"))

    d <- NULL

    if(length(dateExportVP) ==1) dateExportVP <- rep(dateExportVP,length(nomFileVP))

    nfile <- length(nomFileVP)
    cat("\n",nfile,"fichier(s) VigiePlume à importer\n")

    for(i in 1:length(nomFileVP)) {
        nomFileVPi <- paste0(repImport,nomFileVP[i])
        cat(" <-- (",i,"/",nfile,") Fichier VigiePlume: ",nomFileVPi," ",sep="")
        flush.console()
        di <- read.csv(nomFileVPi,h=TRUE,stringsAsFactors=FALSE,fileEncoding="utf-8",sep="\t")
        cat("  DONE ! \n")
        flush.console()
        di <- data.table(di)
        cat(" vérification colonnes\n")

        col_abs <- setdiff(d_nom[,old],colnames(di))
        n_col_abs <- length(col_abs)
        if(n_col_abs > 0) {
            cat("  !! ATTENTION !! ",n_col_abs," absente(s):\n")
            the_cols_txt <- paste(col_abs,collapse=", ")
            cat(the_cols_txt,"\n")
            txt <- "\n Choix: \n1- ne pas importer le fichier \n2- importer le fichier et ajoute NA dans les colonnes manquantes \n3- Arreter le processus\n"
            cat(txt,"\n")
            rep <- readline(prompt = txt)


        } else {

            setnames(di,old=d_nom[,old],new=d_nom[,new])
            cat("  DONE ! \n")

            di[,date_export:= dateExportVP[i]]
            d <- rbind(d,di)
        }
    }
    ## renome les colonnes

    cat("\n    !!! suppression des lignes  issue d'une etude LPO aquitaine hors echantillonage STOC-EPS 'PRANAT','Frolet'\n")

    ligneExclu <- union(grep("NAT",d$carre),grep("Frolet",d$carre))
    cat(length(ligneExclu)," lignes exclues\n\n")
    ligneConserv <- setdiff(1:nrow(d),ligneExclu)
    d <- d[ligneConserv,]



    cat("\n    !!! suppression des lignes pour les quelles le numéros du point n'est pas saisie\n")

    d.sansPoint <- d[nom_point==""]

    if(nrow(d.sansPoint) > 0) {
        cat("\   il y a ",nrow(d.sansPoint)," observation dans les données vigiplume qui n'ont pas de numéros de point\n")
        fileCSV <- paste0(repOut,"_erreur_abscence_numeros_point.csv")
        cat("  -->",fileCSV,"\n")
        write.csv(d.sansPoint,fileCSV,row.names=FALSE)
        d <- d[nom_point !=  ""]
    }



    d <- d[!is.na(abondance) & !is.na(espece) & espece != ""]

    d[,etude_detail := etude]
    d[,insee := sprintf("%05d", insee)]
    d[,departement := sprintf("%02d", departement)]
    d[,date := as.Date(date,format="%d.%m.%Y")]
    d[,id_carre := substring(id_carre,9,nchar(id_carre))]
    d[,nom_carre := paste("Carre",id_carre,site)]
    d[,point := gsub("Point N°","",nom_point)]
    d[,id_point := paste0(id_carre,"P",point)]
    d[,num_point := as.numeric(point)]
    d[,jour_julien := as.numeric(format(date,"%j"))]
    d[,annee := as.numeric(format(date,"%Y"))]
    d[,id_inventaire := paste0(format(date,"%Y%m%d"),id_point)]
    d[,duree_minute := as.numeric(difftime(as.POSIXct(heure_fin,format="%H:%M"),as.POSIXct(heure,format="%H:%M")))]
    d[,id_data := paste0(id_inventaire,"_",substring(espece,1,6),"_",distance_contact)]
    d <- setorder(d, id_inventaire,espece,distance_contact)
    d[,inventaire_inc := 1:.N,by=id_inventaire]
    d[,inventaire_inc :=sprintf("%02d",inventaire_inc)]
    d[,id_observation := paste0(id_inventaire,inventaire_inc)]

    d[, altitude := as.numeric(altitude)]

    d[,db:="vigieplume"]
    d[,nom_carre_fnat := NA]
    d[,info_passage := NA]
    d[,passage_stoc := NA]
    d[,espece := toupper(espece)]
    d[,code_sp := substr(espece,1,6)]
    d[,abondance := as.numeric(abondance)]

    d[, p_milieu := toupper(ifelse(p_milieu == "",NA,p_milieu))]
    d[p_type == "", p_type := NA]
    d[p_cat1 == "", p_cat1 := NA]
    d[p_cat2 == "", p_cat2 := NA]
    d[, s_milieu := toupper(ifelse(s_milieu == "",NA,s_milieu))]
    d[s_type == "", s_type := NA]
    d[s_cat1 == "", s_cat1 := NA]
    d[s_cat2 == "", s_cat2 := NA]

    return(d)
}



vp2point <- function(d,dateConstruction,repOut="",output=FALSE) {

    ## d=dVP;dateExport=dateExportVP;repOut=repOut;output=TRUE

    dagg <- d[, .(commune = get_mode(commune),
                  site = get_mode(site),
                  insee = get_mode(insee),
                  nom_point = get_mode(nom_point),
                  departement = get_mode(departement),
                  altitude = median(altitude),
                  latitude_wgs84 = median(latitude_wgs84),
                  longitude_wgs84 = median(longitude_wgs84),
                  latitude_wgs84_sd = sd(latitude_wgs84),
                  longitude_wgs84_sd = sd(longitude_wgs84),
                  date_export = max(date_export)),
              by=id_point]

    dd <- unique(d[,.(id_point,id_carre,num_point)])
    dd[,db := "vigieplume"]

    setkey(dagg,"id_point")
    setkey(dd,"id_point")

    dd <- dd[dagg]
    setnames(dd,old=c("id_point"),new=c("pk_point"))

    colorder <- c("pk_point","id_carre","commune","site","insee","departement","nom_point","num_point","altitude","longitude_wgs84","latitude_wgs84","longitude_wgs84_sd","latitude_wgs84_sd","db","date_export")
    setcolorder(dd,colorder)


    filename <- paste0(repOut,"point_VP_",dateConstruction,".csv")
    write.csv(dd,filename,row.names=FALSE)
    cat("  (->",filename,")\n",sep="")

    if(output) return(dd)
}

vp2carre <- function(d,dateConstruction,repOut="",output=FALSE) {
    ## d <- dVP; dateExport = dateExportVP; output=FALSE; repOut=""

###dcarrenat <- read.csv("data_generic/carrenat.csv",encoding="UTF-8")
###dcarrenat$pk_carre <- sprintf("%06d",    dcarrenat$pk_carre)

    ## la table de l'ensemble des carré de la grille nationale
    dcarrenat <- fread("data_generic/carrenat.csv")
    dcarrenat<- dcarrenat[,pk_carre := sprintf("%06d",pk_carre)]

    setkey(dcarrenat,"pk_carre")

    dcarrenat <- dcarrenat[,.(pk_carre,lat_WGS84,lon_WGS84)]
    setnames(dcarrenat, old= c("lat_WGS84","lon_WGS84"),new=c("latitude_grid_wgs84","longitude_grid_wgs84"))


    ##  d_col <- c("id_carre","Commune","Site","INSEE","Département","N..Carré.EPS","nom_carre_fnat","EXPORT_STOC_TEXT_EPS_POINT","num_point","Altitude","Longitude","Latitude","db","date_export")
    ## dd <- d[,d_col,with=FALSE]
    ## changement des noms de colonnes

    dd <- d[, .(commune = get_mode(commune),
                site = get_mode(site),
                insee = get_mode(insee),
                etude = get_mode(etude),
                nom_carre = get_mode(nom_carre),
                nom_carre_fnat = NA,
                departement = get_mode(departement),
                altitude_median = median(altitude),
                latitude_median_wgs84 = median(latitude_wgs84),
                longitude_median_wgs84 = median(longitude_wgs84),
                date_export = max(date_export)),
            by=id_carre]


    dd[,db := "vigieplume"]
    setnames(dd,old=c("id_carre"),new=c("pk_carre"))

  ##
    setkey(dd,"pk_carre")

    dd <- merge(dd,dcarrenat,by="pk_carre",all.x=TRUE)
   ### dd <- dd[dcarrenat]


    colorder <- c("pk_carre","commune","site","etude","insee","departement","nom_carre","nom_carre_fnat","altitude_median","latitude_median_wgs84","longitude_median_wgs84","latitude_grid_wgs84","longitude_grid_wgs84","db","date_export")


    dd <- dd[,colorder,with=FALSE]

    filename <- paste0(repOut,"carre_VP_",dateConstruction,".csv")
    write.csv(dd,filename,row.names=FALSE)
    cat("  (->",filename,")\n")

    if(output) return(dd)

}

vp2inventaire <- function(d,dateExport,version = "V.1",dateConstruction="",repOut="",output=FALSE) {

    ## dateExport=dateExportVP
    ## d=dVP

    require(maptools)

    dd <- dVP[,.(unique_inventaire_fnat=NA,
               etude = etude,
               id_point = id_point,
               id_carre = id_carre,
               num_point = num_point,
               date = date,
               jour_julien = jour_julien,
               annee= annee,
               passage_observateur = passage_observateur,
               heure_debut = heure,
               heure_fin = NA,
               duree_minute = duree_minute,
               observateur = observateur,
               email = email,
               nuage = nuage,
               pluie = pluie,
               vent = vent,
               visibilite = visibilite,
               neige = neige,
               altitude = get_mode(altitude),
               latitude_wgs84 = median(latitude_wgs84),
               longitude_wgs84 = median(longitude_wgs84),
               db = "vigieplume",
               date_export=date_export,version=version), by = id_inventaire]

    dd <- unique(dd)
    aa <- table(dd$id_inventaire)
    aa_unique <- names(aa)[aa==1]
    aa_doublon <- names(aa)[aa>1]

    if(length(aa_doublon)>0) {

        cat(" ! WARNING: gestion de ",length(aa_doublon)," doublon(s) dans les inventaires !\n")
        dd_unique <- dd[id_inventaire %in% aa_unique,]

        dd_doub <- dd[id_inventaire %in% aa_doublon,]

        dd_doub <- unique(dd_doub[,.(unique_inventaire_fnat=NA,
               etude = get_mode(etude),
               id_point = get_mode(id_point),
               id_carre = get_mode(id_carre),
               num_point = get_mode(num_point),
               date = get_mode(date),
               jour_julien = get_mode(jour_julien),
               annee= get_mode(annee),
               passage_observateur = get_mode(passage_observateur),
               heure_debut = get_mode(heure_debut),
               heure_fin = NA,
               duree_minute = get_mode(duree_minute),
               observateur = concat_value(observateur),
               email = concat_value(email),
               nuage = get_mode(nuage),
               pluie = get_mode(pluie),
               vent = get_mode(vent),
               visibilite = get_mode(visibilite),
               neige = get_mode(neige),
               altitude = get_mode(altitude),
               latitude_wgs84 = median(latitude_wgs84),
               longitude_wgs84 = median(longitude_wgs84),
               db = "vigieplume",
               date_export=get_mode(date_export),version=version), by = id_inventaire])

        dd <- rbind(dd_unique,dd_doub)
        setDT(dd)
        setorder(dd,id_inventaire)
    }

    setnames(dd,"id_inventaire","pk_inventaire")
     dd[,heure_fin := format(as.POSIXct(heure_debut,format="%H:%M")+ duree_minute*60,"%H:%M")]

    dd[,datetime := paste(date,heure_debut)]

    dd[,periode_passage := ""]
    dd[jour_julien < 61 ,periode_passage:= "winter_end"]
    dd[,passage_stoc := 999]

    ## precoce
    dd[jour_julien > 60 & jour_julien <= 91 , passage_stoc :=  0] # 01/03 -> 31/03

    ## en plaine
    dd[jour_julien > 91 & jour_julien <= 128 & altitude < 800,passage_stoc :=  1] #01/04 -> 08/05
    dd[jour_julien >= 129 & jour_julien <= 167 & altitude < 800,passage_stoc :=  2] #09/05 -> 15/06

    ## en altitude
    dd[jour_julien > 91 & jour_julien <= 134 & altitude >= 800,passage_stoc :=  1] #01/04 -> 14/05
    dd[jour_julien >= 135 & jour_julien <= 167 & altitude >= 800,passage_stoc :=  2] #15/05 -> 15/06
    ## tardif
    dd[jour_julien > 167 & jour_julien <= 197 , passage_stoc :=  3] # 16/06 -> 15/07

    dd[passage_stoc == 0 , periode_passage := "early"]
    dd[passage_stoc == 1 , periode_passage := "first"]
    dd[passage_stoc == 2 , periode_passage := "second"]
    dd[passage_stoc == 3, periode_passage :=  "late"]
    dd[jour_julien > 197 ,periode_passage:= "year_end"]
    dd[passage_stoc == 999, passage_stoc := NA]

 dd[,datetime_UTC := as.POSIXct(datetime,format="%Y-%m-%d %H:%M")]

    dd_sun <- dd[!is.na(date) & !is.na(datetime) & !is.na(longitude_wgs84) & !is.na(latitude_wgs84),]
    dd_sun[,date_UTC := as.POSIXct(date,tz = "GMT")]


    cat(nrow(dd_sun),"samples with valid date and time and location\n")
    coordinates(dd_sun) <- c("longitude_wgs84", "latitude_wgs84")
    lonlat <- SpatialPoints(coordinates(dd_sun),proj4string=CRS("+proj=longlat +datum=WGS84"))

    cat("calculating sunrise...")
    dd_sun$sunrise <- sunriset(crds = lonlat, dateTime = dd_sun$date_UTC, direction="sunrise", POSIXct=TRUE)$time
    cat("Done !\n")

     cat("calculating civil dawn...")
    dd_sun$dawn_civil <- crepuscule(crds = lonlat, dateTime = dd_sun$date_UTC,solarDep=6, direction="dawn", POSIXct=TRUE)$time
    cat("Done !\n")

    dd_sun <- as.data.table(dd_sun)

    dd_sun[,diff_sunrise_h := round(as.numeric(difftime(datetime_UTC,sunrise,units = "hours")),2)]
    dd_sun[,diff_dawn_h := round(as.numeric(difftime(datetime_UTC,dawn_civil,units = "hours")),2)]
    dd_sun[,info_heure_debut := ifelse(diff_dawn_h < 1, "too_early",ifelse(diff_sunrise_h < 1, "early",ifelse(diff_sunrise_h > 4, "late","OK")))]

    dd_sun <- dd_sun[,.(pk_inventaire,diff_sunrise_h,diff_dawn_h,info_heure_debut)]

    dd <- merge(dd,dd_sun,by="pk_inventaire",all.x=TRUE)


    dd[,nombre_passage_annee := .N ,by=.(id_point,annee)]
    dd[,nombre_passage_stoc_annee := length(unique(passage_stoc)) ,by=.(id_point,annee)]

    dd[,repetition_passage := .N ,by=.(id_point,annee,passage_stoc)]
    dd[,inc_passage := 1:.N ,by=.(id_point,annee,passage_stoc)]
    dd[,heure_ok := info_heure_debut %in% c("early","OK")]

  ##  dd[,repetition_passage_heure_ok := .N ,by=.(id_point,annee,passage_stoc,heure_ok)]
  ##  dd[,inc_passage_heure_OK := 1:.N ,by=.(id_point,annee,passage_stoc,heure_ok)]

    dd[repetition_passage != inc_passage ,passage_stoc := NA]
    dd[,info_passage_an := ifelse(1 %in% passage_stoc & 2 %in% passage_stoc,"OK_1_and_2",ifelse(1 %in% passage_stoc, "only_1",ifelse(2 %in% passage_stoc,"only_2",""))),by = .(id_point,annee)]

    dd[,info_passage := ifelse(repetition_passage == 1,"unique",ifelse(is.na(passage_stoc),"sample_not_selected","last_sample_selected"))]



    dd[info_passage_an == "OK_1_and_2",nb_sem_entre_passage := round(as.numeric(difftime(max(datetime_UTC),min(datetime_UTC), units = "weeks")),1),by = .(id_point,annee)]
    dd[,nb_sem_entre_passage := as.numeric(nb_sem_entre_passage)]

    ## recommendation 4 -> 6 semaines
    dd[,info_temps_entre_passage := ifelse(!is.na(nb_sem_entre_passage),ifelse(round(nb_sem_entre_passage) < 4, "inf_4",ifelse(round(nb_sem_entre_passage) > 6, "sup_6","OK")),"")]


    the_col <- c("pk_inventaire","unique_inventaire_fnat","etude","id_point","id_carre","num_point",
                 "date","jour_julien","annee","passage_observateur","heure_debut","heure_fin","duree_minute","datetime",
                 "periode_passage","passage_stoc","repetition_passage","inc_passage",
                 "diff_sunrise_h","diff_dawn_h","info_heure_debut",
                 "nombre_passage_annee","nombre_passage_stoc_annee","info_passage_an","info_passage",
                 "nb_sem_entre_passage","info_temps_entre_passage",
                 "observateur","email","nuage","pluie","vent","visibilite","neige","db","date_export","version")

    dd<-dd[,the_col,with=FALSE]


    filename <- paste0(repOut,"inventaire_VP_",dateConstruction,".csv")
    write.csv(dd,filename,row.names=FALSE)
    cat("  (->",filename,")\n")

    if(output) return(dd)

}



vp2observation <- function(d,dateExport,dateConstruction="",repOut="",output=FALSE) {

    d[,id_fnat_unique_citation := NA]
    dd <- d[,.(id_observation,id_fnat_unique_citation ,id_inventaire,id_point,id_carre, num_point,date,annee,classe,id_data,espece,code_sp,abondance,distance_contact,db,date_export)]

    dd <- unique(dd)
    setorder(dd,id_data)
    setnames(dd,"id_observation","pk_observation")

    contDoublon <- table(dd$id_data)
    doublon <- names(contDoublon)[contDoublon>1]

    if(length(doublon)>0) {
        tDoublon <- dd[id_data %in% doublon,]

        file <- paste0(repOut,"_doublonObservationVigiePlume_",dateConstruction,".csv")
        cat("  \n !!! Doublon saisies \n c est a dire plusieurs lignes pour un inventaire une espece et une distance\n Ceci concerne ", nrow(tDoublon)," lignes\n")
        cat("  Toutes lignes sont présenté dans le fichier: \n  --> ",file,"\n")
        fwrite(tDoublon,file)

        cat(" \n Pour chaque doublon nous conservons la valeur maximum\n")

        tDoublon <- setorder(tDoublon,id_data, -abondance)
        tDoublon[,conservee := !duplicated(id_data)]

        dataExclues <- tDoublon[conservee == FALSE,]
        col <- c("pk_observation","id_fnat_unique_citation","id_inventaire","id_point","id_carre","num_point","date","annee","classe","espece","code_sp","distance_contact","abondance","db","date_export")
        dataEclues <-dataExclues[,col,with=FALSE]

        file <- paste0(repOut,"_doublonObservationVigiePlume_Exclu_",dateConstruction,".csv")
        cat("  \n !!! Doublon exclue enregistre dans le fichier \n --> ",file,"\n")
        write.csv(tDoublon[,-ncol(tDoublon)],file,row.names=FALSE)

        dd <- dd[!(pk_observation %in% dataExclues[,pk_observation]),]

        setorder(dd,id_inventaire)
    }

    filename <- paste0(repOut,"observation_VP_",dateConstruction,".csv")
    write.csv(dd,filename,row.names=FALSE)
    cat("  (->",filename,")\n")

    if(output) return(dd)

}


vp2habitat <- function(d,dateExport,dateConstruction="",repOut="",output=FALSE) {

##    d <- dVP

    d[,`:=`(p_habitat = NA,s_habitat = NA)]

    dd <- unique(d[,.(id_inventaire, id_point,date,annee,
                      p_habitat,p_milieu,p_type,p_cat1,p_cat2,
                      s_habitat,s_milieu,s_type,s_cat1,s_cat2,
                     db,date_export)])
    setnames(dd, "id_inventaire","pk_habitat")

 ##   aa <- table(dd[,pk_habitat])
 ##
 ##   aa_unique <- names(aa)[aa==1]
 ##   aa_doublon <- names(aa)[aa>1]

   ## suppression des valeur d habitat abberante
    milieuPossible <- LETTERS[1:7]
    dd[,`:=`(p_milieu = toupper(p_milieu),s_milieu = toupper(s_milieu))]
    dd[!(p_milieu %in% milieuPossible),p_milieu := NA ]
    dd[!(s_milieu %in% milieuPossible), s_milieu := NA ]

    dd[,`:=`(p_habitat = paste0(p_milieu,p_type),s_habitat = paste0(s_milieu,s_type))]
    dd[,p_habitat := gsub("NA","",p_habitat)][p_habitat == "", p_habitat := NA]
    dd[,s_habitat := gsub("NA","",s_habitat)][s_habitat == "", s_habitat := NA]

    setorder(dd, date,id_point,p_habitat,s_habitat)
    ## construction de la suggestion de correction en fonction des habitats saisis pour les inventaire precedent et suivant

    ## inventaire precedent
    dd[,inc_point := 1:.N,by = id_point]
    dd_pre <- dd[,.(id_point,inc_point, p_habitat,s_habitat,date)]
    dd_pre[,inc_point := inc_point + 1]
    setnames(dd_pre,c("p_habitat","s_habitat","date"),c("p_habitat_pre","s_habitat_pre","date_pre"))

    ## inventaire suivent
    dd_post <- dd[,.(id_point,inc_point, p_habitat,s_habitat,date)]
    dd_post[,inc_point := inc_point - 1]
    setnames(dd_post,c("p_habitat","s_habitat","date"),c("p_habitat_post","s_habitat_post","date_post"))

    dd <- merge(dd,dd_pre,by=c("id_point","inc_point"),all.x=TRUE)
    dd <- merge(dd,dd_post,by=c("id_point","inc_point"),all.x=TRUE)

    ## suggestion
    dd[,p_habitat_sug := ifelse(p_habitat_pre == p_habitat_post,p_habitat_pre,NA)]
    dd[is.na(p_habitat_sug), p_habitat_sug := p_habitat]
    dd[,p_habitat_consistent := p_habitat == p_habitat_sug ]
    dd[,s_habitat_sug := ifelse(s_habitat_pre == s_habitat_post,s_habitat_pre,NA)]
    dd[is.na(s_habitat_sug), s_habitat_sug := s_habitat]
    dd[,s_habitat_consistent := s_habitat == s_habitat_sug]

    dd[,habitat_point_id := paste0(id_point,p_habitat_sug,s_habitat_sug)]
    setorder(dd, date,id_point,p_habitat,s_habitat)
    dd[,inc_habitat_point := 1:.N,by = habitat_point_id]
    setorder(dd,id_point,inc_point,inc_habitat_point)

    ## construction de suggestion pour les p_habitat NA en fonction des saisie precedentes et suivante

    ## creation d'un indentifiant de serie temporel d'habitat constant
    inc_decal <- c(0,dd[1:(nrow(dd)-1),inc_habitat_point])
    dd[,habitat_point_inc_id := 1+ cumsum(as.numeric(inc_habitat_point <= inc_decal))]

    ## table des saisies précedentes
    dd_habitat_inc_pre <- unique( dd[,.(id_point,p_habitat_sug, s_habitat_sug,last_date=max(date)),by = habitat_point_inc_id])

    setnames(dd_habitat_inc_pre,c("p_habitat_sug","s_habitat_sug","last_date"),paste0(c("p_habitat_sug","s_habitat_sug","last_date"),"_pre"))
    dd_habitat_inc_pre[, habitat_point_inc_id_pre :=  habitat_point_inc_id ]
    dd_habitat_inc_pre[, habitat_point_inc_id :=  habitat_point_inc_id +1]


    dd <- merge(dd,dd_habitat_inc_pre,by=c("habitat_point_inc_id","id_point"),all.x=TRUE)

    ## table des saisies suivante
    dd_habitat_inc_post <- unique( dd[,.(id_point,p_habitat_sug, s_habitat_sug,first_date = min(date)),by = habitat_point_inc_id])

    setnames(dd_habitat_inc_post,c("p_habitat_sug","s_habitat_sug","first_date"),paste0(c("p_habitat_sug","s_habitat_sug","first_date"),"_post"))
    dd_habitat_inc_post[, habitat_point_inc_id_post :=  habitat_point_inc_id ]
    dd_habitat_inc_post[, habitat_point_inc_id :=  habitat_point_inc_id -1]



    dd <- merge(dd,dd_habitat_inc_post,by=c("habitat_point_inc_id","id_point"),all.x=TRUE)

    ## suggestion si p_habitat_sug est NA
    dd[,time_last_declaration_sug_j := ifelse(p_habitat_consistent==TRUE,0,ifelse(!is.na(p_habitat_sug),difftime(as.Date(date),as.Date(date_pre),units="days"),NA))]
    dd[is.na(p_habitat_sug) & !(is.na( p_habitat_sug_pre)),time_last_declaration_sug_j :=difftime(as.Date(date),as.Date(last_date_pre),units="days") ]
    dd[is.na(p_habitat_sug) & !(is.na( p_habitat_sug_pre)),p_habitat_sug := p_habitat_sug_pre ]
    dd[is.na(p_habitat_sug) & !(is.na( p_habitat_sug_pre)),s_habitat_sug := s_habitat_sug_pre ]
    dd[is.na(p_habitat_sug) & !(is.na( p_habitat_sug_pre)),p_habitat_consistent := FALSE ]

    ## suggestion si p_habitat_sug est encore NA
    dd[is.na(p_habitat_sug) & !(is.na( p_habitat_sug_post)),time_last_declaration_sug_j :=difftime(as.Date(date),as.Date(first_date_post),units="days") ]
    dd[is.na(p_habitat_sug) & !(is.na( p_habitat_sug_post)),p_habitat_sug := p_habitat_sug_post ]
    dd[is.na(p_habitat_sug) & !(is.na( p_habitat_sug_post)),s_habitat_sug := s_habitat_sug_post ]
    dd[is.na(p_habitat_sug) & !(is.na( p_habitat_sug_post)),p_habitat_consistent := FALSE ]

    dd <- dd[,.(pk_habitat, id_point, date,annee,p_habitat_sug,p_habitat,p_habitat_consistent,p_milieu,p_type,p_cat1, p_cat2, s_habitat_sug,s_habitat,s_habitat_consistent, s_milieu, s_type, s_cat1, s_cat2 ,time_last_declaration_sug_j, db ,date_export)]

    filename <- paste0(repOut,"habitat_VP_",dateConstruction,".csv")
    write.csv(dd,filename,row.names=FALSE)
    cat("  (->",filename,")\n")

    if(output) return(dd)

}





#####################################################
##  Les fonctions pour les tables issue de FNat
#####################################################




FNat_importation <- function(nomFileFNat,dateExportFNat,repImport="data_raw/",repOut) {

    ## file=nomDBFNat;fichierAPlat=nomFileFNat;output=TRUE;repImport="data_raw/"

    require(data.table)
    require(sf)

    d_nom <- data.table(old=c("unique_inventaire","unique_citation","numobs","etude","unique_localite","section_cadastrale","lieudit","pays","dept","insee","dateobs","heure","duree","nbr_echantillon","nom","email","altitude","classe","espece","nbr_ind","classe_dist","longitude_lambert_93","latitude_lambert_93","nuage","pluie","vent","visibilite","hab_p_milieu","hab_p_type_milieu","hab_p_cat_1","hab_p_cat_2","hab_p_sous_cat_1","hab_p_sous_cat_2","hab_s_milieu","hab_s_type_milieu","hab_s_cat_1","hab_s_cat_2","hab_s_sous_cat_1","hab_s_sous_cat_2"),new=c("id_inventaire_fnat","id_fnat_unique_citation","numobs","etude","unique_localite","nom_point","nom_carre","pays","departement","insee","date","heure","duree_minute","passage_observateur","observateur","email","altitude","classe","espece","abondance","distance_contact","longitude_lambert_93","latitude_lambert_93","nuage","pluie","vent","visibilite","p_milieu","p_type","p_cat1","p_cat2","p_sous_cat1","p_sous_cat2","s_milieu","s_type","s_cat1","s_cat2","s_sous_cat1","s_sous_cat2"))


    d <- NULL

    if(length(dateExportFNat) ==1) dateExportFNat <- rep(dateExportFNat,length(nomFileFNat))

    nfile <- length(nomFileFNat)
    cat("\n",nfile,"fichier(s) FNat à importer\n")

    for(i in 1:length(nomFileFNat)) {
        nomFileFNati <- paste0(repImport,nomFileFNat[i])
        cat(" <-- (",i,"/",nfile,") Fichier FNat: ",nomFileFNati," ",sep="")
        flush.console()
        di <- read.csv(nomFileFNati,h=TRUE,fileEncoding="iso-8859-1",stringsAsFactors=FALSE)
        cat("  DONE ! \n")
        flush.console()
        di <- data.table(di)
        cat(" vérification colonnes\n")

        col_abs <- setdiff(d_nom[,old],colnames(di))
        n_col_abs <- length(col_abs)
        if(n_col_abs > 0) {
            cat("  !! ATTENTION !! ",n_col_abs," absente(s):\n")
            the_cols_txt <- paste(col_abs,collapse=", ")
            cat(the_cols_txt,"\n")
            txt <- "\n Choix: \n1- ne pas importer le fichier \n2- importer le fichier et ajoute NA dans les colonnes manquantes \n3- Arreter le processus\n"
            cat(txt,"\n")
            rep <- readline(prompt = txt)


        } else {
            ## renome les colonnes
            di <- setnames(di,old=d_nom[,old],new=d_nom[,new])
            cat("  DONE ! \n")

            di[,date_export:= dateExportFNat[i]]
            d <- rbind(d,di)
        }
    }



    cat("\n    !!! suppression des lignes pour les quelles le numéros du point n'est pas saisie\n")

    d.sansPoint <- d[nom_point==""]

    if(nrow(d.sansPoint) > 0) {
        cat("\   il y a ",nrow(d.sansPoint)," observation dans les données fnat qui n'ont pas de numéros de point\n")
        fileCSV <- paste0(repOut,"FNAT_erreur_abscence_numeros_point.csv")
        cat("  -->",fileCSV,"\n")
        write.csv(d.sansPoint,fileCSV,row.names=FALSE)
        d[nom_point !=  ""]
    }


    d <- d[!is.na(abondance) & !is.na(espece) & espece != ""]

    d[,insee := sprintf("%05d", insee)]
    d[,date := as.character(date)]
    d[,date := as.Date(date,format="%Y%m%d")]
    d[,heure := as.POSIXct(sprintf("%04d", heure),format="%H%M")]

    d[,numobs := as.character(numobs)]
    d[,nom_carre := as.character(nom_carre)]
    d[,point := gsub("Point N°","",nom_point)]
    d[,num_point := as.numeric(point)]

    d[,etude_detail := etude]
    d[etude %in% c("STOC-EPS","STOC-EPS MAMMIFERES"),etude := "STOC_EPS"]
    d[etude =="STOC-SITES ONF",etude := "STOC_ONF"]
    d[grep("SITE",etude), etude := "STOC_SITE"]


    dCarreONF <- fread("data_generic/siteONF.csv",encoding="UTF-8")
    dCarreONF <- dCarreONF[,c(1,2,5)]
    dCarreONF$nouveau_lieudit <- sprintf("%06d",dCarreONF$nouveau_lieudit)
    colnames(dCarreONF) <- c("nom_carre","departement","id_carre_onf")
    dCarreONF[,departement := sprintf("%02d", departement)]

    d[dCarreONF,on = .(nom_carre,departement),id_carre_onf := id_carre_onf]


    d_abbrev <- fread("library/abbrev_carre.csv")
    d_abbrev_l <- d_abbrev[lower == TRUE ,]
    d_abbrev_u <- d_abbrev[lower == FALSE ,]

    d[etude == "STOC_SITE",id_carre := nom_carre]

    pat <- c(d_abbrev_l[,txt])
    repl <-  c(d_abbrev_l[,abbrev])
    d[etude == "STOC_SITE",id_carre := mgsub(pat,repl,id_carre)]
    d[etude == "STOC_SITE",id_carre := gsub(".*Maille ","",id_carre)]
    d[etude == "STOC_SITE",id_carre := gsub(".*: ","",id_carre)]

    d[etude == "STOC_SITE", id_carre := toupper(id_carre)]

    vec <- c("LE ","LA ","LES ","DE ","A ","AU ","L'","DU ","DES ","D'","& ")
    d[etude == "STOC_SITE",id_carre := mgsub(vec,"",id_carre)]
    pat <- c(d_abbrev_u[,txt])
    repl <-  c(d_abbrev_u[,abbrev])
    d[etude == "STOC_SITE",id_carre := mgsub(pat,repl,id_carre)]
    vec <- c("(",")","_","-",".",",")
    d[etude == "STOC_SITE",id_carre := mgsub(vec," ",id_carre)]

    d[etude == "STOC_SITE" & nchar(gsub(' ','',id_carre)) <= 10 ,id_carre := gsub(' ','',id_carre)]
    d[etude == "STOC_SITE" & nchar(gsub(' ','',id_carre)) > 10, id_carre := gsub(" ","",paste0(substr(id_carre,1,3),substr(id_carre,nchar(id_carre)-2,nchar(id_carre)),nchar(id_carre),nchar(gsub("[^aeiouy]","",id_carre, ignore.case = TRUE))))]


    d[etude == "STOC_EPS"|substring(nom_carre,1,9)=="Carré EPS" ,id_carre := substring(nom_carre,13,nchar(nom_carre))]
    d[, id_carre:= paste0(departement,id_carre)]

##################

    d[etude == "STOC_SITE", id_carre := paste0("S",toupper(id_carre))]

    d[,id_carre := ifelse(is.na(id_carre_onf),id_carre,id_carre_onf)]

    d[id_carre %in% unique(d[etude=="STOC_ONF",id_carre]), etude := "STOC_ONF"]
    d[,id_point := paste0(id_carre,"P",point)]


    d[,jour_julien := as.numeric(format(date,"%j"))]
    d[,annee := as.numeric(format(date,"%Y"))]

    d[,id_inventaire :=  paste0(format(date,"%Y%m%d"),id_point)]
    d[,heure_fin :=  format(heure+as.difftime(paste0("00:",sprintf("%02d", duree_minute)),"%H:%M"),"%H:%M")]
    d[,heure :=  format(heure,"%H:%M")]

    d[,id_data := paste0(id_inventaire,"_",substring(espece,1,6),"_",distance_contact)]
    setorder(d, id_inventaire,espece,distance_contact)
    d <- d[,inventaire_inc := 1:.N,by=id_inventaire]
    d <- d[,inventaire_inc :=sprintf("%02d",inventaire_inc)]
    d <- d[,id_observation := paste0(id_inventaire,inventaire_inc)]
    d[,altitude := as.numeric(altitude)]




    d[,db:="fnat"]
    d[,commune := NA]

    d[,info_passage := NA]
    d[,passage_stoc := NA]
    d[,espece := toupper(espece)]
    d[,code_sp := substr(espece,1,6)]
    d[,abondance := as.numeric(abondance)]

    d[, p_milieu := toupper(ifelse(p_milieu == "",NA,p_milieu))]
    d[p_type == "", p_type := NA]
    d[p_cat1 == "", p_cat1 := NA]
    d[p_cat2 == "", p_cat2 := NA]
    d[, s_milieu := toupper(ifelse(s_milieu == "",NA,s_milieu))]
    d[s_type == "", s_type := NA]
    d[s_cat1 == "", s_cat1 := NA]
    d[s_cat2 == "", s_cat2 := NA]



    st_d <- st_as_sf(d,coords = c("longitude_lambert_93","latitude_lambert_93"),crs=2154) %>%
        st_transform(,crs=4326) %>% st_coordinates()
    colnames(st_d) <- c("longitude_wgs84","latitude_wgs84")

    d <- cbind(d,st_d)
    d[latitude_wgs84 < 41 | latitude_wgs84 > 52 ,latitude_wgs84 := NA]
    d[longitude_wgs84 < -5 | longitude_wgs84 > 9 ,longitude_wgs84 := NA]



    return(d)


}




FNat2point <- function(d,dateExport,repOut="",output=TRUE) {
                                        #   d <- dFNat
                                        #   dateExport <- dateExportFNat
                                        #    output=TRUE

d <- dFNat

    dagg <- d[, .(commune = get_mode(commune),
                  site = get_mode(site),
                  insee = get_mode(insee),
                  nom_point = get_mode(nom_point),
                  departement = get_mode(departement),
                  altitude = median(altitude),
                  latitude_wgs84 = median(latitude_wgs84),
                  longitude_wgs84 = median(longitude_wgs84),
                  latitude_wgs84_sd = sd(latitude_wgs84),
                  longitude_wgs84_sd = sd(longitude_wgs84),
                  date_export = max(date_export)),
              by=id_point]

    dd <- unique(d[,.(id_point,id_carre,num_point)])
    dd[,db := "vigieplume"]

    setkey(dagg,"id_point")
    setkey(dd,"id_point")

    dd <- dd[dagg]
    setnames(dd,old=c("id_point"),new=c("pk_point"))

    colorder <- c("pk_point","id_carre","commune","site","insee","departement","nom_point","num_point","altitude","longitude_wgs84","latitude_wgs84","longitude_wgs84_sd","latitude_wgs84_sd","db","date_export")
    setcolorder(dd,colorder)





    dagg <- d[, .(insee = get_mode(insee),
                  nom_point = get_mode(nom_point),
                  departement = get_mode(departement),
                  altitude = median(altitude),
                  latitude_wgs84 = median(latitude_wgs84),
                  longitude_wgs84 = median(longitude_wgs84),
                  latitude_wgs84_sd = sd(latitude_wgs84),
                  longitude_wgs84_sd = sd(longitude_wgs84),
                  date_export = max(date_export)),
              by=id_point]

    dd <- unique(d[,.(id_point,id_carre,num_point)])
    dd[,`:=` (db = "FNat",commune = NA,site = NA)]

    setkey(dagg,"id_point")
    setkey(dd,"id_point")

    dd <- dd[dagg]
    setnames(dd,old=c("id_point"),new=c("pk_point"))


    colorder <- c("pk_point","id_carre","commune","site","insee","departement","nom_point","num_point","altitude","longitude_wgs84","latitude_wgs84","longitude_wgs84_sd","latitude_wgs84_sd","db","date_export")
    dd <- dd[,colorder,with=FALSE]

    filename <- paste0(repOut,"point_FNat_",dateExport,".csv")
    write.csv(dd,filename,row.names=FALSE)
    cat("  (-> ",filename,")\n",sep="")

    if(output) return(dd)
}



FNat2carre <- function(d,dateExport,repOut="",output=FALSE) {
    library(rgdal)
    d = dFNat;dateExport=dateExportFNat;output=FALSE;repOut=""
    dcarrenat <- read.csv("data_generic/carrenat.csv",encoding="UTF-8")
    dcarrenat$pk_carre <- sprintf("%06d",    dcarrenat$pk_carre)


    dcoord <- data.frame(lon=d$longitude_lambert_93, lat=d$latitude_lambert_93)
    coordinates(dcoord) <- c("lon", "lat")
    proj4string(dcoord) <- CRS("+init=epsg:2154") # Lambert 93
    dcoord_WGS84 <- spTransform(dcoord, CRS("+init=epsg:4326"))

    dd <- data.frame(pk_carre =d$id_carre,
                     nom_carre = paste0("Carré EPS N°",d$id_carre),
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



    filename <- paste0(repOut,"carre_FNat_",dateExport,".csv")
    write.csv(dd,filename,row.names=FALSE)
    cat("  (->",filename,")\n")


    if(output) return(dd)



}




FNat2inventaire <-  function(d,dateExport,version = "V.1",dateConstruction="",repOut="",output=FALSE) {
    library(lubridate)

                                        #d = dFNat; dateExport = dateExportFNat ;verion = "VV";output=FALSE
    the_date <- as.Date(as.character(d$dateobs),format="%Y%m%j")
    carre <-  d$id_carre
    point <- substring(d$section_cadastrale,nchar(d$section_cadastrale)-1)
    id_point = paste0(carre,"P",point)
    heure <- strptime(d$heure,"%H%M")


    ddbrut <- data.frame(pk_inventaire = paste0(format(the_date,"%Y%m%d"),id_point),
                         unique_inventaire_fnat=d$unique_inventaire,
                         etude = d$etude,
                         id_point = id_point,
                         id_carre =  d$id_carre,
                         num_point = as.numeric(point),
                         date = the_date,
                         jour_julien =  as.numeric(format(the_date,"%j")),
                         annee=year(the_date),
                         passage = d$nbr_echantillon,
                         info_passage=NA,
                         heure_debut = format(heure,"%H:%M"),
                         heure_fin = format(heure+as.difftime(paste0("00:",sprintf("%02d", d$duree)),"%H:%M"),"%H:%M"),
                         duree_minute = d$duree,
                         observateur = d$nom,
                         email = d$email,
                         nuage = d$nuage,
                         pluie = d$pluie,
                         vent = d$vent,
                         visibilite = d$visibilite,
                         neige = NA,
                         db = "FNat",date_export=dateExport,altitude = d$altitude,version=version)
    dd <- unique(ddbrut)


    dd$altitude <-ifelse(is.na(dd$altitude),0,dd$altitude)


    aa <- table(dd$pk_inventaire)

    aa_unique <- names(aa)[aa==1]
    aa_doublon <- names(aa)[aa>1]

    if(length(aa_doublon)>0) {

        dd_unique <- subset(dd,pk_inventaire %in% aa_unique)

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




    filename <- paste0(repOut,"inventaire_FNat_",dateConstruction,".csv")
    write.csv(dd,filename,row.names=FALSE)
    cat("  (->",filename,")\n")


    if(output) return(dd)

}





FNat2observation <- function(d,dateExport,dateConstruction="",repOut="",output=FALSE) {
    library(doBy)
    ##  d = dFNat; dateExport = dateExportFNat ;verion = "VV";output=FALSE
    dd <- data.frame(pk_observation= paste0(d$dateobs,d$id_carre,"P",substring(d$section_cadastrale,nchar(d$section_cadastrale)-1)),
                     id_fnat_unique_citation = d$unique_citation,
                     id_inventaire =  paste0(d$dateobs,d$id_carre,"P",substring(d$section_cadastrale,nchar(d$section_cadastrale)-1)),
                     id_point =  paste0(d$id_carre,"P",substring(d$section_cadastrale,nchar(d$section_cadastrale)-1)),
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

    dd$pk_observation <- paste0(dd$pk_observation,sprintf("%02d",(1:nrow(dd))-iFirst_id_inventaire[as.character(dd$id_inventaire)]+1))

    dd$id_data=paste0(dd$id_inventaire,dd$code_sp,dd$distance_contact)


    contDoublon <- table(dd$id_data)
    doublon <- names(contDoublon)[contDoublon>1]
    if(length(doublon)>0) {
        tDoublon <- subset(dd,id_data %in% doublon)
        file <- paste0(repOut,"_doublonObservationFNat_",dateConstruction,".csv")
        cat("  \n !!! Doublon saisies \n c est a dire plusieurs lignes pour un inventaire une espece et une distance\n Ceci concerne ", nrow(tDoublon)," lignes\n")
        cat("  Toutes lignes sont présenté dans le fichier: \n  --> ",file,"\n")
        write.csv(tDoublon[,-ncol(tDoublon)],file,row.names=FALSE)

        cat(" \n Pour chaque doublon nous conservons la valeur maximum\n")



        tDoublon <- tDoublon[order(tDoublon$id_data, -tDoublon$abondance),]
        tDoublon$conservee <- !duplicated(tDoublon$id_data)

        dataExclues <- subset(tDoublon,!conservee)
        dataEclues <-subset(dataExclues,select=c("pk_observation","id_fnat_unique_citation","id_inventaire","id_point","id_carre","num_point","passage","date","annee","classe","espece","code_sp","distance_contact","abondance","db","date_export"))

        file <- paste0(repOut,"_doublonObservationFNat_Exclu_",dateConstruction,".csv")
        cat("  \n !!! Doublon exclue enregistre dans le fichier \n --> ",file,"\n")
        write.csv(tDoublon[,-ncol(tDoublon)],file,row.names=FALSE)

        dd <- subset(dd,!(pk_observation %in% tDoublon$pk_observation[!tDoublon$conservee]))

        dd <- dd[order(dd$id_inventaire,dd$espece,dd$distance_contact),]
    }


    dd <- dd[,-ncol(dd)]


    filename <-  paste0(repOut,"observation_FNat_",dateConstruction,".csv")
    write.csv(dd,filename,row.names=FALSE)
    cat("  (->",filename,")\n")

    if(output) return(dd)

}





FNat2habitat <- function(d,dateExport,repOut="",output=FALSE) {

    dd <- unique(data.frame(pk_habitat= paste0(d$dateobs,d$dept,substring(d$lieudit,13),"P",substring(d$section_cadastrale,nchar(d$section_cadastrale)-1)),


                            id_point = paste0(d$dept,substring(d$lieudit,13),"P",substring(d$section_cadastrale,nchar(d$section_cadastrale)-1)),
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


        ddbrut <- data.frame(pk_habitat= paste0(d$dateobs,d$dept,substring(d$lieudit,13),"P",substring(d$section_cadastrale,nchar(d$section_cadastrale)-1)),


                             id_point = paste0(d$dept,substring(d$lieudit,13),"P",substring(d$section_cadastrale,nchar(d$section_cadastrale)-1)),
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



    filename <- paste0(repOut,"habitat_FNat_",dateExport,".csv")
    write.csv(dd,filename,row.names=FALSE)
    cat("  (->",filename,")\n")





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



union.inventaire <- function(ddFNat.inv,ddVP.inv,dateConstruction,repOut="") {
    dd.inv <- rbind(ddVP.inv,subset(ddFNat.inv,!(pk_inventaire %in% unique(ddVP.inv$pk_inventaire))))
    dd.inv <- dd.inv[order(as.character(dd.inv$pk_inventaire)),]
    dd.invDoublon <- subset(ddFNat.inv,(pk_inventaire %in% unique(ddVP.inv$pk_inventaire)))
    dd.inv$etude[dd.inv$id_carre %in% unique(subset(dd.inv,etude == "STOC_ONF")$id_carre)] <- "STOC_ONF"
    if(nrow(dd.invDoublon)>0) {
        file <- paste0(repOut,"_doublonInventaireExclu_",dateConstruction,".csv")
        cat("\n !!! ",nrow(dd.invDoublon) ," inventaires saisies dans les deux bases de données (FNat et VigiePlume) \n Les donnees de FNat sont exclu et les donnees de VigiePlume conservees\n Retrouvees les donnees exclu dans le fichier:\n --> ",file,"\n")
        write.csv(dd.invDoublon,file,row.names=FALSE)

    }
    return(dd.inv)
}

union.observation <- function(ddFNat.obs,ddVP.obs,dateConstruction,repOut="") {
    ddFNat.obs$id_data=paste0(ddFNat.obs$id_inventaire,ddFNat.obs$code_sp,ddFNat.obs$distance_contact)
    ddVP.obs$id_data=paste0(ddVP.obs$id_inventaire,ddVP.obs$code_sp,ddVP.obs$distance_contact)

    dd.obs <- rbind(ddVP.obs,subset(ddFNat.obs,!(id_data%in% unique(ddVP.obs$id_data))))
    dd.obs <- dd.obs[order(as.character(dd.obs$pk_observation)),]
    dd.obs <- dd.obs[,-ncol(dd.obs)]
    dd.obsDoublon <-subset(ddFNat.obs,(id_data %in% unique(ddVP.obs$id_data)))
    if(nrow(dd.obsDoublon)>0) {
        file <- paste0(repOut,"_doublonObsExclu_",dateConstruction,".csv")
        cat("\n !!! ",nrow(dd.obsDoublon) ," observation saisies dans les deux bases de données (FNat et VigiePlume) \n Les donnees de FNat sont exclu et les donnees de VigiePlume conservees\n Retrouvees les donnees exclu dans le fichier:\n --> ",file,"\n")
        write.csv(dd.obsDoublon,file,row.names=FALSE)

    }
    return(dd.obs)
}


union.habitat <- function(ddFNat.hab,ddVP.hab,dateConstruction,repOut="") {
    dd.hab <- rbind(ddVP.hab,subset(ddFNat.hab,!(pk_habitat %in% unique(ddVP.hab$pk_habitat))))
    dd.hab <- dd.hab[order(as.character(dd.hab$pk_abitat)),]
    dd.habDoublon <- subset(ddFNat.hab,(pk_habitat %in% unique(ddVP.hab$pk_habitat)))
    if(nrow(dd.habDoublon)>0) {
        file <- paste0(repOut,"_doublonHabitatExclu_",dateConstruction,".csv")
        cat("\n !!! ",nrow(dd.habDoublon) ," habitats saisies dans les deux bases de données (FNat et VigiePlume) \n Les donnees de FNat sont exclu et les donnees de VigiePlume conservees\n Retrouvees les donnees exclu dans le fichier:\n --> ",file,"\n")
        write.csv(dd.habDoublon,file,row.names=FALSE)

    }
    return(dd.hab)
}


##########################################################################################


createDB_postgres <- function(dateConstruction,nomDBpostgresql=NULL,postgresUser="romain",postgresPassword=NULL,postgresql_createAll = TRUE,postGIS_initiation=TRUE,postgresql_abondanceSeuil=TRUE,seuilAbondance = .99,repertoire=NULL,repOut="",fileTemp=FALSE) {

    if(is.null(dateConstruction)) dateConstruction = format(start, "%Y-%m-%d")
    if(is.null(repertoire)) repertoire <- paste0(getwd(),"/")
    if(is.null(nomDBpostgresql)) nomDBpostgresql <- "stoc_eps"

########  linux <- Sys.info()[1]=="Linux"


    if(postgresql_createAll)  maketableGenerique(repertoire,nomDBpostgresql,postgresUser, fileTemp)

    cat("\n  Importation des tables des données STOC-eps et creation des index\n   ----------------------------------\n")
    cat("     1- point\n")
    cat("     2- carre\n")
    cat("     3- inventaire\n")
    cat("     4- observation\n")
    cat("     5- habitat\n")
    commande <- paste0("psql -U ",postgresUser," ",nomDBpostgresql," < ",repertoire,"library_sql/postgres_createTableBBS.sql")
    myshell(commande)

    ## importation data
    cat(" \\copy point FROM ",repertoire,
        "data_DB_import/point_",dateConstruction,".csv",
        " with (format csv, header, delimiter ';')\n",sep="",
        file=paste0(repertoire,"library_sql/_sqlR_importationData.sql"),append=FALSE)
    cat(" \\copy carre FROM ",repertoire,
        "data_DB_import/carre_",dateConstruction,".csv",
        " with (format csv, header, delimiter ';')\n",sep="",
        file=paste0(repertoire,"library_sql/_sqlR_importationData.sql"),append=TRUE)
    cat(" \\copy inventaire FROM ",repertoire,
        "data_DB_import/inventaire_",dateConstruction,".csv",
        " with (format csv, header, delimiter ';')\n",sep="",
        file=paste0(repertoire,"library_sql/_sqlR_importationData.sql"),append=TRUE)
    cat(" \\copy observation FROM ",repertoire,
        "data_DB_import/observation_",dateConstruction,".csv",
        " with (format csv, header, delimiter ';')\n",sep="",
        file=paste0(repertoire,"library_sql/_sqlR_importationData.sql"),append=TRUE)
    cat(" \\copy habitat (pk_habitat, id_point, passage, date, annee, p_milieu, p_type, p_cat1, p_cat2,s_milieu, s_type, s_cat1, s_cat2, db, date_export) FROM ",repertoire,
        "data_DB_import/habitat_",dateConstruction,".csv",
        " with (format csv, header, delimiter ';')\n",sep="",
        file=paste0(repertoire,"library_sql/_sqlR_importationData.sql"),append=TRUE)


    cmd_import <- paste0("psql -U ",postgresUser," ",nomDBpostgresql," < ",repertoire,"library_sql/_sqlR_importationData.sql")
                                        # commande <- paste0("psql -U ",postgresUser," ",nomDBpostgresql," -f ",repertoire,"library_sql/_sqlR_importationData.sql")
    myshell(cmd_import)


    cmd_import <- paste0("psql -U ",postgresUser," ",nomDBpostgresql," < ",repertoire,"library_sql/postgres_createIndexBBS.sql")
                                        # commande <- paste0("psql -U ",postgresUser," ",nomDBpostgresql," -f ",repertoire,"library_sql/postgres_createIndexBBS.sql")
    myshell(cmd_import)

    if(postgresql_abondanceSeuil) makeAbondanceTrunc(dateConstruction,seuilAbondance,repertoire,nomDBpostgresql,postgresUser,postgresPassword,repOut=repOut,fileTemp)

    if(postGIS_initiation) {
        cat("\n  Creation des champs postGIS\n   ----------------------\n")
        cat("     1- point.geom93\n")
        cat("     2- carre.grid_geom93\n")
        cat("     3- carre.points_geom93\n")


        commande <- paste0("psql -U ",postgresUser," ",nomDBpostgresql," < ",repertoire,"library_sql/postgis_initialImport.sql")
        myshell(commande)
    }

    if(postgresql_createAll & postGIS_initiation){
        cat("\n  Creation des champs postGIS\n   ----------------------\n")
        cat("     1- carrenat.geom93\n")


        commande <- paste0("psql -U ",postgresUser," ",nomDBpostgresql," < ",repertoire,"library_sql/postgis_tableGenerique.sql")
        myshell(commande)
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

    cat(DBname,user,ifelse(is.null(pw),"","****"),"\n")
                                        # about when I use windows a have to define the user
    if(is.null(user)) {
        con <- dbConnect(drv, dbname=DBname)
    } else {
        con <- dbConnect(drv, dbname=DBname,user=user, password=pw)
    }

    return(con)
}


maketableGenerique <- function(repertoire=NULL,nomDBpostgresql=NULL,postgresUser="romain", fileTemp=TRUE,savePostgres=TRUE) {

                                        # require(ggplot2)
                                        #  repertoire=NULL;savePostgres=TRUE;nomDBpostgresql=NULL;postgresUser="romain"; fileTemp=FALSE;
    cat("\n  Importation des tables generiques et creation des index\n   ------------------------------------\n")
    cat("     1- carrenat\n")
    cat("     2- espece\n")
    cat("     3- espece_list_indicateur\n")
    cat("     4- espece_indicateur_fonctionnel\n")

    if(is.null(repertoire)) repertoire <- paste0(getwd(),"/")
    if(is.null(nomDBpostgresql)) nomDBpostgresql <- "stoc_eps"

    commande <- paste0("psql -U ",postgresUser," ",nomDBpostgresql," < ",repertoire,"library_sql/postgres_createTableGenerique.sql")
    myshell(commande)

    cat(" \\copy carrenat FROM ",repertoire,
        "data_generic/carrenat.csv",
        " with (format csv, header, delimiter ',')\n",sep="",
        file=paste0(repertoire,"library_sql/_sqlR_importationDataTableGenerique.sql"),append=FALSE)

    cat(" \\copy species FROM ",repertoire,
        "data_generic/espece.csv",
        " with (format csv, header, delimiter ',')\n",sep="",
        file=paste0(repertoire,"library_sql/_sqlR_importationDataTableGenerique.sql"),append=TRUE)

    cat(" \\copy species_list_indicateur FROM ",repertoire,
        "data_generic/espece_list_indicateur.csv",
        " with (format csv, header, delimiter ',')\n",sep="",
        file=paste0(repertoire,"library_sql/_sqlR_importationDataTableGenerique.sql"),append=TRUE)

    cat(" \\copy species_indicateur_fonctionnel FROM ",repertoire,
        "data_generic/espece_indicateur_fonctionel.csv",
        " with (format csv, header, delimiter ',')\n",sep="",
        file=paste0(repertoire,"library_sql/_sqlR_importationDataTableGenerique.sql"),append=TRUE)

    cat(" \\copy habitat_code_ref FROM ",repertoire,
        "data_generic/code_habitat_ref.csv",
        " with (format csv, header, delimiter ';')\n",sep="",
        file=paste0(repertoire,"library_sql/_sqlR_importationDataTableGenerique.sql"),append=TRUE)

    cat(" \\copy habitat_cat FROM ",repertoire,
        "data_generic/habitat_cat.csv",
        " with (format csv, header, delimiter ';')\n",sep="",
        file=paste0(repertoire,"library_sql/_sqlR_importationDataTableGenerique.sql"),append=TRUE)





    commande <- paste0("psql -U ",postgresUser," ",nomDBpostgresql," < ",repertoire,"library_sql/_sqlR_importationDataTableGenerique.sql")
    myshell(commande)


    commande <- paste0("psql -U ",postgresUser," ",nomDBpostgresql," < ",repertoire,"library_sql/postgres_createIndexGenerique.sql")
    myshell(commande)

    if (!fileTemp){
        cat("REMOVE FILE TEMP\n")
        rmfile <- paste0(repertoire,"library_sql/_sqlR_importationDataTableGenerique.sql")
        file.remove(rmfile)
                                        #commande <- paste0("rm  ",repertoire,"library_sql/_sqlR_importationDataTableGenerique.sql")
                                        #myshell(commande)
    }


}


makeAbondanceTrunc <- function(dateConstruction,seuilAbondance = .99,repertoire=NULL,repOut="",nomDBpostgresql=NULL,postgresUser="romain",postgresPassword=NULL, fileTemp=TRUE,graphe=TRUE) {

    require(ggplot2)

    ## repertoire=NULL;savePostgres=TRUE;nomDBpostgresql=NULL;postgresUser="romain";
    ## fileTemp=TRUE; seuilAbondance = .99


    seuilAbondanceTxt <- as.character(seuilAbondance*100)

    cat("\n  Creation de la table des seuil d abondance\n   ----------------------------------\n")
    cat("     1- query table abondance\n")


    if(is.null(repertoire)) repertoire <- paste0(getwd(),"/")
    if(is.null(nomDBpostgresql)) nomDBpostgresql <- "stoc_eps"

    con <- openDB.PSQL(user=postgresUser,pw=postgresPassword,DBname=nomDBpostgresql)

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
    colnames(tseuil100) <- c("pk_species",paste0("abond100_seuil",seuilAbondanceTxt))

    cat("           * 200m\n")
    tAbond200 <- subset(tAbond,distance_contact %in% c("LESS25","LESS100","MORE100","LESS200"))
    tAbondSum200 <- aggregate(abondance~code_sp+id_inventaire,data=tAbond200,sum)
    tseuil200 <- aggregate(abondance~code_sp,data=tAbondSum200,FUN = function(X) ceiling(quantile(X,seuilAbondance))+1)

    ##tseuil200 <- data.frame(as.character(aggAbond[,1]),aggAbond[,2][,1],stringsAsFactors=FALSE)
    colnames(tseuil200) <- c("pk_species",paste0("abond200_seuil",seuilAbondanceTxt))

    cat("           * All\n")
    tAbondSumAll <- aggregate(abondance~code_sp+id_inventaire,data=tAbond,sum)
    tseuilAll <- aggregate(abondance~code_sp,data=tAbondSumAll,FUN = function(X) ceiling(quantile(X,seuilAbondance))+1)

    ##tseuilAll <- data.frame(as.character(aggAbond[,1]),aggAbond[,2][,1],stringsAsFactors=FALSE)
    colnames(tseuilAll) <- c("pk_species",paste0("abondAll_seuil",seuilAbondanceTxt))


    tseuil <- merge(tseuil100,tseuil200,by="pk_species",merge=ALL)
    tseuil <- merge(tseuil,tseuilAll,by="pk_species",merge=ALL)

    file_seuil <- paste0(repOut,"espece_abondance_point_seuil",dateConstruction,".csv")
    write.csv(tseuil,file_seuil,row.names=FALSE)

    cat("     3- importation de la table\n")
    cat("
DROP table if exists espece_abondance_point_seuil;
CREATE TABLE espece_abondance_point_seuil
	(pk_species varchar(6),
	abond100_seuil",seuilAbondanceTxt," real,
	abond200_seuil",seuilAbondanceTxt," real,
	abondAll_seuil",seuilAbondanceTxt," real);

 \\copy espece_abondance_point_seuil FROM ",repertoire,
file_seuil,
" with (format csv, header, delimiter ',')\n", sep="",file=paste0(repertoire,"library_sql/_sqlR_espece_abondance_point_seuil.sql"))


    cat("\n -- RUN SQL --\n")

    commande <- paste0("psql -U ",postgresUser," ",nomDBpostgresql," < ",repertoire,"library_sql/_sqlR_espece_abondance_point_seuil.sql")
    myshell(commande)


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

            ggfile <- paste0(repOut,"AbondanceSeuil_",vecSp[i1],"-",vecSp[i2],".png")
            ggsave(ggfile,gg,width = 12,height = 9)


            gg <- ggplot(tAbondSum.i,aes(x=abondanceTrunc,fill=groupe,colour=groupe,group=groupe))
            gg <- gg + geom_density(alpha=.2)
            gg <- gg + facet_wrap(~code.nomfr,scales="free")

            ggfile <- paste0(repOut,"AbondanceTruncSeuil_",vecSp[i1],"-",vecSp[i2],".png")
            ggsave(ggfile,gg,width = 12,height = 9)



        }


        if (!fileTemp){
            cat("REMOVE FILE TEMP\n")
            commande <- paste0("rm ",repertoire,"library_sql/_sqlR_espece_abondance_point_seuil.sql")
            myshell(commande)
            commande <- paste0("rm ",file_seuil)
            myshell(commande)
        }
    }

}



###############################################################################################



point_carre_annee <- function(dateConstruction=NULL,version="V.1",con=NULL,importation=TRUE,repertoire=NULL,nomDBpostgresql=NULL,postgresUser="romain",postgresPassword=NULL) {

    require(data.table)
    dateConstruction=NULL;version="V.1";con=NULL;importation=TRUE;repertoire=NULL;nomDBpostgresql=NULL;postgresUser="romain"
    start <- Sys.time()
    dateExport <- format(start,"%Y-%m-%d")

    if(is.null(dateConstruction)) dateConstruction = format(start, "%Y-%m-%d")
    if(is.null(repertoire)) repertoire <- paste0(getwd(),"/")
    if(is.null(nomDBpostgresql)) nomDBpostgresql <- "stoc_eps"

    cat("\n  Creation des tables point_annee et carre_annee\n   ----------------------------------\n")

    cat("     1- construction table point_annee\n")

#######################################

    if(is.null(con))  con <- openDB.PSQL(user=postgresUser,pw=postgresPassword,DBname=nomDBpostgresql)

    query <-"
SELECT
i.annee::varchar(4)||i.id_point AS pk_point_annee, i.id_point,i.annee,i.date,passage_stoc,info_passage,nombre_de_passage,
COALESCE(h.p_milieu,'')||COALESCE(h.p_type::character,'') AS p_habitat,
COALESCE(h.s_milieu,'')||COALESCE(h.s_type::character,'') AS s_habitat
FROM inventaire AS i
LEFT JOIN habitat AS h ON i.pk_inventaire = h.pk_habitat
ORDER BY id_point, annee;
"

    cat("\nRequete:\n",query,"\n\n",sep="")
    d <- data.table(dbGetQuery(con,query))

    d[p_habitat=="",p_habitat := NA]
    d[s_habitat=="",s_habitat := NA]


    did <- unique(subset(d,select=c("pk_point_annee","id_point","annee")))
    did <- did[order(did$id_point,did$annee),]
    rownames(did) <- did$pk_point_annee
    did$p_habitat <- NA
    did$s_habitat <- NA
    did$p_dernier_descri <- 0

    ## suppression des valeur d habitat abberante
    habitatPossible <- LETTERS[1:7]
    d$p_milieu[!(d$p_milieu %in% habitatPossible)] <- NA
    d$s_milieu[!(d$s_milieu %in% habitatPossible)] <- NA

    ## ## Habitat principal
    dp<- d[,.(pk_point_annee,id_point,annee,passage_stoc,p_habitat,p_milieu,p_type)]

    ## on ne garde que les deux passages STOC donc on vire passage_stoc == NA et on vire aussi les habitat non renseigne
    dp <- dp[!(is.na(p_habitat)),]

    dp <- dp[,!("passage_stoc"),with=FALSE]
    udp <- unique(dp,by=colnames(dp))
    ## recherche les lignes dubliquees
    i.dup.dp <- which(duplicated(udp[,.(pk_point_annee,id_point,annee)]))
    ## table des duplications
    dup.dp <- udp[i.dup.dp,.(pk_point_annee,id_point,annee)]
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



    query <- "
SELECT pk_point_annee, count(passage_stoc)::real/2 AS qualite_inventaire_stoc
FROM (
     SELECT annee::varchar(4)||id_point AS pk_point_annee, passage_stoc
     FROM inventaire
     WHERE passage_stoc in (1,2)
     ) AS p
GROUP BY pk_point_annee;"

    cat("\nRequete:\n",query,"\n\n",sep="")

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
    ggsave(repOut,"habitatPoint.png",gg)


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
    ggsave(repOut,"habitatNbPoint.png",gg)

    did$id_carre <- substr(did$id_point,1,6)




    did <- did[,c("pk_point_annee","id_point","id_carre","annee","qualite_inventaire_stoc","p_milieu","s_milieu","p_dernier_descri","foret_p","ouvert_p","agri_p","urbain_p","foret_ps","ouvert_ps","agri_ps","urbain_ps")]

    did$date_export <- dateExport
    did$version <- version
    write.table(did,paste0("data_DB_import/point_annee.csv"),row.names=FALSE,na = "",quote=TRUE, sep=";",dec=".",fileEncoding="UTF-8")


    ## table carre
    cat("     2- construction table carre_annee\n")


    did$id_carre <- substr(did$id_point,1,6)

    dcarre <- aggregate(did[,c("qualite_inventaire_stoc","foret_p","ouvert_p","agri_p","urbain_p","foret_ps","ouvert_ps","agri_ps","urbain_ps")],list(did$id_carre ,did$annee), FUN = function(x) sum(as.numeric(x)))

    colnames(dcarre) <-c("id_carre","annee","qualite_inventaire_stoc",paste0("nbp_",c("foret_p","ouvert_p","agri_p","urbain_p","foret_ps","ouvert_ps","agri_ps","urbain_ps")))

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
    ggsave(repOut,"habitatNbCarre.png",gg)



    dcarre$pk_carre_annee <- paste0(dcarre$annee,dcarre$id_carre)

    dcarre <- dcarre[,c("pk_carre_annee","id_carre","annee","qualite_inventaire_stoc","foret_p","ouvert_p","agri_p","urbain_p","foret_ps","ouvert_ps","agri_ps","urbain_ps",paste0("nbp_",c("foret_p","ouvert_p","agri_p","urbain_p","foret_ps","ouvert_ps","agri_ps","urbain_ps")))]

    dcarre$date_export <- dateExport
    dcarre$version <- version

    write.table(dcarre,paste0("data_DB_import/carre_annee.csv"),row.names=FALSE,na = "",quote=TRUE, sep=";",dec=".",fileEncoding="UTF-8")


    if(importation) {
        cat("     3- Importation des tables\n")

        cat("\n -- RUN SQL --\n")
        commande <- paste0("psql -U ",postgresUser," ",nomDBpostgresql," < ",repertoire,"library_sql/postgres_point_carre_annee.sql")
        myshell(commande)



        cat(" \\copy point_annee FROM ",repertoire,
            "data_DB_import/point_annee.csv",
            " with (format csv, header, delimiter ';')\n",sep="",
            file=paste0(repertoire,"library_sql/_sqlR_importationData_point_carre_annee.sql"),append=FALSE)
        cat(" \\copy carre_annee FROM ",repertoire,
            "data_DB_import/carre_annee.csv",
            " with (format csv, header, delimiter ';')\n",sep="",
            file=paste0(repertoire,"library_sql/_sqlR_importationData_point_carre_annee.sql"),append=TRUE)

        commande <- paste0("psql -U ",postgresUser," ",nomDBpostgresql," < ",repertoire,"library_sql/_sqlR_importationData_point_carre_annee.sql")
        myshell(commande)


    }



}



###############################################################################################


historicCarre  <- function(con=NULL,nomDBpostgresql="stoc_eps",postgresUser="romain",postgresPassword=NULL,anneeMax=NULL) {

    ##  con=NULL;anneeMax=2017 ##

    require(ggplot2)
    require(reshape2)
    require(maps)
    require(maptools)
    require(animation)

    if(is.null(con))     con <- openDB.PSQL(user=postgresUser,pw=postgresPassword,DBname=nomDBpostgresql)



    if(is.null(anneeMax)) anneeTxt <- NULL else anneeTxt <- anneeMax
    if(is.null(anneeMax)) anneeMax = 9999

    ##        query <-paste0("select  from carre_annee where annee <= ",anneeMax," and annee > 2000 and qualite_inventaire_stoc > 0 group by id_carre, annee order by id_carre, annee;")



    query <-paste0("select  * from carre_annee where annee <= ",anneeMax," and annee > 2000 and qualite_inventaire_stoc > 0;")

    cat("Query:\n",query,"\n")

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

    fileCSV <- paste0(repOut,"carreSTOCactif_",anneeTxt,".csv")
    cat( "\n  CSV --> ", fileCSV,"\n")
    write.csv2(dan,fileCSV)

    ggAnnee <- melt(dan,"annee")

    gg <- ggplot(ggAnnee,aes(x=annee,y=value,colour=variable))+geom_line(size=1.1)+geom_point(size=1.3)
    gg <- gg + scale_colour_manual(values=c("nbCarre" = "#0d259f","Nouveaux"="#0d9f1b","Arrete" = "#9f0d0d" ,"NonRealise" = "#ff9d00"),
                                   labels=c("nbCarre" = "Carrés actif","Nouveaux"="Nouveaux carrés","Arrete" = "Carrés arrêtés","NonRealise" = "Carrés non réalisés"),name="" )
    gg <- gg + labs(title="",x="",y="")

    filePNG <- paste0(repOut,"carreSTOC_",anneeTxt,".png")
    cat( "\n  PNG --> ", filePNG,"\n")
    ggsave(filePNG,gg)



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

    filePNG <- paste0(repOut,"carreSTOC_pyramideAge_",anneeTxt,".png")
    cat( "\n  PNG --> ", filePNG,"\n")
    ggsave(filePNG,gg)



    fileCSV <- paste0(repOut,"carreSTOCage_",anneeTxt,".csv")
    cat( "\n  CSV --> ", fileCSV,"\n")
    write.csv2(dan,fileCSV)



    query <-paste0("select id_carre, longitude_grid_wgs84, latitude_grid_wgs84
from carre_annee as i, carre as c
where i.id_carre = c.pk_carre and annee <= 9999 and annee > 2000 and qualite_inventaire_stoc > 0
group by id_carre, longitude_grid_wgs84, latitude_grid_wgs84
order by id_carre;")

    cat("Query:\n",query,"\n")

    dcoord <- dbGetQuery(con, query)

    fileCSV <- paste0(repOut,"coord_carreSTOC_",anneeTxt,".csv")
    cat( "\n  CSV --> ", fileCSV,"\n")
    write.csv2(dcoord,fileCSV)



    france <- map_data("france")

    gg <- ggplot(dcoord,aes(longitude_grid_wgs84,latitude_grid_wgs84))+
        geom_polygon( data=france, aes(x=long, y=lat, group = group),colour="gray", fill="white",size=0.3 )+
        geom_point(size=.8,alpha=.8,colour="black")
    gg <- gg + theme(axis.ticks = element_blank(), axis.text = element_blank()) + labs(title="Localisation des carré STOC-EPS suivis au moins 1 année depuis 2001",x="",y="")
    gg <- gg + coord_fixed(ratio=1.2)

    filePNG <- paste0(repOut,"carreSTOC_map_simple_",anneeTxt,".png")
    cat( "\n  PNG --> ", filePNG,"\n")
    ggsave(filePNG,gg)




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

    fileGIF <- paste0(repOut,"carreSTOC_mapGIF_",anneeTxt,".gif")
    cat( "\n  GIF --> ", fileGIF,"\n")
    ggsave(fileGIF,gg,height = 10.5,width = 13)




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
    movie.name = fileGIF,ani.width = 500, ani.height = 500)





}




import_shape <- function(vecShape=c("pra_93","L93_10x10_TerreMer"),vecNameTable=c("pra","maille_atlas"),vecEPSG = NULL,
                         repertoire=NULL,nomDBpostgresql=NULL,postgresUser="romain",
                         fileTemp=TRUE,savePostgres=TRUE) {


                                        # require(ggplot2)
    ## vecShape=c("pra_93","L93_10x10_TerreMer");vecNameTable=c("pra","maille_atlas");vecEPSG = NULL
    ##    repertoire=NULL;savePostgres=TRUE;nomDBpostgresql=NULL;postgresUser="romain"; fileTemp=FALSE;


    cat("\n  Importation des shapes files \n   ------------------------------------\n")
    cat(vecShape)


    if(is.null(repertoire)) repertoire <- paste0(getwd(),"/")
    if(is.null(nomDBpostgresql)) nomDBpostgresql <- "stoc_eps"



    for (i in length(vecShape)) {

        sh <- vecShape[i]
        epsg <- vecEPSG[i]
        if(is.null(epsg)) epsg <- 2154 #Lambert 93

        fsh <- fsql <- paste0("data_DB_import/tableGeneriques/",sh,"/",sh)
        fsql <- paste0("data_DB_import/tableGeneriques/",sh,"/",sh,".sql")

        cmd <- paste0("shp2pgsql -I -s ",epsg,"  ",fsh," >  ",fsql," \n")
        cat(cmd,"\n")
        myshell(cmd)

        name_table <- tail(readLines(fsql, n=4),1)
        name_table <- gsub("CREATE TABLE \"","",name_table)
        name_table <- gsub("\" (gid serial,","",name_table,fixed=TRUE)


        query <- paste0("DROP TABLE IF EXISTS ",name_table,";")
        cat("drop query:", query,"\n")
        dbSendQuery(con, query)

### HERE !!!

        cmd <- paste0("psql -U ",postgresUser," -d ",nomDBpostgresql," < ",fsql)
        ## \copy species_list_indicator FROM c:/git/BirdLab/generic_data/espece_list_indicateur.csv with (format csv, header, delimiter ',')
        myshell(cmd,invisible=TRUE)

        name_table <-  vecNameTable[i]
        if(!(is.na(name_table))) {

        }
    }

    cat("\n\n --- Importation SQL ---\n\n")
    cat("\n\n - file: ",fsql,"\n")

    commande <- paste0("psql -U ",postgresUser," ",nomDBpostgresql," < ",repertoire,"library_sql/postgres_createTableGenerique.sql")
    shell(commande)

}

