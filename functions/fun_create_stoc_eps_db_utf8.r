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


library(RODBC)
library(reshape)
library(data.table)
library(rgdal)
library(sf)
library(dplyr)
library(qdap)


if("STOC_eps_database" %in% dir()) setwd("STOC_eps_database")

source("functions/fun_generic.r")
source("functions/fun_raw_data.r")
source("functions/fun_raw2table.r")
source("functions/fun_postgres_create.r")


f_prepaData <- function(
                        dateExportFaune=c("2023-03-23"),nomFileFaune=c("2023_03_23_data_stoc_eps_shoc.csv"),
                        dateExportVP=c("2019-11-25","2019-12-03"),nomFileVP=c("export_stoc_25112019.txt","export_stoc_onf_03122019.txt"),
                        dateExportFNat="2017-01-04",
                        importACCESS=FALSE,
                        nomFileFNat="FNat_plat_2017-01-04.csv",nomDBFNat="Base FNat2000.MDB",import_raw = TRUE,
                        importationDataBrut_FNat=TRUE,importationDataBrut_VP=TRUE,importationDataBrut_Faune=TRUE,
                        construction_tables = TRUE,
                        constructionPoint=TRUE,constructionCarre=TRUE,constructionInventaire=TRUE,
                        constructionObservation = TRUE, constructionSeuil = TRUE, constructionHabitat = TRUE,
                        constructionPointAnnee = TRUE, constructionCarreAnnee = TRUE,
                        dateConstruction="2023-05-25",postgresql_import=TRUE,
                        nomDBpostgresql="stoc_eps",
                        postgresql_createAll=TRUE,postgresUser="postgres",postgresPassword="postgres",
                        postGIS_initiation=TRUE,import_shape=FALSE,repertoire=NULL,fileTemp=FALSE)
{





    cat("               ===========================================================================\n")
    cat("               |                                                                         |\n")
    cat("               |            Importation des Donnees STOC-eps des 2 base de donnnes       |\n")
    cat("               |                              FNat et VigiePlume                         |\n")
    cat("               |                                                                         |\n")
    cat("               ===========================================================================\n")

    version <- "V.2.1_2023-06-01"

    cat(paste("\n\n Version",version,"\n"))
    cat(" ----------------------------\n\n")


##dateExportFaune=c("2023-03-23");nomFileFaune=c("2023_03_23_data_stoc_eps_shoc.csv");
##
##    dateExportVP=c("2019-11-25","2019-12-03");nomFileVP=c("export_stoc_25112019.txt","export_stoc_onf_03122019.txt");
##    dateExportFNat="2017-01-04";
##    importACCESS=FALSE;
##    nomFileFNat="FNat_plat_2017-01-04.csv";nomDBFNat="Base FNat2000.MDB";
##    importationDataBrut_FNat=TRUE;importationDataBrut_VP=TRUE;importationDataBrut_Faune=TRUE;
##    constructionPoint=TRUE;constructionCarre=TRUE;constructionInventaire=TRUE;
##    constructionObservation = TRUE; constructionSeuil = TRUE; constructionHabitat = TRUE;
##    constructionPointAnnee = TRUE; constructionCarreAnnee;
## dateConstruction=NULL;postgresql_import=TRUE;nomDBpostgresql=NULL;seuilAbondance = .99
##    nomDBpostgresql="stoc_eps";
##    postgresql_createAll=TRUE;postgresUser="postgres";
##    postGIS_initiation=TRUE;import_shape=FALSE;repertoire=NULL;postgresql_abondanceSeuil=TRUE;historiqueCarre=TRUE;
##    pointCarreAnnee=TRUE;importPointCarreAnnee=TRUE;fileTemp=FALSE
##    ##
##    postgresPassword = "postgres"
##

    start <- Sys.time() ## heure de demarage est utiliser comme identifiant par defaut
    if(is.null(dateConstruction)) dateConstruction = format(start, "%Y-%m-%d")

    repOutInfo <- paste0("output_import/",dateConstruction,"/")
    dir.create(repOutInfo,showWarnings=FALSE)

    repOutData <- paste0("data_DB_import/",dateConstruction,"/")
    dir.create(repOutData,showWarnings=FALSE)

    cat("\n   # Debut du process:",format(start, "%Y-%m-%d %H:%M\n"))

    ##    nomFileVP=paste0("data_raw/",nomFileVP)
    ##    nomFileVPonf=paste0("data_raw/",nomFileVP_ONF)
    ##
    ##    nomDBFNat=paste0("data_raw/",nomDBFNat)
    ##    nomFileFNat=paste0("data_raw/",nomFileFNat)
    ##




    cat("\n\n   Les parametres:\n---------------------\n\n")

    cat("dateExportFaune: ",dateExportFaune,"\n")
    cat("nomFilefaune: ",nomFileFaune,"\n")
    cat("dateExportVP: ",dateExportVP,"\n")
    cat("nomFileVP: ",nomFileVP,"\n")
    cat("dateExportFNat: ",dateExportFNat,"\n")
    cat("importACCESS: ",importACCESS,"\n")
    cat("nomFileFNat: ",nomFileFNat,"\n")
    cat("nomDBFNat: ",nomDBFNat,"\n")
    cat("import_raw: ",import_raw,"\n")
    cat("importationDataBrut_Faune: ",importationDataBrut_Faune,"\n")
    cat("importationDataBrut_FNat: ",importationDataBrut_FNat,"\n")
    cat("importationDataBrut_VP: ",importationDataBrut_VP,"\n")
    cat("construction_table: ",construction_tables,"\n")
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
    cat("fileTemp: ",fileTemp,"\n")

    cat("\n\ndateConstruction: ",dateConstruction,"\n")

    cat("\n\n repertoire output import:",repOutData,"\n")
    cat("\n\n repertoire output information:",repOutInfo,"\n")


### I) Importation et preparation des données récente vigie-plume
    cat("\n\n I) Importation des données brutes \n=================================\n")

if(import_raw & construction_tables) {

    cat("\n\n 1) Données au format faune-france \n----------------------\n")
    if(importationDataBrut_Faune) {

        cat("\n - Faune-France \n <-- Fichier a plat:",nomFileFaune,"\n")
        flush.console()

        dFaune <- faune_importation(nomFileFaune,dateExportFaune,repImport="data_Faune/",repOutInfo=repOutInfo,repOutData = repOutData)

    } else { cat(" ---> SKIP\n") }





  cat("\n\n 2) Données au format Vigie-Plume \n----------------------\n")
    if(importationDataBrut_VP) {

        cat("\n - VigiePlume\n <-- Fichier a plat:",nomFileVP,"\n")
        flush.console()

        dVP <- vp_importation(nomFileVP,dateExportVP,repImport="data_VP/",repOutInfo=repOutInfo,repOutData = repOutData)

    } else { cat(" ---> SKIP\n") }




    cat("\n\n 3) Données au format FNat \n----------------------\n")
    if(importationDataBrut_FNat) {

        cat("\n - FNat\n")
        if(importACCESS) {
            cat(" <-- Base de données ACCESS:",nomDBFNat,"\n")
            cat(" !! ATTENTION cette fonctionne uniquement sous windows avec une version 32bit de R (.../bin/i386/Rgui.exe)\n")
            cat(" Cette importation peut prendre prendre plusieurs minutes,\nmerci de patienter...\n")
            flush.console()
            dFNat <- import_FNat_accessFile(file=nomDBFNat,fichierAPlat=nomFileFNat,output=TRUE)
            cat("   --> OK \n")
            flush.console()

        } else {
            cat(" <-- Fichier à plat:",nomFileFNat,"\n")
            cat(" Cette importation peut prendre un peu de temps,\nmerci de patienter .... \n")
            flush.console()
            dFNat <-  FNat_importation(nomFileFNat,dateExportFNat,repImport="data_Fnat/",repOutInfo=repOutInfo,repOutData = repOutData)
            cat("   --> OK \n")
            flush.console()
        }

    } else { cat(" ---> SKIP\n") }



### II) Union des tables brutes
    cat("\n\n II) Union des données brutes \n=================================\n")


    d.all <- union_raw(dFaune,dVP,dFNat,dHist = NULL,dateConstruction,repOutInfo=repOutInfo,repOutData = repOutData,output = TRUE)
} else {
    if(construction_tables) {
       filename <- paste0(repOutData,"alldata_",dateConstruction,".csv")
            cat(" importation \n",filename," -> ")
            d.all <- fread(filename)
       cat(" Table complète: ", nrow(d.all)," lignes et ",ncol(d.all)," colonnes dont ", nrow(d.all[keep == TRUE,])," conservées \n    DONE ! \n")
    }

}

### III) Fabrication des tables
    cat("\n\n III) Fabrication des tables \n=================================\n")

if(construction_tables) {
    ## 1) Table points
    cat("\n\n 1) Table points\n----------------------\n")
    flush.console()

    if(constructionPoint) {

            cat(" construction -> ")
            d.point <- raw2point(d.all,dateConstruction,repOutInfo=repOutInfo,repOutData = repOutData,TRUE)
        } else {
            filename <- paste0(repOutData,"point_",dateConstruction,".csv")
            cat(" importation \n",filename," -> ")
            d.point <- fread(filename)
        }
        cat(nrow(d.point)," lignes \n")
        flush.console()




    ## 2) Table carres
    cat("\n\n 2) Table carres\n----------------------\n")
    flush.console()

    if(constructionCarre) {
            cat(" construction -> ")
            d.carre <- raw2carre(d.all,dateConstruction,repOutInfo=repOutInfo,repOutData = repOutData,TRUE)
        } else {
            filename <- paste0(repOutData,"carre_",dateConstruction,".csv")
            cat(" importation \n",filename," -> ")
            d.carre <- fread(filename)
        }
        cat(nrow(d.carre)," lignes \n")



    ## 3) Table inventaires
    cat("\n\n3) Table inventaires\n----------------------\n")
    flush.console()

    if(constructionInventaire | constructionObservation) {
            cat(" construction -> ")
            d.inv <- raw2inventaire(d.all,version,dateConstruction,repOutInfo=repOutInfo,repOutData = repOutData,TRUE)
        } else {
            filename <- paste0(repOutData,"inventaire_",dateConstruction,".csv")
            cat(" importation  \n",filename," -> ")
            d.inv <- fread(filename)
        }
        cat(nrow(d.inv)," lignes \n")


    ## 4) Table observations
    cat("\n\n 4) Table observations\n----------------------\n")
    flush.console()

    if(constructionObservation) {
            cat(" construction -> ")
            d.obs <- raw2observation(d.all,dateConstruction,repOutInfo=repOutInfo,repOutData = repOutData,TRUE)
        } else {
            filename <- paste0(repOutData,"observation_",dateConstruction,".csv")
            cat(" importation \n",filename," -> ")
            d.obs <- fread(filename)
        }
        cat(nrow(d.obs)," lignes \n")



    ## 5) Table seuil
    cat("\n\n 5) Table seuil\n----------------------\n")
    flush.console()

    if(constructionSeuil) {
            cat(" construction -> ")
            d.seuil <- obs2seuil(d.obs,d.inv,dateConstruction,repOutInfo=repOutInfo,repOutData = repOutData,TRUE)
        } else {
            filename <- paste0(repOutData,"seuil_",dateConstruction,".csv")
            cat(" importation \n",filename," -> ")
            d.seuil <- fread(filename)
        }
        cat(nrow(d.seuil)," lignes \n")



    ##  6) Table habitats
    cat("\n\n 6) Table habitats\n----------------------\n")
    flush.console()

    if(constructionHabitat) {
            cat(" construction -> ")
            d.hab <- raw2habitat(d.all,dateConstruction,repOutInfo=repOutInfo,repOutData = repOutData,TRUE)
        } else {

            filename <- paste0(repOutData,"habitat_",dateConstruction,".csv")
            cat(" importation \n",filename," -> ")
            d.hab <- fread(filename)
        }
        cat(nrow(d.hab)," lignes \n")


    ##  7) Table point_annee
    cat("\n\n 7) Table point_annee\n----------------------\n")
    flush.console()

    if(constructionPointAnnee) {
            cat(" construction -> ")
            d.point_annee <- hab2point_annee(d.hab,d.inv,dateConstruction,repOutInfo=repOutInfo,repOutData = repOutData,TRUE)
        } else {

            filename <- paste0(repOutData,"point_annee_",dateConstruction,".csv")
            cat(" importation \n",filename," -> ")
            d.point_annee <- fread(filename)
        }
        cat(nrow(d.point_annee)," lignes \n")



    ##  8) Table carre_annee
    cat("\n\n 8) Table carre_annee\n----------------------\n")
    flush.console()

    if(constructionCarreAnnee) {
            cat(" construction -> ")
            d.carre_annee <- point_annee2carre_annee(d.point_annee,dateConstruction,repOutInfo=repOutInfo,repOutData = repOutData,TRUE)
        } else {

            filename <- paste0(repOutData,"carre_annee_",dateConstruction,".csv")
            cat(" importation \n",filename," -> ")
            d.carre_annee <- fread(filename)
        }
        cat(nrow(d.carre_annee)," lignes \n")


} else {
 cat(" ---> SKIP\n")
    }

### IV) Base de Donnees

    cat("\n\n IV) Base de Donnees\n=================================\n")
    flush.console()

    ## Creation de la base de donnees PostGreSQL
    if(postgresql_import) {
        cat("\n 1) Creation de la base de donnees PostGreSQL :\n")
        createDB_postgres(dateConstruction,nomDBpostgresql,postgresUser,postgresPassword,postgresql_createAll,postGIS_initiation,postgresql_abondanceSeuil,seuilAbondance,repertoire,repOut=repOutData,fileTemp)
        cat("\n    -> OK\n")
    } else { cat(" ---> SKIP\n") }



    end <- Sys.time() ## heure de fin

    cat("\n\n\n     # Fin du process:",format(end, "%Y-%m-%d %H:%M\n"))
    cat("     #      ==> Duree:",round(as.difftime(end - start,units="mins")),"minutes\n")





}








###############################################################################################

