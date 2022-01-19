######################################################################################
##
##                     Fonctions d'utilisation de la base de données STOC_eps
##  R. Lorrilliere
######################################################################f################



if(!("stoc_eps_db_functions.r" %in% dir())) stop("ERREUR !!! \n Changer le repertoire de travail \n STOC_eps/\n\n")


source("functions/fun_create_stoc_eps_db.r")



#source("functions/fun_export.r")

### 1- création et importation de la base de données



##' .. content for \description{} (no empty lines) ..
##'
##' .. content for \details{} ..
##' @title Fonction de création de la base de données STOC EPS
##' @param dateExportVP date de l'export des données VP au format YYYY-MM-JJ
##' @param nomFileVP nom du fichier exporté de VP
##' @param nomFileVP_ONF nom du fichier exporté de VP pour les données ONF
##' @param dateExportFNat [="2017-01-04" ] date de l'export FNAT par default
##' @param importACCESS [=FALSE] valeur booleenne permet l'export des données FNAT directement dans la base access. ATTENTION cette fonctionne uniquement sous windows avec une version 32bit de R (.../bin/i386/Rgui.exe)
##' @param nomFileFNat [="FNat_plat_2017-01-04.csv"] nom du fichier à plat de FNat
##' @param nomDBFNat "Base FNat2000.MDB"
##' @param importationDataBrut [=TRUE] valeur booleene qui permet d'activé la récupération des données brut
##' @param constructionPoint [=TRUE] valeur booleene qui active la création de la table point
##' @param constructionCarre [=TRUE] valeur booleene qui active la création de la table carré
##' @param constructionInventaire [=TRUE] valeur booleene qui active la création de la table inventaire
##' @param constructionObservation [=TRUE] valeur booleene qui active la création de la table observation
##' @param constructionHabitat [=TRUE] valeur booleene qui active la création de la table habitat
##' @param dateConstruction [=NULL] date de la constuction de la base si null la date du jour si au format "YYYY-MM-JJ"
##' @param postgresql_import [=TRUE]  valeur booleene pour importer les tables dans la base postgres
##' @param nomDBpostgresql [=NULL] nom de la base postgres si null "stoc_eps"
##' @param postgresql_createAll [=TRUE] valeur booleene qui active la création complete de la base de données avec l'import des tables génériques
##' @param postgresUser [="postgres"] nom de l'utilisateur de de la base de données par default "postgres"
##' @param postgresPassword [="postgres"] mot de passe de l'utilisateur de de la base de données par default "postgres"
##' @param postGIS_initiation [=FALSE] valeur booleene qui permet d'activé l'initialisation de l'extension posgis de la base de données
##' @param import_shape [=FALSE] valeur booleene qui permet d'activé l'importation de shape_file dans la base de donnée
##' @param repertoire [=NULL] repertoire de travail du script par default le repertoire courant
##' @param postgresql_abondanceSeuil [=TRUE] valeur booleene qui permet d'activé le calcul du seuil des abondances par espèces et par classe de distance
##' @param seuilAbondance [0.99] valeur du seuil par default 0.99
##' @param historiqueCarre [=TRUE]  valeur booleene qui permet d'activé le calcul d'un historique des carrés
##' @param pointCarreAnnee [=TRUE] valeur booleene qui permet d'activé la création des tables point_annee et carre_annee
##' @param importPointCarreAnnee [=TRUE]  valeur booleene qui permet d'activé l'importation des tables point_annee et carre_annee
##' @param fileTemp[=FALSE]  valeur booleene qui permet d'activé qui de conservé les fichier sql créer lors du processus, par default FALSE
##' @return nothing just data importation in postgres database
##' @author Romain Lorrilliere

## ## ex:
## ## Importatoin à partir des fichiers bruts issues de VigiePlume
## laDate <- as.Date(Sys.time())
## prepaData(dateExportVP=laDate,nomFileVP="export_stoc_25112019.txt",nomFileVP_ONF="export_stoc_onf_03122019.txt",dateExportFNat="2017-01-04", importACCESS=FALSE, nomFileFNat="FNat_plat_2017-01-04.csv", importationDataBrut=TRUE, constructionPoint=TRUE,constructionCarre=TRUE, constructionInventaire=TRUE,constructionObservation = TRUE, constructionHabitat = TRUE,dateConstruction=NULL,postgresql_import=TRUE,nomDBpostgresql=NULL,postgresql_createAll=TRUE,postgresUser="posgres", postgresPassword="postgres",postGIS_initiation=TRUE,import_shape=FALSE,repertoire=NULL, postgresql_abondanceSeuil=TRUE,seuilAbondance = .99, historiqueCarre=TRUE, pointCarreAnnee=TRUE,importPointCarreAnnee=TRUE,fileTemp=FALSE)

## ## si l'ordinateur n'a pas assez de mémoire RAM (16Go recommandé), voici comment lancé l'importationà partir des fichiers pré-construits

## laDate <- ["la date noté dans les noms des fichier"]
## prepaData(dateExportVP=laDate, importationDataBrut=FALSE, constructionPoint=FALSE,constructionCarre=FALSE, constructionInventaire=FALSE,constructionObservation = FALSE, constructionHabitat = FALSE,dateConstruction=NULL,postgresql_import=TRUE,nomDBpostgresql=NULL,postgresql_createAll=TRUE,postgresUser="postgres", postgresPassword="postgres",postGIS_initiation=TRUE,import_shape=FALSE,repertoire=NULL, postgresql_abondanceSeuil=TRUE,seuilAbondance = .99, historiqueCarre=TRUE, pointCarreAnnee=TRUE,importPointCarreAnnee=TRUE,fileTemp=FALSE)


prepaData <- function(dateExportVP,nomFileVP,nomFileVP_ONF,
          dateExportFNat="2017-01-04", importACCESS=FALSE,
          nomFileFNat="FNat_plat_2017-01-04.csv",nomDBFNat="Base FNat2000.MDB",
          importationDataBrut=TRUE,
          constructionPoint=TRUE,constructionCarre=TRUE,
          constructionInventaire=TRUE,
          constructionObservation = TRUE, constructionHabitat = TRUE,
          dateConstruction=NULL,postgresql_import=TRUE,nomDBpostgresql=NULL,
          postgresql_createAll=TRUE,
          postgresUser="postgres",postgresPassword="postgres",
          postGIS_initiation=FALSE,import_shape=FALSE,repertoire=NULL,
          postgresql_abondanceSeuil=TRUE,seuilAbondance = .99,
          historiqueCarre=TRUE,
          pointCarreAnnee=TRUE,importPointCarreAnnee=TRUE,fileTemp=FALSE) {


    f_prepaData(dateExportVP=dateExportVP,nomFileVP=nomFileVP,
                nomFileVP_ONF=nomFileVP_ONF,
                dateExportFNat=dateExportFNat, importACCESS=importACCESS,
                nomFileFNat=nomFileFNat,nomDBFNat=nomDBFNat,
                importationDataBrut=importationDataBrut,
                constructionPoint=constructionPoint,constructionCarre=constructionCarre,
                constructionInventaire=constructionInventaire,
                constructionObservation = constructionObservation, constructionHabitat = constructionHabitat,
                dateConstruction=dateConstruction,
                postgresql_import=postgresql_import,nomDBpostgresql=nomDBpostgresql,
                postgresql_createAll=postgresql_createAll,postgresUser=postgresUser,
                postgresPassword=postgresPassword,
                postGIS_initiation=postGIS_initiation,import_shape=import_shape,repertoire=repertoire,
                postgresql_abondanceSeuil=postgresql_abondanceSeuil,seuilAbondance = seuilAbondance,
                historiqueCarre=historiqueCarre,
                pointCarreAnnee=pointCarreAnnee,importPointCarreAnnee=importPointCarreAnnee,fileTemp=fileTemp)

}


### 2- export







prepare_carrenat <- function() {

carrenat <- st_read("data_sig/carrenat.shp")

    library(ggplot2)
    gg <- ggplot(carrenat) + geom_sf(aes(fill=NUMNAT),colour=NA)
    gg

    centroid <- st_centroid(carrenat)

    st_transform(carrenat,crs=4326)

    centroid <- st_centroid(carrenat)

}

