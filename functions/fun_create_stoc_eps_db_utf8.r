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
source("functions/fun_postgres_create.r"


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
   #multigsub


    cat("               ===========================================================================\n")
    cat("               |                                                                         |\n")
    cat("               |            Importation des Donnees STOC-eps des 2 base de donnnes       |\n")
    cat("               |                              FNat et VigiePlume                         |\n")
    cat("               |                                                                         |\n")
    cat("               ===========================================================================\n")

    version <- "V.2.1_2023-06-01"

    cat(paste("\n\n Version",version,"\n"))
    cat(" ----------------------------\n\n")


dateExportFaune=c("2023-03-23");nomFileFaune=c("2023_03_23_data_stoc_eps_shoc.csv");

    dateExportVP=c("2019-11-25","2019-12-03");nomFileVP=c("export_stoc_25112019.txt","export_stoc_onf_03122019.txt");
    dateExportFNat="2017-01-04";
    importACCESS=FALSE;
    nomFileFNat="FNat_plat_2017-01-04.csv";nomDBFNat="Base FNat2000.MDB";
    importationDataBrut_FNat=TRUE;importationDataBrut_VP=TRUE;importationDataBrut_Faune=TRUE;
    constructionPoint=TRUE;constructionCarre=TRUE;constructionInventaire=TRUE;
    constructionObservation = TRUE; constructionHabitat = TRUE;
    dateConstruction="2023-05-25";postgresql_import=TRUE;nomDBpostgresql=NULL;seuilAbondance = .99
    nomDBpostgresql="stoc_eps";
    postgresql_createAll=TRUE;postgresUser="romain";
    postGIS_initiation=TRUE;import_shape=FALSE;repertoire=NULL;postgresql_abondanceSeuil=TRUE;historiqueCarre=TRUE;
    pointCarreAnnee=TRUE;importPointCarreAnnee=TRUE;fileTemp=FALSE
    ##
    postgresPassword = "postgres"


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
    cat("importationDataBrut_Faune: ",importationDataBrut_Faune,"\n")
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

    cat("\n\n repertoire output import:",repOutData,"\n")
    cat("\n\n repertoire output information:",repOutInfo,"\n")


### I) Importation et preparation des données récente vigie-plume
    cat("\n\n I) Importation des données brutes \n=================================\n")



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


    d.all <- union_raw(dFaune,dVP,dFNat)

    cat(" Table complète: ", nrow(d.all)," lignes et ",ncol(d.all)," colonnes dont ", nrow(d.all[keep == TRUE,])," conservées \n    DONE ! \n")


### III) Fabrication des tables
    cat("\n\n III) Fabrication des tables \n=================================\n")


    ## 1) Table points
    cat("\n\n 1) Table points\n----------------------\n")
    flush.console()

    if(constructionPoint) {

            cat(" construction -> ")
            d.point <- raw2point(d.all,dateConstruction,repOutInfo=repOutInfo,repOutData = repOutData,TRUE)
        } else {
            filename <- paste0(repOutData,"point_VP_",dateConstruction,".csv")
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


    ##  5) Table habitats
    cat("\n\n 5) Table habitats\n----------------------\n")
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





### IV) Base de Donnees

    cat("\n\n IV) Base de Donnees\n=================================\n")
    flush.console()

    ## Creation de la base de donnees PostGreSQL
    if(postgresql_import) {
        cat("\n 1) Creation de la base de donnees PostGreSQL :\n")
        createDB_postgres(dateConstruction,nomDBpostgresql,postgresUser,postgresPassword,postgresql_createAll,postGIS_initiation,postgresql_abondanceSeuil,seuilAbondance,repertoire,repOut=repOutData,fileTemp)
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

