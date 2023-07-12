
##########################################################################################


createDB_postgres <- function(dateConstruction,nomDBpostgresql=NULL,postgresUser="postgres",postgresPassword=NULL,postgresql_createAll = TRUE,postGIS_initiation=TRUE,postgresql_abondanceSeuil=TRUE,seuilAbondance = .99,repertoire=NULL,repOut="",fileTemp=FALSE) {

## dateConstruction  ;nomDBpostgresql=NULL;postgresUser="postgres";postgresPassword=NULL;postgresql_createAll = TRUE;postGIS_initiation=TRUE;postgresql_abondanceSeuil=TRUE;seuilAbondance = .99;repertoire=NULL;repOut=repOutData;fileTemp=FALSE


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
    cat("     5- seuil\n")
    cat("     6- habitat\n")
    cat("     7- point_annee\n")
    cat("     8- carre_annee\n")
    cat("     9- all_data\n")
    commande <- paste0("psql -U ",postgresUser," ",nomDBpostgresql," < ",repertoire,"library_sql/postgres_createTableBBS.sql")
    myshell(commande)

    ## importation data
    cat(" \\copy point FROM ",repertoire,repOut,"point_",dateConstruction,".csv",
        " with (format csv, header, delimiter ',')\n",sep="",
        file=paste0(repertoire,"library_sql/_sqlR_importationData.sql"),append=FALSE)
    cat(" \\copy carre FROM ",repertoire,repOut,"carre_",dateConstruction,".csv",
        " with (format csv, header, delimiter ',')\n",sep="",
        file=paste0(repertoire,"library_sql/_sqlR_importationData.sql"),append=TRUE)
    cat(" \\copy inventaire FROM ",repertoire,repOut,"inventaire_",dateConstruction,".csv",
        " with (format csv, header, delimiter ',')\n",sep="",
        file=paste0(repertoire,"library_sql/_sqlR_importationData.sql"),append=TRUE)
    cat(" \\copy observation FROM ",repertoire,repOut,"observation_",dateConstruction,".csv",
        " with (format csv, header, delimiter ',')\n",sep="",
        file=paste0(repertoire,"library_sql/_sqlR_importationData.sql"),append=TRUE)
    cat(" \\copy seuil_obs FROM ",repertoire,repOut,"seuil_",dateConstruction,".csv",
        " with (format csv, header, delimiter ',')\n",sep="",
        file=paste0(repertoire,"library_sql/_sqlR_importationData.sql"),append=TRUE)
    cat(" \\copy habitat FROM ",repertoire,repOut,"habitat_",dateConstruction,".csv",
        " with (format csv, header, delimiter ',')\n",sep="",
        file=paste0(repertoire,"library_sql/_sqlR_importationData.sql"),append=TRUE)
    cat(" \\copy point_annee FROM ",repertoire,repOut,"point_annee_",dateConstruction,".csv",
        " with (format csv, header, delimiter ',')\n",sep="",
        file=paste0(repertoire,"library_sql/_sqlR_importationData.sql"),append=TRUE)
    cat(" \\copy carre_annee FROM ",repertoire,repOut,"carre_annee_",dateConstruction,".csv",
        " with (format csv, header, delimiter ',')\n",sep="",
        file=paste0(repertoire,"library_sql/_sqlR_importationData.sql"),append=TRUE)
    cat(" \\copy all_data FROM ",repertoire,repOut,"alldata_",dateConstruction,".csv",
        " with (format csv, header, delimiter ',')\n",sep="",
        file=paste0(repertoire,"library_sql/_sqlR_importationData.sql"),append=TRUE)

##    cat(" \\copy habitat (pk_habitat, id_point, passage, date, annee, p_milieu, p_type, p_cat1, p_cat2,s_milieu, s_type, s_cat1, s_cat2, db, date_export) FROM ",repertoire,
##        "data_DB_import/habitat_",dateConstruction,".csv",
##        " with (format csv, header, delimiter ',')\n",sep="",
##        file=paste0(repertoire,"library_sql/_sqlR_importationData.sql"),append=TRUE)


    cmd_import <- paste0("psql -U ",postgresUser," ",nomDBpostgresql," < ",repertoire,"library_sql/_sqlR_importationData.sql")
                                        # commande <- paste0("psql -U ",postgresUser," ",nomDBpostgresql," -f ",repertoire,"library_sql/_sqlR_importationData.sql")
    myshell(cmd_import)


    cmd_import <- paste0("psql -U ",postgresUser," ",nomDBpostgresql," < ",repertoire,"library_sql/postgres_createIndexBBS.sql")
                                        # commande <- paste0("psql -U ",postgresUser," ",nomDBpostgresql," -f ",repertoire,"library_sql/postgres_createIndexBBS.sql")
    myshell(cmd_import)

   # if(postgresql_abondanceSeuil) makeAbondanceTrunc(dateConstruction,seuilAbondance,repertoire,nomDBpostgresql,postgresUser,postgresPassword,repOut=repOut,fileTemp)

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


maketableGenerique <- function(repertoire=NULL,nomDBpostgresql=NULL,postgresUser="postgres", fileTemp=TRUE,savePostgres=TRUE) {

    ## require(ggplot2)
    repertoire=NULL;savePostgres=TRUE;nomDBpostgresql=NULL;postgresUser="postgres"; fileTemp=FALSE;
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

