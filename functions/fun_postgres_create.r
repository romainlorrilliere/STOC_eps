
##########################################################################################


createDB_postgres <- function(dateConstruction,nomDBpostgresql=NULL,postgresUser="postgres",postgresPassword=NULL,postgresql_createAll = TRUE,postGIS_initiation=TRUE,postgresql_abondanceSeuil=TRUE,seuilAbondance = .99,repertoire=NULL,repOut="",fileTemp=FALSE) {

dateConstruction  ;nomDBpostgresql=NULL;postgresUser="postgres";postgresPassword=NULL;postgresql_createAll = TRUE;postGIS_initiation=TRUE;postgresql_abondanceSeuil=TRUE;seuilAbondance = .99;repertoire=NULL;repOut=repOutData;fileTemp=FALSE


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
    cat(" \\copy habitat FROM ",repertoire,repOut,"habitat_",dateConstruction,".csv",
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


makeAbondanceTrunc <- function(dateConstruction,repertoire=NULL,repOut="",nomDBpostgresql=NULL,postgresUser="postgres",postgresPassword=NULL, fileTemp=TRUE,graphe=TRUE) {

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


