

library(RODBC)
library(data.table)
library(rgdal)
library(lubridate)
library(RPostgreSQL)
#library(doBy)
library(reshape2)
require(dplyr)

source("functions/fun_generic.r")


if("STOC_eps_database" %in% dir()) setwd("STOC_eps_database/")
if(!("output_export" %in% dir())) dir.create("output_export")


makeTableCarre <- function(con=NULL,user=NULL,mp=NULL,nomDB=NULL,savePostgres=FALSE,output=TRUE,sp=NULL,
                           champSp = "code_sp",nomChampSp="espece",
                           spExcluPassage1=c("MOTFLA","SAXRUB","ANTPRA","OENOEN","PHYTRO","TURPIL"),# (Prince et al. 2013 Env. Sc. and Pol.) + "OENOEN","PHYTRO" avis d'expert F. Jiguet
                           spTransit = c("APUAPU","DELURB","HIRRUS","RIPRIP","ACCNIS","FALTIN","BUTBUT","CIRAER","CIRCYA","CIRPYG","MILMIG"),
                           champsHabitat=TRUE,selectTypeHabitat=NULL,selectTypeHabitatPoint=NULL,
                           altitude_min=NULL,altitude_max=NULL,firstYear=NULL,lastYear=NULL,
                           departement=NULL,onf=TRUE,
                           id_carre=NULL,insee=NULL,
                           distance_contact="inf",formatTrend = FALSE,isEnglish=FALSE,addAbscence=FALSE,
                           id_output="France", #"NicolasM",
                           operateur=c("Lorrilliere Romain",
                                       "lorrilliere@mnhn.fr"),
                           encodingSave="utf-8") {#"ISO-8859-1") {


##user=NULL;mp=NULL;nomDB=NULL;savePostgres=FALSE;output=TRUE;sp=NULL;
##                           champSp = "code_sp";nomChampSp="espece";
##                           spExcluPassage1=c("MOTFLA","SAXRUB","ANTPRA","OENOEN","PHYTRO");# (Prince et al. 2013 Env. Sc. and Pol.) + "OENOEN","PHYTRO" avis d'expert F. Jiguet
##                           seuilAbondance=.99;
##                           champsHabitat=TRUE;selectTypeHabitat=NULL;selectTypeHabitatPoint=NULL
##                           altitude_min=NULL;altitude_max=NULL;firstYear=NULL;lastYear=NULL;
##                           departement=NULL;onf=TRUE;
##                           id_carre=NULL;insee=NULL;
##                           distance_contact="200";formatTrend = FALSE;isEnglish=FALSE;addAbscence=FALSE;
##                           id_output="France"; #"NicolasM";
##                           operateur=c("Lorrilliere Romain",
##                                       "lorrilliere@mnhn.fr");
##                           encodingSave="utf-8"
##



    start <- Sys.time()
    dateExport <- format(start,"%Y-%m-%d")

    if(is.null(con)) con <- openDB.PSQL(user,mp,nomDB)
    if(is.null(firstYear)) firstYear <- 2001
    if(is.null(lastYear)) lastYear <- as.numeric(format(start,"%Y"))
    if(is.null(altitude_max)) altitude_max <- 8000
    if(is.null(altitude_min)) altitude_min <- 0
##browser()

    queryObs <- makeQueryCarre(sp=sp,distance_contact=distance_contact,champSp = champSp,nomChampSp=nomChampSp,
                             spExcluPassage1 = spExcluPassage1,
                                                      champsHabitat=champsHabitat,selectTypeHabitat=selectTypeHabitat,selectTypeHabitatPoint=selectTypeHabitatPoint,
                           altitude_min=altitude_min,altitude_max=altitude_max,firstYear=firstYear,lastYear=lastYear,
                           departement=departement,onf=onf,
                           id_carre=id_carre,insee=insee, dateExport = dateExport, operateur = operateur)
    d <- dbGetQuery(con, queryObs)

    d <- d %>% mutate_if(is.character, Encoding_utf8)
    setDT(d)
    spTrans <- intersect(spTransit, unique(d[,code_sp]))
    if(length(spTrans > 0)) {
        d <- d[!(code_sp %in% spTransit),]

        queryObs <- makeQueryCarre(sp=spTrans, distance_contact="all",nomChampSp=nomChampSp,
                                 spExcluPassage1 = spExcluPassage1,
                                     champsHabitat=champsHabitat,selectTypeHabitat=selectTypeHabitat,selectTypeHabitatPoint=selectTypeHabitatPoint,
                           altitude_min=altitude_min,altitude_max=altitude_max,firstYear=firstYear,lastYear=lastYear,
                           departement=departement,onf=onf,
                           id_carre=id_carre,insee=insee, dateExport = dateExport, operateur = operateur)
        dTrans <- dbGetQuery(con, queryObs)

        dTrans <- dTrans %>% mutate_if(is.character, Encoding_utf8)

        setDT(dTrans)
        d <- rbind(d,dTrans)

        setorder(d,carre,annee,code_sp)

    }






    if(addAbscence) {
        cat("\n Ajout des absences: \n")

        colSp <- c("code_sp","euring","taxref",colnames(d)[grep("name",colnames(d))])
        colSp <- setdiff(colSp,"data_base_name")
        colCarreAn<- setdiff(colnames(d),c(colSp,"abondance_brut","abondance","qualite_inventaire_stoc"))
        colAbond <- c("code_sp","id_carre_annee","abondance_brut","abondance","qualite_inventaire_stoc")

        dsp <- unique(subset(d,select=colSp))
        dCarreAn <- unique(subset(d,select=colCarreAn))
        dAbund <- subset(d,select=colAbond)

        tabInv <- unique(d$id_carre_annee)

        tabInv <- expand.grid(dCarreAn$id_carre_annee,dsp$code_sp)
        colnames(tabInv) <- c("id_carre_annee","code_sp")

        dAbund <- merge(dAbund,tabInv,by=c("code_sp","id_carre_annee"),all=TRUE)

        dAbund$abondance_brut[is.na(dAbund$abondance_brut)] <- 0
        dAbund$abondance[is.na(dAbund$abondance)] <- 0

        dd <- merge(dAbund,dsp, by = "code_sp")
        dd <- merge(dd,dCarreAn,by="id_carre_annee")


        dd <- dd[,colnames(d)]


        queryCarreAn <- paste("select ca.id_carre as carre, ca.annee, pk_carre_annee as id_carre_annee, etude,
 c.commune,c.insee,c.departement,qualite_inventaire_stoc,
c.altitude, longitude_grid_wgs84 ,latitude_grid_wgs84, ",
ifelse(champsHabitat," foret_p, ouvert_p , agri_p , urbain_p ,
foret_ps , ouvert_ps, agri_ps , urbain_ps,
nbp_foret_p, nbp_ouvert_p , nbp_agri_p , nbp_urbain_p,
nbp_foret_ps, nbp_ouvert_ps , nbp_agri_ps, nbp_urbain_ps, ",""),
" c.db as data_base_name, ",
"'",dateExport,"'::varchar(10) as date_export,
'",operateur[1],"'::varchar(50) as operateur,
'",operateur[2],"'::varchar(50) as email_operateur
from
carre as c , carre_annee as ca
where
ca.id_carre = c.pk_carre and  c.altitude <= ",altitude_max," and  c.altitude >= ",altitude_min,ifelse(is.null(departement),"",paste(" and departement in ",depList," "))," and
 ca.qualite_inventaire_stoc > 0 and ca.annee >= ",firstYear,"  and ca.annee <= ",lastYear," and c.etude in ('STOC_EPS'",ifelse(onf,", 'STOC_ONF'",""),")
order by
ca.id_carre, annee;",sep="")



        cat("\nRequete inventaire: Recherche de tous les inventaires\n\n",queryCarreAn,"\n\n")

        dToutCarreAn <- dbGetQuery(con, queryCarreAn)

        dToutCarreAn <- dToutCarreAn %>% mutate_if(is.character, Encoding_utf8)

        dCarreAbs <- subset(dToutCarreAn,!(id_carre_annee %in% dd$id_carre_annee))

        if(nrow(dCarreAbs)>0) {
            nbSp <- nrow(dsp)
            nbCarreAbs <- nrow(dCarreAbs)

            dCarreAbs2 <- as.data.frame(lapply(dCarreAbs, base::rep,nbSp))
            dspAbs <- as.data.frame(lapply(dsp, base::rep,each= nbCarreAbs))
            dCarreAbs <- cbind(dCarreAbs2,dspAbs)
            dCarreAbs$abondance <- 0
            dCarreAbs$abondance_brut <- 0

            dCarreAbs <- dCarreAbs[,colnames(d)]

            d <- rbind(dd,dCarreAbs)
        } else {
            d <- dd
        }

                                        #  browser()
        d <- d[order(d$carre,d$annee,d$code_sp),]

    }

    if (formatTrend){
##                 browser()                       #  browser()
        d <- subset(d,select=c("carre","annee","code_sp","abondance"))
        colnames(d)[3:4] <- c(nomChampSp,"abond")
    }

    if(isEnglish) {
        cat(" - Traduction des noms de colonnes\n\n")
        d <- trad_fr2eng(d)

    }


################### as ",ifelse(formatTrend,"abond","abondance"),"
############# ",ifelse(!(is.null(nomChampSp)),paste(" as ",nomChampSp,sep="")),"
                                        #if(savePostgres) sqlCreateTable(con,"outcome.FrenchBBS_montaneOpen",d)
    ##  as longitude_square_grid_wgs84   as latitude_squarre_grid_wgs84


    if(is.null(sp)) suffSp <- "allSp" else if(length(sp)<4) suffSp <- paste(sp,collapse="-") else suffSp <- paste(length(sp),"sp",sep="")
    fileOut <- paste("export/data_FrenchBBS_squarre_",id_output,"_",suffSp,"_",firstYear,"_",lastYear,".csv",sep="")
    write.csv2(d,fileOut,row.names=FALSE)
    cat(" --> ",fileOut,"\n")
    end <- Sys.time() ## heure de fin

    dbDisconnect(con)

     diff_time <- end - start
    cat("\n     #      ==> Duree:",round(diff_time[[1]],2),units(diff_time),"\n")
    if(output) return(d)

}









makeQueryCarre <- function(sp = NULL, distance_contact="inf",champSp = "code_sp",nomChampSp="espece",
                           spExcluPassage1=c("MOTFLA","SAXRUB","ANTPRA","OENOEN","PHYTRO","TURPIL"),# (Prince et al. 2013 Env. Sc. and Pol.) + "OENOEN","PHYTRO" avis d'expert F. Jiguet
                           seuilAbondance=.99,
                           champsHabitat=TRUE,selectTypeHabitat=NULL,selectTypeHabitatPoint=NULL,
                           altitude_min=NULL,altitude_max=NULL,firstYear=NULL,lastYear=NULL,
                           departement=NULL,onf=TRUE,
                           id_carre=NULL,insee=NULL, dateExport ="", operateur=c("Lorrilliere Romain","lorrilliere@mnhn.fr")) {

  if(is.null(distance_contact) | distance_contact == "" | distance_contact == "all") {
        distance_contact_txt <- "" ; distance_contact <- "all"
    } else {
        if(distance_contact == "100") {
            distance_contact_txt <- " distance_contact in ('inf_25m','25-50m','50-100m','25-100m') and "
        } else {
            if(distance_contact == "200") {
                distance_contact_txt <- " distance_contact in ('inf_25m','25-50m','50-100m','25-100m','100-200m','sup_100m') and "
            } else {
                if(distance_contact == "inf") {
                    distance_contact_txt <- " distance_contact in ('inf_25m','25-50m','50-100m','25-100m','100-200m','sup_100m','sup_200m','') and "
                } else {
                    stop("distance_contact non pris en charge")
                }}}}

    seuil_txt <- as.character(trunc(seuilAbondance*100))

    if(distance_contact %in% c("all","inf","100","200")) {
        champ_seuil1 <- paste0("seuil_",distance_contact,"_tukey_outlier")
        champ_seuil2 <- paste0("seuil_",distance_contact,"_tukey_farout")
    } else {
        stop("distance_contact non pris en charge")
    }

    flag.query1 <- flag.query2 <- FALSE
    if(!is.null(sp)) {
        if(champSp != "code_sp") {

            sp <- getCode_sp(con,champSp,sp)

        }

        sp1 <- sp[!(sp %in% spExcluPassage1)]
        if(length(sp1) > 0){
            spList1 <- paste("('",paste(sp1,collapse="' , '"),"')",sep="")
            flag.query1 <- TRUE
        }

        sp2 <- sp[sp %in% spExcluPassage1]
        if(length(sp2) > 0){
            spList2 <- paste("('",paste(sp2,collapse="' , '"),"')",sep="")
            flag.query2 <- TRUE
        }

    } else {

        spListExclud <- paste("('",paste(spExcluPassage1,collapse="' , '"),"')",sep="")
        flag.query1 <- flag.query2 <- TRUE
    }


    selectQueryOp <- ""
    selectQueryOp1 <- paste0("SELECT id_inventaire, code_sp, sum(abondance) as abond
				FROM
				observation as o, inventaire as i,carre_annee as ca, carre as c ",ifelse(!is.null(selectTypeHabitatPoint),", point_annee as pa"," "),"
				WHERE
				o.id_inventaire = i.pk_inventaire and
				o.id_carre = c.pk_carre and
				o.id_carre = ca.id_carre and o.annee=ca.annee and
                                ",ifelse(!is.null(selectTypeHabitatPoint)," o.id_point = pa.id_point and o.annee=pa.annee and"," "),"
				ca.qualite_inventaire_stoc > 0 and
                                ")
    selectQueryOp2 <- "
			GROUP BY
			id_inventaire, code_sp"

    if(!is.null(departement)) depList <- paste("('",paste(departement,collapse="' , '"),"')",sep="")
    if(!is.null(id_carre)) carreList <- paste("('",paste(id_carre,collapse="' , '"),"')",sep="")
    if(!is.null(insee)) inseeList <- paste("('",paste(insee,collapse="' , '"),"')",sep="")
    if(!is.null(selectTypeHabitat)) queryTypeHab <- paste(" AND ( ",paste0("ca.",selectTypeHabitat,collapse=" = TRUE OR ")," = TRUE) ",sep="") else queryTypeHab <- " "
 if(!is.null(selectTypeHabitatPoint)) queryTypeHabPoint <- paste(" AND ( ",paste0("pa.",selectTypeHabitat,collapse=" = TRUE OR ")," = TRUE) ",sep="") else queryTypeHab <- " "
    selectQuery <- paste(distance_contact_txt," i.annee >= ",firstYear,"
and i.annee <= ",lastYear," and c.etude in ('STOC_EPS'",ifelse(onf,", 'STOC_ONF'",""),")  and  c.altitude_median <= ",altitude_max," and  c.altitude_median >= ",altitude_min," ",ifelse(is.null(departement),"",paste(" and departement in ",depList," ")),"
",
ifelse(is.null(id_carre),"",paste(" and o.id_carre in ",carreList," ")),"
",
ifelse(is.null(insee),"",paste(" and p.insee in ",inseeList," ")),"
",
ifelse(!is.null(selectTypeHabitat),queryTypeHab," "),"
",
ifelse(!is.null(selectTypeHabitatPoint),queryTypeHabPoint," "),"
",
sep="")



    if(flag.query1) {
        selectQueryOp <- paste(selectQueryOp,selectQueryOp1,
                               ifelse(is.null(sp),paste("code_sp not  in ",spListExclud," and "),paste(" code_sp in ",spList1," and ")),"  passage_stoc in (1,2) and",
                               selectQuery,selectQueryOp2)
        if(flag.query2) {
            selectQueryOp <- paste(selectQueryOp,"
                                union
                                ")
        }
    }

    if(flag.query2) {
        selectQueryOp <- paste(selectQueryOp," --  ## begin ## ajout des especes tardives dont on ne garde que le second passage
            ",
            selectQueryOp1,
            ifelse(is.null(sp),paste("code_sp in ",spListExclud," and "),paste(" code_sp in ",spList2," and ")),"  passage_stoc = 2 and",
            selectQuery,selectQueryOp2)
    }










    queryObs <- paste0("
select oc.id_carre as carre, oc.annee, (oc.annee::varchar(4)||oc.id_carre::varchar(100))::varchar(100) as id_carre_annee, c.etude,
qualite_inventaire_stoc,ca.nombre_passage_stoc_annee,ca.info_passage_an, commune,insee,departement,
oc.code_sp, e.scientific_name, e.french_name, e.english_name, e.euring,e.taxref, '",distance_contact,"'::varchar(10) as distance_contact_max,
abond_brut as abondance_brut, abond_tuckey_outlier  as abondance_filtre_tuckey,abond_tuckey_farout  as abondance_filtre_tuckey_farout, 
altitude_median, longitude_grid_wgs84,latitude_grid_wgs84, ",
ifelse(champsHabitat," foret_p, ouvert_p, agri_p, urbain_p,
foret_ps, ouvert_ps, agri_ps, urbain_ps,
nbp_foret_p, nbp_ouvert_p, nbp_agri_p, nbp_urbain_p,
nbp_foret_ps, nbp_ouvert_ps, nbp_agri_ps, nbp_urbain_ps, ",""),
" c.db as data_base_name, ",
"'",dateExport,"'::varchar(10) as date_export,
'",operateur[1],"'::varchar(50) as operateur,
'",operateur[2],"'::varchar(50) as email_operateur

from( -- ## begin ## somme sur carre des max par points sur 2 passages
	select id_carre, annee,code_sp,sum(abond_brut) as abond_brut,sum(abond_tuckey_outlier) as abond_tuckey_outlier,sum(abond_tuckey_farout) as abond_tuckey_farout
	from -- ## begin ## obs max par point sur 2 passages
		(select id_point, annee,code_sp,max(abond_brut) as abond_brut,max(abond_tuckey_outlier) as abond_tuckey_outlier,max(abond_tuckey_farout) as abond_tuckey_farout
		from -- ## begin ## correction abondance obs par seuil
			(select id_inventaire,id_point, passage_stoc, annee, opb.code_sp,abond as abond_brut, LEAST(abond, ",champ_seuil1,") as abond_tuckey_outlier, ",champ_seuil1," as seuil_tuckey_outlier, LEAST(abond, ",champ_seuil2,") as abond_tuckey_farout, ",champ_seuil2," as seuil_tuckey_farout
			from -- ## begin ## selection classe de distance et different filtre
				(",selectQueryOp,"
				) -- ## end ## selection classe de distance et different filtre
			as opb, seuil_obs as s, inventaire as i
			WHERE
			opb.code_sp = s.code_sp and opb.id_inventaire = i.pk_inventaire

			) -- ## end ## correction abondance obs par seuil
		as op
		group by id_point,annee,code_sp
		) -- ## end ## obs max par point sur 2 passages
	as omp, point as p
	where omp.id_point = p.pk_point
	group by id_carre, annee,code_sp) -- ## end ## somme sur carre des max par points sur 2 passages
as oc, carre as c, species as e, carre_annee as ca, seuil_obs as s
where
oc.id_carre = c.pk_carre and oc.code_sp = e.pk_species and oc.code_sp = s.code_sp and oc.id_carre = ca.id_carre and oc.annee = ca.annee
order by
oc.id_carre, annee,code_sp;",sep="")



                                        # browser()
    cat("\nRequete principale:\n\n",queryObs,"\n\n")

return(queryObs)

}







makeTablePoint <- function(con=NULL,user=NULL,mp=NULL,nomDB=NULL,savePostgres=FALSE,output=TRUE,sp=NULL,champSp = "code_sp",nomChampSp="espece",
                           spExcluPassage1=c("MOTFLA","SAXRUB","ANTPRA","OENOEN","PHYTRO"),# (Prince et al. 2013 Env. Sc. and Pol.) + "OENOEN","PHYTRO" avis d'expert F. Jiguet
                           spTransit = c("APUAPU","DELURB","HIRRUS","RIPRIP","ACCNIS","FALTIN","BUTBUT","CIRAER","CIRCYA","CIRPYG","MILMIG"),
                           champsHabitat=TRUE,champsCoordGrid=TRUE,altitude_min=NULL,altitude_max=NULL,
                           firstYear=NULL,lastYear=NULL,
                           departement=NULL,insee=NULL,onf=TRUE,
                           id_carre=NULL,
                           selectHabitat = NULL, selectTypeHabitat= NULL,
                           distance_contact="inf",formatTrend = FALSE,isEnglish=FALSE,addAbscence=FALSE,
                           id_output="AlexR_2017-11-23", #"NicolasM",
                           operateur=c("Lorrilliere Romain",
                                       "lorrilliere@mnhn.fr"),
                           encodingSave="utf-8") {#"ISO-8859-1") {

##   con=NULL;savePostgres=FALSE;output=TRUE;sp=NULL;champSp = "code_sp";
##   spExcluPassage1=c("MOTFLA","SAXRUB","ANTPRA")  ;seuilAbondance=.99;
##   champsHabitat=TRUE;champsCoordGrid=TRUE;altitude_min=NULL; altitude_max= NULL;firstYear=NULL;lastYear=NULL;
##   departement="44";onf=TRUE;distance_contact="100";
##   insee = NULL;id_carre = NULL;
##   selectHabitat = NULL; selectTypeHabitat= NULL#c("urbain_ps","agri_ps");
##   formatTrend = FALSE;anglais=FALSE;addAbscence=FALSE;
##   id_output=""; #"NicolasM";
##   operateur=c("Lorrilliere Romain",
##               "lorrilliere@mnhn.fr");
##   encodingSave="utf-8"



    start <- Sys.time()
    dateExport <- format(start,"%Y-%m-%d")

 


    if(is.null(con)) con <- openDB.PSQL(user,mp,nomDB)
    if(is.null(firstYear)) firstYear <- 2001
    if(is.null(lastYear)) lastYear <- as.numeric(format(start,"%Y"))
    if(is.null(altitude_max)) altitude_max <- 8000
    if(is.null(altitude_min)) altitude_min <- 0




    queryObs <- makeQueryPoint(sp=sp,distance_contact=distance_contact,champSp = champSp,nomChampSp=nomChampSp,
                           spExcluPassage1=spExcluPassage1,
                           champsHabitat=champsHabitat,champsCoordGrid=champsCoordGrid,altitude_min=altitude_min,altitude_max=altitude_max,
                           firstYear=firstYear,lastYear=lastYear,
                           departement=departement,insee=insee,onf=onf,
                           id_carre=id_carre,
                           selectHabitat = selectHabitat, selectTypeHabitat= selectTypeHabitat, dateExport =dateExport, operateur= operateur)
    d <- dbGetQuery(con, queryObs)

    d <- d %>% mutate_if(is.character, Encoding_utf8)
    setDT(d)
    spTrans <- intersect(spTransit, unique(d[,code_sp]))
    if(length(spTrans > 0)) {
        d <- d[!(code_sp %in% spTransit),]

        queryObs <- makeQueryPoint(sp=spTrans, distance_contact="all",nomChampSp=nomChampSp,
                           spExcluPassage1=spExcluPassage1,
                           champsHabitat=champsHabitat,champsCoordGrid=champsCoordGrid,altitude_min=altitude_min,altitude_max=altitude_max,
                           firstYear=firstYear,lastYear=lastYear,
                           departement=departement,insee=insee,onf=onf,
                           id_carre=id_carre,
                           selectHabitat = selectHabitat, selectTypeHabitat= selectTypeHabitat, dateExport =dateExport, operateur = operateur)
        dTrans <- dbGetQuery(con, queryObs)

        dTrans <- dTrans %>% mutate_if(is.character, Encoding_utf8)

        setDT(dTrans)
        d <- rbind(d,dTrans)

        setorder(d,point,annee,code_sp)

    }







    if(addAbscence) {
        cat("\n Ajout des absences: \n")
                                        #  browser()
        colSp <- c("code_sp","code_espece_euring","code_espece_taxref",colnames(d)[grep("nom",colnames(d))])
        colPointAn<- setdiff(colnames(d),c(colSp,"abondance_brut","abondance","qualite_inventaire_stoc"))
        colAbond <- c("code_sp","id_point_annee","abondance_brut","abondance","qualite_inventaire_stoc")

        dsp <- unique(subset(d,select=colSp))
        dPointAn <- unique(subset(d,select=colPointAn))
        dAbund <- subset(d,select=colAbond)

        tabInv <- unique(d$id_point_annee)

        tabInv <- expand.grid(dPointAn$id_point_annee,dsp$code_sp)
        colnames(tabInv) <- c("id_point_annee","code_sp")

        dAbund <- merge(dAbund,tabInv,by=c("code_sp","id_point_annee"),all=TRUE)

        dAbund$abondance_brut[is.na(dAbund$abondance_brut)] <- 0
        dAbund$abondance[is.na(dAbund$abondance)] <- 0

        dd <- merge(dAbund,dsp, by = "code_sp")
        dd <- merge(dd,dPointAn,by="id_point_annee")


        dd <- dd[,colnames(d)]

        if(!is.null(sp)) {
            queryPointAn <- paste("select pa.id_point as point, pa.id_carre as carre, pa.annee as annee, pk_point_annee as id_point_annee, ''::varchar(25) as etude,
 p.commune,p.insee,p.departement as departement,pa.qualite_inventaire_stoc as qualite_inventaire_stoc,p.altitude, longitude_wgs84,  latitude_wgs84,
 ",ifelse(champsCoordGrid," longitude_grid_wgs84,latitude_grid_wgs84, ",""),"
 ",ifelse(champsHabitat,listChampsHabitat,""),
 " p.db as data_base_name, ",
 "'",dateExport,"'::varchar(10) as date_export,
'",operateur[1],"'::varchar(50) as operateur,
'",operateur[2],"'::varchar(50) as email_operateur
from
point_annee as pa, point as p, carre as c ", ifelse(champsHabitat,", carre_annee as ca ",""), "
where
pa.id_point = p.pk_point and pa.id_carre = c.pk_carre and  p.altitude <= ",altitude_max," and  p.altitude >= ",altitude_min," and  pa.annee >= ",firstYear,"  and pa.annee <= ",lastYear,ifelse(champsHabitat," and pa.id_carre = ca.id_carre and pa.annee = ca.annee ",""),ifelse(is.null(departement),"",paste(" and p.departement in ",depList," ")),
ifelse(!is.null(selectHabitat),paste(" and p_milieu in ",habList," ")," "),ifelse(!is.null(selectTypeHabitat),queryTypeHab," "),"
order by
pa.id_point, annee;",sep="")



            cat("\nRequete inventaire: Recherche de tous les inventaires\n\n",queryPointAn,"\n\n")

            dToutPointAn <- dbGetQuery(con, queryPointAn)

            dPointAbs <- subset(dToutPointAn,!(id_point_annee %in% dd$id_point_annee))
            if(nrow(dPointAbs)>0) {
                nbSp <- nrow(dsp)
                nbPointAbs <- nrow(dPointAbs)

                dPointAbs2 <- as.data.frame(lapply(dPointAbs, rep,nbSp))
                dspAbs <- as.data.frame(lapply(dsp, rep,each= nbPointAbs))
                dPointAbs <- cbind(dPointAbs2,dspAbs)
                dPointAbs$abondance <- 0
                dPointAbs$abondance_brut <- 0
                                        #    browser()
                dPointAbs <- dPointAbs[,colnames(d)]

                d <- rbind(dd,dPointAbs)
            } else {
                d <- dd
            }
        }else {
            d <- dd

        }


        d <- d[order(d$point,d$annee,d$code_sp),]

    }


    if (formatTrend){
                                        #  browser()
        d <- subset(d,select=c("carre","annee","code_sp","abondance"))
        colnames(d)[3:4] <- c(nomChampSp,"abond")
    }

    if(isEnglish) {
        cat(" - Traduction des noms de colonnes\n")
        d <- trad_fr2eng(d)

    }



                                        #if(savePostgres) sqlCreateTable(con,"outcome.FrenchBBS_montaneOpen",d)
    if(is.null(sp)) suffSp <- "allSp" else if(length(sp)<4) suffSp <- paste(sp,collapse="-") else suffSp <- paste(length(sp),"sp",sep="")

    fileOut <- paste("export/data_FrenchBBS_point_",id_output,"_",suffSp,"_",firstYear,"_",lastYear,ifelse(isEnglish,"eng","fr"),".csv",sep="")
    write.csv2(d,fileOut,row.names=FALSE,fileEncoding=encodingSave)
    cat(" --> ",fileOut,"\n")

    end <- Sys.time() ## heure de fin

    dbDisconnect(con)

    diff_time <- end - start
    cat("\n     #      ==> Duree:",round(diff_time[[1]],2),units(diff_time),"\n")
    if(output) return(d)
}



makeQueryPoint <- function(sp=NULL, distance_contact="inf",champSp = "code_sp",nomChampSp="espece",
                           spExcluPassage1=c("MOTFLA","SAXRUB","ANTPRA","OENOEN","PHYTRO"),# (Prince et al. 2013 Env. Sc. and Pol.) + "OENOEN","PHYTRO" avis d'expert F. Jiguet
                           champsHabitat=TRUE,champsCoordGrid=TRUE,altitude_min=NULL,altitude_max=NULL,
                           firstYear=NULL,lastYear=NULL,
                           departement=NULL,insee=NULL,onf=TRUE,
                           id_carre=NULL,
                           selectHabitat = NULL, selectTypeHabitat= NULL, dateExport ="", operateur=c("Lorrilliere Romain",
                                                                                                   "lorrilliere@mnhn.fr")) {


  flag.query1 <- FALSE
  flag.query2 <- FALSE
  

      if(is.null(distance_contact) | distance_contact == "" | distance_contact == "all") {
        distance_contact_txt <- "" ; distance_contact <- "all"
    } else {
        if(distance_contact == "100") {
            distance_contact_txt <- " distance_contact in ('inf_25m','25-50m','50-100m','25-100m') and "
        } else {
            if(distance_contact == "200") {
                distance_contact_txt <- " distance_contact in ('inf_25m','25-50m','50-100m','25-100m','100-200m','sup_100m') and "
            } else {
                if(distance_contact == "inf") {
                    distance_contact_txt <- " distance_contact in ('inf_25m','25-50m','50-100m','25-100m','100-200m','sup_100m','sup_200m','') and "
                } else {
                    stop("distance_contact non pris en charge")
                }}}}

     if(distance_contact %in% c("all","inf","100","200")) {
        champ_seuil1 <- paste0("seuil_",distance_contact,"_tukey_outlier")
        champ_seuil2 <- paste0("seuil_",distance_contact,"_tukey_farout")
    } else {
        stop("distance_contact non pris en charge")
    }

    if(!is.null(sp)) {
        if(champSp != "code_sp") {

            sp <- getCode_sp(con,champSp,sp)

        }

        sp1 <- sp[!(sp %in% spExcluPassage1)]
        if(length(sp1) > 0){
            spList1 <- paste("('",paste(sp1,collapse="' , '"),"')",sep="")
            flag.query1 <- TRUE
        }

        sp2 <- sp[sp %in% spExcluPassage1]
        if(length(sp2) > 0){
            spList2 <- paste("('",paste(sp2,collapse="' , '"),"')",sep="")
            flag.query2 <- TRUE
        }

    } else {

        spListExclud <- paste("('",paste(spExcluPassage1,collapse="' , '"),"')",sep="")
        flag.query1 <- flag.query2 <- TRUE
    }

    if(!is.null(selectHabitat)) habList <- paste("('",paste(selectHabitat,collapse="' , '"),"')",sep="")
    if(!is.null(selectTypeHabitat)) queryTypeHab <- paste(" AND ( pa.",paste(selectTypeHabitat,collapse=" = TRUE OR pa.")," = TRUE) ",sep="") else queryTypeHab <- " "
    if(!is.null(departement)) depList <- paste("('",paste(departement,collapse="' , '"),"')",sep="")
    if(!is.null(insee)) inseeList <- paste("('",paste(insee,collapse="' , '"),"')",sep="")


listChampsHabitat <- " pa.p_habitat_sug_pass1 as code_habitat_principal_pass1,hcp1.habitat_nom as habitat_principal_pass1,
 pa.p_habitat_sug_pass2 as code_habitat_principal_pass2, hcp2.habitat_nom as habitat_principal_pass2,
 pa.s_habitat_sug_pass1 as code_habitat_secondaire_pass1, hcs1.habitat_nom as habitat_secondaire_pass1,
 pa.s_habitat_sug_pass2 as code_habitat_secondaire_pass2,hcs2.habitat_nom as habitat_secondaire_pass2,
pa.time_declaration_sug_j as temps_depuis_derniere_description_habitat,
   pa.foret_p as foret_p, pa.agri_p as agricole_p, pa.urbain_p as urbain_p, pa.ouvert_p as ouvert_p, pa.foret_ps as foret_ps, pa.agri_ps as agricole_ps, pa.urbain_ps as urbain_ps, pa.ouvert_ps as ouvert_ps,
  nbp_foret_p  as carre_nb_pts_foret_p, nbp_ouvert_p as carre_nb_pts_ouvert_p, nbp_agri_p as carre_nb_pts_agricole_p, nbp_urbain_p as carre_nb_pts_urbain_p,
nbp_foret_ps as carre_nb_pts_foret_ps, nbp_ouvert_ps as carre_nb_pts_ouvert_ps, nbp_agri_ps as carre_nb_pts_agricole_ps, nbp_urbain_ps  as carre_nb_pts_urbain_ps, "

    selectQueryOp <- ""
    selectQueryOp1 <- "SELECT id_inventaire, code_sp, sum(abondance) as abond
				FROM
				observation as o, inventaire as i,point as p, point_annee as pa, carre as c
				WHERE
				o.id_inventaire = i.pk_inventaire and
				o.id_carre = c.pk_carre and
				o.id_point = p.pk_point and
				o.id_point = pa.id_point and o.annee=pa.annee and
				pa.qualite_inventaire_stoc > 0 and
                                "
    selectQueryOp2 <- "
			GROUP BY
			id_inventaire, code_sp"

    if(!is.null(departement)) depList <- paste("('",paste(departement,collapse="' , '"),"')",sep="")
    if(!is.null(id_carre)) carreList <- paste("('",paste(id_carre,collapse="' , '"),"')",sep="")

    selectQuery <- paste(distance_contact_txt," i.annee >= ",firstYear,"  and i.annee <= ",lastYear,
                         " and c.etude in ('STOC_EPS'",ifelse(onf,", 'STOC_ONF'",""),")  and  p.altitude <= ",altitude_max," and p.altitude >= ",altitude_min,"
",
ifelse(is.null(departement),"",paste(" and p.departement in ",depList," ")),"
",
ifelse(is.null(insee),"",paste(" and p.insee in ",inseeList," ")),"
",
ifelse(is.null(id_carre),"",paste(" and o.id_carre in ",carreList," ")),"
",
ifelse(!is.null(selectHabitat),paste(" and p_milieu in ",habList," ")," "),"
",
ifelse(!is.null(selectTypeHabitat),queryTypeHab," ")," ", sep="")



    if(flag.query1) {
        selectQueryOp <- paste(selectQueryOp,selectQueryOp1,
                               ifelse(is.null(sp),paste("code_sp not  in ",spListExclud," and
                                "),paste(" code_sp in ",spList1," and
                                ")),"
                                passage_stoc in (1,2) and",
                               selectQuery,selectQueryOp2)
        if(flag.query2) {
            selectQueryOp <- paste(selectQueryOp,"
                                union
                                ")
        }
    }

    if(flag.query2) {
        selectQueryOp <- paste(selectQueryOp," --  ajout des especes tardives dont on ne garde que le second passage
            ",
            selectQueryOp1,
            ifelse(is.null(sp),paste("code_sp in ",spListExclud," and "),paste(" code_sp in ",spList2," and ")),"  passage_stoc = 2 and",
            selectQuery,selectQueryOp2)
    }


    queryObs <- paste(
        "select om.id_point as point, p.id_carre as carre, om.annee as annee,
(om.annee::varchar(4)||om.id_point::varchar(100))::varchar(100) as id_point_annee,
c.etude as etude, p.commune,p.insee,p.departement as departement,
om.code_sp , e.scientific_name as nom_scientifique, e.french_name as nom_francais, e.english_name as nom_anglais, e.euring as code_espece_euring,e.taxref as code_espece_taxref, '",distance_contact,"'::varchar(10) as distance_contact_max,
abond_brut as abondance_brut, abond_tuckey_outlier  as abondance_filtre_tuckey,abond_tuckey_farout  as abondance_filtre_tuckey_farout, ",champ_seuil1," as seuil_tuckey_outlier, ",champ_seuil2," as seuil_tuckey_farout,
 pa.qualite_inventaire_stoc as qualite_inventaire_stoc,
p.altitude, longitude_wgs84,  latitude_wgs84, " , ifelse(champsCoordGrid," longitude_grid_wgs84,latitude_grid_wgs84, ",""),"
" , ifelse(champsHabitat,listChampsHabitat,""),
" p.db as data_base_name, ",
"'",dateExport,"'::varchar(10) as date_export,
'",operateur[1],"'::varchar(50) as operateur,
'",operateur[2],"'::varchar(50) as email_operateur

from(
	select id_point, annee,code_sp,max(abond_brut) as abond_brut,max(abond_tuckey_outlier) as abond_tuckey_outlier,max(abond_tuckey_farout) as abond_tuckey_farout
		from -- ## begin ## correction abondance obs par seuil
			(select id_inventaire,id_point, passage_stoc, annee, opb.code_sp,abond as abond_brut, LEAST(abond, ",champ_seuil1,") as abond_tuckey_outlier, ",champ_seuil1," as seuil_tuckey_outlier, LEAST(abond, ",champ_seuil2,") as abond_tuckey_farout, ",champ_seuil2," as seuil_tuckey_farout
			from -- ## begin ## selection classe de distance et different filtre
				( ",selectQueryOp,"
	                        ) --  ## end ## selection classe de distance et different filtre
			as opb, seuil_obs as s, inventaire as i
			WHERE
			opb.code_sp = s.code_sp and opb.id_inventaire = i.pk_inventaire

			) -- ## end ## correction abondance obs par seuil
		as op
		group by id_point,annee,code_sp
		 )-- ## end ## obs max par point sur 2 passages
		 as om
 INNER JOIN point as p ON om.id_point = p.pk_point
INNER JOIN carre as c ON p.id_carre = c.pk_carre
LEFT JOIN species as e ON om.code_sp = e.pk_species
INNER JOIN seuil_obs as s  ON om.code_sp = s.code_sp
INNER JOIN point_annee as pa ON (om.id_point = pa.id_point and om.annee = pa.annee)
INNER JOIN carre_annee as ca ON (p.id_carre = ca.id_carre and om.annee = ca.annee)
 " , ifelse(champsHabitat," LEFT JOIN habitat_cat as hcp1  ON pa.p_habitat_sug_pass1 = hcp1.habitat_code
LEFT JOIN habitat_cat as hcp2 ON pa.p_habitat_sug_pass2 = hcp2.habitat_code
LEFT JOIN habitat_cat as hcs1 ON pa.s_habitat_sug_pass1 = hcs1.habitat_code
LEFT JOIN habitat_cat as hcs2  ON pa.s_habitat_sug_pass2 = hcs2.habitat_code ",""),"
order by
om.id_point, annee,code_sp; ",sep="")



                                        # browser()
    cat("\nRequete principale:\n\n",queryObs,"\n\n")
return(queryObs)



}













makeTableBrut <- function(con=NULL,user=NULL,mp=NULL,nomDB=NULL,output=TRUE,sp=NULL,champSp = "code_sp",nomChampSp="espece",
                          altitude_min=NULL,altitude_max=NULL,firstYear=NULL,lastYear=NULL,
                          departement=NULL,onf=TRUE,
                          id_carre=NULL,insee=NULL,distance_contact="inf",
                          selectHabitat = NULL, selectTypeHabitat= NULL,champsHabitat=FALSE,
                          formatTrend = FALSE,isEnglish=FALSE,addAbscence=FALSE,
                          id_output="AlexR_2017-11-23", #"NicolasM",
                          operateur=c("Lorrilliere Romain",
                                      "lorrilliere@mnhn.fr"),
                          encodingSave="utf-8",test=FALSE) {


        sp=c("DENMAJ","DENMED","DRYMAR")
        nomDB=NULL;id_carre=NULL;insee=NULL;
        sp=NULL;champSp = "code_sp";nomChampSp="espece";
        altitude=NULL;firstYear=NULL;lastYear=NULL;
        departement=44;onf=TRUE;
        selectHabitat = NULL; selectTypeHabitat= "urbain_p";
        formatTrend = FALSE;isEnglish=FALSE;addAbscence=FALSE;
        operateur=c("Lorrilliere Romain","lorrilliere@mnhn.fr")
        test=FALSE


    depts <- paste("'",paste(departement,collapse="','"),"'",sep="")
    start <-  Sys.time()
    dateExport <- format(start,"%Y-%m-%d")



    if(!is.null(sp)) {
        if(champSp != "code_sp") {

            sp <- getCode_sp(con,champSp,sp)

        }

    }
    if(is.null(con)) con <- openDB.PSQL(user,mp,nomDB)
    if(is.null(firstYear)) firstYear <- 2001
    if(is.null(lastYear)) lastYear <- as.numeric(format(start,"%Y"))
    if(is.null(altitude_max)) altitude_max <- 8000
    if(is.null(altitude_min)) altitude_min <- 0


    listChampsHabitat <- "h.habitat as code_habitat_sug, h.habitat_consistent as habitat_robuste, h.time_last_declaration_sug_j as temps_depuis_derniere_description_habitat,
 hcp.habitat_nom as habitat_principal, hcs.habitat_nom as habitat_secondaire,
o.p_habitat as code_habitat_principal_obs,o.p_cat1, o.p_cat2, o.p_sous_cat1, o.p_sous_cat2,
o.s_habitat as code_habitat_secondaire_obs,o.s_cat1, o.s_cat2, o.s_sous_cat1, o.s_sous_cat2,
   pa.foret_p as foret_p, pa.agri_p as agricole_p, pa.urbain_p as urbain_p, pa.ouvert_p as ouvert_p, pa.foret_ps as foret_ps, pa.agri_ps as agricole_ps, pa.urbain_ps as urbain_ps, pa.ouvert_ps as ouvert_ps,
  nbp_foret_p  as carre_nb_pts_foret_p, nbp_ouvert_p as carre_nb_pts_ouvert_p, nbp_agri_p as carre_nb_pts_agricole_p, nbp_urbain_p as carre_nb_pts_urbain_p,
nbp_foret_ps as carre_nb_pts_foret_ps, nbp_ouvert_ps as carre_nb_pts_ouvert_ps, nbp_agri_ps as carre_nb_pts_agricole_ps, nbp_urbain_ps  as carre_nb_pts_urbain_ps, "


        if(is.null(distance_contact) | distance_contact == "" | distance_contact == "all") {
        distance_contact_txt <- "" ; distance_contact <- "all"
    } else {
        if(distance_contact == "100") {
            distance_contact_txt <- " distance_contact in ('inf_25m','25-50m','50-100m','25-100m') and "
        } else {
            if(distance_contact == "200") {
                distance_contact_txt <- " distance_contact in ('inf_25m','25-50m','50-100m','25-100m','100-200m','sup_100m') and "
            } else {
                if(distance_contact == "inf") {
                    distance_contact_txt <- " distance_contact in ('inf_25m','25-50m','50-100m','25-100m','100-200m','sup_100m','sup_200m','') and "
                } else {
                    stop("distance_contact non pris en charge")
                }}}}



        if(!is.null(sp)) {
            if(champSp != "code_sp") {

                sp <- getCode_sp(con,champSp,sp)

            }
            spList <- paste("('",paste(sp,collapse="' , '"),"')",sep="")

        }

        if(!is.null(id_carre)) carreList <- paste("('",paste(id_carre,collapse="' , '"),"')",sep="")


        if(!is.null(selectHabitat)) habList <- paste("('",paste(selectHabitat,collapse="' , '"),"')",sep="")
        if(!is.null(departement)) depList <- paste("('",paste(departement,collapse="' , '"),"')",sep="")

        if(!is.null(insee)) inseeList <- paste("('",paste(insee,collapse="' , '"),"')",sep="")

        selectQuery <- paste(distance_contact_txt,ifelse(is.null(sp),"",paste(" code_sp in ",spList," and
                                ")),"
"," i.annee >= ",firstYear,"  and i.annee <= ",lastYear,
" and c.etude in ('STOC_EPS'",ifelse(onf,", 'STOC_ONF'",""),")  and  c.altitude_median <= ",altitude_max," and  c.altitude_median >= ",altitude_min,"
",
ifelse(is.null(id_carre),"",paste(" and o.id_carre in ",carreList," ")),"
",
ifelse(is.null(departement),"",paste(" and p.departement in ",depList," ")),"
",
ifelse(is.null(insee),"",paste(" and p.insee in ",inseeList," ")),"
",
ifelse(!is.null(selectHabitat),paste(" and p_milieu in ",habList," ")," "),"
"," ",sep="")


        if(!is.null(selectTypeHabitat)) queryTypeHab <- paste(" AND ( pa.",paste(selectTypeHabitat,collapse=" = TRUE OR pa.")," = TRUE) ",sep="") else queryTypeHab <- ""

        selectQuery <- paste(" WHERE ",selectQuery,queryTypeHab)



        query <- paste("
SELECT
  o.pk_observation as code_observation,  o.id_raw_observation,
  o.id_inventaire as code_inventaire,  i.etude,   i.observateur,  i.email, p.site as nom_site,  p.commune,  p.insee,  p.departement,  o.id_carre as code_carre,    o.id_point as code_point,   o.num_point,     o.date,i.jour_julien,  o.annee,  i.heure_debut,  i.heure_fin,  i.duree_minute, o.espece,   s.scientific_name as nom_scientifique,  s.french_name as nom_francais,s.english_name as nom_anglais,s.euring as code_espece_euring,s.taxref as code_espece_taxref,
 o.distance_contact,  o.abondance,seuil_100_tukey_outlier,seuil_100_tukey_outlier,seuil_200_tukey_outlier,seuil_inf_tukey_outlier,seuil_all_tukey_outlier,
  i.info_passage,  i.passage_stoc as numero_passage_stoc, info_passage, i.info_passage_an, i.nombre_passage_stoc_annee,  i.nb_sem_entre_passage,i.info_temps_entre_passage,i.diff_sunrise_h,i.info_heure_debut,   o.nuage,  o.pluie,  o.vent,  o.visibilite,  o.neige,  p.altitude,  p.longitude_wgs84,  p.latitude_wgs84,  c.longitude_grid_wgs84,  c.latitude_grid_wgs84," , ifelse(champsHabitat,listChampsHabitat,""),"   o.db,  o.date_export as date_import, '",dateExport,"'::varchar(10) as date_export,
'",operateur[1],"'::varchar(50) as operateur,
'",operateur[2],"'::varchar(50) as email_operateur
FROM
 observation as o
  INNER JOIN inventaire as i ON o.id_inventaire = i.pk_inventaire
  INNER JOIN point as p ON  o.id_point = p.pk_point
  INNER JOIN carre as c ON o.id_carre = c.pk_carre
  INNER JOIN habitat as h ON o.id_inventaire = h.pk_habitat
  INNER JOIN point_annee as pa ON (o.id_point = pa.id_point AND  o.annee = pa.annee)
  INNER JOIN carre_annee as ca ON (o.id_carre = ca.id_carre AND  o.annee = ca.annee)
  LEFT JOIN species as s ON o.espece = s.pk_species
  LEFT JOIN seuil_obs as so ON o.espece = so.code_sp
" , ifelse(champsHabitat," LEFT JOIN habitat_cat as hcp  ON h.p_habitat_sug = hcp.habitat_code
LEFT JOIN habitat_cat as hcs ON h.s_habitat_sug = hcs.habitat_code ",""),"
    ",selectQuery,";",sep="")
        if(test) paste(query," LIMIT 100;") else paste(query,";")

    cat("\n QUERY donnees BRUT:\n--------------\n\n",query,"\n")




        d <- dbGetQuery(con, query)

                                        #if(savePostgres) sqlCreateTable(con,"outcome.FrenchBBS_montaneOpen",d)


        if(is.null(sp)) suffSp <- "allSp" else if(length(sp)<4) suffSp <- paste(sp,collapse="-") else suffSp <- paste(length(sp),"sp",sep="")

        fileOut <- paste("export/data_FrenchBBS_BRUT_",id_output,"_",suffSp,"_",firstYear,"_",lastYear,ifelse(isEnglish,"eng","fr"),".csv",sep="")
        write.csv2(d,fileOut,row.names=FALSE)
        cat(" --> ",fileOut,"\n")

        end <- Sys.time() ## heure de fin

        dbDisconnect(con)

        diff_time <- end - start
    cat("\n     #      ==> Duree:",round(diff_time[[1]],2),units(diff_time),"\n")

    if(output) return(d)





    }







