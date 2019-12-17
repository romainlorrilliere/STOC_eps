source("scriptExportation.r")



testDistanceContact <- function(con=NULL,user=NULL,mp=NULL,nomDB=NULL) {
    require(reshape2)
    con <- NULL
    start <- Sys.time()
    dateExport <- format(start,"%Y-%m-%d")

    if(is.null(con)) con <- openDB.PSQL(user,mp,nomDB)

    query <- "
select id_inventaire,date,annee,code_sp, abondance,distance_contact,db
from observation;
"
    d <- dbGetQuery(con, query)

    di <- unique(subset(d,select=c("id_inventaire","date","annee","code_sp","db")))
    ddist <- subset(d,select=c("id_inventaire","code_sp","distance_contact","abondance"))


    dd <- dcast(ddist,id_inventaire + code_sp ~ distance_contact)





    dbDisconnect(con)
}



precosse_passage <- function() {

#    d1 <- makeTableBrut(user="romain",mp=pw,sp=c("DENMAJ","DENMED","DRYMAR"),id_carre=subset(dn2000,withinN2000==1,select="site")[,1],selectTypeHabitat="urbain_p")

    vec_sp <- c("DENMAJ","DENMED","DENMIN","DRYMAR","PICVIR","JYNTOR")

  d1 <- makeTableBrut(user="romain",mp=pw,sp=vec_sp,id_carre=subset(dn2000,withinN2000==1,select="site")[,1])


    d2 <- makeTableBrut(user="romain",mp=pw,sp=vec_sp,id_carre=subset(dn2000,outN2000_15km==1,select="site")[,1])


    d3 <- makeTableBrut(user="romain",mp=pw,sp=vec_sp)


    d1$n2000 <- "IN"
    d2$n2000 <- "25km"
    d3$n2000 <- "ALL"

    d <- rbind(d1,d2)
    d <- rbind(d,d3)

    dim(d)
    dim(subset(d,info_passage %in% c("normal","precoce")))

    d <- subset(d,!is.na(numero_passage_stoc) | info_passage %in% c("normal","precoce"))
    dim(d)



    daggsum <- aggregate(abondance~date+annee+code_point+code_carre+espece+info_passage+n2000+latitude_grid_wgs84,d,sum)
    dagg1 <- aggregate(abondance~annee+code_point+espece+code_carre+info_passage+n2000+latitude_grid_wgs84,subset(daggsum,info_passage=="normal"),max)


    dagg1 <- aggregate(abondance ~ annee+espece+code_carre+n2000+latitude_grid_wgs84,dagg1,sum)

    dagg2 <- subset(daggsum,info_passage=="precoce",select=colnames(dagg1))
    dagg2 <- aggregate(abondance ~ annee+espece+code_carre+n2000+latitude_grid_wgs84,dagg2,sum)



    colnames(dagg1)[6] <- "ab_normal"

    colnames(dagg2)[6] <- "ab_precoce"

    nbprecoce <- aggregate(ab_precoce~annee+code_carre+espece,dagg2,length)
    nrow(subset(nbprecoce,ab_precoce>1))

    dagg <- merge(dagg1,dagg2,by=c("annee","code_carre","espece","n2000","latitude_grid_wgs84"),all=TRUE)

    dagg$ab_normal[is.na(dagg$ab_normal)] <- 0
    dagg$ab_precoce[is.na(dagg$ab_precoce)] <- 0
    carrePrecoce <- unique(paste(dagg2$code_carre,dagg2$annee,sep="_"))
    dagg$carreAnnee <- paste(dagg$code_carre,dagg$annee,sep="_")


    dim(dagg)
    dagg <- subset(dagg,carreAnnee %in% carrePrecoce)
    dim(dagg)

    dagg$diff <- dagg$ab_normal - dagg$ab_precoce


    require(ggplot2)

    gg <- ggplot(dagg,aes(y=diff,x=n2000,fill=n2000))+geom_violin()+facet_wrap(~espece,scales="free")
    gg <- gg + labs(title="max(1 + 2) - precoce")
    gg <- gg + geom_hline(yintercept=0)
    gg
    ggsave("DENMAJ_precoce_N2000.png",gg)


    write.csv(d,"dataPrecoce_N2000.csv",row.names=FALSE)





}



bouDeCode_table_sti <- function() {





    dsp0 <- read.csv("DB_import/tablesGeneriques/espece.csv",encoding="UTF-8")
    dsp <- dsp0[,c("pk_species","euring","taxref","english_name","scientific_name","french_name")]
    d.sti.e <- read.csv("DB_import/tablesGeneriques/Europe_STIb.csv")
    d.sti.eNA <- subset(d.sti.e,is.na(EURING))
    d.sti.e <- subset(d.sti.e,!(is.na(EURING)))
    d.sti.e <- merge(d.sti.e,dsp,by.x="EURING",by.y="euring",all.x=TRUE)

    subset(d.sti.e,is.na(pk_species))

    dsp1 <- dsp0[grep("Bunting",dsp0$english_name),][3,]

    dsp0$euring[dsp0$euring==520] <- 510
    dsp1$euring <- 18730
    dsp1$scientific_name <- "Emberiza rustica"
    dsp1$english_name <- "Rustic Bunting"
    dsp1$french_name <- "Bruant rustique"
    dsp1$taxref <- NA
    dsp1$pk_species <- "_EMBRUS"


    dsp0 <- rbind(dsp0,dsp1)



dsti2 <- merge(d.sti.eNA,dsp,by.x="SPECIES",by.y="scientific_name",all.x=TRUE)
    dsti2$EURING <- dsti2$euring
      subset(dsti2,is.na(pk_species))
   d.sti.eNA <- dsti2[,1:4]
    d.sti.e <- rbind(d.sti.e,d.sti.eNA)
    write.csv(d.sti.e,"DB_import/tablesGeneriques/Europe_STIbis.csv",fileEncoding="UTF-8",row.names=FALSE)

    write.csv(dsp0,"DB_import/tablesGeneriques/espece.csv",fileEncoding="UTF-8",row.names=FALSE)


 dsp0 <- read.csv("DB_import/tablesGeneriques/espece.csv",encoding="UTF-8")
    dsp <- dsp0[,c("pk_species","euring","taxref","english_name","scientific_name","french_name")]

    dsti<- read.csv("DB_import/tablesGeneriques/Europe_STIbis.csv",encoding="UTF-8")
    dindic <-  read.csv("DB_import/tablesGeneriques/espece_indicateur_fonctionel.csv",encoding="UTF-8")

dsti <- subset(dsti,!(is.na(EURING)))
    dsti <- merge(dsti,dsp,by.x="EURING","euring")

dindic <- merge(dindic,dsti,by="pk_species",all=TRUE)
dindic$sti <- dindic$STI
dindic <- dindic[,c(1,14,18,20,19,21,2:3,5:13)]
    dindic <- dindic[,c(1:4,6,5,7:ncol(dindic))]
    colnames(dindic)[2] <- "euring"

    write.csv(dindic,"DB_import/tablesGeneriques/espece_indicateur_fonctionel.csv",fileEncoding="UTF-8",row.names=FALSE)



    }
