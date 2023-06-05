

#####################################################
##  Les fonctions pour les tables issue de FNat
#####################################################




FNat2point <- function(d,dateExport,repOut="",output=TRUE) {
                                        #   d <- dFNat
                                        #   dateExport <- dateExportFNat
                                        #    output=TRUE

#d <- copy(dFNat)

    dagg <- d[, .(commune = get_mode(commune),
                  site = get_mode(nom_carre),
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
    dd[,db := "FNat"]

    setkey(dagg,"id_point")
    setkey(dd,"id_point")

    dd <- dd[dagg]
    setnames(dd,old=c("id_point"),new=c("pk_point"))

    colorder <- c("pk_point","id_carre","commune","site","insee","departement","nom_point","num_point","altitude","longitude_wgs84","latitude_wgs84","longitude_wgs84_sd","latitude_wgs84_sd","db","date_export")
    setcolorder(dd,colorder)




    filename <- paste0(repOut,"point_FNat_",dateConstruction,".csv")
    write.csv(dd,filename,row.names=FALSE)
    cat("  (-> ",filename,")\n",sep="")

    if(output) return(dd)
}



FNat2carre <- function(d,dateExport,repOut="",output=FALSE) {
    ##d = copy(dFNat);dateExport=dateExportFNat;output=FALSE;repOut=""

    dcarrenat <- fread("data_generic/carrenat.csv",encoding="UTF-8")
    dcarrenat[,pk_carre:=sprintf("%06d",pk_carre)]
    dcarrenat <- dcarrenat[,.(pk_carre,lat_WGS84,lon_WGS84)]
    setnames(dcarrenat, old= c("lat_WGS84","lon_WGS84"),new=c("latitude_grid_wgs84","longitude_grid_wgs84"))
#
    dd <- d[,.(commune = get_mode(commune),
                site = "",
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

    dd[,db := "FNat"]
    setnames(dd,old=c("id_carre"),new=c("pk_carre"))
    setkey(dd,"pk_carre")

    dd <- merge(dd,dcarrenat,by="pk_carre",all.x=TRUE)

    colorder <- c("pk_carre","commune","site","etude","insee","departement","nom_carre","nom_carre_fnat","altitude_median","latitude_median_wgs84","longitude_median_wgs84","latitude_grid_wgs84","longitude_grid_wgs84","db","date_export")


    dd <- dd[,colorder,with=FALSE]

    filename <- paste0(repOut,"carre_FNat_",dateConstruction,".csv")
    write.csv(dd,filename,row.names=FALSE)
    cat("  (->",filename,")\n")

    if(output) return(dd)

}




FNat2inventaire <-  function(d,dateExport,version = "V.1",dateConstruction="",repOut="",output=FALSE) {
    library(lubridate)

##    d  <- copy(dFNat); dateExport = dateExportFNat ;verion = "VV";output=FALSE

   ## dateExport=dateExportVP
    ## d=dVP

    require(maptools)

    dd <- d[,.(unique_inventaire_fnat=id_inventaire_fnat,
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
               neige = NA,
               altitude = get_mode(altitude),
               latitude_wgs84 = median(latitude_wgs84),
               longitude_wgs84 = median(longitude_wgs84),
               db = "FNat",
               date_export=date_export,version=version), by = id_inventaire]

    dd <- unique(dd)
    aa <- table(dd$id_inventaire)
    aa_unique <- names(aa)[aa==1]
    aa_doublon <- names(aa)[aa>1]

    if(length(aa_doublon)>0) {

        cat(" ! WARNING: gestion de ",length(aa_doublon)," doublon(s) dans les inventaires !\n")
        dd_unique <- dd[id_inventaire %in% aa_unique,]

        dd_doub <- dd[id_inventaire %in% aa_doublon,]

        dd_doub <- unique(dd_doub[,.(unique_inventaire_fnat = get_mode(unique_inventaire_fnat),
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
               neige = NA,
               altitude = get_mode(altitude),
               latitude_wgs84 = median(latitude_wgs84),
               longitude_wgs84 = median(longitude_wgs84),
               db = "FNat",
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

  filename <- paste0(repOut,"inventaire_FNat_",dateConstruction,".csv")
    write.csv(dd,filename,row.names=FALSE)
    cat("  (->",filename,")\n")

    if(output) return(dd)




}





FNat2observation <- function(d,dateExport,dateConstruction="",repOut="",output=FALSE) {

 ##   d  <- copy(dFNat); dateExport = dateExportFNat;output=FALSE

    dd <- d[,.(id_observation,idat_unique_citation ,id_inventaire,id_point,id_carre, num_point,date,annee,classe,id_data,espece,code_sp,abondance,distance_contact,db,date_export)]

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

        file <- paste0(repOut,"_doublonObservationFNat_Exclu_",dateConstruction,".csv")
        cat("  \n !!! Doublon exclue enregistre dans le fichier \n --> ",file,"\n")
        write.csv(tDoublon[,-ncol(tDoublon)],file,row.names=FALSE)

        dd <- dd[!(pk_observation %in% dataExclues[,pk_observation]),]

        setorder(dd,id_inventaire)
    }

      filename <- paste0(repOut,"observation_FNat_",dateConstruction,".csv")
    write.csv(dd,filename,row.names=FALSE)
    cat("  (->",filename,")\n")

    if(output) return(dd)




}





FNat2habitat <- function(d,dateExport,repOut="",output=FALSE) {
    d  <- copy(dFNat); dateExport = dateExportFNat;output=FALSE


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

    filename <- paste0(repOut,"habitat_FNat_",dateConstruction,".csv")
    write.csv(dd,filename,row.names=FALSE)
    cat("  (->",filename,")\n")

    if(output) return(dd)

}





###########################################################################################
##   Fonctions qui font les unions des table VigiePlume et FNat et qui gerent les doublons
###########################################################################################


union.point <- function(ddVP.point,ddFNat.point) {
    dd.point <- rbind(ddVP.point,ddFNat.point[!(pk_point %in% unique(ddVP.point$pk_point)),])
    setorder(dd.point,pk_point)
    ##  ulf <- data.frame(pk_point=ddFNat.point$pk_point,ulf = ddFNat.point$unique_localite_fnat)

    ##  dd.point <- merge(dd.point,ulf,by="pk_point",all = TRUE)
    ##  dd.point$unique_localite_fnat <- ifelse(is.na(dd.point$unique_localite_fnat),dd.point$ulf)
    ##  dd.point <- dd.point[,-15]
    return(dd.point)

}


union.carre <- function(ddVP.carre,ddFNat.carre) {

    dd.carre <- rbind(ddVP.carre,ddFNat.carre[!(pk_carre %in% unique(ddVP.carre$pk_carre)),])
    setorder(dd.carre,pk_carre)

    ncf <- ddFNat.carre[,.(pk_carre,nom_carre_fnat)]
    setnames(ncf, "nom_carre_fnat","ncf")
    dd.carre <- merge(dd.carre,ncf,by="pk_carre",all = TRUE)
    dd.carre[is.na(nom_carre_fnat) & !(is.na(ncf)), nom_carre_fnat := as.character(ncf)]
    dd.carre <- dd.carre[,ncf:=NULL]

    lesColonnes <- c("pk_carre","nom_carre","nom_carre_fnat","commune","site","etude","insee","departement","altitude_median","longitude_median_wgs84","latitude_median_wgs84","longitude_grid_wgs84","latitude_grid_wgs84","db","date_export")

    setcolorder(dd.carre,lesColonnes)

    return(dd.carre)

}



union.inventaire <- function(ddFNat.inv,ddVP.inv,dateConstruction,repOut="") {
    dd.inv <- rbind(ddVP.inv,ddFNat.inv[!(pk_inventaire %in% unique(ddVP.inv$pk_inventaire)),])
    setorder(dd.inv,pk_inventaire)
    dd.invDoublon <- subset(ddFNat.inv,(pk_inventaire %in% unique(ddVP.inv$pk_inventaire)))
    dd.inv[id_carre %in% unique(dd.inv[etude == "STOC_ONF",id_carre]),etude :=  "STOC_ONF"]

    lesColonnes <- c("pk_inventaire","unique_inventaire_fnat","id_carre","id_point","num_point","etude","annee","date","jour_julien","datetime","passage_observateur","passage_stoc","periode_passage","repetition_passage","nombre_passage_annee","nombre_passage_stoc_annee","info_passage_an","info_passage","nb_sem_entre_passage","info_temps_entre_passage","heure_debut","heure_fin","duree_minute","inc_passage","diff_sunrise_h","diff_dawn_h","info_heure_debut","nuage","pluie","vent","visibilite","neige","observateur","email","db","date_export","version")
      setcolorder(dd.inv,lesColonnes)
    if(nrow(dd.invDoublon)>0) {
        file <- paste0(repOut,"_doublonInventaireExclu_",dateConstruction,".csv")
        cat("\n !!! ",nrow(dd.invDoublon) ," inventaires saisies dans les deux bases de données (FNat et VigiePlume) \n Les donnees de FNat sont exclu et les donnees de VigiePlume conservees\n Retrouvees les donnees exclu dans le fichier:\n --> ",file,"\n")
        write.csv(dd.invDoublon,file,row.names=FALSE)

    }

    return(dd.inv)
}

union.observation <- function(ddFNat.obs,ddVP.obs,dateConstruction,repOut="") {
    ddFNat.obs[,id_data := paste0(id_inventaire,code_sp,distance_contact)]
    ddVP.obs[,id_data := paste0(id_inventaire,code_sp,distance_contact)]

    dd.obs <- rbind(ddVP.obs,ddFNat.obs[!(id_data%in% unique(ddVP.obs[,id_data])),])
    setorder(dd.obs,pk_observation)


    dd.obsDoublon <-ddFNat.obs[id_data %in% unique(ddVP.obs[,id_data])]
    if(nrow(dd.obsDoublon)>0) {
        file <- paste0(repOut,"_doublonObsExclu_",dateConstruction,".csv")
        cat("\n !!! ",nrow(dd.obsDoublon) ," observation saisies dans les deux bases de données (FNat et VigiePlume) \n Les donnees de FNat sont exclu et les donnees de VigiePlume conservees\n Retrouvees les donnees exclu dans le fichier:\n --> ",file,"\n")
        write.csv(dd.obsDoublon,file,row.names=FALSE)

    }
    return(dd.obs)
}


union.habitat <- function(ddFNat.hab,ddVP.hab,dateConstruction,repOut="") {
    dd.hab <- rbind(ddVP.hab,ddFNat.hab[!(pk_habitat %in% unique(ddVP.hab[,pk_habitat]))])
    setorder(dd.hab,pk_habitat)
    dd.habDoublon <- ddFNat.hab[pk_habitat %in% unique(ddVP.hab[,pk_habitat])]
    if(nrow(dd.habDoublon)>0) {
        file <- paste0(repOut,"_doublonHabitatExclu_",dateConstruction,".csv")
        cat("\n !!! ",nrow(dd.habDoublon) ," habitats saisies dans les deux bases de données (FNat et VigiePlume) \n Les donnees de FNat sont exclu et les donnees de VigiePlume conservees\n Retrouvees les donnees exclu dans le fichier:\n --> ",file,"\n")
        write.csv(dd.habDoublon,file,row.names=FALSE)

    }
    return(dd.hab)
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
.

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





stGIS_initiation=FALSE,import_shape=FALSE,repertoire=NULL, postgresql_abondanceSeuil=FALSE,seuilAbondance = .99, historiqueCarre=TRUE, pointCarreAnnee=TRUE,importPointCarreAnnee=TRUE,fileTemp=FALSE)




prepaData(dateExportFaune=c("2023-03-23"),nomFileFaune=c("2023_03_23_data_stoc_eps_shoc.csv"),
    dateExportVP=c("2019-11-25","2019-12-03"),nomFileVP=c("export_stoc_25112019.txt","export_stoc_onf_03122019.txt"),
    dateExportFNat="2017-01-04",
    importACCESS=FALSE,
    nomFileFNat="FNat_plat_2017-01-04.csv",nomDBFNat="Base FNat2000.MDB",
    importationDataBrut_FNat=TRUE,importationDataBrut_VP=TRUE,importationDataBrut_Faune=TRUE,
    constructionPoint=TRUE,constructionCarre=TRUE,constructionInventaire=TRUE,
    constructionObservation = TRUE, constructionSeuil = TRUE, constructionHabitat = TRUE,
    constructionPointAnnee = TRUE, constructionCarreAnnee,
    dateConstruction="2023-05-25",postgresql_import=TRUE,
