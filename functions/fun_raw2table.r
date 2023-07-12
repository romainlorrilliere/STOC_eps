union_raw <- function(dFaune = NULL, dVP = NULL, dFNat = NULL, dHist = NULL,dateConstruction,repOutInfo="", repOutData="",output=FALSE) {

    dFaune[,keep := TRUE]
    dVP[,keep := !(id_inventaire %in% dFaune[,id_inventaire])]
    dFNat[,keep := !(id_inventaire %in% c(dFaune[,id_inventaire],dVP[,id_inventaire]))]
##    dHist[,keep := !(id_inventaire %in% c(dFaune[,id_inventaire],dVP[,id_inventaire],dFNat[,id_inventaire]))]

    lesColonnes <- intersect(intersect(colnames(dFaune),colnames(dVP)),colnames(dFNat))

    d <- rbind(rbind(dFaune[,lesColonnes,with=FALSE],dVP[,lesColonnes,with=FALSE]),dFNat[,lesColonnes,with=FALSE])

    setorder(d,date,id_carre)

    filename <- paste0(repOutData,"alldata_",dateConstruction,".csv")
    write.csv(d,filename,row.names=FALSE,na="")
    cat("  (->",filename,")\n",sep="")

    if(output) return(d)

}







raw2point <- function(d,dateConstruction,repOutInfo="", repOutData="",output=FALSE) {

    ## d=d.all;dateConstruction=dateConstruction;repOuData=repOutData;output=TRUE

    dagg <- d[keep == TRUE, .(commune = get_mode(commune),
                  site = get_mode(site),
                  insee = get_mode(insee),
                  departement = get_mode(departement),
                  altitude = median(altitude,na.rm=TRUE),
                  latitude_wgs84 = median(latitude_wgs84,na.rm=TRUE),
                  longitude_wgs84 = median(longitude_wgs84,na.rm=TRUE),
                  latitude_wgs84_sd = sd(latitude_wgs84,na.rm=TRUE),
                  longitude_wgs84_sd = sd(longitude_wgs84,na.rm=TRUE),
                  date_export = max(date_export)),
              by=.(id_point,db)]

    dagg[,keep := TRUE]
    dagg[db == "FNat",keep := !(id_point %in% dagg[db %in% c("FauneFrance","VigiePlume"),id_point])]
    dagg[db == "VigiePlume",keep := !(id_point %in% dagg[db =="FauneFrance",id_point])]

    dagg <- dagg[keep == TRUE,]
   dagg[,keep := NULL]

    dd <- unique(d[,.(id_point,id_carre,num_point)])


    setkey(dagg,"id_point")
    setkey(dd,"id_point")

    dd <- dd[dagg]
    dd[,nom_point := sprintf("%02d",num_point)]


    dd_sp <- d[keep == TRUE,.(i=1) ,by=.(id_point,code_sp)][,.(nb_sp = .N),by = id_point]
    dd_ab <- d[keep == TRUE,.(abondance = sum(abondance)) ,by=.(id_point,id_inventaire,annee)][,.(abondance_mean = mean(abondance)),by = id_point]
    dd_spmean <- d[keep == TRUE,.(i = 1) ,by=.(id_point,annee,code_sp)][,.(nb_sp = .N),by = .(id_point,annee)][,.(nb_sp_mean = mean(nb_sp)), by= id_point]


    setkey(dd,"id_point")
    setkey(dd_sp,"id_point")
    setkey(dd_ab,"id_point")
    setkey(dd_spmean,"id_point")


    dd <- dd[dd_sp[dd_spmean[dd_ab]]]



    setnames(dd,old=c("id_point"),new=c("pk_point"))

    colorder <- c("pk_point","id_carre","commune","site","insee","departement","num_point","nom_point","altitude","longitude_wgs84","latitude_wgs84","longitude_wgs84_sd","latitude_wgs84_sd","nb_sp","nb_sp_mean","abondance_mean","db","date_export")
    setcolorder(dd,colorder)


    filename <- paste0(repOutData,"point_",dateConstruction,".csv")
    write.csv(dd,filename,row.names=FALSE,na="")
    cat("  (->",filename,")\n",sep="")

    if(output) return(dd)
}

raw2carre <- function(d,dateConstruction,repOutInfo="", repOutData="",output=FALSE) {
    ##  d = copy(d.all); output=FALSE; repOut=""


    ## la table de l'ensemble des carré de la grille nationale
    dcarrenat <- fread("data_generic/carrenat.csv")
    dcarrenat<- dcarrenat[,pk_carre := sprintf("%06d",pk_carre)]

    setkey(dcarrenat,"pk_carre")

    dcarrenat <- dcarrenat[,.(pk_carre,lat_WGS84,lon_WGS84,area,perimeter)]
    setnames(dcarrenat, old= c("lat_WGS84","lon_WGS84","area","perimeter"),new=c("latitude_grid_wgs84","longitude_grid_wgs84","aire","perimetre"))


    ##  d_col <- c("id_carre","Commune","Site","INSEE","Département","N..Carré.EPS","nom_carre_fnat","EXPORT_STOC_TEXT_EPS_POINT","num_point","Altitude","Longitude","Latitude","db","date_export")
    ## dd <- d[,d_col,with=FALSE]
    ## changement des noms de colonnes

    dd <- d[keep == TRUE, .(commune = get_mode(commune),
                site = get_mode(site),
                insee = get_mode(insee),
                etude = get_mode(etude),
                etude_detail = get_mode(etude_detail),
                nom_carre = get_mode(nom_carre),
                departement = get_mode(departement),
                altitude_median = as.integer(median(altitude,na.rm=TRUE)),
                latitude_median_wgs84 = median(latitude_wgs84,na.rm=TRUE),
                longitude_median_wgs84 = median(longitude_wgs84,na.rm=TRUE),
                date_export = max(date_export)),
            by=.(id_carre,db)]

    dd[,keep := TRUE]
    dd[db == "FNat",keep := !(id_carre %in% dd[db %in% c("FauneFrance","VigiePlume"),id_carre])]
    dd[db == "VigiePlume",keep := !(id_carre %in% dd[db =="FauneFrance",id_carre])]

    dd <- dd[keep == TRUE,]
   dd[,keep := NULL]

    dd_sum <- d[keep == TRUE,.(i=1) ,by=.(id_carre,annee)][,.(first_year = min(annee),last_year = max(annee),duration = max(annee)-min(annee)+1, nb_year = .N),by = id_carre]
    dd_sp <- d[keep == TRUE,.(i=1) ,by=.(id_carre,code_sp)][,.(nb_sp = .N),by = id_carre]
    dd_ab <- d[keep == TRUE,.(abondance = sum(abondance)) ,by=.(id_carre,id_point,id_inventaire,annee)][,.(abondance_point_mean = mean(abondance)),by = id_carre]
    dd_spmean <- d[keep == TRUE,.(i = 1) ,by=.(id_carre,annee,code_sp)][,.(nb_sp = .N),by = .(id_carre,annee)][,.(nb_sp_mean = mean(nb_sp)), by= id_carre]
    dd_point <- d[keep == TRUE, .(i=1), by = .(id_carre,id_point)][,.(nb_point = .N),by = id_carre]



    setkey(dd,"id_carre")
    setkey(dd_sum,"id_carre")
    setkey(dd_sp,"id_carre")
    setkey(dd_ab,"id_carre")
    setkey(dd_spmean,"id_carre")
    setkey(dd_point,"id_carre")


    dd <- dd[dd_sp[dd_spmean[dd_ab[dd_sum[dd_point]]]]]





    setnames(dd,old=c("id_carre"),new=c("pk_carre"))

  ##





    dd <- merge(dd,dcarrenat,by="pk_carre",all.x=TRUE)
   ### dd <- dd[dcarrenat]


    colorder <- c("pk_carre","nom_carre","commune","site","etude","etude_detail","insee","departement","altitude_median","nb_point","aire","perimetre","latitude_median_wgs84","longitude_median_wgs84","latitude_grid_wgs84","longitude_grid_wgs84","first_year","last_year","duration","nb_year","nb_sp","nb_sp_mean","abondance_point_mean","db","date_export")


    dd <- dd[,colorder,with=FALSE]

    filename <- paste0(repOutData,"carre_",dateConstruction,".csv")
    write.csv(dd,filename,row.names=FALSE,na="")
    cat("  (->",filename,")\n")

    if(output) return(dd)

}

raw2inventaire <- function(d,version = "V.1",dateConstruction="",repOutInfo="", repOutData="",output=FALSE) {

    ## dateExport=dateExportVP
    ## d=copy(d.all)

    require(maptools)

    dd <- d[keep == TRUE,.(id_raw_inventaire = id_raw_inventaire,
                           etude = etude,
                           etude_detail = etude_detail,
                           id_point = id_point,
                           id_carre = id_carre,
                           num_point = num_point,
                           date = date,
                           jour_julien = jour_julien,
                           annee= annee,
                           passage_observateur = passage_observateur,
                           heure_debut = heure,
                           heure_fin = heure_fin,
                           duree_minute = duree_minute,
                           observateur = observateur,
                           email = email,
                           nuage = nuage,
                           pluie = pluie,
                           vent = vent,
                           visibilite = visibilite,
                           neige = neige,
                           altitude = get_mode(altitude),
                           latitude_wgs84 = median(latitude_wgs84,na.rm=TRUE),
                           longitude_wgs84 = median(longitude_wgs84,na.rm=TRUE),
                           db = get_mode(db),
                           date_export=date_export,version=version), by = id_inventaire]

    dd <- unique(dd)
    aa <- table(dd$id_inventaire)
    aa_unique <- names(aa)[aa==1]
    aa_doublon <- names(aa)[aa>1]

    if(length(aa_doublon)>0) {

        cat(" ! WARNING: gestion de ",length(aa_doublon)," doublon(s) dans les inventaires !\n")
        dd_unique <- dd[id_inventaire %in% aa_unique,]

        dd_doub <- dd[id_inventaire %in% aa_doublon,]

        dd_doub <- unique(dd_doub[,.(id_raw_inventaire = paste0(id_raw_inventaire,collapse="|"),
               etude = get_mode(etude),
               etude_detail = get_mode(etude_detail),
               id_point = get_mode(id_point),
               id_carre = get_mode(id_carre),
               num_point = get_mode(num_point),
               date = get_mode(date),
               jour_julien = get_mode(jour_julien),
               annee= get_mode(annee),
               passage_observateur = get_mode(passage_observateur),
               heure_debut = get_mode(heure_debut),
               heure_fin = get_mode(heure_fin),
               duree_minute = get_mode(duree_minute),
               observateur = concat_value(observateur),
               email = concat_value(email),
               nuage = get_mode(nuage),
               pluie = get_mode(pluie),
               vent = get_mode(vent),
               visibilite = get_mode(visibilite),
               neige = get_mode(neige),
               altitude = get_mode(altitude),
               latitude_wgs84 = median(latitude_wgs84,na.rm=TRUE),
               longitude_wgs84 = median(longitude_wgs84,na.rm=TRUE),
               db = get_mode(db),
               date_export=get_mode(date_export),version=version), by = id_inventaire])

        dd <- rbind(dd_unique,dd_doub)
        setDT(dd)
        setorder(dd,id_inventaire)
    }

    setnames(dd,"id_inventaire","pk_inventaire")
  ##   dd[,heure_fin := format(as.POSIXct(heure_debut,format="%H:%M")+ duree_minute*60,"%H:%M")]

    dd[,datetime := paste(date,heure_debut)]

    dd[,periode_passage := ""]
    dd[jour_julien < 61 ,periode_passage:= "winter_end"]
    dd[,passage_stoc := 999]

    dd[is.na(altitude), altitude := -999]
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

  dd[altitude == -999, altitude := NA]

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


    the_col <- c("pk_inventaire","id_raw_inventaire","etude","etude_detail","id_carre","id_point","num_point",
                 "annee","date","jour_julien","datetime","passage_observateur",
                 "passage_stoc","periode_passage","repetition_passage","inc_passage",
                 "nombre_passage_annee","nombre_passage_stoc_annee","info_passage_an","info_passage",
                 "nb_sem_entre_passage","info_temps_entre_passage",
                 "heure_debut","heure_fin","duree_minute",
                 "diff_sunrise_h","diff_dawn_h","info_heure_debut",
                 "nuage","pluie","vent","visibilite","neige",
                 "observateur","email",
                 "db","date_export","version")

    dd<-dd[,the_col,with=FALSE]


    filename <- paste0(repOutData,"inventaire_",dateConstruction,".csv")
    write.csv(dd,filename,row.names=FALSE,na="")
    cat("  (->",filename,")\n")

    if(output) return(dd)

}



raw2observation <- function(d,dateConstruction="",repOutInfo="", repOutData="",output=FALSE) {
    ## d <- copy(d.all)

    dd <- d[keep == TRUE,.(id_observation,id_raw_observation ,id_inventaire,id_point,id_carre, num_point,
                           date,annee,classe,id_data,espece,code_sp,abondance,distance_contact,
                           nuage, pluie, vent, visibilite, neige,
                           p_milieu,p_type, p_cat1, p_cat2, p_sous_cat1, p_sous_cat2,
                           s_milieu, s_type, s_cat1, s_cat2, s_sous_cat1, s_sous_cat2,
                           db,date_export)]

    dd <- unique(dd)
    setorder(dd,id_data)
    setnames(dd,"id_observation","pk_observation")

    contDoublon <- table(dd$id_data)
    doublon <- names(contDoublon)[contDoublon>1]

    if(length(doublon)>0) {
        tDoublon <- dd[id_data %in% doublon,]

        file <- paste0(repOutInfo,"_doublonObservation_",dateConstruction,".csv")
        cat("  \n !!! Doublon saisies \n c est a dire plusieurs lignes pour un inventaire une espece et une distance\n Ceci concerne ", nrow(tDoublon)," lignes\n")
        cat("  Toutes lignes sont présentées dans le fichier: \n  --> ",file,"\n")
        fwrite(tDoublon,file)

        cat(" \n Pour chaque doublon nous conservons la valeur maximum\n")

        tDoublon <- setorder(tDoublon,id_data, -abondance)
        tDoublon[,conservee := !duplicated(id_data)]

        dataExclues <- tDoublon[conservee == FALSE,]
        colonnes <- c("pk_observation","id_raw_observation","id_inventaire","id_point","id_carre","num_point","date","annee","classe","espece","code_sp","distance_contact","abondance","nuage","pluie","vent","visibilite","neige","p_milieu","p_type","p_cat1","p_cat2","p_sous_cat1","p_sous_cat2","s_milieu","s_type","s_cat1","s_cat2","s_sous_cat1","s_sous_cat2","db","date_export")
        dataEclues <-dataExclues[,colonnes,with=FALSE]

        file <- paste0(repOutInfo,"_doublonObservation_Exclu_",dateConstruction,".csv")
        cat("  \n !!! Doublon exclue enregistre dans le fichier \n --> ",file,"\n")
        write.csv(tDoublon[,-ncol(tDoublon)],file,row.names=FALSE)

        dd <- dd[!(pk_observation %in% dataExclues[,pk_observation]),]

        setorder(dd,id_inventaire)
    }

  ## suppression des valeur d habitat abberante
    milieuPossible <- LETTERS[1:7]
    dd[,`:=`(p_milieu = toupper(p_milieu),s_milieu = toupper(s_milieu))]
    dd[!(p_milieu %in% milieuPossible),p_milieu := NA ]
    dd[!(s_milieu %in% milieuPossible), s_milieu := NA ]

    dd[,`:=`(p_habitat = paste0(p_milieu,p_type),s_habitat = paste0(s_milieu,s_type))]
    dd[,p_habitat := gsub("NA","",p_habitat)][p_habitat == "", p_habitat := NA]
    dd[,s_habitat := gsub("NA","",s_habitat)][s_habitat == "", s_habitat := NA]




 the_col <- c("pk_observation","id_raw_observation","id_inventaire","id_point","id_carre","num_point","date","annee","classe","id_data","espece","code_sp","distance_contact","abondance","nuage","pluie","vent","visibilite","neige","p_habitat","p_milieu","p_type","p_cat1","p_cat2","p_sous_cat1","p_sous_cat2","s_habitat","s_milieu","s_type","s_cat1","s_cat2","s_sous_cat1","s_sous_cat2","db","date_export")

    dd<-dd[,the_col,with=FALSE]

    filename <- paste0(repOutData,"observation_",dateConstruction,".csv")
    write.csv(dd,filename,row.names=FALSE,na="")
    cat("  (->",filename,")\n")

    if(output) return(dd)

}



obs2seuil <- function(d.obs,d.inv,dateConstruction="",repOutInfo="", repOutData="",output=FALSE){
    dseuil <- d.obs[,.(id_inventaire,id_carre,id_point,code_sp,distance_contact,abondance)]
    dpass <- d.inv[,.(pk_inventaire,passage_stoc)]


    setkey(dseuil,"id_inventaire")
    setkey(dpass,"pk_inventaire")

    dseuil <- dseuil[dpass]
    dseuil <- dseuil[passage_stoc %in% c(1,2),]

    dseuil_all <- dseuil[,.(seuil_all_tukey_outlier  = quantile(abondance, .75) + 1.5 * (quantile(abondance,0.75) - quantile(abondance,.25)),seuil_all_tukey_farout  = quantile(abondance, .75) + 30 * (quantile(abondance,0.75) - quantile(abondance,.25))),by = code_sp]
    dseuil_inf <- dseuil[distance_contact %in% c("25-100m","100-200m","sup_200m","inf_25m","sup_100m","25-50m","50-100m")  ,.(seuil_inf_tukey_outlier  = quantile(abondance, .75) + 1.5 * (quantile(abondance,0.75) - quantile(abondance,.25)),seuil_inf_tukey_farout  = quantile(abondance, .75) + 30 * (quantile(abondance,0.75) - quantile(abondance,.25))),by = code_sp]
    dseuil_200 <- dseuil[distance_contact %in% c("25-100m","100-200m","inf_25m","25-50m","50-100m") ,.(seuil_200_tukey_outlier  = quantile(abondance, .75) + 1.5 * (quantile(abondance,0.75) - quantile(abondance,.25)),seuil_200_tukey_farout  = quantile(abondance, .75) + 30 * (quantile(abondance,0.75) - quantile(abondance,.25))),by = code_sp]
    dseuil_100 <- dseuil[distance_contact %in% c("25-100m","inf_25m","25-50m","50-100m"),.(seuil_100_tukey_outlier  = quantile(abondance, .75) + 1.5 * (quantile(abondance,0.75) - quantile(abondance,.25)),seuil_100_tukey_farout  = quantile(abondance, .75) + 30 * (quantile(abondance,0.75) - quantile(abondance,.25))),by = code_sp]

    setkey(dseuil_all,"code_sp")
    setkey(dseuil_inf,"code_sp")
    setkey(dseuil_200,"code_sp")
    setkey(dseuil_100,"code_sp")
    dd <- dseuil_all[dseuil_inf[dseuil_200[dseuil_100]]]

    filename <- paste0(repOutData,"seuil_",dateConstruction,".csv")
    write.csv(dd,filename,row.names=FALSE,na="")
    cat("  (->",filename,")\n")

    if(output) return(dd)

}

raw2habitat <- function(d,dateConstruction="",repOutInfo="", repOutData="",output=FALSE) {


## d <- copy(d.all)

##    d[,`:=`(p_habitat = NA,s_habitat = NA)]

       ## suppression des valeur d habitat abberante
    milieuPossible <- LETTERS[1:7]
    d[,`:=`(p_milieu = toupper(p_milieu),s_milieu = toupper(s_milieu))]
    d[!(p_milieu %in% milieuPossible),p_milieu := NA ]
    d[!(s_milieu %in% milieuPossible), s_milieu := NA ]


    d[,`:=`(p_habitat = paste0(p_milieu,sprintf("%02d",p_type)),s_habitat = paste0(s_milieu,sprintf("%02d",s_type)))]
    d[,p_habitat := gsub("NA","",p_habitat)][p_habitat == "", p_habitat := "_"]
    d[,s_habitat := gsub("NA","",s_habitat)][s_habitat == "", s_habitat := "_"]

    d[,habitat := paste0(p_habitat,"-",s_habitat)]
    d[habitat == "_-_",habitat := "_"]


    dd <- unique(d[keep == TRUE,.(id_inventaire,id_point ,date , annee, db ,date_export)])

    dd_hab <- d[keep == TRUE,.(habitat = get_mode(habitat),
                               habitat_collapse = paste(unique(habitat),collapse = "|"),
                               p_habitat = get_mode(p_habitat),
                               p_habitat_collapse = paste(unique(p_habitat),collapse = "|"),
                               p_milieu = get_mode(p_milieu),p_type = get_mode(p_type),
                               p_cat1 = get_mode(p_cat1),p_cat2 = get_mode(p_cat2),
                               p_sous_cat1 = get_mode(p_sous_cat1),p_sous_cat2 = get_mode(p_sous_cat2),
                               s_habitat = get_mode(s_habitat),
                               s_habitat_collapse = paste(unique(s_habitat),collapse = "|"),
                               s_milieu = get_mode(s_milieu),s_type = get_mode(s_type),
                               s_cat1 = get_mode(s_cat1),s_cat2 = get_mode(s_cat2),
                               s_sous_cat1 = get_mode(s_sous_cat1),s_sous_cat2 = get_mode(s_sous_cat2)
                               ),by = id_inventaire]

    dd_nb <- unique(d[keep == TRUE,.(id_inventaire,p_habitat ,
                               p_milieu ,p_type ,
                               p_cat1 ,p_cat2  ,
                               p_sous_cat1 ,p_sous_cat2,
                               s_habitat ,
                               s_milieu ,s_type ,
                               s_cat1 ,s_cat2 ,
                               s_sous_cat1 ,s_sous_cat2 )])[,.(nb_description = .N),by=id_inventaire]
    setkey(dd,id_inventaire)
    setkey(dd_hab,id_inventaire)
    setkey(dd_nb,id_inventaire)

    dd <- dd[dd_hab[dd_nb]]


    setnames(dd, "id_inventaire","pk_habitat")




 ##   aa <- table(dd[,pk_habitat])
 ##
 ##   aa_unique <- names(aa)[aa==1]
 ##   aa_doublon <- names(aa)[aa>1]


    ## construction de la suggestion de correction en fonction des habitats saisis pour les inventaire precedent et suivant


    dd[,habitat_point_id := paste0(id_point,"_",habitat)]
    dd[,inc_point := 1:.N, by = id_point]

    setorder(dd, id_point,inc_point,habitat)

    ddprev <- dd[,.(habitat_point_id,id_point,inc_point)]
    setnames(ddprev,"habitat_point_id","habitat_point_id_prev")
    ddprev[,inc_point := inc_point + 1]
    dd <- merge(dd, ddprev,by = c("id_point","inc_point"),all.x= TRUE)

    dd[,habitat_point_inc := as.numeric(habitat_point_id_prev != habitat_point_id | is.na(habitat_point_id_prev)) ]
    dd[,inc_habitat_point := cumsum(habitat_point_inc),by = id_point]
    dd[,habitat_point_inc_id := paste0(id_point,"_",inc_habitat_point)]


    setorder(dd,id_point,inc_point,inc_habitat_point)

    ## construction de suggestion pour les p_habitat NA en fonction des saisie precedentes et suivante

    ## creation d'un indentifiant de serie temporel d'habitat constant

    ## table des saisies pr�cedentes
    dd_habitat_inc_pre <-  dd[,.(habitat = get_mode(habitat),last_date=max(date)),by = .(habitat_point_inc_id,id_point,inc_habitat_point)]

    vcol <- colnames(dd_habitat_inc_pre)
    setnames(dd_habitat_inc_pre,vcol,paste0(vcol,"_pre"))
    dd_habitat_inc_pre[,habitat_point_inc_id :=  paste0(id_point_pre,"_",inc_habitat_point_pre + 1)]

    dd <- merge(dd,dd_habitat_inc_pre,by="habitat_point_inc_id",all.x=TRUE)

    ## table des saisies suivante
    dd_habitat_inc_post <-  dd[,.(habitat = get_mode(habitat),first_date = min(date)),by = .(habitat_point_inc_id,id_point,inc_habitat_point)]

    vcol <- colnames(dd_habitat_inc_post)
    setnames(dd_habitat_inc_post,vcol,paste0(vcol,"_post"))

    dd_habitat_inc_post[,habitat_point_inc_id :=  paste0(id_point_post,"_",inc_habitat_point_post - 1)]

    dd <- merge(dd,dd_habitat_inc_post,by="habitat_point_inc_id",all.x=TRUE)

    dd[,time_declaration_sug_j_pre := as.numeric(difftime(as.Date(date),as.Date(last_date_pre),units="days"))]
    dd[is.na(time_declaration_sug_j_pre),time_declaration_sug_j_pre := 99999]
    dd[,time_declaration_sug_j_post := as.numeric(difftime(as.Date(date),as.Date(first_date_post),units="days"))]
    dd[is.na(time_declaration_sug_j_post),time_declaration_sug_j_post := -99999]
    dd[,closest_pre := time_declaration_sug_j_pre <= abs(time_declaration_sug_j_post)]


    ## suggestion
    dd[,habitat_sug := ifelse(habitat_pre == habitat_post,habitat_pre,NA)]
    dd[is.na(habitat_sug) & habitat != "_" , habitat_sug := habitat]
    dd[,habitat_consistent := (habitat == habitat_sug) | (habitat== "_" & !is.na(habitat_sug)) ]

###############

    dd[,time_declaration_sug_j := 99999]
    dd[habitat_consistent==TRUE & habitat != "_",time_declaration_sug_j := 0]
    dd[habitat_consistent==TRUE & habitat == "_" & closest_pre ==  TRUE ,time_declaration_sug_j :=  time_declaration_sug_j_pre]
    dd[habitat_consistent==TRUE & habitat == "_" & closest_pre == FALSE ,time_declaration_sug_j :=  time_declaration_sug_j_post]



    ## suggestion si p_habitat_sug est NA


    dd[is.na(habitat_sug) & habitat_pre != "_" ,`:=`(time_declaration_sug_j = time_declaration_sug_j_pre, habitat_sug = habitat_pre,habitat_consistent = FALSE )]

    ## suggestion si p_habitat_sug est encore NA

    dd[is.na(habitat_sug) & habitat_post != "_" ,`:=`(time_declaration_sug_j = time_declaration_sug_j_post, habitat_sug = habitat_post,habitat_consistent = FALSE) ]

    dd[habitat == "_", habitat := NA]
    dd[abs(time_declaration_sug_j) == 99999,time_declaration_sug_j == NA]

    dd[,`:=`(p_habitat_sug = substr(habitat_sug,1,3),s_habitat_sug = substr(habitat_sug,5,7))]
    dd[p_habitat_sug %in% c("__","_"), p_habitat_sug := NA]
    dd[s_habitat_sug %in% c("__","_"), s_habitat_sug := NA]
    dd[p_habitat %in% c("__","_"), p_habitat := NA]
    dd[s_habitat %in%c("__","_"), s_habitat := NA]

    dd[,`:=`(p_milieu_sug = substr(p_habitat_sug,1,1),s_milieu_sug = substr(s_habitat_sug,1,1))]

    dd <- dd[,.(pk_habitat, id_point, date,annee,nb_description,habitat_sug,habitat,habitat_collapse, habitat_consistent,time_declaration_sug_j,p_habitat_sug,p_habitat,p_habitat_collapse,p_milieu_sug,p_milieu,p_type,p_cat1, p_cat2, p_sous_cat1,p_sous_cat2,s_habitat_sug,s_habitat,s_habitat_collapse,s_milieu_sug,s_milieu, s_type, s_cat1, s_cat2 ,s_sous_cat1,s_sous_cat2, db ,date_export)]

    filename <- paste0(repOutData,"habitat_",dateConstruction,".csv")
    write.csv(dd,filename,row.names=FALSE,na="")
    cat("  (->",filename,")\n")

    if(output) return(dd)

}




hab2point_annee <- function(d.hab,d.inv,dateConstruction="",repOutInfo="", repOutData="",output=FALSE) {

    setkey(d.hab,"pk_habitat")
    setkey(d.inv,"pk_inventaire")

    dd.hab <- d.hab[d.inv][passage_stoc %in% c(1,2),.(annee,id_point,id_carre,habitat_collapse,p_habitat_collapse,s_habitat_collapse,nombre_passage_stoc_annee,info_passage_an, nb_description, time_declaration_sug_j)]

    dd.hab <- dd.hab[,.(
        id_carre = get_mode(id_carre),
        habitat_collapse = paste(unique(habitat_collapse),collapse= "|"),
        p_habitat_collapse = paste(unique(p_habitat_collapse),collapse= "|"),
        s_habitat_collapse = paste(unique(s_habitat_collapse),collapse= "|"),
        nombre_passage_stoc_annee= get_mode(nombre_passage_stoc_annee),
        info_passage_an = get_mode(info_passage_an),
        nb_description = sum(nb_description),
        time_declaration_sug_j = mean(time_declaration_sug_j)),by = .(id_point,annee)]
dd.hab[,pk_point_annee := paste0(annee,id_point)]

    dd.hab1 <- d.hab[d.inv][passage_stoc  == 1,.(annee,id_point, info_heure_debut,nb_description, habitat_collapse,habitat_sug, habitat_consistent,p_habitat_sug, p_habitat,  p_milieu_sug, s_habitat_sug, s_habitat, s_milieu,s_milieu_sug)]
    colonnes <- colnames(dd.hab1)[3:ncol(dd.hab1)]
setnames(dd.hab1,colonnes, paste0(colonnes,"_pass1"))
    dd.hab2 <- d.hab[d.inv][passage_stoc  == 2,.(annee,id_point, info_heure_debut,nb_description, habitat_collapse,habitat_sug, habitat_consistent,p_habitat_sug, p_habitat,  p_milieu_sug, s_habitat_sug, s_habitat, s_milieu,s_milieu_sug)]
    colonnes <- colnames(dd.hab2)[3:ncol(dd.hab2)]
setnames(dd.hab2,colonnes, paste0(colonnes,"_pass2"))


    dd.hab <- merge(dd.hab, merge(dd.hab1,dd.hab2, by = c("id_point","annee"),all=TRUE), by = c("id_point","annee"),all = TRUE)

    dd.hab[,`:=`(qualite_inventaire_stoc_pass1 = ifelse(is.na(info_heure_debut_pass1),0,ifelse(info_heure_debut_pass1 == "OK", 1,0.75)),
                 qualite_inventaire_stoc_pass2 = ifelse(is.na(info_heure_debut_pass2),0,ifelse(info_heure_debut_pass2 == "OK", 1,0.75)))]

    dd.hab[,`:=`(qualite_inventaire_stoc = (qualite_inventaire_stoc_pass1 + qualite_inventaire_stoc_pass2 )/ 2,
                 qualite_inventaire_stoc_pass1 = NULL,
                 qualite_inventaire_stoc_pass2 = NULL )]

    dd.hab[,`:=`(
        foret_p = p_milieu_sug_pass1 %in% c("A","B") | p_milieu_sug_pass2 %in% c("A","B"),
        foret_ps = p_milieu_sug_pass1 %in% c("A","B") | p_milieu_sug_pass2 %in% c("A","B") | s_milieu_sug_pass1 %in% c("A","B") | s_milieu_sug_pass2 %in% c("A","B"),
          ouvert_p = p_milieu_sug_pass1 %in% c("C","G") | p_milieu_sug_pass2 %in% c("C","G"),
        ouvert_ps = p_milieu_sug_pass1 %in% c("C","G") | p_milieu_sug_pass2 %in% c("C","G") | s_milieu_sug_pass1 %in% c("C","G") | s_milieu_sug_pass2 %in% c("C","G"),
          agri_p = p_milieu_sug_pass1 %in% c("D") | p_milieu_sug_pass2 %in% c("D"),
        agri_ps = p_milieu_sug_pass1 %in% c("D") | p_milieu_sug_pass2 %in% c("D") | s_milieu_sug_pass1 %in% c("D") | s_milieu_sug_pass2 %in% c("D"),

        urbain_p = p_milieu_sug_pass1 %in% c("E") | p_milieu_sug_pass2 %in% c("E"),
        urbain_ps = p_milieu_sug_pass1 %in% c("E") | p_milieu_sug_pass2 %in% c("E") | s_milieu_sug_pass1 %in% c("E") | s_milieu_sug_pass2 %in% c("E"))]




    the_col <- c("pk_point_annee","id_point","id_carre","annee",  "qualite_inventaire_stoc","nombre_passage_stoc_annee","info_passage_an",
                 "habitat_collapse","p_habitat_collapse","s_habitat_collapse","time_declaration_sug_j",
                 "foret_p","foret_ps","ouvert_p","ouvert_ps","agri_p","agri_ps","urbain_p","urbain_ps",
                 "info_heure_debut_pass1","nb_description_pass1","habitat_sug_pass1","habitat_collapse_pass1","habitat_consistent_pass1","p_habitat_sug_pass1","p_habitat_pass1","p_milieu_sug_pass1","s_habitat_sug_pass1","s_habitat_pass1","s_milieu_sug_pass1",
                 "info_heure_debut_pass2","nb_description_pass2","habitat_sug_pass2","habitat_collapse_pass2","habitat_consistent_pass2","p_habitat_sug_pass2","p_habitat_pass2","p_milieu_sug_pass2","s_habitat_sug_pass2","s_habitat_pass2","s_milieu_sug_pass2")



    dd.hab <-dd.hab[,the_col,with=FALSE]

    filename <- paste0(repOutData,"point_annee_",dateConstruction,".csv")
    write.csv(dd.hab,filename,row.names=FALSE,na="")
    cat("  (->",filename,")\n")

    if(output) return(dd.hab)

}






point_annee2carre_annee <- function(d.point_annee,dateConstruction="",repOutInfo="", repOutData="",output=FALSE) {

    dd.hab <- d.point_annee[,.(
        qualite_inventaire_stoc  = sum(qualite_inventaire_stoc) / 10,
        nombre_passage_stoc_annee = get_mode(nombre_passage_stoc_annee),
        info_passage_an = get_mode(info_passage_an),
        nbp_foret_p = sum(as.numeric(foret_p)),
        nbp_foret_ps = sum(as.numeric(foret_ps)),
        nbp_ouvert_p = sum(as.numeric(ouvert_p)),
        nbp_ouvert_ps = sum(as.numeric(ouvert_ps)),
        nbp_agri_p = sum(as.numeric(agri_p)),
        nbp_agri_ps = sum(as.numeric(agri_ps)),
        nbp_urbain_p = sum(as.numeric(urbain_p)),
        nbp_urbain_ps = sum(as.numeric(urbain_ps)) ),by=.(id_carre,annee)]

    dd.hab[,pk_carre_annee := paste0(annee,id_carre)]

    dd.hab[,`:=`(
        foret_p = nbp_foret_p >= 5,
        foret_ps = nbp_foret_ps >= 5,
        ouvert_p = nbp_ouvert_p >= 5,
        ouvert_ps = nbp_ouvert_ps >= 5,
        agri_p = nbp_agri_p >= 5,
        agri_ps = nbp_agri_ps >= 5,
        urbain_p = nbp_urbain_p >= 5,
        urbain_ps = nbp_urbain_ps >= 5)]




    the_col <- c("pk_carre_annee","id_carre","annee","qualite_inventaire_stoc","nombre_passage_stoc_annee","info_passage_an","nbp_foret_p","nbp_foret_ps","nbp_ouvert_p","nbp_ouvert_ps","nbp_agri_p","nbp_agri_ps","nbp_urbain_p","nbp_urbain_ps","foret_p","foret_ps","ouvert_p","ouvert_ps","agri_p","agri_ps","urbain_p","urbain_ps")


    dd.hab <-dd.hab[,the_col,with=FALSE]

    filename <- paste0(repOutData,"carre_annee_",dateConstruction,".csv")
    write.csv(dd.hab,filename,row.names=FALSE,na="")
    cat("  (->",filename,")\n")

    if(output) return(dd.hab)

}
