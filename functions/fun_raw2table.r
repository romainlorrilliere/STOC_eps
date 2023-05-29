union_raw <- function(dFaune, d_VP, d_FNat) {

    dFaune[,keep := TRUE]
    dVP[,keep := !(id_inventaire %in% dFaune[,id_inventaire])]
    dFNat[,keep := !(id_inventaire %in% c(dFaune[,id_inventaire],dVP[,id_inventaire]))]

    lesColonnes <- intersect(intersect(colnames(dFaune),colnames(dVP)),colnames(dFNat))

    d <- rbind(rbind(dFaune[,lesColonnes,with=FALSE],dVP[,lesColonnes,with=FALSE]),dFNat[,lesColonnes,with=FALSE])

    setorder(d,date,id_carre)


return(d)

}







raw2point <- function(d,dateConstruction,repOutInfo="", repOutData="",output=FALSE) {

    ## d=d.all;dateConstruction=dateConstruction;repOuData=repOutData;output=TRUE

    dagg <- d[keep == TRUE, .(commune = get_mode(commune),
                  site = get_mode(site),
                  insee = get_mode(insee),
                  departement = get_mode(departement),
                  altitude = median(altitude),
                  latitude_wgs84 = median(latitude_wgs84),
                  longitude_wgs84 = median(longitude_wgs84),
                  latitude_wgs84_sd = sd(latitude_wgs84),
                  longitude_wgs84_sd = sd(longitude_wgs84),
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
    setnames(dd,old=c("id_point"),new=c("pk_point"))

    colorder <- c("pk_point","id_carre","commune","site","insee","departement","num_point","nom_point","altitude","longitude_wgs84","latitude_wgs84","longitude_wgs84_sd","latitude_wgs84_sd","db","date_export")
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
                nom_carre_fnat = NA,
                departement = get_mode(departement),
                altitude_median = median(altitude),
                latitude_median_wgs84 = median(latitude_wgs84),
                longitude_median_wgs84 = median(longitude_wgs84),
                date_export = max(date_export)),
            by=.(id_carre,db)]

    dd[,keep := TRUE]
    dd[db == "FNat",keep := !(id_carre %in% dd[db %in% c("FauneFrance","VigiePlume"),id_carre])]
    dd[db == "VigiePlume",keep := !(id_carre %in% dd[db =="FauneFrance",id_carre])]

    dd <- dd[keep == TRUE,]
   dd[,keep := NULL]


    setnames(dd,old=c("id_carre"),new=c("pk_carre"))

  ##
    setkey(dd,"pk_carre")

    dd <- merge(dd,dcarrenat,by="pk_carre",all.x=TRUE)
   ### dd <- dd[dcarrenat]


    colorder <- c("pk_carre","nom_carre","nom_carre_fnat","commune","site","etude","etude_detail","insee","departement","altitude_median","aire","perimetre","latitude_median_wgs84","longitude_median_wgs84","latitude_grid_wgs84","longitude_grid_wgs84","db","date_export")


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
                           latitude_wgs84 = median(latitude_wgs84),
                           longitude_wgs84 = median(longitude_wgs84),
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
               latitude_wgs84 = median(latitude_wgs84),
               longitude_wgs84 = median(longitude_wgs84),
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


raw2habitat <- function(d,dateConstruction="",repOutInfo="", repOutData="",output=FALSE) {

 ##d <- copy(d.all)

    d[,`:=`(p_habitat = NA,s_habitat = NA)]

    dd <- unique(d[keep == TRUE,.(id_inventaire,id_point ,date , annee, db ,date_export)])

    dd_hab <- d[keep == TRUE,.(nb_description = .N,
                               p_habitat = get_mode(p_habitat),
                               p_milieu = get_mode(p_milieu),p_type = get_mode(p_type),
                               p_cat1 = get_mode(p_cat1),p_cat2 = get_mode(p_cat2),
                               p_sous_cat1 = get_mode(p_sous_cat1),p_sous_cat2 = get_mode(p_sous_cat2),
                               s_habitat = get_mode(s_habitat),
                               s_milieu = get_mode(s_milieu),s_type = get_mode(s_type),
                               s_cat1 = get_mode(s_cat1),s_cat2 = get_mode(s_cat2),
                               s_sous_cat1 = get_mode(s_sous_cat1),s_sous_cat2 = get_mode(s_sous_cat2)
                               ),by = id_inventaire]

dd <- merge(dd,dd_hab,by = "id_inventaire")

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

    ## inventaire suivant
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
    dd_habitat_inc_pre <-  dd[,.(id_point = get_mode(id_point),p_habitat_sug = get_mode(p_habitat_sug), s_habitat_sug = get_mode(s_habitat_sug),last_date=max(date)),by = habitat_point_inc_id]

    setnames(dd_habitat_inc_pre,c("p_habitat_sug","s_habitat_sug","last_date"),paste0(c("p_habitat_sug","s_habitat_sug","last_date"),"_pre"))
    dd_habitat_inc_pre[, habitat_point_inc_id_pre :=  habitat_point_inc_id ]
    dd_habitat_inc_pre[, habitat_point_inc_id :=  habitat_point_inc_id +1]


    dd <- merge(dd,dd_habitat_inc_pre,by=c("habitat_point_inc_id","id_point"),all.x=TRUE)

    ## table des saisies suivante
    dd_habitat_inc_post <-  dd[,.(id_point = get_mode(id_point),p_habitat_sug = get_mode(p_habitat_sug), s_habitat_sug = get_mode(s_habitat_sug),first_date = min(date)),by = habitat_point_inc_id]

    setnames(dd_habitat_inc_post,c("p_habitat_sug","s_habitat_sug","first_date"),paste0(c("p_habitat_sug","s_habitat_sug","first_date"),"_post"))
    dd_habitat_inc_post[, habitat_point_inc_id_post :=  habitat_point_inc_id ]
    dd_habitat_inc_post[, habitat_point_inc_id :=  habitat_point_inc_id -1]

####ERROR !

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

    dd <- dd[,.(pk_habitat, id_point, date,annee,p_habitat_sug,p_habitat,p_habitat_consistent,p_milieu,p_type,p_cat1, p_cat2, p_sous_cat1,p_sous_cat2,s_habitat_sug,s_habitat,s_habitat_consistent, s_milieu, s_type, s_cat1, s_cat2 ,s_sous_cat1,s_sous_cat2,time_last_declaration_sug_j, db ,date_export)]

    filename <- paste0(repOutData,"habitat_",dateConstruction,".csv")
    write.csv(dd,filename,row.names=FALSE,na="")
    cat("  (->",filename,")\n")

    if(output) return(dd)

}


