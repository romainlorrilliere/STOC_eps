

    require(data.table)
    require(sf)
    require(stringi)
    require(stringr)


#####################################################
##  Les fonctions d'importation
#####################################################
  # exp reg pour recuperer des code alpha numerique dans les nom des sites
    get_id_alphanum <- function(X,exp ="([A-Zaz]*[0-9]+[A-Za-z]*)+" ){
        require(stringr)
        paste(unlist(str_extract_all(X,exp)),collapse="")
    }




faune_importation <- function(nomFileFaune,dateExportFaune,repImport="data_faune/",repOutInfo, repOutData) {
    require(data.table)
    require(stringi)
    require(stringr)
   ##repImport="data_faune/"
    d_nom <- fread("library/colnames_faune.csv",encoding = "Latin-1")

    d <- NULL

    if(length(dateExportFaune) ==1) dateExportFaune <- rep(dateExportFaune,length(nomFileFaune))

    nfile <- length(nomFileFaune)
    cat("\n",nfile,"fichier(s) Faune-France à importer\n")

    for(i in 1:length(nomFileFaune)) {
        nomFileFaunei <- paste0(repImport,nomFileFaune[i])
        cat(" <-- (",i,"/",nfile,") Fichier VigiePlume: ",nomFileFaunei," ",sep="")
        flush.console()
        di <- fread(nomFileFaunei)
        cat("  DONE ! \n")
        flush.console()
        cat(" vérification colonnes\n")

        col_abs <- setdiff(d_nom[,old],colnames(di))
        n_col_abs <- length(col_abs)
        if(n_col_abs > 0) {
            cat("  !! ATTENTION !! ",n_col_abs," absente(s):\n")
            the_cols_txt <- paste(col_abs,collapse=", ")
            cat(the_cols_txt,"\n")
            txt <- "\n Choix: \n1- ne pas importer le fichier \n2- importer le fichier et ajoute NA dans les colonnes manquantes \n3- Arreter le processus\n"
            cat(txt,"\n")
            rep <- readline(prompt = txt)


        } else {

            setnames(di,old=d_nom[,old],new=d_nom[,new])
            cat("  DONE ! \n")

            di[,date_export:= dateExportFaune[i]]
            d <- rbind(d,di)
        }
    }
    ## renome les colonnes

    d[,insee := sprintf("%05d", as.numeric(as.character(insee)))]
    d[,id_carre := as.character(id_carre)]
    d[!is.na(id_carre),id_carre :=  sprintf("%06d", as.numeric(id_carre))]
    d[,departement := ifelse(is.na(id_carre),substr(insee,1,2),substr(id_carre,1,2))]
 d[,etude_detail := etude]

     d[(is.na(num_point)| is.na(id_carre)) & etude %in% c("STOC_EPS","SHOC"), etude := NA]




    d_abbrev <- fread("library/abbrev_carre.csv")
    d_abbrev_l <- d_abbrev[lower == TRUE ,]
    d_abbrev_u <- d_abbrev[lower == FALSE ,]

  d[is.na(id_carre),id_carre_temp := site]

    pat <- c(d_abbrev_l[,txt])
    repl <-  c(d_abbrev_l[,abbrev])
    d[is.na(id_carre),id_carre_temp:= mgsub(pat,repl,id_carre_temp)]
    d[is.na(id_carre),id_carre_temp := gsub(".*Maille ","",id_carre_temp)]
    d[is.na(id_carre),id_carre_temp := gsub(".*: ","",id_carre_temp)]

    d[is.na(id_carre), id_carre_temp := toupper(id_carre_temp)]

    vec <- c("LE ","LA ","LES ","DE ","A ","AU ","L'","DU ","DES ","D'","& ","ET ")
    d[is.na(id_carre),id_carre_temp := mgsub(vec,"",id_carre_temp)]


    pat <- c(d_abbrev_u[,txt])
    repl <-  c(d_abbrev_u[,abbrev])
    d[is.na(id_carre),id_carre_temp := mgsub(pat,repl,id_carre_temp)]
    vec <- c("(",")","_","-",".",",","/")
    d[is.na(id_carre),id_carre_temp := mgsub(vec," ",id_carre_temp)]

     d[is.na(id_carre), id_carre_temp := str_to_title(id_carre_temp)]

    ## fabrication des id STOC_SITE



    exp ="([A-Zaz]*[0-9]+[A-Za-z]*)+"

  d[is.na(id_carre),`:=`(flag = TRUE,n_char =  nchar(gsub(' ','',id_carre_temp)),id_carre_temp_alphanum = unlist(lapply(id_carre_temp, get_id_alphanum,exp)),id_carre_temp_txt = trimws(gsub(exp,"",id_carre_temp)))]
    d[is.na(id_carre), `:=`(n_char_alphanum = nchar(id_carre_temp_alphanum),n_char_txt = nchar(gsub(" ","",id_carre_temp_txt)), n_word_txt = stri_count_words(id_carre_temp_txt))]
    d[is.na(id_carre), `:=`(n_char_tot =  n_char_alphanum + n_char_txt, nb_letter_word = (8-n_char_alphanum)%/%n_word_txt) ]


    ## cas 1 moins de 9 caracteres informatif on garde tout
    d[flag == TRUE & n_char_tot <= 8 ,`:=`(id_carre_temp = paste0("S",departement,id_carre_temp_alphanum,gsub(' ','',id_carre_temp_txt)),flag = FALSE)]
    ## cas 2 plus de 8 caracteres moins de 2 mots
    d[flag == TRUE & n_word_txt < 2 ,`:=`(id_carre_temp = paste0("S",departement,substr(paste0(id_carre_temp_alphanum,gsub(' ','',id_carre_temp_txt)),1,8)),flag = FALSE)]

    ## cas 2 plus de 8 caracteres et plus de 1 mot

    d[flag == TRUE & nb_letter_word == 4 ,`:=`(id_carre_temp = paste0("S",departement,paste0(id_carre_temp_alphanum,gsub("(\\b([A-z0-9]{1,4}))|.", "\\1", id_carre_temp_txt, perl=TRUE))),flag = FALSE)]
    d[flag == TRUE &nb_letter_word == 2 ,`:=`(id_carre_temp = paste0("S",departement,paste0(id_carre_temp_alphanum,gsub("(\\b([A-z0-9]{1,2}))|.", "\\1", id_carre_temp_txt, perl=TRUE))),flag = FALSE)]
    d[flag == TRUE & nb_letter_word < 2 ,`:=`(id_carre_temp = paste0("S",departement,substr(paste0(id_carre_temp_alphanum,gsub("(\\b([A-z0-9]{1}))|.", "\\1", id_carre_temp_txt, perl=TRUE)),1,8)),flag = FALSE)]

    d[is.na(id_carre), id_carre := id_carre_temp]
## nettoyage de la table de toutes les colonnes ajouter pour la construction de l'id_carre_temp
  d[,`:=`(flag = NULL,n_char =  NULL, id_carre_temp_txt = NULL, id_carre_temp_alphanum = NULL, n_char_txt = NULL, n_char_alphanum = NULL, n_char_tot = NULL, nb_letter_word = NULL, n_word_txt=NULL)]






    d <- d[!is.na(abondance) & !is.na(espece) & espece != "",]


    d[,date := as.Date(date,format="%d.%m.%Y")]
    d[,nom_carre := paste("Carre",id_carre,site)]
    d[,point :=  sprintf("%02d", num_point)]
    d[,id_point := paste0(id_carre,"P",point)]
    d[,jour_julien := as.numeric(format(date,"%j"))]
    d[,annee := as.numeric(format(date,"%Y"))]
    d[,id_inventaire := paste0(format(date,"%Y%m%d"),id_point)]
    d[,heure := substr(heure,1,5)]
    d[,heure_fin := substr(heure_fin,1,5)]
    d[,duree_minute := as.numeric(difftime(as.POSIXct(heure_fin,format="%H:%M"),as.POSIXct(heure,format="%H:%M")))]
    d[,distance_contact := gsub("> ","sup_",distance_contact)]
    d[,distance_contact := gsub("< ","inf_",distance_contact)]
    d[,distance_contact := gsub("En vol","transit",distance_contact)]
    d[,id_data := paste0(id_inventaire,"_",substring(espece,1,6),"_",distance_contact)]
    d <- setorder(d, id_inventaire,espece,distance_contact)
    d[,inventaire_inc := 1:.N,by=id_inventaire]
    d[,inventaire_inc :=sprintf("%02d",inventaire_inc)]
    d[,id_observation := paste0(id_inventaire,inventaire_inc)]

    d[, altitude := as.numeric(altitude)]

    d[,db:="FauneFrance"]
    d[,nom_carre_fnat := NA]
    d[,info_passage := NA]
    d[,passage_stoc := NA]
    d[,passage_observateur := NA]
    d[,email := ""]
    d[,espece := toupper(espece)]
    d[,code_sp := substr(espece,1,6)]
    d[,abondance := as.numeric(abondance)]

    d[, p_milieu := toupper(ifelse(p_milieu == "",NA,p_milieu))]
    d[p_type == "", p_type := NA]
    d[p_cat1 == "", p_cat1 := NA]
    d[p_cat2 == "", p_cat2 := NA]
    d[, s_milieu := toupper(ifelse(s_milieu == "",NA,s_milieu))]
    d[s_type == "", s_type := NA]
    d[s_cat1 == "", s_cat1 := NA]
    d[s_cat2 == "", s_cat2 := NA]

    return(d)
}







vp_importation <- function(nomFileVP,dateExportVP,repImport="data_raw/",repOutInfo, repOutData) {
    require(data.table)

    d_nom <- data.table(old=c("Code.inventaire","Etude","Site","Pays","Département","INSEE","Commune","N..Carré.EPS","Date","Heure","Heure.fin","N..Passage","Observateur","Email","EXPORT_STOC_TEXT_EPS_POINT","Altitude","Classe","Espèce","Nombre","Distance.de.contact","Longitude","Latitude","Type.de.coordonnées","Type.de.coordonnées.lambert","EPS.Nuage","EPS.Pluie","EPS.Vent","EPS.Visibilité","EPS.Neige","EPS.Transport","EPS.P.Milieu","EPS.P.Type","EPS.P.Cat1","EPS.P.Cat2","EPS.P.Sous.Cat1","EPS.P.Sous.Cat2","EPS.S.Milieu","EPS.S.Type","EPS.S.Cat1","EPS.S.Cat2","EPS.S.Sous.Cat1","EPS.S.Sous.Cat2"),new=c("Code.inventaire","etude","site","pays","departement","insee","commune","id_carre","date","heure","heure_fin","passage_observateur","observateur","email","nom_point","altitude","classe","espece","abondance","distance_contact","longitude_wgs84","latitude_wgs84","type_coord","type_coord_lambert","nuage","pluie","vent","visibilite","neige","transport","p_milieu","p_type","p_cat1","p_cat2","p_sous_cat1","p_sous_cat2","s_milieu","s_type","s_cat1","s_cat2","s_sous_cat1","s_sous_cat2"))

    d <- NULL

    if(length(dateExportVP) ==1) dateExportVP <- rep(dateExportVP,length(nomFileVP))

    nfile <- length(nomFileVP)
    cat("\n",nfile,"fichier(s) VigiePlume à importer\n")

    for(i in 1:length(nomFileVP)) {
        nomFileVPi <- paste0(repImport,nomFileVP[i])
        cat(" <-- (",i,"/",nfile,") Fichier VigiePlume: ",nomFileVPi," ",sep="")
        flush.console()
        di <- read.csv(nomFileVPi,h=TRUE,stringsAsFactors=FALSE,fileEncoding="utf-8",sep="\t")
        cat("  DONE ! \n")
        flush.console()
        di <- data.table(di)
        cat(" vérification colonnes\n")

        col_abs <- setdiff(d_nom[,old],colnames(di))
        n_col_abs <- length(col_abs)
        if(n_col_abs > 0) {
            cat("  !! ATTENTION !! ",n_col_abs," absente(s):\n")
            the_cols_txt <- paste(col_abs,collapse=", ")
            cat(the_cols_txt,"\n")
            txt <- "\n Choix: \n1- ne pas importer le fichier \n2- importer le fichier et ajoute NA dans les colonnes manquantes \n3- Arreter le processus\n"
            cat(txt,"\n")
            rep <- readline(prompt = txt)


        } else {

            setnames(di,old=d_nom[,old],new=d_nom[,new])
            cat("  DONE ! \n")

            di[,date_export:= dateExportVP[i]]
            d <- rbind(d,di)
        }
    }
    ## renome les colonnes


    d[,etude_detail := etude]
    d[nom_point == "" & etude %in% c("STOC_EPS","SHOC"), etude := NA]


    d <- d[!is.na(abondance) & !is.na(espece) & espece != "",]


    d[,insee := sprintf("%05d", insee)]
    d[,departement := sprintf("%02d", departement)]
    d[,date := as.Date(date,format="%d.%m.%Y")]
    d[,id_carre := substring(id_carre,9,nchar(id_carre))]
    d[,nom_carre := paste("Carre",id_carre,site)]
    d[,point := gsub("Point N°","",nom_point)]
    d[,id_point := paste0(id_carre,"P",point)]
    d[,num_point := as.numeric(point)]
    d[,jour_julien := as.numeric(format(date,"%j"))]
    d[,annee := as.numeric(format(date,"%Y"))]
    d[,id_inventaire := paste0(format(date,"%Y%m%d"),id_point)]
    d[,duree_minute := as.numeric(difftime(as.POSIXct(heure_fin,format="%H:%M"),as.POSIXct(heure,format="%H:%M")))]
    d[,distance_contact := gsub("LESS25","inf_25m",distance_contact)]
    d[,distance_contact := gsub("LESS100","25-100m",distance_contact)]
    d[,distance_contact := gsub("LESS200","100-200m",distance_contact)]
    d[,distance_contact := gsub("MORE200","sup_200m",distance_contact)]
    d[,distance_contact := gsub("TRANSIT","transit",distance_contact)]
    d[,distance_contact := gsub("U","",distance_contact)]


    d[,id_data := paste0(id_inventaire,"_",substring(espece,1,6),"_",distance_contact)]
    d <- setorder(d, id_inventaire,espece,distance_contact)
    d[,inventaire_inc := 1:.N,by=id_inventaire]
    d[,inventaire_inc :=sprintf("%02d",inventaire_inc)]
    d[,id_observation := paste0(id_inventaire,inventaire_inc)]
    d[, altitude := as.numeric(altitude)]

    d[,db:="VigiePlume"]
    d[,nom_carre_fnat := NA]
    d[,info_passage := NA]
    d[,passage_stoc := NA]
    d[,id_raw_observation := NA]
    d[,id_raw_inventaire:= NA]
    d[,espece := toupper(espece)]
    d[,code_sp := substr(espece,1,6)]
    d[,abondance := as.numeric(abondance)]




    d[, p_milieu := toupper(ifelse(p_milieu == "",NA,p_milieu))]
    d[p_type == "", p_type := NA]
    d[p_cat1 == "", p_cat1 := NA]
    d[p_cat2 == "", p_cat2 := NA]
    d[, s_milieu := toupper(ifelse(s_milieu == "",NA,s_milieu))]
    d[s_type == "", s_type := NA]
    d[s_cat1 == "", s_cat1 := NA]
    d[s_cat2 == "", s_cat2 := NA]


    return(d)
}





FNat_importation <- function(nomFileFNat,dateExportFNat,repImport="data_raw/",repOutInfo, repOutData) {

   ## file=nomDBFNat;fichierAPlat=nomFileFNat;output=TRUE;repImport="data_raw/"

    require(data.table)
    require(sf)
    require(stringi)
    require(stringr)

    d_nom <- data.table(old=c("unique_inventaire","unique_citation","numobs","etude","unique_localite","section_cadastrale","lieudit","pays","dept","insee","dateobs","heure","duree","nbr_echantillon","nom","email","altitude","classe","espece","nbr_ind","classe_dist","longitude_lambert_93","latitude_lambert_93","nuage","pluie","vent","visibilite","hab_p_milieu","hab_p_type_milieu","hab_p_cat_1","hab_p_cat_2","hab_p_sous_cat_1","hab_p_sous_cat_2","hab_s_milieu","hab_s_type_milieu","hab_s_cat_1","hab_s_cat_2","hab_s_sous_cat_1","hab_s_sous_cat_2"),new=c("id_raw_inventaire","id_raw_observation","numobs","etude","unique_localite","nom_point","nom_carre","pays","departement","insee","date","heure","duree_minute","passage_observateur","observateur","email","altitude","classe","espece","abondance","distance_contact","longitude_lambert_93","latitude_lambert_93","nuage","pluie","vent","visibilite","p_milieu","p_type","p_cat1","p_cat2","p_sous_cat1","p_sous_cat2","s_milieu","s_type","s_cat1","s_cat2","s_sous_cat1","s_sous_cat2"))


    d <- NULL

    if(length(dateExportFNat) ==1) dateExportFNat <- rep(dateExportFNat,length(nomFileFNat))

    nfile <- length(nomFileFNat)
    cat("\n",nfile,"fichier(s) FNat à importer\n")

    for(i in 1:length(nomFileFNat)) {
        nomFileFNati <- paste0(repImport,nomFileFNat[i])
        cat(" <-- (",i,"/",nfile,") Fichier FNat: ",nomFileFNati," ",sep="")
        flush.console()
        di <- read.csv(nomFileFNati,h=TRUE,fileEncoding="iso-8859-1",stringsAsFactors=FALSE)
        cat("  DONE ! \n")
        flush.console()
        setDT(di)
        cat(" vérification colonnes\n")

        col_abs <- setdiff(d_nom[,old],colnames(di))
        n_col_abs <- length(col_abs)
        if(n_col_abs > 0) {
            cat("  !! ATTENTION !! ",n_col_abs," absente(s):\n")
            the_cols_txt <- paste(col_abs,collapse=", ")
            cat(the_cols_txt,"\n")
            txt <- "\n Choix: \n1- ne pas importer le fichier \n2- importer le fichier et ajoute NA dans les colonnes manquantes \n3- Arreter le processus\n"
            cat(txt,"\n")
            rep <- readline(prompt = txt)


        } else {
            ## renome les colonnes
            di <- setnames(di,old=d_nom[,old],new=d_nom[,new])
            cat("  DONE ! \n")

            di[,date_export:= dateExportFNat[i]]
            d <- rbind(d,di)
        }
    }




    d <- d[!is.na(abondance) & !is.na(espece) & espece != "",]

    d[,insee := sprintf("%05d", insee)]
    d[,date := as.character(date)]
    d[,date := as.Date(date,format="%Y%m%d")]
    d[,heure := as.POSIXct(sprintf("%04d", heure),format="%H%M")]

    d[,numobs := as.character(numobs)]
    d[,nom_carre := as.character(nom_carre)]
    d[,site := nom_carre]
    d[,point := gsub("Point N°","",nom_point)]
    d[,num_point := as.numeric(point)]

    d[,etude_detail := etude]
    d[etude %in% c("STOC-EPS","STOC-EPS MAMMIFERES"),etude := "STOC_EPS"]
    d[etude =="STOC-SITES ONF",etude := "STOC_ONF"]
    d[grep("SITE",etude), etude := "STOC_SITE"]


     d[is.na(num_point) & etude %in% c("STOC_EPS","SHOC"), etude := NA]


    dCarreONF <- fread("data_generic/siteONF.csv",encoding="UTF-8")
    dCarreONF <- dCarreONF[,c(1,2,5)]
    dCarreONF$nouveau_lieudit <- sprintf("%06d",dCarreONF$nouveau_lieudit)
    colnames(dCarreONF) <- c("nom_carre","departement","id_carre_onf")
    dCarreONF[,departement := sprintf("%02d", departement)]

    d[dCarreONF,on = .(nom_carre,departement),id_carre_onf := id_carre_onf]


    d_abbrev <- fread("library/abbrev_carre.csv")
    d_abbrev_l <- d_abbrev[lower == TRUE ,]
    d_abbrev_u <- d_abbrev[lower == FALSE ,]

    d[etude == "STOC_SITE",id_carre := nom_carre]

    pat <- c(d_abbrev_l[,txt])
    repl <-  c(d_abbrev_l[,abbrev])
    d[etude == "STOC_SITE",id_carre := mgsub(pat,repl,id_carre)]
    d[etude == "STOC_SITE",id_carre := gsub(".*Maille ","",id_carre)]
    d[etude == "STOC_SITE",id_carre := gsub(".*: ","",id_carre)]

    d[etude == "STOC_SITE", id_carre := toupper(id_carre)]

    vec <- c("LE ","LA ","LES ","DE ","A ","AU ","L'","DU ","DES ","D'","& ","ET ")
    d[etude == "STOC_SITE",id_carre := mgsub(vec,"",id_carre)]


    pat <- c(d_abbrev_u[,txt])
    repl <-  c(d_abbrev_u[,abbrev])
    d[etude == "STOC_SITE",id_carre := mgsub(pat,repl,id_carre)]
    vec <- c("(",")","_","-",".",",","/")
    d[etude == "STOC_SITE",id_carre := mgsub(vec," ",id_carre)]

     d[etude == "STOC_SITE", id_carre := str_to_title(id_carre)]

    ## fabrication des id STOC_SITE

    exp ="([A-Zaz]*[0-9]+[A-Za-z]*)+"

    d[etude == "STOC_SITE",`:=`(flag = TRUE,n_char =  nchar(gsub(' ','',id_carre)),id_carre_alphanum = unlist(lapply(id_carre, get_id_alphanum,exp)),id_carre_txt = trimws(gsub(exp,"",id_carre)))]
    d[etude == "STOC_SITE", `:=`(n_char_alphanum = nchar(id_carre_alphanum),n_char_txt = nchar(gsub(" ","",id_carre_txt)), n_word_txt = stri_count_words(id_carre_txt))]
    d[etude == "STOC_SITE", `:=`(n_char_tot =  n_char_alphanum + n_char_txt, nb_letter_word = (8-n_char_alphanum)%/%n_word_txt) ]


    ## cas 1 moins de 9 caracteres informatif on garde tout
    d[flag == TRUE & n_char_tot <= 8 ,`:=`(id_carre = paste0("S",departement,id_carre_alphanum,gsub(' ','',id_carre_txt)),flag = FALSE)]
    ## cas 2 plus de 8 caracteres moins de 2 mots
    d[flag == TRUE & n_word_txt < 2 ,`:=`(id_carre = paste0("S",departement,substr(paste0(id_carre_alphanum,gsub(' ','',id_carre_txt)),1,8)),flag = FALSE)]

    ## cas 2 plus de 8 caracteres et plus de 1 mot

    d[flag == TRUE & nb_letter_word == 4 ,`:=`(id_carre = paste0("S",departement,paste0(id_carre_alphanum,gsub("(\\b([A-z0-9]{1,4}))|.", "\\1", id_carre_txt, perl=TRUE))),flag = FALSE)]
    d[flag == TRUE &nb_letter_word == 2 ,`:=`(id_carre = paste0("S",departement,paste0(id_carre_alphanum,gsub("(\\b([A-z0-9]{1,2}))|.", "\\1", id_carre_txt, perl=TRUE))),flag = FALSE)]
    d[flag == TRUE & nb_letter_word < 2 ,`:=`(id_carre = paste0("S",departement,substr(paste0(id_carre_alphanum,gsub("(\\b([A-z0-9]{1}))|.", "\\1", id_carre_txt, perl=TRUE)),1,8)),flag = FALSE)]

## nettoyage de la table de toutes les colonnes ajouter pour la construction de l'id_carre
  d[,`:=`(flag = NULL,n_char =  NULL, id_carre_txt = NULL, id_carre_alphanum = NULL, n_char_txt = NULL, n_char_alphanum = NULL, n_char_tot = NULL, nb_letter_word = NULL, n_word_txt=NULL)]



    d[etude == "STOC_EPS"|substring(nom_carre,1,9)=="Carré EPS" ,id_carre := paste0(departement,substring(nom_carre,13,nchar(nom_carre)))]

    d[,id_carre := ifelse(is.na(id_carre_onf),id_carre,id_carre_onf)]

    d[id_carre %in% unique(d[etude=="STOC_ONF",id_carre]), etude := "STOC_ONF"]
    d[,id_point := paste0(id_carre,"P",point)]


    d[,jour_julien := as.numeric(format(date,"%j"))]
    d[,annee := as.numeric(format(date,"%Y"))]

    d[,id_inventaire :=  paste0(format(date,"%Y%m%d"),id_point)]
    d[,heure_fin :=  format(heure+as.difftime(paste0("00:",sprintf("%02d", duree_minute)),"%H:%M"),"%H:%M")]
    d[,heure :=  format(heure,"%H:%M")]
    d[,distance_contact := gsub("> ","sup_",distance_contact)]
    d[,distance_contact := gsub("< ","inf_",distance_contact)]
    d[,distance_contact := gsub("En vol","transit",distance_contact)]
    d[,distance_contact := gsub("Non indiquée","",distance_contact)]


    d[,id_data := paste0(id_inventaire,"_",substring(espece,1,6),"_",distance_contact)]
    setorder(d, id_inventaire,espece,distance_contact)
    d <- d[,inventaire_inc := 1:.N,by=id_inventaire]
    d <- d[,inventaire_inc :=sprintf("%02d",inventaire_inc)]
    d <- d[,id_observation := paste0(id_inventaire,inventaire_inc)]
    d[,altitude := as.numeric(altitude)]



    d[,db:="FNat"]
    d[,commune := NA]

    d[,info_passage := NA]
    d[,passage_stoc := NA]
    d[,espece := toupper(espece)]
    d[,code_sp := substr(espece,1,6)]
    d[,abondance := as.numeric(abondance)]

    d[, p_milieu := toupper(ifelse(p_milieu == "",NA,p_milieu))]
    d[p_type == "", p_type := NA]
    d[p_cat1 == "", p_cat1 := NA]
    d[p_cat2 == "", p_cat2 := NA]
    d[, s_milieu := toupper(ifelse(s_milieu == "",NA,s_milieu))]
    d[s_type == "", s_type := NA]
    d[s_cat1 == "", s_cat1 := NA]
    d[s_cat2 == "", s_cat2 := NA]
    d[,neige := NA]


    st_d <- st_as_sf(d,coords = c("longitude_lambert_93","latitude_lambert_93"),crs=2154) %>%
        st_transform(,crs=4326) %>% st_coordinates()
    colnames(st_d) <- c("longitude_wgs84","latitude_wgs84")

    d <- cbind(d,st_d)
    d[latitude_wgs84 < 41 | latitude_wgs84 > 52 ,latitude_wgs84 := NA]
    d[longitude_wgs84 < -5 | longitude_wgs84 > 9 ,longitude_wgs84 := NA]



    return(d)


}










#####################################################
## Requete sur la base access de FNat
#####################################################



import_FNat_accessFile <- function(file="Base FNat2000.MDB",fichierAPlat="FNat_plat_2017-01-04.csv ") {
### Fonction fonctionnant seulement sous windows avec une version 32 bit de R
### ...\bin\i386\Rgui.exe
    library(RODBC)
    library(reshape)
    library(data.table)

    cat("\nConnexion à la base de données ACCESS\n")
    flush.console()
    con <- odbcConnectAccess(file)

    cat("\nRequete principale\nC'est l'heure d'un petit café :-o\n")
    flush.console()
    query <- "SELECT TCCitations.Unique_Inventaire, TCCitations.Unique_Citation,TCInventaires.NumObs, TCInventaires.Etude, TCInventaires.Unique_Localité, TCInventaires.section_cadastrale, TCInventaires.Lieudit, TCInventaires.Pays, TCInventaires.Dept, TCInventaires.INSEE, TCInventaires.DateObs, TCInventaires.Heure, TCInventaires.Durée, TCInventaires.NbrEchantillon, TOObservateurs.Nom, TOObservateurs.EMail, TLCoord.Altitude, TCCitations.Classe, TCCitations.Espèce, TCCitations.NbrInd, TPDistance_Contact.Libellé, TLCoord.Longitude_Lambert_93, TLCoord.Latitude_Lambert_93
FROM TLCoord INNER JOIN (TPDistance_Contact INNER JOIN (TOObservateurs INNER JOIN (TCInventaires INNER JOIN TCCitations ON TCInventaires.Unique_Inventaire = TCCitations.Unique_Inventaire) ON (TOObservateurs.Observateur = TCInventaires.Observateur) AND (TOObservateurs.Organisme = TCInventaires.Organisme)) ON TPDistance_Contact.Code = TCCitations.Distance_Contact) ON TLCoord.Unique_Localité = TCInventaires.Unique_Localité;"
    d1 <- sqlQuery(con,query)

    cat("\nRequete météo\n")
    flush.console()
    query <- "SELECT TCMétéo.Unique_Inventaire, TCMétéo.Code, TCMétéo.Valeur_Discrète
FROM TCMétéo;"

    d_meteo <- sqlQuery(con,query)
    d_meteo <- cast(d_meteo,Unique_Inventaire~Code)[,1:5]

    cat("\nRequetes habitats\n il va encore falloir être un peu patient :-/")
    flush.console()
    query <- "SELECT TLDescEPS.Unique_Localité, TLDescEPS.DateEPS, TLDescEPS.Milieu, TLDescEPS.Type_Milieu, TLDescEPS.Catégorie_1, TLDescEPS.Catégorie_2, TLDescEPS.Sous_Catégorie_1, TLDescEPS.Sous_Catégorie_2
FROM TLDescEPS
WHERE (((TLDescEPS.Habitat)='P'));"
    d_habP <- sqlQuery(con,query)
    colnames(d_habP)[3:ncol(d_habP)]<- paste0("hab_p_",colnames(d_habP)[3:ncol(d_habP)])



    query <- "SELECT TLDescEPS.Unique_Localité, TLDescEPS.DateEPS, TLDescEPS.Milieu, TLDescEPS.Type_Milieu, TLDescEPS.Catégorie_1, TLDescEPS.Catégorie_2, TLDescEPS.Sous_Catégorie_1, TLDescEPS.Sous_Catégorie_2
FROM TLDescEPS
WHERE (((TLDescEPS.Habitat)='S'));"
    d_habS <- sqlQuery(con,query)
    colnames(d_habS)[3:ncol(d_habS)]<- paste0("hab_s_",colnames(d_habS)[3:ncol(d_habS)])

    d_hab <- merge(d_habP,d_habS,by=c("Unique_Localité","DateEPS"),all=TRUE)
    d_hab <- d_hab[order(d_hab$Unique_Localité,d_hab$DateEPS),]



    d2 <- subset(d1,!is.na(NbrInd)& NbrEchantillon < 7)
    d3 <- d2[grep("Point",d2$section_cadastrale),]
    rm(list=c("d1","d2"))




    t3 <- data.table(d3,id="Unique_Citation")
    t_meteo <- data.table(d_meteo, id="Unique_Inventaire")
    t_hab <- data.table(d_hab)
    t_hab$id_hab <- paste(t_hab$Unique_Localité,t_hab$DateEPS,sep="_")
    t3$id_hab <- paste(t3$Unique_Localité,t3$DateObs,sep="_")
    tt <- merge(t3, t_meteo,by="Unique_Inventaire",all.x = TRUE, all.y=FALSE, allow.cartesian=TRUE)

    tt <- merge(tt, t_hab, ,by="id_hab",all.x = TRUE, all.y=FALSE, allow.cartesian=TRUE)


    tt <- subset(tt, select=c("Unique_Inventaire","Unique_Citation","NumObs","Etude","Unique_Localité.x","section_cadastrale","Lieudit","Pays","Dept","INSEE","DateObs","Heure","Durée","NbrEchantillon","Nom","EMail","Altitude","Classe","Espèce","NbrInd","Libellé","Longitude_Lambert_93","Latitude_Lambert_93","EPS-Nuage","EPS-Pluie","EPS-Vent","EPS-Visibilité","hab_p_Milieu","hab_p_Type_Milieu","hab_p_Catégorie_1","hab_p_Catégorie_2","hab_p_Sous_Catégorie_1","hab_p_Sous_Catégorie_2","hab_s_Milieu","hab_s_Type_Milieu","hab_s_Catégorie_1","hab_s_Catégorie_2","hab_s_Sous_Catégorie_1","hab_s_Sous_Catégorie_2"))

    colnames(tt) <- c("unique_inventaire","unique_citation","numobs","etude","unique_localite","section_cadastrale","lieudit","pays","dept","insee","dateobs","heure","duree","nbr_echantillon","nom","email","altitude","classe","espece","nbr_ind","classe_dist","longitude_lambert_93","latitude_lambert_93","nuage","pluie","vent","visibilite","hab_p_milieu","hab_p_type_milieu","hab_p_cat_1","hab_p_cat_2","hab_p_sous_cat_1","hab_p_sous_cat_2","hab_s_milieu","hab_s_type_milieu","hab_s_cat_1","hab_s_cat_2","hab_s_sous_cat_1","hab_s_sous_cat_2"
                      )


    cat("\nEcriture du fichier:", fichierAPlat,"\n Tu peux aller boire un second café\n")
    write.csv(tt,fichierAPlat,row.names=FALSE)

    cat("Exportation OK !!\n")

    odbcClose(con)

}




read.data <-  function(file=NULL,decimalSigne=".",encode="utf-8") {
    cat("1) IMPORTATION \n--------------\n")
    cat("<--",file,"\n")
    data <- read.table(file,sep="\t",stringsAsFactors=FALSE,header=TRUE,dec=decimalSigne,fileEncoding=encode)
    ## verification qu'il y a plusieur colonnes et essaye different separateur
    if(ncol(data)==1) {
        data <- read.table(file,sep=";",stringsAsFactors=FALSE,header=TRUE,dec=decimalSigne,fileEncoding=encode)
        if(ncol(data)==1) {
            data <- read.table(file,sep=",",stringsAsFactors=FALSE,header=TRUE,dec=decimalSigne,fileEncoding=encode)
            if(ncol(data)==1) {
                data <- read.table(file,sep=" ",stringsAsFactors=FALSE,header=TRUE,dec=decimalSigne,fileEncoding=encode)
                if(ncol(data)==1) {
                    stop("!!!! L'importation a echoue\n   les seperatateurs de colonne utilise ne sont pas parmi ([tabulation], ';' ',' [espace])\n   -> veuillez verifier votre fichier de donnees\n")
                }
            }
        }
    }

    return(data)
}


