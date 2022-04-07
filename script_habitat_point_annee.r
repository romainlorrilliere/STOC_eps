

library(data.table)

d <- fread("output_import/habitat_VP.csv")

d  <-  d[,.(pk_habitat,id_point,date,annee,p_habitat_sug,p_habitat,p_habitat_consistent,p_milieu,p_type,s_habitat_sug,s_habitat,s_habitat_consistent,s_milieu,s_type)]

d <- d[,jour_julien := format(as.Date(date),"%j")]

d[,passage_stoc := 999]
   ## precoce
    d[jour_julien > 60 & jour_julien <= 91 , passage_stoc :=  0] # 01/03 -> 31/03

d[jour_julien > 91 & jour_julien <= 130 ,passage_stoc :=  1] #01/04 -> 08/05
d[jour_julien >= 131 & jour_julien <= 167 ,passage_stoc :=  2] #09/05 -> 15/06
   ## tardif
    d[jour_julien > 167 & jour_julien <= 197 , passage_stoc :=  3] # 16/06 -> 15/07


d[passage_stoc == 0 , periode_passage := "early"]
d[passage_stoc == 1 , periode_passage := "first"]
d[passage_stoc == 2 , periode_passage := "second"]
d[passage_stoc == 3, periode_passage :=  "late"]
d[jour_julien > 197 ,periode_passage:= "year_end"]
d[passage_stoc == 999, passage_stoc := NA]

dim(d)

d <- d[passage_stoc %in% c(1,2),]

d <- unique(d)
d <- d[substr(id_point,8,9) != "NA",]
dim(d)

d_an  <- d[,.(nb = .N),by = .(id_point, annee)]

summary(d_an)


summary(d_an)

dim(d_an[nb > 2])

d_an[nb > 2]


d[id_point == "030640P01" & annee == 2018,]


d_an <- d_an[nb<3,]
d_an[,id_point_annee := paste0(annee,id_point)]

d[,id_point_annee := paste0(annee,id_point)]


d_hab <- d[id_point_annee  %in% d_an[,id_point_annee],]

dim(d_hab)
head(d_hab)
dim(d)
d <- unique(d)

dim(d)
d_an  <- d[,.(nb = .N),by = .(id_point, annee)]
summary(d_an)
dim(d_an[nb > 2])
head(d_an[nb > 2])


d_hab_2  <- d_hab[,.(id_point_annee,p_habitat_sug)]
d_hab_2 <- unique(d_hab_2)

d_an_hab <- d_hab_2[,.(nb= .N),by = id_point_annee]
summary(d_an_hab)
dim(d_an_hab[nb > 1])
head(d_an_hab[nb > 1])


dd <- d[id_point_annee %in% d_an_hab[nb ==1,id_point_annee],.(id_point_annee,id_point,annee,p_habitat_sug)]
dim(dd)
dd <- unique(dd)
dim(dd)

dd <- dd[,p_milieu := substr(p_habitat_sug,1,1)]
dd <- dd[,p_type := as.numeric(p_habitat_sug,2,2)

dd_cons <- d[id_point_annee %in% d_an_hab[nb ==1,id_point_annee],.(id_point_annee,id_point,annee,p_habitat_sug,p_habitat_consistent)]
dim(dd_cons)
dd_cons <- unique(dd_cons)
dim(dd_cons)

ddd  <- dd_cons[,.(p_habitat_consistent = all(p_habitat_consistent)),by = id_point_annee]
ddd[is.na(p_habitat_consistent),p_habitat_consistent := FALSE]

dim(dd)
dd <- merge(dd,ddd,by = "id_point_annee")
dim(dd)

fwrite(dd,"output_import/id_point_annee_habitat_VP.csv")
