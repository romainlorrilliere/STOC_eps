
vecPackage=c("RODBC","reshape","data.table","rgdal","lubridate","RPostgreSQL","doBy")
ip <- installed.packages()[,1]

for(p in vecPackage)
    if (!(p %in% ip))
        install.packages(pkgs=p,repos = "http://cran.univ-paris1.fr/",dependencies=TRUE)

library(RODBC)
library(reshape)
library(data.table)
library(rgdal)
library(lubridate)
library(RPostgreSQL)
library(doBy)




openDB.PSQL <- function(nomDB=NULL){
    
    library(RPostgreSQL)
   if(is.null(nomDB)) nomDB <- "stoc_eps"

    drv <- dbDriver("PostgreSQL")
    con <- dbConnect(drv, dbname="stoc_eps")
    return(con)
}



make_annne_point_sp <- function(con=null) {

    if(is.null(con)) con <- openDB.PSQL()
    querySp <- paste("select pk_point,count(*)
from topology.point_buffer50
    group by pk_point;",sep="")
    d1 <- dbGetQuery(con, querySp)
    hist(d$count)
    seuilNbPoint <- 200

    querySp <- paste("select pb.pk_point as id_point1,p1.id_carre as id_carre1, nbpoint, pb.id_point_in_buffer50 as id_point2, p2.id_carre as id_carre2, pb.distance
from topology.point_buffer50 as pb, point as p1, point as p2,
(select pk_point,count(*) as nbpoint
from topology.point_buffer50
group by pk_point) as nb
where pb.pk_point = p1.pk_point and pb.pk_point = nb.pk_point and pb.id_point_in_buffer50 = p2.pk_point
and nbpoint > ",seuilNbPoint,";",sep="")
    d2 <- dbGetQuery(con, querySp)
    d2[d2$id_carre1 == d2$id_carre2 & d2$distance > 20000,] <- NA

    seuilDistance <- 3000

    d3 <- subset(d2,distance>seuilDistance)

    dbWriteTable(con, "point_echantillon_buffer50", d2)

    
    
}
