
vecPackage=c("RODBC","reshape","data.table","rgdal","lubridate","RPostgreSQL","doBy","arm","ggplot2","scales","mgcv","visreg","plyr","lmerTest")
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
library(arm)
library(ggplot2)
library(scales)
library(mgcv)
library(visreg)
library(plyr)
source("scriptExportation.r")




openDB.PSQL <- function(user=NULL,pw=NULL,DBname=NULL){
    ## --- initializing parameters for debugging ----
                                        #DBname=NULL;
                                        #user="romain" # windows
                                        #user = NULL # linux
                                        #  pw=NULL
    ## ---

    library(RPostgreSQL)
                                        # browser()
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



fr2eng <- function(d) {

    trad <- read.csv("Librairie/traduc_fr2eng.csv",stringsAsFactors=FALSE)
    f2eng <- trad$eng
    names(f2eng) <- trad$fr
    coln <- colnames(d)
    coln <- ifelse(coln %in% trad$fr,f2eng[coln],coln)
    colnames(d) <- coln
    return(d)
}







csi_national <- function(con=NULL,query=FALSE,carre = TRUE,
                          firstYear=NULL,lastYear=NULL,altitude=800,departement=NULL,onf=TRUE,distance_contact=NULL,
                         spExcluPassage1=c("MOTFLA","SAXRUB","ANTPRA","OENOEN","PHYTRO"),# (Prince et al. 2013 Env. Sc. and Pol.) + "OENOEN","PHYTRO" avis d'expert F. Jiguet,
                         seuilAbondance=.99,init_1989 = FALSE,plot_smooth=TRUE,
                          champSp = "code_sp", sp=NULL,champsHabitat=FALSE,
                          anglais=TRUE,seuilSignif=0.05,
                          couleur="#4444c3",
                          titreY="Index de sp�cialistation des Communaut�s",titreX="Ann�es",titre="CSI",
                          savePostgres=FALSE,output=FALSE,
                          operateur=c("Lorrilliere Romain","lorrilliere@mnhn.fr"), encodingSave="ISO-8859-1",fileName="dataCSI",id="France"){



#con=NULL;query=FALSE;carre = TRUE;
#                          firstYear=1989;lastYear=NULL;altitude=800;departement=NULL;onf=TRUE;distance_contact=NULL;
#                                                   spExcluPassage1=c("MOTFLA","SAXRUB","ANTPRA","OENOEN","PHYTRO")# (Prince et al. 2013 Env. Sc.# and Pol.) + "OENOEN","PHYTRO" avis d'expert F. Jiguet,
#    seuilAbondance=.99;
#                          champSp = "code_sp"; sp=NULL;champsHabitat=FALSE;
#                          anglais=TRUE;seuilSignif=0.05;
#                          couleur="#4444c3";
#                          titreY="Index de sp�cialistation des Communaut�s";titreX="Ann�es";titre="CSI";
#                          savePostgres=FALSE;output=FALSE;
#                          operateur=c("Lorrilliere Romain","lorrilliere@mnhn.fr"); encodingSave="ISO-8859-1";fileName="dataCSI";id="France"

    if(carre){
        if(query) {



            start <- Sys.time()
            dateExport <- format(start,"%Y-%m-%d")

   if(is.null(con)) con <- openDB.PSQL()
            if(is.null(firstYear)) firstYear <- 2001
    if(is.null(lastYear)) lastYear <- 9999
    if(is.null(altitude)) altitude <- 10000
    if(is.null(distance_contact))
        distance_contact_txt <- "" else if(distance_contact == "100")
                                       distance_contact_txt <- " distance_contact in ('LESS25','LESS100') and " else if(distance_contact == "200")
                                                                                                                    distance_contact_txt <- " distance_contact in ('LESS25','LESS100','MORE100','LESS200') and " else stop("distance_contact non pris en charge")

    seuil_txt <- as.character(trunc(seuilAbondance*100))
    if(is.null(distance_contact))
        champ_seuil <- paste("abondAll_seuil",seuil_txt,sep="") else if(distance_contact == "100")
                                                                    champ_seuil <- paste("abond100_seuil",seuil_txt,sep="") else if(distance_contact == "200")
                                                                                                                                champ_seuil <- paste("abond200_seuil",seuil_txt,sep="")else stop("distance_contact non pris en charge")

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

            selectQuery <- paste(distance_contact_txt," i.annee >= ",firstYear,"  and i.annee <= ",lastYear," and c.etude in ('STOC_EPS'",ifelse(onf,", 'STOC_ONF'",""),")  and  c.altitude <= ",altitude," ",ifelse(is.null(departement),"",paste(" and departement in ",depList," "))," ", sep="")

    selectQueryOp <- ""
    selectQueryOp1 <- "SELECT id_inventaire, code_sp, sum(abondance) as abond
				FROM
				observation as o, inventaire as i,carre_annee as ca, carre as c
				WHERE
				o.id_inventaire = i.pk_inventaire and
				o.id_carre = c.pk_carre and
				o.id_carre = ca.id_carre and o.annee=ca.annee and
				ca.qualite_inventaire_stoc > 0 and
                                "
     selectQueryOp2 <- "
			GROUP BY
			id_inventaire, code_sp"
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






    if(!is.null(departement)) depList <- paste("('",paste(departement,collapse="' , '"),"')",sep="")






        queryCsi <- paste("
select csi.id_carre_annee, ca.id_carre, ca.annee, csi, csi_brut,qualite_inventaire_stoc, commune,insee,departement,
 altitude, longitude_grid_wgs84,latitude_grid_wgs84, ",
ifelse(champsHabitat," foret_p, ouvert_p, agri_p, urbain_p,
foret_ps, ouvert_ps, agri_ps, urbain_ps,
nbp_foret_p, nbp_ouvert_p, nbp_agri_p, nbp_urbain_p,
nbp_foret_ps, nbp_ouvert_ps, nbp_agri_ps, nbp_urbain_ps, ",""),
"'",dateExport,"'::varchar(10) as date_export,
'",operateur[1],"'::varchar(50) as operateur,
'",operateur[2],"'::varchar(50) as email_operateur
  from -- ## begin ## calcul csi
  (select id_carre_annee, sum(ssi_ab) as sum_ssi_ab, sum(abond) as sum_abond, sum(ssi_ab)/sum(abond)::real as csi ,
   sum(ssi_ab_brut) as sum_ssi_ab_brut, sum(abond_brut) as sum_abond_brut, sum(ssi_ab_brut)/sum(abond_brut)::real as csi_brut
   from ( -- ## begin ## ajout des valeur ssi
     select  id_carre_annee, code_sp, abond_brut, abond,ssi, ssi * abond::real as ssi_ab , ssi * abond_brut::real as ssi_ab_brut
     from (-- ## begin ##  abondance carre annee
     select oc.id_carre, oc.annee, (oc.annee::varchar(4)||oc.id_carre::varchar(100))::varchar(100) as id_carre_annee,
     code_sp,abond_brut, abond
     from( -- ## begin ## somme sur carre des max par points sur 2 passages
     	select id_carre, annee,code_sp,sum(abond_brut) as abond_brut,sum(abond) as abond
     	from -- ## begin ## obs max par point sur 2 passages
     		(select id_point, annee,code_sp,max(abond_brut) as abond_brut,max(abond) as abond
     		from -- ## begin ## correction abondance obs par seuil
     			(select id_inventaire,id_point, passage_stoc, annee, code_sp,abond as abond_brut, abondAll_seuil99 as seuil_abondance_sp,LEAST(abond, abondAll_seuil99) as abond
     			from -- ## begin ## selection classe de distance et different filtre
     				(",selectQueryOp,"
     				) -- selection classe de distance et different filtre
     			as opb, espece_abondance_point_seuil as s, inventaire as i
     			WHERE
     			opb.code_sp = s.pk_species and opb.id_inventaire = i.pk_inventaire

     			) -- ## end ## correction abondance obs par seuil
     		as op
     		group by id_point,annee,code_sp
     		) -- ## end ## obs max par point sur 2 passages
     	as omp, point as p
     	where omp.id_point = p.pk_point
     	group by id_carre, annee,code_sp) -- ## end ## somme sur carre des max par points sur 2 passages
     as oc ) -- ## end ## abondance carre annee
   as abc,  species_indicateur_fonctionnel as sif
   where abc.code_sp = sif.pk_species
   ) -- ## end ## ajout des valeur ssi
 as abssi
group by id_carre_annee) -- ## end ##  calcul crsi
 as csi, carre_annee as ca,carre as c
where
csi.id_carre_annee = ca.pk_carre_annee and ca.id_carre = c.pk_carre
order by
ca.id_carre, annee ;",sep="")



                                        # browser()
            cat("\nRequete csi:\n\n",queryCsi,"\n\n")

            dd <- dbGetQuery(con, queryCsi)


            if(anglais)
                dd <- fr2eng(dd)
            ## if(savePostgres) sqlCreateTable(con,tableSQL,d)


                write.csv(dd,paste("export/",fileName,"_carre_",id,".csv",sep=""),row.names=FALSE)

                end <- Sys.time() ## heure de fin




            } else {
                dd <- read.csv(paste("export/",fileName,"_carre_",id,".csv",sep=""),dec=".")
                if(anglais)
                    dd <- fr2eng(dd)
            }


            annee <- sort(unique(dd$year))
            nban <- length(annee)
            pasdetemps <-nban-1

            ## GAMM csi annual variations
            cat("\nEstimation de la variation annuelle csi~ factor(year)+s(longitude_grid_wgs84,latitude_grid_wgs84,bs='sos')\n")
        gammf <- gamm(csi~ factor(year)+s(longitude_grid_wgs84,latitude_grid_wgs84,bs="sos"), data=dd,random=reStruct(object = ~ 1| id_plot, pdClass="pdDiag"),correlation=corAR1(form=~year))

            sgammf<-summary(gammf$gam)
            coefdata=coefficients(gammf$gam)
            coefannee <- c(0,sgammf$p.coeff[2:nban])
            erreuran <- c(0,sgammf$se[2:nban])
            pval <-  c(1,sgammf$p.pv[2:nban])

            ## confindence intervalles

            tabfgamm <- data.frame(model = "gamm factor(year) plot",annee,coef=coefannee,se = erreuran,pval,signif=pval<seuilSignif)






if(plot_smooth) {
            cat("\nGam pour la figure csi~s(year)\n")
            ## create a sequence of temperature that spans your temperature
            ## http://zevross.com/blog/2014/09/15/recreate-the-gam-partial-regression-smooth-plots-from-r-package-mgcv-with-a-little-style/

            gammgg <- gamm(csi~s(year), data=dd,random=reStruct(object = ~ 1| id_plot, pdClass="pdDiag"),correlation=corAR1(form=~year))

            maxyear<-max(dd$year)
            minyear<-min(dd$year)
            year.seq<-sort(unique(c(minyear:maxyear,(seq(minyear, maxyear,length=1000)))))
            year.seq<-data.frame(year=year.seq)

                                        # predict only the temperature term (the sum of the
                                        # term predictions and the intercept gives you the overall
                                        # prediction)

            preds<-predict(gammgg$gam, newdata=year.seq, type="terms", se.fit=TRUE)


                                        # set up the temperature, the fit and the upper and lower
                                        # confidence interval

            year <-year.seq$year
            realYear <- sort(unique(dd$year))
            fit<-as.vector(preds$fit)
            init <- fit[1]

            fit.up95<-fit-1.96*as.vector(preds$se.fit)

            fit.low95<-fit+1.96*as.vector(preds$se.fit)

           # ggGamData <- data.frame(year=year, csi=fit,ic_low95 = fit.low95, ic_up95 = fit.up95)

        fit <- fit - init
         fit.up95 <- fit.up95 - init
        fit.low95 <- fit.low95 - init

        ggGamData <- data.frame(year=year, csi=fit,ic_low95 = fit.low95, ic_up95 = fit.up95)

      #  browser()
        ## The ggplot:
            gg <- ggplot(data=ggGamData,aes(x=year,y=csi))
           gg <- gg + geom_ribbon(aes(ymin=ic_low95, ymax=ic_up95),fill = couleur,alpha=.2)+ geom_line(size=1,colour=couleur)
            gg <- gg +  geom_point(data = subset(ggGamData,year %in% realYear),size=3,colour=couleur) + geom_point(data = subset(ggGamData,year %in% realYear),size=1.5,colour="white")
            gg <- gg + labs(y=titreY,x=titreX,title=titre)+scale_x_continuous(breaks=pretty_breaks())

            ggsave(paste("Output/figCSI_plot",id,".png",sep=""),gg)
            cat("\n--> Output/figCSI_plot",id,".png\n",sep="")

            tabPredict <- subset(ggGamData,year %in% realYear)
            colnames(tabPredict)[1:2] <- c("annee","csi_predict")
            tabgamm <- merge(tabfgamm,tabPredict,by="annee")
} else {
    if(init_1989) {
        histo <- read.csv2("csi_init.csv")
        init <- subset(histo,annee == 2001)$csi

        tabgamm <-  tabfgamm
        tabgamm$coef <-  tabgamm$coef+init
        histo <- subset(histo,annee<=2001)
        tabgamm <- subset(tabgamm,annee>2001)
        tabHisto <- data.frame(model= tabgamm$model[1],annee = histo$annee,
                               coef = histo$csi,se=histo$se,pval=NA,signif=NA)
        tabgamm <- rbind(tabgamm,tabHisto)
        tabgamm <- tabgamm[order(tabgamm$annee),]

        tabgamm$ic_up95<-tabgamm$coef-1.96*as.vector(tabgamm$se)

           tabgamm$ic_low95<-tabgamm$coef+1.96*as.vector(tabgamm$se)

    }
#    browser()
         ## The ggplot:
            gg <- ggplot(data=tabgamm,aes(x=annee,y=coef))
           gg <- gg + geom_ribbon(aes(ymin=ic_low95, ymax=ic_up95),fill = couleur,alpha=.2)+ geom_line(size=1,colour=couleur)
            gg <- gg +  geom_point(size=3,colour=couleur)
            gg <- gg + labs(y=titreY,x=titreX,title=titre)+scale_x_continuous(breaks=pretty_breaks())

            ggsave(paste("Output/figCSI_plot",id,".png",sep=""),gg)
        cat("\n--> Output/figCSI_plot",id,".png\n",sep="")


    write.csv(tabgamm,paste("Output/csi_gammPlot_",id,".csv",sep=""),row.names=FALSE)
        cat("\n  --> Output/csi_gammPlot_",id,".csv\n",sep="")





}

            cat("\nEstimation de la tendence  csi~ year+s(longitude_grid_wgs84,latitude_grid_wgs84,bs='sos')\n")

            gammc <- gamm(csi~year+s(longitude_grid_wgs84,latitude_grid_wgs84,bs="sos"), data=dd,random=reStruct(object = ~ 1| id_plot, pdClass="pdDiag"),correlation=corAR1(form=~year))

            sgammc <-summary(gammc$gam)



            coefannee <- sgammc$p.coeff[2]
            ## se
            erreuran <- sgammc$se[2]
            ## erreur standard
            pval <-  sgammc$p.pv[2]



            tabcgamm <- data.frame(model = "gamm numeric(year) plot",annee=NA,coef=coefannee,se = erreuran,pval,signif=pval<seuilSignif,csi_predict=NA,ic_low95=NA,ic_up95=NA)

            tabgamm <- tabgamm[,colnames(tabcgamm)]


            tabgamm <- rbind(tabgamm,tabcgamm)

            write.csv(tabgamm,paste("Output/csi_gammPlot_",id,".csv",sep=""),row.names=FALSE)
        cat("\n  --> Output/csi_gammPlot_",id,".csv\n",sep="")

    } else {
        if(query) {
            if(is.null(con)) con <- openDB.PSQL()
            if(is.null(firstYear)) firstYear <- 2000
            if(is.null(lastYear)) lastYear <- 9999


            start <- Sys.time()
            dateExport <- format(start,"%Y-%m-%d")
            queryCSI <- paste("select  id_point, p.id_carre, annee, p.commune,p.insee,p.departement,
code_sp, e.scientific_name, e.french_name, e.english_name, e.euring,
abond_brut as abondance_brut, abond as abondance, ssi, ssi * abond::real as ssi_ab , ssi * abond_brut::real as ssi_ab_brut,
p.altitude, c.altitude as altitude_carre, longitude_wgs84,latitude_wgs84,longitude_grid_wgs84,latitude_grid_wgs84,
'",dateExport,"'::varchar(10) as date_export,
'",operateur[1],"'::varchar(50) as operateur,
'",operateur[2],"'::varchar(50) as email_operateur
from (select id_point, annee,code_sp,max(abond_brut) as abond_brut,max(abond) as abond
	from
        (select id_point, annee,date,passage_stoc,code_sp,sum(abond_brut) as abond_brut,sum(abond) as abond
        from (select id_inventaire,code_sp,abond as abond_brut, abond100_seuil95 as seuil_abondance_sp,
        LEAST(abond,abond100_seuil95) as abond
                      from
                      espece_abondance_point_seuil as s,
                      (SELECT
                      id_inventaire,
                      code_sp,
                      sum(abondance) as abond
                      FROM
                      observation as o, inventaire as i
                      WHERE
                      distance_contact in ('LESS25','LESS100') and passage_stoc in (1,2) and etude in ('STOC-EPS') and i.annee >= ",firstYear," and i.annee <= ",lastYear," and
                      o.id_inventaire = i.pk_inventaire
                      GROUP BY
                      id_inventaire, code_sp) as op
                      WHERE
                      op.code_sp = s.pk_species) as ot,
                      inventaire as i
                      where
                      ot.id_inventaire = i.pk_inventaire
                      group by id_point, annee,date,passage_stoc,code_sp) as op
         group by id_point, annee,code_sp) as om, carre as c, point as p, species as e, species_indicateur_fonctionnel as i
where
om.id_point = p.pk_point and p.id_carre = c.pk_carre and om.code_sp = e.pk_species and om.code_sp = i.pk_species and c.altitude <= ",altitude,"
order by id_point, annee,code_sp;",sep="")
            d2 <- dbGetQuery(con, queryCSI)

                                        #if(savePostgres) sqlCreateTable(con,"outcome.FrenchBBS_montaneOpen",d)

            d2_ssi <- aggregate(ssi_ab~id_point + annee, data = d2, sum)
            d2_ab_tot <-  aggregate(abondance~id_point + annee, data = d2, sum)
            d2x <- aggregate(longitude_grid_wgs84~id_point,data=d2,mean)
            d2y <- aggregate(latitude_grid_wgs84~id_point,data=d2,mean)
            d2alt <- aggregate(altitude ~ id_point,data=d2,mean)
            dd2 <- merge(d2_ssi,d2_ab_tot ,by=c("id_point","annee"))
            dd2$csi <- dd2$ssi_ab/dd2$abondance
            dd2 <- merge(dd2,d2x,by="id_point")
            dd2 <- merge(dd2,d2y,by="id_point")
            dd2 <- merge(dd2,d2alt,by="id_point")

            write.csv2(dd,paste("export/",fileName,"_point.csv",sep=""),row.names=FALSE)

            end <- Sys.time() ## heure de fin




        } else {
            dd2 <- read.csv2(paste("export/",fileName,"_point.csv",sep=""),dec=".")
        }


        ## gamm to assess annual csi values






        annee <- sort(unique(dd$annee))
        nban <- length(annee)
        pasdetemps <-nban-1

        ## GLM csi annual variations



        gammf <- gamm(csi~factor(annee)+s(longitude_grid_wgs84,latitude_grid_wgs84,bs="sos"), data=dd2,random=reStruct(object = ~ 1| id_point, pdClass="pdDiag"),correlation=corAR1(form=~annee))

        sgammf<-summary(gammf$gam)


        coefdata=coefficients(gammf$gam)

        coefannee <- c(0,sgammf$p.coeff[2:nban])
        ## se
        erreuran <- c(0,sgammf$se[2:nban])
        ## erreur standard
        pval <-  c(1,sgammf$p.pv[2:nban])

        ## calcul des intervalle de confiance
        if(ic) {
            gammf.sim <- sim(gammf)
            ic_inf_sim <- c(0,tail(apply(coef(gammf.sim), 2, quantile,.025),pasdetemps))
            ic_sup_sim <- c(0,tail(apply(coef(gammf.sim), 2, quantile,.975),pasdetemps))
        } else
        {
            ic_inf_sim <- "not assessed"
            ic_sup_sim <- "not assessed"
        }



        tabfgamm <- data.frame(model = "gamm factor(year) point",annee,coef=coefannee,se = erreuran,lower_ci=ic_inf_sim,upper_ci=ic_sup_sim,pval,signif=pval<seuilSignif)



        gg <- ggplot(data=tabfgamm,aes(x=annee,y=coef))
        gg <- gg + geom_errorbar(aes(ymin=coef-se, ymax=coef+se), width=0,colour=couleur,alpha=0.5) + geom_line(size=1.5,colour=couleur)
        gg <- gg + geom_ribbon(aes(ymin=coef-se, ymax=coef+se),fill = couleur,alpha=.2)+ geom_point(size=3,colour=couleur)+ geom_point(size=1.5,colour="white")
        gg <- gg + labs(y="CSI",x="")+scale_x_continuous(breaks=pretty_breaks())


        ggsave(paste("Output/figCSI_point_",id,".png"),gg)



        gammc <- gamm(csi~annee+s(longitude_grid_wgs84,latitude_grid_wgs84,bs="sos"), data=dd,random=reStruct(object = ~ 1| id_point, pdClass="pdDiag"),correlation=corAR1(form=~annee))

        sgammc <-summary(gammc$gam)



        coefannee <- sgammc$p.coeff[2]
        ## se
        erreuran <- sgammc$se[2]
        ## erreur standard
        pval <-  sgammc$p.pv[2]


        if(ic) {
            gammc.sim <- sim(gammc)
            ic_inf_sim <- c(0,tail(apply(coef(gammc.sim), 2, quantile,.025),pasdetemps))
            ic_sup_sim <- c(0,tail(apply(coef(gammc.sim), 2, quantile,.975),pasdetemps))
        } else
        {
            ic_inf_sim <- "not assessed"
            ic_sup_sim <- "not assessed"
        }


        tabcgamm <- data.frame(model = "gamm numeric(year) point",annee=NA,coef=coefannee,se = erreuran,lower_ci=ic_inf_sim,upper_ci=ic_sup_sim,pval,signif=pval<seuilSignif)




        tabgamm <- rbind(tabfgamm,tabcgamm)
        write.csv(tabgamm,paste("Output/CSI_gammPoint_",id,".csv",sep=""),row.names=FALSE)

    }

    if(output) return(tabgamm)







}





























cti_national <- function(con=NULL,query=FALSE,carre = TRUE,methode = "lmer",
                          firstYear=NULL,lastYear=NULL,altitude=800,departement=NULL,onf=TRUE,distance_contact=NULL,
                         spExcluPassage1=c("MOTFLA","SAXRUB","ANTPRA","OENOEN","PHYTRO"),# (Prince et al. 2013 Env. Sc. and Pol.) + "OENOEN","PHYTRO" avis d'expert F. Jiguet,
                         seuilAbondance=.99,
                          champSp = "code_sp", sp=NULL,champsHabitat=FALSE,
                          anglais=TRUE,seuilSignif=0.05,
                          couleur="#4444c3",
                          titreY="Index Thermique des Communaut�s",titreX="Ann�es",titre="CTI",
                          savePostgres=FALSE,output=FALSE,
                          operateur=c("Lorrilliere Romain","lorrilliere@mnhn.fr"), encodingSave="ISO-8859-1",fileName="dataCTI",id="France"){




###  con=NULL;query=FALSE;ic =FALSE; carre = TRUE,seuilSignif = 0.05 ; fileName="dataCTI_Brut";firstYear=2001;lastYear=2015;altitude=800;operateur=c("Lorrilliere Romain","lorrilliere@mnhn.fr");id = "Lehikoinen"
    if(carre){
        if(query) {

           start <- Sys.time()
            dateExport <- format(start,"%Y-%m-%d")

   if(is.null(con)) con <- openDB.PSQL()
    if(is.null(firstYear)) firstYear <- 2001
    if(is.null(lastYear)) lastYear <- 9999
    if(is.null(altitude)) altitude <- 10000
    if(is.null(distance_contact))
        distance_contact_txt <- "" else if(distance_contact == "100")
                                       distance_contact_txt <- " distance_contact in ('LESS25','LESS100') and " else if(distance_contact == "200")
                                                                                                                    distance_contact_txt <- " distance_contact in ('LESS25','LESS100','MORE100','LESS200') and " else stop("distance_contact non pris en charge")

    seuil_txt <- as.character(trunc(seuilAbondance*100))
    if(is.null(distance_contact))
        champ_seuil <- paste("abondAll_seuil",seuil_txt,sep="") else if(distance_contact == "100")
                                                                    champ_seuil <- paste("abond100_seuil",seuil_txt,sep="") else if(distance_contact == "200")
                                                                                                                                champ_seuil <- paste("abond200_seuil",seuil_txt,sep="")else stop("distance_contact non pris en charge")

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

       selectQuery <- paste(distance_contact_txt," i.annee >= ",firstYear,"  and i.annee <= ",lastYear," and c.etude in ('STOC_EPS'",ifelse(onf,", 'STOC_ONF'",""),")  and  c.altitude <= ",altitude," ",ifelse(is.null(departement),"",paste(" and departement in ",depList," "))," ", sep="")


    selectQueryOp <- ""
    selectQueryOp1 <- "SELECT id_inventaire, code_sp, sum(abondance) as abond
				FROM
				observation as o, inventaire as i,carre_annee as ca, carre as c
				WHERE
				o.id_inventaire = i.pk_inventaire and
				o.id_carre = c.pk_carre and
				o.id_carre = ca.id_carre and o.annee=ca.annee and
				ca.qualite_inventaire_stoc > .5 and
                                "
     selectQueryOp2 <- "
			GROUP BY
			id_inventaire, code_sp"
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






    if(!is.null(departement)) depList <- paste("('",paste(departement,collapse="' , '"),"')",sep="")




        queryCti <- paste("
select cti.id_carre_annee, ca.id_carre, ca.annee, cti, cti_brut,qualite_inventaire_stoc, commune,insee,departement,
 altitude, longitude_grid_wgs84,latitude_grid_wgs84, ",
ifelse(champsHabitat," foret_p, ouvert_p, agri_p, urbain_p,
foret_ps, ouvert_ps, agri_ps, urbain_ps,
nbp_foret_p, nbp_ouvert_p, nbp_agri_p, nbp_urbain_p,
nbp_foret_ps, nbp_ouvert_ps, nbp_agri_ps, nbp_urbain_ps, ",""),
"'",dateExport,"'::varchar(10) as date_export,
'",operateur[1],"'::varchar(50) as operateur,
'",operateur[2],"'::varchar(50) as email_operateur
  from -- ## begin ## calcul crti
  (select id_carre_annee, sum(sti_ab) as sum_sti_ab, sum(abond) as sum_abond, sum(sti_ab)/sum(abond)::real as cti ,
   sum(sti_ab_brut) as sum_sti_ab_brut, sum(abond_brut) as sum_abond_brut, sum(sti_ab_brut)/sum(abond_brut)::real as cti_brut
   from ( -- ## begin ## ajout des valeur sti
     select  id_carre_annee, code_sp, abond_brut, abond,sti, sti * abond::real as sti_ab , sti * abond_brut::real as sti_ab_brut
     from (-- ## begin ##  abondance carre annee
     select oc.id_carre, oc.annee, (oc.annee::varchar(4)||oc.id_carre::varchar(100))::varchar(100) as id_carre_annee,
     code_sp,abond_brut, abond
     from( -- ## begin ## somme sur carre des max par points sur 2 passages
     	select id_carre, annee,code_sp,sum(abond_brut) as abond_brut,sum(abond) as abond
     	from -- ## begin ## obs max par point sur 2 passages
     		(select id_point, annee,code_sp,max(abond_brut) as abond_brut,max(abond) as abond
     		from -- ## begin ## correction abondance obs par seuil
     			(select id_inventaire,id_point, passage_stoc, annee, code_sp,abond as abond_brut, abondAll_seuil99 as seuil_abondance_sp,LEAST(abond, abondAll_seuil99) as abond
     			from -- ## begin ## selection classe de distance et different filtre
     				(",selectQueryOp,"
     				) -- selection classe de distance et different filtre
     			as opb, espece_abondance_point_seuil as s, inventaire as i
     			WHERE
     			opb.code_sp = s.pk_species and opb.id_inventaire = i.pk_inventaire

     			) -- ## end ## correction abondance obs par seuil
     		as op
     		group by id_point,annee,code_sp
     		) -- ## end ## obs max par point sur 2 passages
     	as omp, point as p
     	where omp.id_point = p.pk_point
     	group by id_carre, annee,code_sp) -- ## end ## somme sur carre des max par points sur 2 passages
     as oc ) -- ## end ## abondance carre annee
   as abc,  species_indicateur_fonctionnel as sif
   where abc.code_sp = sif.pk_species
   ) -- ## end ## ajout des valeur sti
 as absti
group by id_carre_annee) -- ## end ##  calcul crti
 as cti, carre_annee as ca,carre as c
where
cti.id_carre_annee = ca.pk_carre_annee and ca.id_carre = c.pk_carre
order by
ca.id_carre, annee ;",sep="")



                                        # browser()
            cat("\nRequete cti:\n\n",queryCti,"\n\n")

            dd <- dbGetQuery(con, queryCti)


            if(anglais)
                dd <- fr2eng(dd)
            ## if(savePostgres) sqlCreateTable(con,tableSQL,d)


                write.csv(dd,paste("export/",fileName,"_carre_",id,".csv",sep=""),row.names=FALSE)

                end <- Sys.time() ## heure de fin




            } else {
                dd <- read.csv(paste("export/",fileName,"_carre_",id,".csv",sep=""),dec=".")
                if(anglais)
                    dd <- fr2eng(dd)
            }

       annee <- sort(unique(dd$year))
            nban <- length(annee)
            pasdetemps <-nban-1

        if(methode == "gam") {

            cat("Methode: gam\n")


            ## GAMM cti annual variations
            cat("\nEstimation de la variation annuelle cti~ factor(year)+s(longitude_grid_wgs84,latitude_grid_wgs84,bs='sos')\n")
        gammf <- gamm(cti~ factor(year)+s(longitude_grid_wgs84,latitude_grid_wgs84,bs="sos"), data=dd,random=reStruct(object = ~ 1| id_plot, pdClass="pdDiag"),correlation=corAR1(form=~year))

            sgammf<-summary(gammf$gam)
            coefdata=coefficients(gammf$gam)
            coefannee <- c(0,sgammf$p.coeff[2:nban])
            erreuran <- c(0,sgammf$se[2:nban])
            pval <-  c(1,sgammf$p.pv[2:nban])

            ## confindence intervalles

            tabfgamm <- data.frame(model = "gamm factor(year) plot",annee,coef=coefannee,se = erreuran,pval,signif=pval<seuilSignif)


            cat("\nGam pour la figure cti~s(year)\n")
            ## create a sequence of temperature that spans your temperature
            ## http://zevross.com/blog/2014/09/15/recreate-the-gam-partial-regression-smooth-plots-from-r-package-mgcv-with-a-little-style/

            gammgg <- gamm(cti~s(year), data=dd,random=reStruct(object = ~ 1| id_plot, pdClass="pdDiag"),correlation=corAR1(form=~year))

            maxyear<-max(dd$year)
            minyear<-min(dd$year)
            year.seq<-sort(unique(c(minyear:maxyear,(seq(minyear, maxyear,length=1000)))))
            year.seq<-data.frame(year=year.seq)

                                        # predict only the temperature term (the sum of the
                                        # term predictions and the intercept gives you the overall
                                        # prediction)

            preds<-predict(gammgg$gam, newdata=year.seq, type="terms", se.fit=TRUE)


                                        # set up the temperature, the fit and the upper and lower
                                        # confidence interval

            year <-year.seq$year
            realYear <- sort(unique(dd$year))
            fit<-as.vector(preds$fit)
            init <- fit[1]

            fit.up95<-fit-1.96*as.vector(preds$se.fit)

            fit.low95<-fit+1.96*as.vector(preds$se.fit)

           # ggGamData <- data.frame(year=year, cti=fit,ic_low95 = fit.low95, ic_up95 = fit.up95)

        fit <- fit - init
         fit.up95 <- fit.up95 - init
        fit.low95 <- fit.low95 - init

        ggGamData <- data.frame(year=year, cti=fit,ic_low95 = fit.low95, ic_up95 = fit.up95)

      #  browser()
        ## The ggplot:
            gg <- ggplot(data=ggGamData,aes(x=year,y=cti))
           gg <- gg + geom_ribbon(aes(ymin=ic_low95, ymax=ic_up95),fill = couleur,alpha=.2)+ geom_line(size=1,colour=couleur)
            gg <- gg +  geom_point(data = subset(ggGamData,year %in% realYear),size=3,colour=couleur) + geom_point(data = subset(ggGamData,year %in% realYear),size=1.5,colour="white")
            gg <- gg + labs(y=titreY,x=titreX,title=titre)+scale_x_continuous(breaks=pretty_breaks())

            ggsave(paste("Output/figCTI_plot",id,".png",sep=""),gg)
            cat("\n--> Output/figCTI_plot",id,".png\n",sep="")

            tabPredict <- subset(ggGamData,year %in% realYear)
            colnames(tabPredict)[1:2] <- c("annee","cti_predict")
            tabgamm <- merge(tabfgamm,tabPredict,by="annee")

            cat("\nEstimation de la tendence  cti~ year+s(longitude_grid_wgs84,latitude_grid_wgs84,bs='sos')\n")

            gammc <- gamm(cti~year+s(longitude_grid_wgs84,latitude_grid_wgs84,bs="sos"), data=dd,random=reStruct(object = ~ 1| id_plot, pdClass="pdDiag"),correlation=corAR1(form=~year))

            sgammc <-summary(gammc$gam)



            coefannee <- sgammc$p.coeff[2]
            ## se
            erreuran <- sgammc$se[2]
            ## erreur standard
            pval <-  sgammc$p.pv[2]



            tabcgamm <- data.frame(model = "gamm numeric(year) plot",annee=NA,coef=coefannee,se = erreuran,pval,signif=pval<seuilSignif,cti_predict=NA,ic_low95=NA,ic_up95=NA)

            tabgamm <- tabgamm[,colnames(tabcgamm)]


            tabgamm <- rbind(tabgamm,tabcgamm)

            write.csv(tabgamm,paste("Output/cti_gammPlot_",id,".csv",sep=""),row.names=FALSE)
        cat("\n  --> Output/cti_gammPlot_",id,".csv\n",sep="")
             }
        if (methode == "lmer") {
            cat("Method : lmer \n")


###################
browser()
   ## LMER cti annual variations
            cat("\nEstimation de la variation annuelle lmer(cti~ factor(year)+(1|id_plot)\n")
        md.f <- lmer(cti~ factor(year)+(1|id_plot),data=dd)

            smd.f<-summary(md.f)
            coefdata.f <-  as.data.frame(smd.f$coefficients)
            coefdata.f <- data.frame(model="Annual fluctuation", variable = rownames(coefdata.f),coefdata.f)


              md.c <- lmer(cti~ year+(1|id_plot),data=dd)
            smd.c<-summary(md.c)

            coefdata.c <-  as.data.frame(smd.c$coefficients)
            coefdata.c <- data.frame(model = "Linear trend", variable = rownames(coefdata.c),coefdata.c)

            coefdata <- rbind(coefdata.c,coefdata.f)



write.csv(coefdata,"Output/lmer_coefficient_CTI.csv",row.names=FALSE)

            coefannee <- c(0,sgammf$p.coeff[2:nban])
            erreuran <- c(0,sgammf$se[2:nban])
            pval <-  c(1,sgammf$p.pv[2:nban])

            ## confindence intervalles

            tabfgamm <- data.frame(model = "gamm factor(year) plot",annee,coef=coefannee,se = erreuran,pval,signif=pval<seuilSignif)


            cat("\nGam pour la figure cti~s(year)\n")
            ## create a sequence of temperature that spans your temperature
            ## http://zevross.com/blog/2014/09/15/recreate-the-gam-partial-regression-smooth-plots-from-r-package-mgcv-with-a-little-style/

            gammgg <- gamm(cti~s(year), data=dd,random=reStruct(object = ~ 1| id_plot, pdClass="pdDiag"),correlation=corAR1(form=~year))

            maxyear<-max(dd$year)
            minyear<-min(dd$year)
            year.seq<-sort(unique(c(minyear:maxyear,(seq(minyear, maxyear,length=1000)))))
            year.seq<-data.frame(year=year.seq)

                                        # predict only the temperature term (the sum of the
                                        # term predictions and the intercept gives you the overall
                                        # prediction)

            preds<-predict(gammgg$gam, newdata=year.seq, type="terms", se.fit=TRUE)


                                        # set up the temperature, the fit and the upper and lower
                                        # confidence interval

            year <-year.seq$year
            realYear <- sort(unique(dd$year))
            fit<-as.vector(preds$fit)
            init <- fit[1]

            fit.up95<-fit-1.96*as.vector(preds$se.fit)

            fit.low95<-fit+1.96*as.vector(preds$se.fit)

           # ggGamData <- data.frame(year=year, cti=fit,ic_low95 = fit.low95, ic_up95 = fit.up95)

        fit <- fit - init
         fit.up95 <- fit.up95 - init
        fit.low95 <- fit.low95 - init

        ggGamData <- data.frame(year=year, cti=fit,ic_low95 = fit.low95, ic_up95 = fit.up95)

      #  browser()
        ## The ggplot:
            gg <- ggplot(data=ggGamData,aes(x=year,y=cti))
           gg <- gg + geom_ribbon(aes(ymin=ic_low95, ymax=ic_up95),fill = couleur,alpha=.2)+ geom_line(size=1,colour=couleur)
            gg <- gg +  geom_point(data = subset(ggGamData,year %in% realYear),size=3,colour=couleur) + geom_point(data = subset(ggGamData,year %in% realYear),size=1.5,colour="white")
            gg <- gg + labs(y=titreY,x=titreX,title=titre)+scale_x_continuous(breaks=pretty_breaks())

            ggsave(paste("Output/figCTI_plot",id,".png",sep=""),gg)
            cat("\n--> Output/figCTI_plot",id,".png\n",sep="")

            tabPredict <- subset(ggGamData,year %in% realYear)
            colnames(tabPredict)[1:2] <- c("annee","cti_predict")
            tabgamm <- merge(tabfgamm,tabPredict,by="annee")

            cat("\nEstimation de la tendence  cti~ year+s(longitude_grid_wgs84,latitude_grid_wgs84,bs='sos')\n")

            gammc <- gamm(cti~year+s(longitude_grid_wgs84,latitude_grid_wgs84,bs="sos"), data=dd,random=reStruct(object = ~ 1| id_plot, pdClass="pdDiag"),correlation=corAR1(form=~year))

            sgammc <-summary(gammc$gam)



            coefannee <- sgammc$p.coeff[2]
            ## se
            erreuran <- sgammc$se[2]
            ## erreur standard
            pval <-  sgammc$p.pv[2]



            tabcgamm <- data.frame(model = "gamm numeric(year) plot",annee=NA,coef=coefannee,se = erreuran,pval,signif=pval<seuilSignif,cti_predict=NA,ic_low95=NA,ic_up95=NA)

            tabgamm <- tabgamm[,colnames(tabcgamm)]


            tabgamm <- rbind(tabgamm,tabcgamm)

            write.csv(tabgamm,paste("Output/cti_gammPlot_",id,".csv",sep=""),row.names=FALSE)
        cat("\n  --> Output/cti_gammPlot_",id,".csv\n",sep="")



#####################






        }


    } else {
        if(query) {
            if(is.null(con)) con <- openDB.PSQL()
            if(is.null(firstYear)) firstYear <- 2000
            if(is.null(lastYear)) lastYear <- 9999


            start <- Sys.time()
            dateExport <- format(start,"%Y-%m-%d")
            queryCTI <- paste("select  id_point, p.id_carre, annee, p.commune,p.insee,p.departement,
code_sp, e.scientific_name, e.french_name, e.english_name, e.euring,
abond_brut as abondance_brut, abond as abondance, sti, sti * abond::real as sti_ab , sti * abond_brut::real as sti_ab_brut,
p.altitude, c.altitude as altitude_carre, longitude_wgs84,latitude_wgs84,longitude_grid_wgs84,latitude_grid_wgs84,
'",dateExport,"'::varchar(10) as date_export,
'",operateur[1],"'::varchar(50) as operateur,
'",operateur[2],"'::varchar(50) as email_operateur
from (select id_point, annee,code_sp,max(abond_brut) as abond_brut,max(abond) as abond
	from
        (select id_point, annee,date,passage_stoc,code_sp,sum(abond_brut) as abond_brut,sum(abond) as abond
        from (select id_inventaire,code_sp,abond as abond_brut, abond100_seuil95 as seuil_abondance_sp,
        LEAST(abond,abond100_seuil95) as abond
                      from
                      espece_abondance_point_seuil as s,
                      (SELECT
                      id_inventaire,
                      code_sp,
                      sum(abondance) as abond
                      FROM
                      observation as o, inventaire as i
                      WHERE
                      distance_contact in ('LESS25','LESS100') and passage_stoc in (1,2) and etude in ('STOC-EPS') and i.annee >= ",firstYear," and i.annee <= ",lastYear," and
                      o.id_inventaire = i.pk_inventaire
                      GROUP BY
                      id_inventaire, code_sp) as op
                      WHERE
                      op.code_sp = s.pk_species) as ot,
                      inventaire as i
                      where
                      ot.id_inventaire = i.pk_inventaire
                      group by id_point, annee,date,passage_stoc,code_sp) as op
         group by id_point, annee,code_sp) as om, carre as c, point as p, species as e, species_indicateur_fonctionnel as i
where
om.id_point = p.pk_point and p.id_carre = c.pk_carre and om.code_sp = e.pk_species and om.code_sp = i.pk_species and c.altitude <= ",altitude,"
order by id_point, annee,code_sp;",sep="")
            d2 <- dbGetQuery(con, queryCTI)

                                        #if(savePostgres) sqlCreateTable(con,"outcome.FrenchBBS_montaneOpen",d)

            d2_sti <- aggregate(sti_ab~id_point + annee, data = d2, sum)
            d2_ab_tot <-  aggregate(abondance~id_point + annee, data = d2, sum)
            d2x <- aggregate(longitude_grid_wgs84~id_point,data=d2,mean)
            d2y <- aggregate(latitude_grid_wgs84~id_point,data=d2,mean)
            d2alt <- aggregate(altitude ~ id_point,data=d2,mean)
            dd2 <- merge(d2_sti,d2_ab_tot ,by=c("id_point","annee"))
            dd2$cti <- dd2$sti_ab/dd2$abondance
            dd2 <- merge(dd2,d2x,by="id_point")
            dd2 <- merge(dd2,d2y,by="id_point")
            dd2 <- merge(dd2,d2alt,by="id_point")

            write.csv2(dd,paste("export/",fileName,"_point.csv",sep=""),row.names=FALSE)

            end <- Sys.time() ## heure de fin




        } else {
            dd2 <- read.csv2(paste("export/",fileName,"_point.csv",sep=""),dec=".")
        }


        ## gamm to assess annual cti values






        annee <- sort(unique(dd$annee))
        nban <- length(annee)
        pasdetemps <-nban-1

        ## GLM cti annual variations



        gammf <- gamm(cti~factor(annee)+s(longitude_grid_wgs84,latitude_grid_wgs84,bs="sos"), data=dd2,random=reStruct(object = ~ 1| id_point, pdClass="pdDiag"),correlation=corAR1(form=~annee))

        sgammf<-summary(gammf$gam)


        coefdata=coefficients(gammf$gam)

        coefannee <- c(0,sgammf$p.coeff[2:nban])
        ## se
        erreuran <- c(0,sgammf$se[2:nban])
        ## erreur standard
        pval <-  c(1,sgammf$p.pv[2:nban])

        ## calcul des intervalle de confiance
        if(ic) {
            gammf.sim <- sim(gammf)
            ic_inf_sim <- c(0,tail(apply(coef(gammf.sim), 2, quantile,.025),pasdetemps))
            ic_sup_sim <- c(0,tail(apply(coef(gammf.sim), 2, quantile,.975),pasdetemps))
        } else
        {
            ic_inf_sim <- "not assessed"
            ic_sup_sim <- "not assessed"
        }



        tabfgamm <- data.frame(model = "gamm factor(year) point",annee,coef=coefannee,se = erreuran,lower_ci=ic_inf_sim,upper_ci=ic_sup_sim,pval,signif=pval<seuilSignif)



        gg <- ggplot(data=tabfgamm,aes(x=annee,y=coef))
        gg <- gg + geom_errorbar(aes(ymin=coef-se, ymax=coef+se), width=0,colour=couleur,alpha=0.5) + geom_line(size=1.5,colour=couleur)
        gg <- gg + geom_ribbon(aes(ymin=coef-se, ymax=coef+se),fill = couleur,alpha=.2)+ geom_point(size=3,colour=couleur)+ geom_point(size=1.5,colour="white")
        gg <- gg + labs(y="CTI",x="")+scale_x_continuous(breaks=pretty_breaks())


        ggsave(paste("Output/figCTI_point_",id,".png"),gg)



        gammc <- gamm(cti~annee+s(longitude_grid_wgs84,latitude_grid_wgs84,bs="sos"), data=dd,random=reStruct(object = ~ 1| id_point, pdClass="pdDiag"),correlation=corAR1(form=~annee))

        sgammc <-summary(gammc$gam)



        coefannee <- sgammc$p.coeff[2]
        ## se
        erreuran <- sgammc$se[2]
        ## erreur standard
        pval <-  sgammc$p.pv[2]


        if(ic) {
            gammc.sim <- sim(gammc)
            ic_inf_sim <- c(0,tail(apply(coef(gammc.sim), 2, quantile,.025),pasdetemps))
            ic_sup_sim <- c(0,tail(apply(coef(gammc.sim), 2, quantile,.975),pasdetemps))
        } else
        {
            ic_inf_sim <- "not assessed"
            ic_sup_sim <- "not assessed"
        }


        tabcgamm <- data.frame(model = "gamm numeric(year) point",annee=NA,coef=coefannee,se = erreuran,lower_ci=ic_inf_sim,upper_ci=ic_sup_sim,pval,signif=pval<seuilSignif)




        tabgamm <- rbind(tabfgamm,tabcgamm)
        write.csv(tabgamm,paste("Output/CTI_gammPoint_",id,".csv",sep=""),row.names=FALSE)

    }

    if(output) return(tabgamm)







}



######## CTI-Europe



cti_europe <- function(con=NULL,user=NULL,mp=NULL,query=TRUE,carre = TRUE,methode = "lmer",
                          firstYear=1989,lastYear=2017,altitude=800,departement=NULL,onf=TRUE,distance_contact=NULL,
                         spExcluPassage1=c("MOTFLA","SAXRUB","ANTPRA","OENOEN","PHYTRO"),# (Prince et al. 2013 Env. Sc. and Pol.) + "OENOEN","PHYTRO" avis d'expert F. Jiguet,
                         seuilAbondance=.99,
                          champSp = "code_sp", sp=NULL,champsHabitat=FALSE,
                          anglais=TRUE,seuilSignif=0.05,
                          couleur="#4444c3",
                          titreY="CTI",titreX="Year",titre="",
                          savePostgres=FALSE,output=FALSE,
                          operateur=c("Lorrilliere Romain","lorrilliere@mnhn.fr"), encodingSave="ISO-8859-1",fileName="dataCTI",id="europe_France_aksu"){


##    query=FALSE;ic =FALSE; carre = TRUE;seuilSignif = 0.05 ; fileName="dataCTI_Brut";firstYear=2001;lastYear=2017;altitude=800;distance_contact=NULL;sp=NULL;seuilAbondance=.99;onf=TRUE;methode = "lmer";departement=NULL;seuilSignif=0.05;anglais=FALSE;champsHabitat=FALSE
##    spExcluPassage1=c("MOTFLA","SAXRUB","ANTPRA","OENOEN","PHYTRO")# (Prince et al. 2013 Env. Sc. and Pol.) + "OENOEN","PHYTRO" avis d'expert F. Jiguet,
##     couleur="#4444c3"; titreY="CTI";titreX="Year";titre="";
##    operateur=c("Lorrilliere Romain","lorrilliere@mnhn.fr");id = "CTI_winterSp_europe_France_aksu"


 ##   con=NULL;user="romain"





    if(carre){

    if(firstYear == 1989) {
        library(foreign)
        library(reshape2)

        if(is.null(con)) con <- openDB.PSQL(user,mp)


        d.sti <- dbReadTable(con,"species_indicateur_fonctionnel")
        d.sti <- d.sti[,c("pk_species","sti","sti_europe")]
        colnames(d.sti)[1] <- "ESPECE"



   ##     d.old <- read.dbf("DB_import/OLD2A.DBF")
   ##     d.old$ESPECE <- toupper(d.old$ESPECE)

   ##     d.old <- merge(d.old,d.sti,by="ESPECE",all.x=TRUE)
   ##     d.old$nsti <- ifelse(is.na(d.old$sti),0,d.old$TOTAL * d.old$sti)
   ##     d.old$nsti_europe <- ifelse(is.na(d.old$sti_europe),0,d.old$TOTAL * d.old$sti_europe)

   ##     d.old$n_france <- d.old$TOTAL*as.numeric(!is.na(d.old$sti))
   ##     d.old$n_europe <- d.old$TOTAL*as.numeric(!is.na(d.old$sti_europe))




   ##     agg.old <- aggregate(cbind(d.old$n_france,d.old$n_europe,d.old$nsti,d.old$nsti_europe)~ ROUTE + ANNEE,data=d.old,FUN=sum)

     ##   colnames(agg.old)[3:6] <- c("N_france","N_europe","Nsti","NSTI_europe")
   ##     agg.old$cti_france <- agg.old$Nsti / agg.old$N_france
   ##     agg.old$cti_europe <- agg.old$NSTI_europe / agg.old$N_europe

   ##     dcti.old <- agg.old[,c("ROUTE","ANNEE","cti_france","cti_europe")]
   ##     colnames(dcti.old)[1:2] <- c("id_plot","year")
   ##     dcti.old$id_plot <- -1 * dcti.old$id_plot

   ##     firstYear <- NULL



        d.old <- read.table("DB_import/point1989-2013trifinal.txt",header=TRUE)
        d.old  <- melt(d.old,
                                        # ID variables - all the variables to keep but not split apart on
                       id.vars=c("carre","point","annee","x","y","alt","inf800","unique","milieu"),
                                        # The source columns
                                        # Name of the destination column that will identify the original
                                        # column that the measurement came from
                       variable.name="ESPECE",
                       value.name="abundance"
                       )

        d.old <- subset(d.old,annee<2001)

        if(!(is.null(sp))) {
            d.old <- subset(d.old,ESPECE %in% sp)
        }

        d.old <- aggregate(abundance~carre+annee+ESPECE,d.old,sum)


       d.old <- merge(d.old,d.sti,by="ESPECE",all.x=TRUE)
        d.old$nsti <- ifelse(is.na(d.old$sti),0,d.old$abundance * d.old$sti)
        d.old$nsti_europe <- ifelse(is.na(d.old$sti_europe),0,d.old$abundance * d.old$sti_europe)

        d.old$n_france <- d.old$abundance*as.numeric(!is.na(d.old$sti))
        d.old$n_europe <- d.old$abundance*as.numeric(!is.na(d.old$sti_europe))




        agg.old <- aggregate(cbind(d.old$n_france,d.old$n_europe,d.old$nsti,d.old$nsti_europe)~ carre +annee ,data=d.old,FUN=sum)

        colnames(agg.old)[3:6] <- c("N_france","N_europe","Nsti","NSTI_europe")
        agg.old$cti_france <- agg.old$Nsti / agg.old$N_france
        agg.old$cti_europe <- agg.old$NSTI_europe / agg.old$N_europe

        dcti.old <- agg.old[,c("carre","annee","cti_europe")]
        colnames(dcti.old) <- c("id_plot","year","cti_eu")
        firstYear <- NULL

        } else { dcti.old <- NULL}



        if(query) {

           start <- Sys.time()
            dateExport <- format(start,"%Y-%m-%d")

   if(is.null(con)) con <- openDB.PSQL()
    if(is.null(firstYear)) firstYear <- 2001
    if(is.null(lastYear)) lastYear <- 9999
    if(is.null(altitude)) altitude <- 10000
    if(is.null(distance_contact))
        distance_contact_txt <- "" else if(distance_contact == "100")
                                       distance_contact_txt <- " distance_contact in ('LESS25','LESS100') and " else if(distance_contact == "200")
                                                                                                                    distance_contact_txt <- " distance_contact in ('LESS25','LESS100','MORE100','LESS200') and " else stop("distance_contact non pris en charge")

    seuil_txt <- as.character(trunc(seuilAbondance*100))
    if(is.null(distance_contact))
        champ_seuil <- paste("abondAll_seuil",seuil_txt,sep="") else if(distance_contact == "100")
                                                                    champ_seuil <- paste("abond100_seuil",seuil_txt,sep="") else if(distance_contact == "200")
                                                                                                                                champ_seuil <- paste("abond200_seuil",seuil_txt,sep="")else stop("distance_contact non pris en charge")

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

       selectQuery <- paste(distance_contact_txt," i.annee >= ",firstYear,"  and i.annee <= ",lastYear," and c.etude in ('STOC_EPS'",ifelse(onf,", 'STOC_ONF'",""),")  and  c.altitude <= ",altitude," ",ifelse(is.null(departement),"",paste(" and departement in ",depList," "))," ", sep="")


    selectQueryOp <- ""
    selectQueryOp1 <- "SELECT id_inventaire, code_sp, sum(abondance) as abond
				FROM
				observation as o, inventaire as i,carre_annee as ca, carre as c
				WHERE
				o.id_inventaire = i.pk_inventaire and
				o.id_carre = c.pk_carre and
				o.id_carre = ca.id_carre and o.annee=ca.annee and
				ca.qualite_inventaire_stoc > 0.5 and
                                "
     selectQueryOp2 <- "
			GROUP BY
			id_inventaire, code_sp"
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






    if(!is.null(departement)) depList <- paste("('",paste(departement,collapse="' , '"),"')",sep="")




        queryCti <- paste("
select cti.id_carre_annee, ca.id_carre, ca.annee, cti_eu, cti_eu_brut,qualite_inventaire_stoc, commune,insee,departement,
 altitude, longitude_grid_wgs84,latitude_grid_wgs84, ",
ifelse(champsHabitat," foret_p, ouvert_p, agri_p, urbain_p,
foret_ps, ouvert_ps, agri_ps, urbain_ps,
nbp_foret_p, nbp_ouvert_p, nbp_agri_p, nbp_urbain_p,
nbp_foret_ps, nbp_ouvert_ps, nbp_agri_ps, nbp_urbain_ps, ",""),
"'",dateExport,"'::varchar(10) as date_export,
'",operateur[1],"'::varchar(50) as operateur,
'",operateur[2],"'::varchar(50) as email_operateur
  from -- ## begin ## calcul crti
  (select id_carre_annee, sum(sti_eu_ab) as sum_sti_eu_ab, sum(abond) as sum_abond, sum(sti_eu_ab)/sum(abond)::real as cti_eu ,
   sum(sti_eu_ab_brut) as sum_sti_eu_ab_brut, sum(abond_brut) as sum_abond_brut, sum(sti_eu_ab_brut)/sum(abond_brut)::real as cti_eu_brut
   from ( -- ## begin ## ajout des valeur sti
     select  id_carre_annee, code_sp, abond_brut, abond,sti_europe as sti_eu, sti_europe * abond::real as sti_eu_ab , sti_europe * abond_brut::real as sti_eu_ab_brut
     from (-- ## begin ##  abondance carre annee
     select oc.id_carre, oc.annee, (oc.annee::varchar(4)||oc.id_carre::varchar(100))::varchar(100) as id_carre_annee,
     code_sp,abond_brut, abond
     from( -- ## begin ## somme sur carre des max par points sur 2 passages
     	select id_carre, annee,code_sp,sum(abond_brut) as abond_brut,sum(abond) as abond
     	from -- ## begin ## obs max par point sur 2 passages
     		(select id_point, annee,code_sp,max(abond_brut) as abond_brut,max(abond) as abond
     		from -- ## begin ## correction abondance obs par seuil
     			(select id_inventaire,id_point, passage_stoc, annee, code_sp,abond as abond_brut, abondAll_seuil99 as seuil_abondance_sp,LEAST(abond, abondAll_seuil99) as abond
     			from -- ## begin ## selection classe de distance et different filtre
     				(",selectQueryOp,"
     				) -- selection classe de distance et different filtre
     			as opb, espece_abondance_point_seuil as s, inventaire as i
     			WHERE
     			opb.code_sp = s.pk_species and opb.id_inventaire = i.pk_inventaire

     			) -- ## end ## correction abondance obs par seuil
     		as op
     		group by id_point,annee,code_sp
     		) -- ## end ## obs max par point sur 2 passages
     	as omp, point as p
     	where omp.id_point = p.pk_point
     	group by id_carre, annee,code_sp) -- ## end ## somme sur carre des max par points sur 2 passages
     as oc ) -- ## end ## abondance carre annee
   as abc,  species_indicateur_fonctionnel as sif
   where abc.code_sp = sif.pk_species
   ) -- ## end ## ajout des valeur sti
 as absti
group by id_carre_annee) -- ## end ##  calcul crti
 as cti, carre_annee as ca,carre as c
where
cti.id_carre_annee = ca.pk_carre_annee and ca.id_carre = c.pk_carre
order by
ca.id_carre, annee ;",sep="")



                                        # browser()
            cat("\nRequete cti:\n\n",queryCti,"\n\n")

            dd <- dbGetQuery(con, queryCti)


            if(anglais)
                dd <- fr2eng(dd)
            ## if(savePostgres) sqlCreateTable(con,tableSQL,d)


                write.csv(dd,paste("export/",fileName,"_carre_",id,".csv",sep=""),row.names=FALSE)

                end <- Sys.time() ## heure de fin
#browser()
            dcti <- dd[,c("id_plot","year","cti_eu")]
dcti <- rbind(dcti.old,dcti)
            dcti$id_plot <- as.factor(dcti$id_plot)

            } else {
                dd <- read.csv(paste("export/",fileName,"_carre_",id,".csv",sep=""),dec=".")
                if(anglais)
                    dd <- fr2eng(dd)
            }

       annee <- sort(unique(dd$year))
            nban <- length(annee)
            pasdetemps <-nban-1

        if(methode == "gam") {

            cat("Methode: gam\n")


            ## GAMM cti annual variations
            cat("\nEstimation de la variation annuelle cti~ factor(year)+s(longitude_grid_wgs84,latitude_grid_wgs84,bs='sos')\n")
        gammf <- gamm(cti~ factor(year)+s(longitude_grid_wgs84,latitude_grid_wgs84,bs="sos"), data=dd,random=reStruct(object = ~ 1| id_plot, pdClass="pdDiag"),correlation=corAR1(form=~year))

            sgammf<-summary(gammf$gam)
            coefdata=coefficients(gammf$gam)
            coefannee <- c(0,sgammf$p.coeff[2:nban])
            erreuran <- c(0,sgammf$se[2:nban])
            pval <-  c(1,sgammf$p.pv[2:nban])

            ## confindence intervalles

            tabfgamm <- data.frame(model = "gamm factor(year) plot",annee,coef=coefannee,se = erreuran,pval,signif=pval<seuilSignif)


            cat("\nGam pour la figure cti~s(year)\n")
            ## create a sequence of temperature that spans your temperature
            ## http://zevross.com/blog/2014/09/15/recreate-the-gam-partial-regression-smooth-plots-from-r-package-mgcv-with-a-little-style/

            gammgg <- gamm(cti~s(year), data=dd,random=reStruct(object = ~ 1| id_plot, pdClass="pdDiag"),correlation=corAR1(form=~year))

            maxyear<-max(dd$year)
            minyear<-min(dd$year)
            year.seq<-sort(unique(c(minyear:maxyear,(seq(minyear, maxyear,length=1000)))))
            year.seq<-data.frame(year=year.seq)

                                        # predict only the temperature term (the sum of the
                                        # term predictions and the intercept gives you the overall
                                        # prediction)

            preds<-predict(gammgg$gam, newdata=year.seq, type="terms", se.fit=TRUE)


                                        # set up the temperature, the fit and the upper and lower
                                        # confidence interval

            year <-year.seq$year
            realYear <- sort(unique(dd$year))
            fit<-as.vector(preds$fit)
            init <- fit[1]

            fit.up95<-fit-1.96*as.vector(preds$se.fit)

            fit.low95<-fit+1.96*as.vector(preds$se.fit)

           # ggGamData <- data.frame(year=year, cti=fit,ic_low95 = fit.low95, ic_up95 = fit.up95)

        fit <- fit - init
         fit.up95 <- fit.up95 - init
        fit.low95 <- fit.low95 - init

        ggGamData <- data.frame(year=year, cti=fit,ic_low95 = fit.low95, ic_up95 = fit.up95)

      #  browser()
        ## The ggplot:
            gg <- ggplot(data=ggGamData,aes(x=year,y=cti))
           gg <- gg + geom_ribbon(aes(ymin=ic_low95, ymax=ic_up95),fill = couleur,alpha=.2)+ geom_line(size=1,colour=couleur)
            gg <- gg +  geom_point(data = subset(ggGamData,year %in% realYear),size=3,colour=couleur) + geom_point(data = subset(ggGamData,year %in% realYear),size=1.5,colour="white")
            gg <- gg + labs(y=titreY,x=titreX,title=titre)+scale_x_continuous(breaks=pretty_breaks())

            ggsave(paste("Output/figCTI_plot",id,".png",sep=""),gg)
            cat("\n--> Output/figCTI_plot",id,".png\n",sep="")

            tabPredict <- subset(ggGamData,year %in% realYear)
            colnames(tabPredict)[1:2] <- c("annee","cti_predict")
            tabgamm <- merge(tabfgamm,tabPredict,by="annee")

            cat("\nEstimation de la tendence  cti~ year+s(longitude_grid_wgs84,latitude_grid_wgs84,bs='sos')\n")

            gammc <- gamm(cti~year+s(longitude_grid_wgs84,latitude_grid_wgs84,bs="sos"), data=dd,random=reStruct(object = ~ 1| id_plot, pdClass="pdDiag"),correlation=corAR1(form=~year))

            sgammc <-summary(gammc$gam)



            coefannee <- sgammc$p.coeff[2]
            ## se
            erreuran <- sgammc$se[2]
            ## erreur standard
            pval <-  sgammc$p.pv[2]



            tabcgamm <- data.frame(model = "gamm numeric(year) plot",annee=NA,coef=coefannee,se = erreuran,pval,signif=pval<seuilSignif,cti_predict=NA,ic_low95=NA,ic_up95=NA)

            tabgamm <- tabgamm[,colnames(tabcgamm)]


            tabgamm <- rbind(tabgamm,tabcgamm)

            write.csv(tabgamm,paste("Output/cti_gammPlot_",id,".csv",sep=""),row.names=FALSE)
        cat("\n  --> Output/cti_gammPlot_",id,".csv\n",sep="")
             }
        if (methode == "lmer") {
            cat("Method : lmer \n")


###################
#browser()
   ## LMER cti annual variations
            cat("\nEstimation de la variation annuelle lmer(cti~ factor(year)+(1|id_plot)\n")
library(lmerTest)

            md.f <- lmer(cti_eu~ factor(year)+(1|id_plot),data=dcti)

            smd.f<-summary(md.f)
            coefdata.f <-  as.data.frame(smd.f$coefficients)
            coefdata.f <- data.frame(model="Annual fluctuation", variable = rownames(coefdata.f),coefdata.f)

            ggdata <- data.frame(year=c(1989,as.numeric(substr(coefdata.f$variable[-1],13,16))),
                                 estimate=c(0,coefdata.f$Estimate[-1]),
                                 se=c(0,coefdata.f$Std..Error[-1]))
            ggdata$estimate <-  ggdata$estimate
            ggdata$se.sup <- ggdata$estimate +  ggdata$se
            ggdata$se.inf <- ggdata$estimate -  ggdata$se
            ggdata$estimate2 <- c(coefdata.f$Estimate[1],coefdata.f$Estimate[1] + coefdata.f$Estimate[-1])

            ggdata$se.sup2 <- ggdata$estimate2 +  ggdata$se
            ggdata$se.inf2 <- ggdata$estimate2 -  ggdata$se



            gg <- ggplot(ggdata,aes(x=year,y=estimate))+ geom_ribbon(ymin=ggdata$se.inf,ymax=ggdata$se.sup,alpha=.25)+geom_errorbar(ymin=ggdata$se.inf,ymax=ggdata$se.sup,width=0,alpha=.25)+ geom_point() + geom_line() + ylim(min(ggdata$se.inf),max(ggdata$se.sup)) + labs(x="Years",y="CTI variation")

            ggfile <- paste("output/cti",id,".png",sep="")
            ggsave(ggfile,gg)




            md.c <- lmer(cti_eu~ year+(1|id_plot),data=dcti)
            smd.c<-summary(md.c)

            coefdata.c <-  as.data.frame(smd.c$coefficients)
            coefdata.c <- data.frame(model = "Linear trend", variable = rownames(coefdata.c),coefdata.c)

            coefdata <- rbind(coefdata.c,coefdata.f)


write.csv(coefdata,paste("Output/lmer_coefficient_CTI",id,".csv",sep=""),row.names=FALSE)
write.csv(ggdata,paste("Output/ggdata_CTI",id,".csv",sep=""),row.names=FALSE)


            smd.file <- paste("output/summary_lmer_CTI_",id,".txt",sep="")

            sink(smd.file)
            print(smd.f)
            print("")
            print("")
            print("_________________________________________________________")
            print("")
            print("")

            print(smd.c)
            sink()



#####################






        }


    } else {
        if(query) {
            if(is.null(con)) con <- openDB.PSQL()
            if(is.null(firstYear)) firstYear <- 2000
            if(is.null(lastYear)) lastYear <- 9999


            start <- Sys.time()
            dateExport <- format(start,"%Y-%m-%d")
            queryCTI <- paste("select  id_point, p.id_carre, annee, p.commune,p.insee,p.departement,
code_sp, e.scientific_name, e.french_name, e.english_name, e.euring,
abond_brut as abondance_brut, abond as abondance, sti_europe as sti, sti_europe * abond::real as sti_ab , sti_europe * abond_brut::real as sti_ab_brut,
p.altitude, c.altitude as altitude_carre, longitude_wgs84,latitude_wgs84,longitude_grid_wgs84,latitude_grid_wgs84,
'",dateExport,"'::varchar(10) as date_export,
'",operateur[1],"'::varchar(50) as operateur,
'",operateur[2],"'::varchar(50) as email_operateur
from (select id_point, annee,code_sp,max(abond_brut) as abond_brut,max(abond) as abond
	from
        (select id_point, annee,date,passage_stoc,code_sp,sum(abond_brut) as abond_brut,sum(abond) as abond
        from (select id_inventaire,code_sp,abond as abond_brut, abond100_seuil95 as seuil_abondance_sp,
        LEAST(abond,abond100_seuil95) as abond
                      from
                      espece_abondance_point_seuil as s,
                      (SELECT
                      id_inventaire,
                      code_sp,
                      sum(abondance) as abond
                      FROM
                      observation as o, inventaire as i
                      WHERE
                      distance_contact in ('LESS25','LESS100') and passage_stoc in (1,2) and etude in ('STOC-EPS') and i.annee >= ",firstYear," and i.annee <= ",lastYear," and
                      o.id_inventaire = i.pk_inventaire
                      GROUP BY
                      id_inventaire, code_sp) as op
                      WHERE
                      op.code_sp = s.pk_species) as ot,
                      inventaire as i
                      where
                      ot.id_inventaire = i.pk_inventaire
                      group by id_point, annee,date,passage_stoc,code_sp) as op
         group by id_point, annee,code_sp) as om, carre as c, point as p, species as e, species_indicateur_fonctionnel as i
where
om.id_point = p.pk_point and p.id_carre = c.pk_carre and om.code_sp = e.pk_species and om.code_sp = i.pk_species and c.altitude <= ",altitude,"
order by id_point, annee,code_sp;",sep="")
            d2 <- dbGetQuery(con, queryCTI)

                                        #if(savePostgres) sqlCreateTable(con,"outcome.FrenchBBS_montaneOpen",d)

            d2_sti <- aggregate(sti_ab~id_point + annee, data = d2, sum)
            d2_ab_tot <-  aggregate(abondance~id_point + annee, data = d2, sum)
            d2x <- aggregate(longitude_grid_wgs84~id_point,data=d2,mean)
            d2y <- aggregate(latitude_grid_wgs84~id_point,data=d2,mean)
            d2alt <- aggregate(altitude ~ id_point,data=d2,mean)
            dd2 <- merge(d2_sti,d2_ab_tot ,by=c("id_point","annee"))
            dd2$cti <- dd2$sti_ab/dd2$abondance
            dd2 <- merge(dd2,d2x,by="id_point")
            dd2 <- merge(dd2,d2y,by="id_point")
            dd2 <- merge(dd2,d2alt,by="id_point")

            write.csv2(dd,paste("export/",fileName,"_point.csv",sep=""),row.names=FALSE)

            end <- Sys.time() ## heure de fin




        } else {
            dd2 <- read.csv2(paste("export/",fileName,"_point.csv",sep=""),dec=".")
        }


        ## gamm to assess annual cti values






        annee <- sort(unique(dd$annee))
        nban <- length(annee)
        pasdetemps <-nban-1

        ## GLM cti annual variations



        gammf <- gamm(cti~factor(annee)+s(longitude_grid_wgs84,latitude_grid_wgs84,bs="sos"), data=dd2,random=reStruct(object = ~ 1| id_point, pdClass="pdDiag"),correlation=corAR1(form=~annee))

        sgammf<-summary(gammf$gam)


        coefdata=coefficients(gammf$gam)

        coefannee <- c(0,sgammf$p.coeff[2:nban])
        ## se
        erreuran <- c(0,sgammf$se[2:nban])
        ## erreur standard
        pval <-  c(1,sgammf$p.pv[2:nban])

        ## calcul des intervalle de confiance
        if(ic) {
            gammf.sim <- sim(gammf)
            ic_inf_sim <- c(0,tail(apply(coef(gammf.sim), 2, quantile,.025),pasdetemps))
            ic_sup_sim <- c(0,tail(apply(coef(gammf.sim), 2, quantile,.975),pasdetemps))
        } else
        {
            ic_inf_sim <- "not assessed"
            ic_sup_sim <- "not assessed"
        }



        tabfgamm <- data.frame(model = "gamm factor(year) point",annee,coef=coefannee,se = erreuran,lower_ci=ic_inf_sim,upper_ci=ic_sup_sim,pval,signif=pval<seuilSignif)



        gg <- ggplot(data=tabfgamm,aes(x=annee,y=coef))
        gg <- gg + geom_errorbar(aes(ymin=coef-se, ymax=coef+se), width=0,colour=couleur,alpha=0.5) + geom_line(size=1.5,colour=couleur)
        gg <- gg + geom_ribbon(aes(ymin=coef-se, ymax=coef+se),fill = couleur,alpha=.2)+ geom_point(size=3,colour=couleur)+ geom_point(size=1.5,colour="white")
        gg <- gg + labs(y="CTI",x="")+scale_x_continuous(breaks=pretty_breaks())


        ggsave(paste("Output/figCTI_point_",id,".png"),gg)



        gammc <- gamm(cti~annee+s(longitude_grid_wgs84,latitude_grid_wgs84,bs="sos"), data=dd,random=reStruct(object = ~ 1| id_point, pdClass="pdDiag"),correlation=corAR1(form=~annee))

        sgammc <-summary(gammc$gam)



        coefannee <- sgammc$p.coeff[2]
        ## se
        erreuran <- sgammc$se[2]
        ## erreur standard
        pval <-  sgammc$p.pv[2]


        if(ic) {
            gammc.sim <- sim(gammc)
            ic_inf_sim <- c(0,tail(apply(coef(gammc.sim), 2, quantile,.025),pasdetemps))
            ic_sup_sim <- c(0,tail(apply(coef(gammc.sim), 2, quantile,.975),pasdetemps))
        } else
        {
            ic_inf_sim <- "not assessed"
            ic_sup_sim <- "not assessed"
        }


        tabcgamm <- data.frame(model = "gamm numeric(year) point",annee=NA,coef=coefannee,se = erreuran,lower_ci=ic_inf_sim,upper_ci=ic_sup_sim,pval,signif=pval<seuilSignif)




        tabgamm <- rbind(tabfgamm,tabcgamm)
        write.csv(tabgamm,paste("Output/CTI_gammPoint_",id,".csv",sep=""),row.names=FALSE)

    }

    if(output) return(tabgamm)







}




######## End CTI-Europe

#### ----------------------------------------------------

trendBBS_listsp <- function(con=NULL,query=TRUE,ic = FALSE,listsp=c("PICPIC"),seuilSignif=0.05,output=FALSE,fileName="dataPICPIC.csv",firstYear=2001,lastYear=2016,altitude=800,
                            operateur=c("Lorrilliere Romain","lorrilliere@mnhn.fr"),id="PICPIC") {

    con=NULL;query=FALSE;ic =FALSE; listsp=c("PICPIC");seuilSignif = 0.05 ; fileName="dataPICPIC.csv";firstYear=2003;lastYear=2015;altitude=800;operateur=c("Lorrilliere Romain","lorrilliere@mnhn.fr");id = "PICPIC"


    if(query) {
        if(is.null(con)) con <- openDB.PSQL()
        if(is.null(firstYear)) firstYear <- 2000
        if(is.null(lastYear)) lastYear <- 9999


        start <- Sys.time()
        dateExport <- format(start,"%Y-%m-%d")
        queryCTI <- paste("select  id_carre, annee, commune,insee,departement,
                      code_sp, e.scientific_name, e.french_name,
                      abond_brut as abondance_brut, abond as abondance, sti,
                      altitude, longitude_grid_wgs84,latitude_grid_wgs84,
                      '",dateExport,"'::varchar(10) as date_export,
                      '",operateur[1],"'::varchar(50) as operateur,
                      '",operateur[2],"'::varchar(50) as email_operateur
                      from(select id_carre, annee,code_sp,max(abond_brut) as abond_brut,max(abond) as abond
                      from
                      (select id_carre, annee,date,passage_stoc,code_sp,sum(abond_brut) as abond_brut,sum(abond) as abond
                      from (select id_inventaire,code_sp,abond as abond_brut, abond100_seuil95 as seuil_abondance_sp,
                      LEAST(abond,abond100_seuil95) as abond
                      from
                      espece_abondance_point_seuil as s,
                      (SELECT
                      id_inventaire,
                      code_sp,
                      sum(abondance) as abond
                      FROM
                      observation as o, inventaire as i
                      WHERE
                      distance_contact in ('LESS25','LESS100') and passage_stoc in (1,2) and code_sp in ",," and
                      o.id_inventaire = i.pk_inventaire
                      GROUP BY
                      id_inventaire, code_sp) as op
                      WHERE
                      op.code_sp = s.pk_species) as ot,
                      inventaire as i
                      where
                      ot.id_inventaire = i.pk_inventaire
                      group by id_carre, annee,date,passage_stoc,code_sp) as oc
                      group by id_carre, annee,code_sp) as om, carre as c, species as e, species_indicateur_fonctionnel as i
                      where
                      om.id_carre = c.pk_carre and om.code_sp = e.pk_species and om.code_sp = i.pk_species
                      order by id_carre, annee,code_sp;",sep="")
        d <- dbGetQuery(con, queryCTI)

                                        #if(savePostgres) sqlCreateTable(con,"outcome.FrenchBBS_montaneOpen",d)

        write.csv2(d,paste("export/",fileName,sep=""),row.names=FALSE)

        end <- Sys.time() ## heure de fin




    } else {
        d <- read.csv2(paste("export/",fileName,sep=""),dec=".")
        d <- subset(d,annee >= firstYear & annee <= lastYear & altitude <= 800)

    }




    annee <- sort(unique(dd$annee))
    pasdetemps <-length(unique(dd$annee))-1



    ## calcul des intervalle de confiance
    if(ic) {
        glm1.sim <- sim(glm1)
        ic_inf_sim <- c(1,tail(apply(coef(glm1.sim), 2, quantile,.025),pasdetemps))
        ic_sup_sim <- c(1,tail(apply(coef(glm1.sim), 2, quantile,.975),pasdetemps))
    } else
    {
        ic_inf_sim <- "not assessed"
        ic_sup_sim <- "not assessed"
    }


    if(output) return(tab)
}









ctri_national <- function(con=NULL,query=FALSE,carre = TRUE,
                          firstYear=NULL,lastYear=NULL,altitude=NULL,departement=NULL,onf=TRUE,distance_contact=NULL,
                          spExcluPassage1=c("MOTFLA","SAXRUB","ANTPRA","OENOEN","PHYTRO"),# (Prince et al. 2013 Env. Sc. and Pol.) + "OENOEN","PHYTRO" avis d'expert F. Jiguet
                         seuilAbondance=.99,
                          champSp = "code_sp", sp=NULL,champsHabitat=FALSE,
                          anglais=TRUE,seuilSignif=0.05,
                          couleur="#4444c3",
                          titreY="Index Trophique des Communaut�s",titreX="Ann�es",titre="R�gime alimentaire des oiseaux",
                          savePostgres=FALSE,output=FALSE,
                          operateur=c("Lorrilliere Romain","lorrilliere@mnhn.fr"), encodingSave="ISO-8859-1",fileName="dataCTrI"){
    if(carre){
        if(query) {


            start <- Sys.time()
            dateExport <- format(start,"%Y-%m-%d")

   if(is.null(con)) con <- openDB.PSQL()
    if(is.null(firstYear)) firstYear <- 2001
    if(is.null(lastYear)) lastYear <- 9999
    if(is.null(altitude)) altitude <- 10000
    if(is.null(distance_contact))
        distance_contact_txt <- "" else if(distance_contact == "100")
                                       distance_contact_txt <- " distance_contact in ('LESS25','LESS100') and " else if(distance_contact == "200")
                                                                                                                    distance_contact_txt <- " distance_contact in ('LESS25','LESS100','MORE100','LESS200') and " else stop("distance_contact non pris en charge")

    seuil_txt <- as.character(trunc(seuilAbondance*100))
    if(is.null(distance_contact))
        champ_seuil <- paste("abondAll_seuil",seuil_txt,sep="") else if(distance_contact == "100")
                                                                    champ_seuil <- paste("abond100_seuil",seuil_txt,sep="") else if(distance_contact == "200")
                                                                                                                                champ_seuil <- paste("abond200_seuil",seuil_txt,sep="")else stop("distance_contact non pris en charge")

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
    selectQueryOp1 <- "SELECT id_inventaire, code_sp, sum(abondance) as abond
				FROM
				observation as o, inventaire as i,carre_annee as ca, carre as c
				WHERE
				o.id_inventaire = i.pk_inventaire and
				o.id_carre = c.pk_carre and
				o.id_carre = ca.id_carre and o.annee=ca.annee and
				ca.qualite_inventaire_stoc > 0 and
                                "
     selectQueryOp2 <- "
			GROUP BY
			id_inventaire, code_sp"
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






    if(!is.null(departement)) depList <- paste("('",paste(departement,collapse="' , '"),"')",sep="")



    selectQuery <- paste(distance_contact_txt," i.annee >= ",firstYear,"  and i.annee <= ",lastYear," and c.etude in ('STOC_EPS'",ifelse(onf,", 'STOC_ONF'",""),")  and  c.altitude <= ",altitude," ",ifelse(is.null(departement),"",paste(" and departement in ",depList," "))," ", sep="")


        queryCTrI <- paste("
select ctri.id_carre_annee, ca.id_carre, ca.annee, ctri, ctri_brut,qualite_inventaire_stoc, commune,insee,departement,
 altitude, longitude_grid_wgs84,latitude_grid_wgs84, ",
ifelse(champsHabitat," foret_p, ouvert_p, agri_p, urbain_p,
foret_ps, ouvert_ps, agri_ps, urbain_ps,
nbp_foret_p, nbp_ouvert_p, nbp_agri_p, nbp_urbain_p,
nbp_foret_ps, nbp_ouvert_ps, nbp_agri_ps, nbp_urbain_ps, ",""),
"'",dateExport,"'::varchar(10) as date_export,
'",operateur[1],"'::varchar(50) as operateur,
'",operateur[2],"'::varchar(50) as email_operateur
  from -- ## begin ## calcul crti
  (select id_carre_annee, sum(stri_ab) as sum_stri_ab, sum(abond) as sum_abond, sum(stri_ab)/sum(abond)::real as ctri ,
   sum(stri_ab_brut) as sum_stri_ab_brut, sum(abond_brut) as sum_abond_brut, sum(stri_ab_brut)/sum(abond_brut)::real as ctri_brut
   from ( -- ## begin ## ajout des valeur stri
     select  id_carre_annee, code_sp, abond_brut, abond,exp_stri, exp_stri * abond::real as stri_ab , exp_stri * abond_brut::real as stri_ab_brut
     from (-- ## begin ##  abondance carre annee
     select oc.id_carre, oc.annee, (oc.annee::varchar(4)||oc.id_carre::varchar(100))::varchar(100) as id_carre_annee,
     code_sp,abond_brut, abond
     from( -- ## begin ## somme sur carre des max par points sur 2 passages
     	select id_carre, annee,code_sp,sum(abond_brut) as abond_brut,sum(abond) as abond
     	from -- ## begin ## obs max par point sur 2 passages
     		(select id_point, annee,code_sp,max(abond_brut) as abond_brut,max(abond) as abond
     		from -- ## begin ## correction abondance obs par seuil
     			(select id_inventaire,id_point, passage_stoc, annee, code_sp,abond as abond_brut, abondAll_seuil99 as seuil_abondance_sp,LEAST(abond, abondAll_seuil99) as abond
     			from -- ## begin ## selection classe de distance et different filtre
     				(",selectQueryOp,"
     				) -- selection classe de distance et different filtre
     			as opb, espece_abondance_point_seuil as s, inventaire as i
     			WHERE
     			opb.code_sp = s.pk_species and opb.id_inventaire = i.pk_inventaire

     			) -- ## end ## correction abondance obs par seuil
     		as op
     		group by id_point,annee,code_sp
     		) -- ## end ## obs max par point sur 2 passages
     	as omp, point as p
     	where omp.id_point = p.pk_point
     	group by id_carre, annee,code_sp) -- ## end ## somme sur carre des max par points sur 2 passages
     as oc ) -- ## end ## abondance carre annee
   as abc,  species_indicateur_fonctionnel as sif
   where abc.code_sp = sif.pk_species
   ) -- ## end ## ajout des valeur stri
 as abstri
group by id_carre_annee) -- ## end ##  calcul crti
 as ctri, carre_annee as ca,carre as c
where
ctri.id_carre_annee = ca.pk_carre_annee and ca.id_carre = c.pk_carre
order by
ca.id_carre, annee ;",sep="")



                                        # browser()
            cat("\nRequete CTrI:\n\n",queryCTrI,"\n\n")

            dd <- dbGetQuery(con, queryCTrI)


            if(anglais)
                dd <- fr2eng(dd)
            ## if(savePostgres) sqlCreateTable(con,tableSQL,d)


                write.csv(dd,paste("export/",fileName,"_carre.csv",sep=""),row.names=FALSE)

                end <- Sys.time() ## heure de fin




            } else {
                dd <- read.csv(paste("export/",fileName,"_carre.csv",sep=""),dec=".")
                if(anglais)
                    dd <- fr2eng(dd)
            }


            annee <- sort(unique(dd$year))
            nban <- length(annee)
            pasdetemps <-nban-1

            ## GAMM cti annual variations
            cat("\nEstimation de la variation annuelle ctri~ factor(year)+s(longitude_grid_wgs84,latitude_grid_wgs84,bs='sos')\n")
        gammf <- gamm(ctri~ factor(year)+s(longitude_grid_wgs84,latitude_grid_wgs84,bs="sos"), data=dd,random=reStruct(object = ~ 1| id_plot, pdClass="pdDiag"),correlation=corAR1(form=~year))

            sgammf<-summary(gammf$gam)
            coefdata=coefficients(gammf$gam)
            coefannee <- c(0,sgammf$p.coeff[2:nban])
            erreuran <- c(0,sgammf$se[2:nban])
            pval <-  c(1,sgammf$p.pv[2:nban])

            ## confindence intervalles

            tabfgamm <- data.frame(model = "gamm factor(year) plot",annee,coef=coefannee,se = erreuran,pval,signif=pval<seuilSignif)


            cat("\nGam pour la figure ctri~s(year)\n")
            ## create a sequence of temperature that spans your temperature
            ## http://zevross.com/blog/2014/09/15/recreate-the-gam-partial-regression-smooth-plots-from-r-package-mgcv-with-a-little-style/

            gammgg <- gamm(ctri~s(year), data=dd,random=reStruct(object = ~ 1| id_plot, pdClass="pdDiag"),correlation=corAR1(form=~year))

            maxyear<-max(dd$year)
            minyear<-min(dd$year)
            year.seq<-sort(unique(c(minyear:maxyear,(seq(minyear, maxyear,length=1000)))))
            year.seq<-data.frame(year=year.seq)

                                        # predict only the temperature term (the sum of the
                                        # term predictions and the intercept gives you the overall
                                        # prediction)

            preds<-predict(gammgg$gam, newdata=year.seq, type="terms", se.fit=TRUE)


                                        # set up the temperature, the fit and the upper and lower
                                        # confidence interval

            year <-year.seq$year
            realYear <- sort(unique(dd$year))
            fit<-as.vector(preds$fit)
            init <- fit[1]

            fit.up95<-fit-1.96*as.vector(preds$se.fit)

            fit.low95<-fit+1.96*as.vector(preds$se.fit)

        #    ggGamData <- data.frame(year=year, CTrI=fit,ic_low95 = fit.low95, ic_up95 = fit.up95)

        fit <- fit - init
         fit.up95 <- fit.up95 - init
        fit.low95 <- fit.low95 - init

         ggGamData <- data.frame(year=year, CTrI=fit,ic_low95 = fit.low95, ic_up95 = fit.up95)
        ## The ggplot:
            gg <- ggplot(data=ggGamData,aes(x=year,y=CTrI))
           gg <- gg + geom_ribbon(aes(ymin=ic_low95, ymax=ic_up95),fill = couleur,alpha=.2)+ geom_line(size=1,colour=couleur)
            gg <- gg +  geom_point(data = subset(ggGamData,year %in% realYear),size=3,colour=couleur) + geom_point(data = subset(ggGamData,year %in% realYear),size=1.5,colour="white")
            gg <- gg + labs(y=titreY,x=titreX,title=titre)+scale_x_continuous(breaks=pretty_breaks())

            ggsave(paste("Output/figCTrI_plot",id,".png",sep=""),gg)
            cat("\n--> Output/figCTrI_plot",id,".png\n",sep="")

            tabPredict <- subset(ggGamData,year %in% realYear)
            colnames(tabPredict)[1:2] <- c("annee","CTrI_predict")
            tabgamm <- merge(tabfgamm,tabPredict,by="annee")

            cat("\nEstimation de la tendence  ctri~ year+s(longitude_grid_wgs84,latitude_grid_wgs84,bs='sos')\n")

            gammc <- gamm(ctri~year+s(longitude_grid_wgs84,latitude_grid_wgs84,bs="sos"), data=dd,random=reStruct(object = ~ 1| id_plot, pdClass="pdDiag"),correlation=corAR1(form=~year))

            sgammc <-summary(gammc$gam)



            coefannee <- sgammc$p.coeff[2]
            ## se
            erreuran <- sgammc$se[2]
            ## erreur standard
            pval <-  sgammc$p.pv[2]



            tabcgamm <- data.frame(model = "gamm numeric(year) plot",annee=NA,coef=coefannee,se = erreuran,pval,signif=pval<seuilSignif,CTrI_predict=NA,ic_low95=NA,ic_up95=NA)

            tabgamm <- tabgamm[,colnames(tabcgamm)]


            tabgamm <- rbind(tabgamm,tabcgamm)

            write.csv(tabgamm,paste("Output/CTrI_gammPlot_",id,".csv",sep=""),row.names=FALSE)
            cat("\n  --> Output/CTrI_gammPlot_",id,".csv\n",sep="")
        } else {

            cat("\n \n !!!!!!!!!! ATTENTION REQUETE ET MODELE STAT PAS A JOUR !!!!! \n\n\n")
            if(query) {
                if(is.null(con)) con <- openDB.PSQL()
                if(is.null(firstYear)) firstYear <- 2000
                if(is.null(lastYear)) lastYear <- 9999


                start <- Sys.time()
                dateExport <- format(start,"%Y-%m-%d")
                queryCTrI <- paste("select  id_point, p.id_carre, annee, p.commune,p.insee,p.departement,
code_sp, e.scientific_name, e.french_name, e.english_name, e.euring,
abond_brut as abondance_brut, abond as abondance, stri, exp_stri * abond::real as stri_ab , exp_stri * abond_brut::real as stri_ab_brut,
p.altitude, c.altitude as altitude_carre, longitude_wgs84,latitude_wgs84,longitude_grid_wgs84,latitude_grid_wgs84,
'",dateExport,"'::varchar(10) as date_export,
'",operateur[1],"'::varchar(50) as operateur,
'",operateur[2],"'::varchar(50) as email_operateur
from (select id_point, annee,code_sp,max(abond_brut) as abond_brut,max(abond) as abond
	from
        (select id_point, annee,date,passage_stoc,code_sp,sum(abond_brut) as abond_brut,sum(abond) as abond
        from (select id_inventaire,code_sp,abond as abond_brut, abondall_seuil99 as seuil_abondance_sp,
        LEAST(abond,abondall_seuil99) as abond
                      from
                      espece_abondance_point_seuil as s,
                      (SELECT
                      id_inventaire,
                      code_sp,
                      sum(abondance) as abond
                      FROM
                      observation as o, inventaire as i
                      WHERE
                      passage_stoc in (1,2) and i.annee >= ",firstYear," and i.annee <= ",lastYear," and
                      o.id_inventaire = i.pk_inventaire
                      GROUP BY
                      id_inventaire, code_sp) as op
                      WHERE
                      op.code_sp = s.pk_species) as ot,
                      inventaire as i
                      where
                      ot.id_inventaire = i.pk_inventaire
                      group by id_point, annee,date,passage_stoc,code_sp) as op
         group by id_point, annee,code_sp) as om, carre as c, point as p, species as e, species_indicateur_fonctionnel as i
where
om.id_point = p.pk_point and p.id_carre = c.pk_carre and om.code_sp = e.pk_species and om.code_sp = i.pk_species and c.altitude <= ",altitude,"
order by id_point, annee,code_sp;",sep="")


                cat("\n",queryCTrI,"\n\n")

                d2 <- dbGetQuery(con, queryCTrI)

                                        #if(savePostgres) sqlCreateTable(con,"outcome.FrenchBBS_montaneOpen",d)

                d2_sti <- aggregate(sti_ab~id_point + annee, data = d2, sum)
                d2_ab_tot <-  aggregate(abondance~id_point + annee, data = d2, sum)
                d2x <- aggregate(longitude_grid_wgs84~id_point,data=d2,mean)
                d2y <- aggregate(latitude_grid_wgs84~id_point,data=d2,mean)
                d2alt <- aggregate(altitude ~ id_point,data=d2,mean)
                dd2 <- merge(d2_sti,d2_ab_tot ,by=c("id_point","annee"))
                dd2$cti <- dd2$sti_ab/dd2$abondance
                dd2 <- merge(dd2,d2x,by="id_point")
                dd2 <- merge(dd2,d2y,by="id_point")
                dd2 <- merge(dd2,d2alt,by="id_point")

                write.csv2(dd,paste("export/",fileName,"_point.csv",sep=""),row.names=FALSE)

                end <- Sys.time() ## heure de fin




            } else {
                dd2 <- read.csv2(paste("export/",fileName,"_point.csv",sep=""),dec=".")
            }


            ## gamm to assess annual cti values






            annee <- sort(unique(dd$annee))
            nban <- length(annee)
            pasdetemps <-nban-1

            ## GLM cti annual variations



            gammf <- gamm(cti~factor(annee)+s(longitude_grid_wgs84,latitude_grid_wgs84,bs="sos"), data=dd2,random=reStruct(object = ~ 1| id_point, pdClass="pdDiag"),correlation=corAR1(form=~annee))

            sgammf<-summary(gammf$gam)


            coefdata=coefficients(gammf$gam)

            coefannee <- c(0,sgammf$p.coeff[2:nban])
            ## se
            erreuran <- c(0,sgammf$se[2:nban])
            ## erreur standard
            pval <-  c(1,sgammf$p.pv[2:nban])

            ## calcul des intervalle de confiance
            if(ic) {
                gammf.sim <- sim(gammf)
                ic_inf_sim <- c(0,tail(apply(coef(gammf.sim), 2, quantile,.025),pasdetemps))
                ic_sup_sim <- c(0,tail(apply(coef(gammf.sim), 2, quantile,.975),pasdetemps))
            } else
            {
                ic_inf_sim <- "not assessed"
                ic_sup_sim <- "not assessed"
            }



            tabfgamm <- data.frame(model = "gamm factor(year) point",annee,coef=coefannee,se = erreuran,lower_ci=ic_inf_sim,upper_ci=ic_sup_sim,pval,signif=pval<seuilSignif)



            gg <- ggplot(data=tabfgamm,aes(x=annee,y=coef))
            gg <- gg + geom_errorbar(aes(ymin=coef-se, ymax=coef+se), width=0,colour=couleur,alpha=0.5) + geom_line(size=1.5,colour=couleur)
            gg <- gg + geom_ribbon(aes(ymin=coef-se, ymax=coef+se),fill = couleur,alpha=.2)+ geom_point(size=3,colour=couleur)+ geom_point(size=1.5,colour="white")
                                        # gg <- gg + geom_abline(slope=tabc$coef[1],intercept = interSlope, linetype="dashed",size=1.2,colour=couleur)
            gg <- gg + labs(y="CTrI",x="")+scale_x_continuous(breaks=pretty_breaks())
                                        # gg <- gg + geom_smooth(method="glm")


            ggsave(paste("Output/figCTrI_point_",id,".png"),gg)



            gammc <- gamm(ctri~annee+s(longitude_grid_wgs84,latitude_grid_wgs84,bs="sos"), data=dd,random=reStruct(object = ~ 1| id_point, pdClass="pdDiag"),correlation=corAR1(form=~annee))

            sgammc <-summary(gammc$gam)



            coefannee <- sgammc$p.coeff[2]
            ## se
            erreuran <- sgammc$se[2]
            ## erreur standard
            pval <-  sgammc$p.pv[2]


            if(ic) {
                gammc.sim <- sim(gammc)
                ic_inf_sim <- c(0,tail(apply(coef(gammc.sim), 2, quantile,.025),pasdetemps))
                ic_sup_sim <- c(0,tail(apply(coef(gammc.sim), 2, quantile,.975),pasdetemps))
            } else
            {
                ic_inf_sim <- "not assessed"
                ic_sup_sim <- "not assessed"
            }


            tabcgamm <- data.frame(model = "gamm numeric(year) point",annee=NA,coef=coefannee,se = erreuran,lower_ci=ic_inf_sim,upper_ci=ic_sup_sim,pval,signif=pval<seuilSignif)




            tabgamm <- rbind(tabfgamm,tabcgamm)
            write.csv(tabgamm,paste("Output/CTrI_gammPoint_",id,".csv",sep=""),row.names=FALSE)

        }

        if(output) return(tabgamm)







    }




stis <- function() {



    d.sti <- dbReadTable(con,"species_indicateur_fonctionnel")
 d.sti1 <- d.sti[,c(1:6,8:9)]
    colnames(d.sti1)[c(1,7,8)] <- c("french_species_id","sti_devictor","sti_lehikoinen")
write.csv(d.sti1,"output/theSTIs.csv",row.names=FALSE)

    d.sti2 <- read.csv("output/theSTIs.csv",encoding="UTF-8")

    d.sti <- d.sti[,c("pk_species","sti","sti_europe")]
    colnames(d.sti)[2:3] <- c("sti_devictor","sti_lehikoinen")
    d.sti <- subset(d.sti,!(is.na(sti_devictor)))

    gg <- ggplot(d.sti,aes(x=sti_lehikoinen,y=sti_devictor,label=pk_species))+ geom_point()
    ggsave("output/theCTIs.png",gg)
}











winterCTI <- function(mp) {

        d <- read.delim("DB_import/export_shoc_04072018_155413.txt",header=TRUE,encoding="UTF-8",stringsAsFactors=FALSE)

        dd <- subset(d,d$Distance.de.contact %in% c("LESS25","LESS100")& N..Passage > 1,select=c("Code.inventaire","N..Carr?PS","Date","N..Passage","EXPORT_STOC_TEXT_EPS_POINT","Esp?","Nombre","EPS.P.Milieu"))

        dd$Esp?[dd$Esp?==""] <- "OTHERS"
        dd$Esp? <- substr(dd$Esp?,1,6)

        dd$annee <- as.numeric(substr(dd$Date,7,10))

        ddinv <- aggregate(Nombre~Code.inventaire+N..Carr?PS+Date+N..Passage+EXPORT_STOC_TEXT_EPS_POINT+Esp?+EPS.P.Milieu,dd,sum)

        ddinv$an <- as.numeric(substr(ddinv$Date,7,10))


        ddan <- aggregate(Nombre~N..Carr?PS+EXPORT_STOC_TEXT_EPS_POINT+Esp?+an,ddinv,max)
        ddcarre <- aggregate(Nombre~N..Carr?PS+Esp?+an,ddan,sum)

        colnames(ddcarre) <- c("carre","ESPECE","annee","abundance")






 ##   con=NULL;user="romain"

        if(is.null(con)) con <- openDB.PSQL(user,mp)


        d.sti <- dbReadTable(con,"species_indicateur_fonctionnel")
        d.sti <- d.sti[,c("pk_species","sti","sti_europe")]
        colnames(d.sti)[1] <- "ESPECE"



       ddcarre <- merge(ddcarre,d.sti,by="ESPECE",all.x=TRUE)
        ddcarre$nsti <- ifelse(is.na(ddcarre$sti),0,ddcarre$abundance * ddcarre$sti)
        ddcarre$nsti_europe <- ifelse(is.na(ddcarre$sti_europe),0,ddcarre$abundance * ddcarre$sti_europe)

        ddcarre$n_france <- ddcarre$abundance*as.numeric(!is.na(ddcarre$sti))
        ddcarre$n_europe <- ddcarre$abundance*as.numeric(!is.na(ddcarre$sti_europe))




        agg.winter <- aggregate(cbind(ddcarre$n_france,ddcarre$n_europe,ddcarre$nsti,ddcarre$nsti_europe)~ carre +annee ,data=ddcarre,FUN=sum)

        colnames(agg.winter)[3:6] <- c("N_france","N_europe","Nsti","NSTI_europe")
        agg.winter$cti_france <- agg.winter$Nsti / agg.winter$N_france
        agg.winter$cti_europe <- agg.winter$NSTI_europe / agg.winter$N_europe

        dcti.winter <- agg.winter[,c("carre","annee","cti_europe")]
        colnames(dcti.winter) <- c("id_plot","year","cti_eu")



   ## LMER cti annual variations
       cat("\nEstimation de la variation annuelle lmer(cti~ factor(year)+(1|id_plot)\n")

       library(lmerTest)
            md.f <- lmer(cti_eu~ factor(year)+(1|id_plot),data=dcti.winter)

            smd.f<-summary(md.f)
            coefdata.f <-  as.data.frame(smd.f$coefficients)
            coefdata.f <- data.frame(model="Annual fluctuation", variable = rownames(coefdata.f),coefdata.f)

            ggdata <- data.frame(year=c(min(dcti.winter$year),as.numeric(substr(coefdata.f$variable[-1],13,16))),
                                 estimate=c(0,coefdata.f$Estimate[-1]),
                                 se=c(0,coefdata.f$Std..Error[-1]))

       ggdata$estimate <-  ggdata$estimate

       ggdata$se.sup <- ggdata$estimate +  ggdata$se
       ggdata$se.inf <- ggdata$estimate -  ggdata$se

       ggdata$estimate2 <- c(coefdata.f$Estimate[1],coefdata.f$Estimate[1] + coefdata.f$Estimate[-1])

     ggdata$se.sup2 <- ggdata$estimate2 +  ggdata$se
       ggdata$se.inf2 <- ggdata$estimate2 -  ggdata$se

            gg <- ggplot(ggdata,aes(x=year,y=estimate2))+ geom_ribbon(ymin=ggdata$se.inf2,ymax=ggdata$se.sup2,alpha=.25)+geom_errorbar(ymin=ggdata$se.inf2,ymax=ggdata$se.sup2,width=0,alpha=.25)+ geom_point() + geom_line() + ylim(min(ggdata$se.inf2),max(ggdata$se.sup2)) + labs(x="Years",y="winter CTI")
            ggsave("output/cti_winter_europe_france_1989_2017.png",gg)




            md.c <- lmer(cti_eu~ year+(1|id_plot),data=dcti)
            smd.c<-summary(md.c)

            coefdata.c <-  as.data.frame(smd.c$coefficients)
            coefdata.c <- data.frame(model = "Linear trend", variable = rownames(coefdata.c),coefdata.c)

            coefdata <- rbind(coefdata.c,coefdata.f)


write.csv(coefdata,"Output/lmer_coefficient_CTIwinter.csv",row.names=FALSE)
write.csv(ggdata,"Output/ggdata_CTIwinter.csv",row.names=FALSE)


       smd.file <- paste("output/summary_lmer_CTIwinter.txt",sep="")

            sink(smd.file)
            print(smd.f)
            print("")
            print("")
            print("_________________________________________________________")
            print("")
            print("")

            print(smd.c)
            sink()



}



figAllCTIs <- function() {

    cti.b <- read.csv("output/ggdata_CTIeurope_France_aksu.csv")
    cti.bw <- read.csv("output/ggdata_CTIbreedingWinterSp_europe_france_Lehikoinen.csv")
    cti.w <- read.csv("output/ggdata_CTIwinter.csv")

    cti.b$variable <- "summer cti\n(all species)"
    cti.bw$variable <- "summer cti\n(wintering species)"
    cti.w$variable <- "winter cti"

    ggdata <- rbind(rbind(cti.b,cti.bw),cti.w)


            gg <- ggplot(ggdata,aes(x=year,y=estimate2,colour=variable,fill=variable,group=variable))+ geom_ribbon(ymin=ggdata$se.inf2,ymax=ggdata$se.sup2,alpha=.25,colour=NA)+geom_errorbar(ymin=ggdata$se.inf2,ymax=ggdata$se.sup2,width=0,alpha=.25)+ geom_point() + geom_line() + ylim(min(ggdata$se.inf2),max(ggdata$se.sup2)) + labs(x="Years",y="CTI")
gg
    ggsave("output/ctis_winter_summer_europe_france_1989_2017.png",gg)




    cti.b <- read.csv("output/lmer_coefficient_CTI_europe_France_Lehikoinen.csv")
    cti.bw <- read.csv("output/lmer_coefficient_CTIbreedingWinterSp_europe_france_Lehikoinen.csv")
    cti.w <- read.csv("output/lmer_coefficient_CTIwinter.csv")

    cti.b$cti <- "summer cti (all species)"
    cti.bw$cti <- "summer cti (wintering species)"
    cti.w$cti <- "winter cti"



    cti <- rbind(rbind(cti.b,cti.bw),cti.w)
cti$year <- as.numeric(substr(cti$variable,13,17))

    cti <- cti[,c(8,1,2,9,3:7)]
colnames(cti)[5:9] <- c("estimate","se","df","t_value","p_value")

    write.csv(cti,"output/CTIs_summer_winter_france_1989_2017.csv",row.names=FALSE)

}