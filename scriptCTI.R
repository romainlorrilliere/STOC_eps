par(mfrow=c(1,2))
ls() 
rm(list=ls(all=TRUE))
memory.size()
gc()
memory.size()
memory.limit(size=22000)





species<-read.table("STI.txt",h=T)
PSSI<-as.matrix(species[,2])
d=read.table("tablopoint1989-2013trifinal.txt",h=T)

MULTI<-function(x){
sum(x*PSSI)/sum(x)}
CTI<-apply(d,1,MULTI)
write.table(CTI, "CTIpoint1989-2013tab.txt")


#################Milieu aquatique##########
#1995 à 2013
csi1=read.table("CTIpoint1989-2013habF.txt",h=T)
summary(csi1)
attach(csi1)

library(nlme)
library(mgcv)
mcsiF<-gamm(CTI~factor(annee)+s(x,y,bs="sos"), data=csi1,random=reStruct(object = ~ 1| unique, pdClass="pdDiag"),correlation=corAR1(form=~annee))
smgF<-summary(mcsiF$gam)
smgF

coefdata=coefficients(mcsiF$gam)
coefdata=matrix(coefdata)
coefdata=coefdata[1:19]
coefdata[[1]]<-0
erreurmatrix=matrix(smgF$se)
erreurmatrix=erreurmatrix[1:19]
erreurmatrix[[1]]<-0
plot(coefdata,pch=16,col="red",ylim=c(min(coefdata-(erreurmatrix)),max(coefdata+(erreurmatrix))),xaxt="n",ylab="",xlab="",main="Variation annuelle du CTI en milieu aquatique") 
axis(1,1:19,labels = c(1995:2013))
abline(h=0,col="grey")
lines(coefdata,col="red")
lines(coefdata-(erreurmatrix),col="blue",lty="dashed")
lines(coefdata+(erreurmatrix),col="blue",lty="dashed")
legend("topleft",legend=c("Variation annuelle du CTI par point STOC","Indice +/- erreur"),col=c("red","blue"),lty=c(1,2),bty="o",pt.cex=1,cex=0.8,text.col="black",horiz=F,inset=c(0.03,0.03))

anneebase1=annee-1994
mcsibisF<-gamm(CTI~anneebase1+s(x,y,bs="sos"), data=csi1,random=reStruct(object = ~ 1|unique, pdClass="pdDiag"),correlation=corAR1(form=~anneebase1))
smgbisF<-summary(mcsibisF$gam)
smgbisF

toto=hist(CTI)
toto
sum(toto$density)



#2003 à 2013
csi1=read.table("CTIpoint2003-2013habF.txt",h=T)
summary(csi1)
attach(csi1)

library(nlme)
library(mgcv)
mcsiF<-gamm(CTI~factor(annee)+s(x,y,bs="sos"), data=csi1,random=reStruct(object = ~ 1| unique, pdClass="pdDiag"),correlation=corAR1(form=~annee))
smgF<-summary(mcsiF$gam)
smgF

coefdata=coefficients(mcsiF$gam)
coefdata=matrix(coefdata)
coefdata=coefdata[1:11]
coefdata[[1]]<-0
erreurmatrix=matrix(smgF$se)
erreurmatrix=erreurmatrix[1:11]
erreurmatrix[[1]]<-0
plot(coefdata,pch=16,col="red",ylim=c(min(coefdata-(erreurmatrix)),max(coefdata+(erreurmatrix))),xaxt="n",ylab="",xlab="",main="Variation annuelle du CTI en milieu aquatique") 
axis(1,1:11,labels = c(2003:2013))
abline(h=0,col="grey")
lines(coefdata,col="red")
lines(coefdata-(erreurmatrix),col="blue",lty="dashed")
lines(coefdata+(erreurmatrix),col="blue",lty="dashed")
legend("topleft",legend=c("Variation annuelle du CTI par point STOC","Indice +/- erreur"),col=c("red","blue"),lty=c(1,2),bty="o",pt.cex=1,cex=0.8,text.col="black",horiz=F,inset=c(0.03,0.03))

anneebase1=annee-2002
mcsibisF<-gamm(CTI~anneebase1+s(x,y,bs="sos"), data=csi1,random=reStruct(object = ~ 1|unique, pdClass="pdDiag"),correlation=corAR1(form=~anneebase1))
smgbisF<-summary(mcsibisF$gam)
smgbisF

#################Milieu ouvert##########
#1989 à 2013
csi1=read.table("CTIpoint1989-2013habC.txt",h=T)
summary(csi1)
attach(csi1)

mcsiC<-gamm(CTI~factor(annee)+s(x,y,bs="sos"), data=csi1,random=reStruct(object = ~ 1| unique, pdClass="pdDiag"),correlation=corAR1(form=~annee))
smgC<-summary(mcsiC$gam)
smgC

coefdata=coefficients(mcsiC$gam)
coefdata=matrix(coefdata)
coefdata=coefdata[1:25]
coefdata[[1]]<-0
erreurmatrix=matrix(smgC$se)
erreurmatrix=erreurmatrix[1:25]
erreurmatrix[[1]]<-0
plot(coefdata,pch=16,col="red",ylim=c(min(coefdata-(erreurmatrix)),max(coefdata+(erreurmatrix))),xaxt="n",ylab="",xlab="",main="Variation annuelle du CTI en milieu ouvert") 
axis(1,1:25,labels = c(1989:2013))
abline(h=0,col="grey")
lines(coefdata,col="red")
lines(coefdata-(erreurmatrix),col="blue",lty="dashed")
lines(coefdata+(erreurmatrix),col="blue",lty="dashed")
legend("topleft",legend=c("Variation annuelle du CTI par point STOC","Indice +/- erreur"),col=c("red","blue"),lty=c(1,2),bty="o",pt.cex=1,cex=0.8,text.col="black",horiz=F,inset=c(0.03,0.03))

anneebase1=annee-1988
mcsibisC<-gamm(CTI~anneebase1+s(x,y,bs="sos"), data=csi1,random=reStruct(object = ~ 1|unique, pdClass="pdDiag"),correlation=corAR1(form=~anneebase1))
smgbisC<-summary(mcsibisC$gam)
smgbisC

toto=hist(CTI)
toto
sum(toto$density)



#2003 à 2013
csi1=read.table("CTIpoint2003-2013habC.txt",h=T)
summary(csi1)
attach(csi1)


mcsiC<-gamm(CTI~factor(annee)+s(x,y,bs="sos"), data=csi1,random=reStruct(object = ~ 1| unique, pdClass="pdDiag"),correlation=corAR1(form=~annee))
smgC<-summary(mcsiC$gam)
smgC

coefdata=coefficients(mcsiC$gam)
coefdata=matrix(coefdata)
coefdata=coefdata[1:11]
coefdata[[1]]<-0
erreurmatrix=matrix(smgC$se)
erreurmatrix=erreurmatrix[1:11]
erreurmatrix[[1]]<-0
plot(coefdata,pch=16,col="red",ylim=c(min(coefdata-(erreurmatrix)),max(coefdata+(erreurmatrix))),xaxt="n",ylab="",xlab="",main="Variation annuelle du CTI en milieu ouvert") 
axis(1,1:11,labels = c(2003:2013))
abline(h=0,col="grey")
lines(coefdata,col="red")
lines(coefdata-(erreurmatrix),col="blue",lty="dashed")
lines(coefdata+(erreurmatrix),col="blue",lty="dashed")
legend("topleft",legend=c("Variation annuelle du CTI par point STOC","Indice +/- erreur"),col=c("red","blue"),lty=c(1,2),bty="o",pt.cex=1,cex=0.8,text.col="black",horiz=F,inset=c(0.03,0.03))

anneebase1=annee-2002
mcsibisC<-gamm(CTI~anneebase1+s(x,y,bs="sos"), data=csi1,random=reStruct(object = ~ 1|unique, pdClass="pdDiag"),correlation=corAR1(form=~anneebase1))
smgbisC<-summary(mcsibisC$gam)
smgbisC

toto=hist(CTI)
toto
sum(toto$density)



#################Milieu bâtis##########
#1989 à 2013
csi1=read.table("CTIpoint1989-2013habE.txt",h=T)
summary(csi1)
attach(csi1)

mcsiE<-gamm(CTI~factor(annee)+s(x,y,bs="sos"), data=csi1,random=reStruct(object = ~ 1| unique, pdClass="pdDiag"),correlation=corAR1(form=~annee))
smgE<-summary(mcsiE$gam)
smgE

coefdata=coefficients(mcsiE$gam)
coefdata=matrix(coefdata)
coefdata=coefdata[1:25]
coefdata[[1]]<-0
erreurmatrix=matrix(smgE$se)
erreurmatrix=erreurmatrix[1:25]
erreurmatrix[[1]]<-0
plot(coefdata,pch=16,col="red",ylim=c(min(coefdata-(erreurmatrix)),max(coefdata+(erreurmatrix))),xaxt="n",ylab="",xlab="",main="Variation annuelle du CTI en milieu bâtis") 
axis(1,1:25,labels = c(1989:2013))
abline(h=0,col="grey")
lines(coefdata,col="red")
lines(coefdata-(erreurmatrix),col="blue",lty="dashed")
lines(coefdata+(erreurmatrix),col="blue",lty="dashed")
legend("topleft",legend=c("Variation annuelle du CTI par point STOC","Indice +/- erreur"),col=c("red","blue"),lty=c(1,2),bty="o",pt.cex=1,cex=0.8,text.col="black",horiz=F,inset=c(0.03,0.03))

anneebase1=annee-1988
mcsibisE<-gamm(CTI~anneebase1+s(x,y,bs="sos"), data=csi1,random=reStruct(object = ~ 1|unique, pdClass="pdDiag"),correlation=corAR1(form=~anneebase1))
smgbisE<-summary(mcsibisE$gam)
smgbisE

toto=hist(CTI)
toto
sum(toto$density)

#2003 à 2013
csi1=read.table("CTIpoint2003-2013habE.txt",h=T)
summary(csi1)
attach(csi1)


mcsiE<-gamm(CTI~factor(annee)+s(x,y,bs="sos"), data=csi1,random=reStruct(object = ~ 1| unique, pdClass="pdDiag"),correlation=corAR1(form=~annee))
smgE<-summary(mcsiE$gam)
smgE

coefdata=coefficients(mcsiE$gam)
coefdata=matrix(coefdata)
coefdata=coefdata[1:11]
coefdata[[1]]<-0
erreurmatrix=matrix(smgE$se)
erreurmatrix=erreurmatrix[1:11]
erreurmatrix[[1]]<-0
plot(coefdata,pch=16,col="red",ylim=c(min(coefdata-(erreurmatrix)),max(coefdata+(erreurmatrix))),xaxt="n",ylab="",xlab="",main="Variation annuelle du CTI en milieu bâtis") 
axis(1,1:11,labels = c(2003:2013))
abline(h=0,col="grey")
lines(coefdata,col="red")
lines(coefdata-(erreurmatrix),col="blue",lty="dashed")
lines(coefdata+(erreurmatrix),col="blue",lty="dashed")
legend("topleft",legend=c("Variation annuelle du CTI par point STOC","Indice +/- erreur"),col=c("red","blue"),lty=c(1,2),bty="o",pt.cex=1,cex=0.8,text.col="black",horiz=F,inset=c(0.03,0.03))

anneebase1=annee-2002
mcsibisE<-gamm(CTI~anneebase1+s(x,y,bs="sos"), data=csi1,random=reStruct(object = ~ 1|unique, pdClass="pdDiag"),correlation=corAR1(form=~anneebase1))
smgbisE<-summary(mcsibisE$gam)
smgbisE

toto=hist(CTI)
toto
sum(toto$density)






#################Milieu agricole##########
#1989 à 2013
csi1=read.table("CTIpoint1989-2013habD.txt",h=T)
summary(csi1)
attach(csi1)

mcsiD<-gamm(CTI~factor(annee)+s(x,y,bs="sos"), data=csi1,random=reStruct(object = ~ 1| unique, pdClass="pdDiag"),correlation=corAR1(form=~annee))
smgD<-summary(mcsiD$gam)
smgD

coefdata=coefficients(mcsiD$gam)
coefdata=matrix(coefdata)
coefdata=coefdata[1:25]
coefdata[[1]]<-0
erreurmatrix=matrix(smgD$se)
erreurmatrix=erreurmatrix[1:25]
erreurmatrix[[1]]<-0
plot(coefdata,pch=16,col="red",ylim=c(min(coefdata-(erreurmatrix)),max(coefdata+(erreurmatrix))),xaxt="n",ylab="",xlab="",main="Variation annuelle du CTI en milieu agricole") 
axis(1,1:25,labels = c(1989:2013))
abline(h=0,col="grey")
lines(coefdata,col="red")
lines(coefdata-(erreurmatrix),col="blue",lty="dashed")
lines(coefdata+(erreurmatrix),col="blue",lty="dashed")
legend("topleft",legend=c("Variation annuelle du CTI par point STOC","Indice +/- erreur"),col=c("red","blue"),lty=c(1,2),bty="o",pt.cex=1,cex=0.8,text.col="black",horiz=F,inset=c(0.03,0.03))

anneebase1=annee-1988
mcsibisD<-gamm(CTI~anneebase1+s(x,y,bs="sos"), data=csi1,random=reStruct(object = ~ 1|unique, pdClass="pdDiag"),correlation=corAR1(form=~anneebase1))
smgbisD<-summary(mcsibisD$gam)
smgbisD


#2003 à 2013
csi1=read.table("CTIpoint2003-2013habD.txt",h=T)
summary(csi1)
attach(csi1)


mcsiD<-gamm(CTI~factor(annee)+s(x,y,bs="sos"), data=csi1,random=reStruct(object = ~ 1| unique, pdClass="pdDiag"),correlation=corAR1(form=~annee))
smgD<-summary(mcsiD$gam)
smgD

coefdata=coefficients(mcsiD$gam)
coefdata=matrix(coefdata)
coefdata=coefdata[1:11]
coefdata[[1]]<-0
erreurmatrix=matrix(smgD$se)
erreurmatrix=erreurmatrix[1:11]
erreurmatrix[[1]]<-0
plot(coefdata,pch=16,col="red",ylim=c(min(coefdata-(erreurmatrix)),max(coefdata+(erreurmatrix))),xaxt="n",ylab="",xlab="",main="Variation annuelle du CTI en milieu agricole") 
axis(1,1:11,labels = c(2003:2013))
abline(h=0,col="grey")
lines(coefdata,col="red")
lines(coefdata-(erreurmatrix),col="blue",lty="dashed")
lines(coefdata+(erreurmatrix),col="blue",lty="dashed")
legend("topleft",legend=c("Variation annuelle du CTI par point STOC","Indice +/- erreur"),col=c("red","blue"),lty=c(1,2),bty="o",pt.cex=1,cex=0.8,text.col="black",horiz=F,inset=c(0.03,0.03))

anneebase1=annee-2002
mcsibisD<-gamm(CTI~anneebase1+s(x,y,bs="sos"), data=csi1,random=reStruct(object = ~ 1|unique, pdClass="pdDiag"),correlation=corAR1(form=~anneebase1))
smgbisD<-summary(mcsibisD$gam)
smgbisD




#################Milieu forestier##########
#1989 à 2013
csi1=read.table("CTIpoint1989-2013habAB.txt",h=T)
summary(csi1)
attach(csi1)

mcsiAB<-gamm(CTI~factor(annee)+s(x,y,bs="sos"), data=csi1,random=reStruct(object = ~ 1| unique, pdClass="pdDiag"),correlation=corAR1(form=~annee))
smgAB<-summary(mcsiAB$gam)
smgAB

coefdata=coefficients(mcsiAB$gam)
coefdata=matrix(coefdata)
coefdata=coefdata[1:25]
coefdata[[1]]<-0
erreurmatrix=matrix(smgAB$se)
erreurmatrix=erreurmatrix[1:25]
erreurmatrix[[1]]<-0
plot(coefdata,pch=16,col="red",ylim=c(min(coefdata-(erreurmatrix)),max(coefdata+(erreurmatrix))),xaxt="n",ylab="",xlab="",main="Variation annuelle du CTI en milieu forestier") 
axis(1,1:25,labels = c(1989:2013))
abline(h=0,col="grey")
lines(coefdata,col="red")
lines(coefdata-(erreurmatrix),col="blue",lty="dashed")
lines(coefdata+(erreurmatrix),col="blue",lty="dashed")
legend("topleft",legend=c("Variation annuelle du CTI par point STOC","Indice +/- erreur"),col=c("red","blue"),lty=c(1,2),bty="o",pt.cex=1,cex=0.8,text.col="black",horiz=F,inset=c(0.03,0.03))

anneebase1=annee-1988
mcsibisAB<-gamm(CTI~anneebase1+s(x,y,bs="sos"), data=csi1,random=reStruct(object = ~ 1|unique, pdClass="pdDiag"),correlation=corAR1(form=~anneebase1))
smgbisAB<-summary(mcsibisAB$gam)
smgbisAB

toto=hist(CTI)
toto
sum(toto$density)



#2003 à 2013
csi1=read.table("CTIpoint2003-2013habAB.txt",h=T)
summary(csi1)
attach(csi1)


mcsiAB<-gamm(CTI~factor(annee)+s(x,y,bs="sos"), data=csi1,random=reStruct(object = ~ 1| unique, pdClass="pdDiag"),correlation=corAR1(form=~annee))
smgAB<-summary(mcsiAB$gam)
smgAB

coefdata=coefficients(mcsiAB$gam)
coefdata=matrix(coefdata)
coefdata=coefdata[1:11]
coefdata[[1]]<-0
erreurmatrix=matrix(smgAB$se)
erreurmatrix=erreurmatrix[1:11]
erreurmatrix[[1]]<-0
plot(coefdata,pch=16,col="red",ylim=c(min(coefdata-(erreurmatrix)),max(coefdata+(erreurmatrix))),xaxt="n",ylab="",xlab="",main="Variation annuelle du CTI en milieu forestier") 
axis(1,1:11,labels = c(2003:2013))
abline(h=0,col="grey")
lines(coefdata,col="red")
lines(coefdata-(erreurmatrix),col="blue",lty="dashed")
lines(coefdata+(erreurmatrix),col="blue",lty="dashed")
legend("topleft",legend=c("Variation annuelle du CTI par point STOC","Indice +/- erreur"),col=c("red","blue"),lty=c(1,2),bty="o",pt.cex=1,cex=0.8,text.col="black",horiz=F,inset=c(0.03,0.03))

anneebase1=annee-2002
mcsibisAB<-gamm(CTI~anneebase1+s(x,y,bs="sos"), data=csi1,random=reStruct(object = ~ 1|unique, pdClass="pdDiag"),correlation=corAR1(form=~anneebase1))
smgbisAB<-summary(mcsibisAB$gam)
smgbisAB


toto=hist(CTI)
toto
sum(toto$density)










tab=read.table("tabhab.txt",h=T)
summary(tab)
attach(tab)

plot(ouvert,pch=16,col="violet",ylim=c(min(bati),max(bati)),xaxt="n",ylab="",xlab="",main="Variation annuelle du CTI en fonction de l'habitat") 
axis(1,1:11,labels = c(2003:2013))
abline(h=0,col="grey")
lines(ouvert,col="violet")
legend("bottomleft",legend=c("milieu ouvert","milieu bâti","milieu forestier","milieu agricole"),col=c("violet","red","green","orange"),lty=c(1),bty="o",pt.cex=1,cex=0.8,text.col="black",horiz=F,inset=c(0.03,0.03))
points(bati,col="red",pch=16)
lines(bati,col="red")
lines(forestier,col="green")
points(forestier,col="green",pch=16)






















#########PAR CARRE#########################
species<-read.table("STI.txt",h=T)
PSSI<-as.matrix(species[,2])
d=read.table("tablocarre1989-2013trifinal129sp.txt",h=T)


MULTI<-function(x){
sum(x*PSSI)/sum(x)}
CTI<-apply(d,1,MULTI)
write.table(CTI, "CTI1989-2013carre.txt")



csi1=read.table("CTIcarre1989-2013trifinal.txt",h=T)
summary(csi1)
attach(csi1)


library(nlme)
library(mgcv)
mcsi<-gamm(CTI~factor(annee)+s(x,y,bs="sos"), data=csi1,random=reStruct(object = ~ 1| carre, pdClass="pdDiag"),correlation=corAR1(form=~annee))
sml<-summary(mcsi$lme)
smg<-summary(mcsi$gam)
sml
smg
plot(mcsi$gam)

graf<-gamm(CTI~s(annee,k=3)+s(x,y,bs="sos"), data=csi1,random=reStruct(object = ~ 1| carre, pdClass="pdDiag"),correlation=corAR1(form=~annee))
plot(graf$gam)

graf<-gamm(CTI~s(annee,k=12)+s(x,y,bs="sos"), data=csi1,random=reStruct(object = ~ 1| carre, pdClass="pdDiag"),correlation=corAR1(form=~annee))

graf<-gamm(CTI~s(annee,k=4)+s(x,y,bs="sos"), data=csi1,random=reStruct(object = ~ 1| carre, pdClass="pdDiag"),correlation=corAR1(form=~annee))
graf<-gamm(CTI~s(annee,k=8)+s(x,y,bs="sos"), data=csi1,random=reStruct(object = ~ 1| carre, pdClass="pdDiag"),correlation=corAR1(form=~annee))

graf<-gamm(CTI~s(annee)+s(x,y,bs="sos"), data=csi1,random=reStruct(object = ~ 1| carre, pdClass="pdDiag"),correlation=corAR1(form=~annee))

sml1<-summary(graf$lme)
smg1<-summary(graf$gam)
sml1
smg1
plot(graf$gam)










coefdata=coefficients(mcsi$gam)
coefdata=matrix(coefdata)
coefdata=coefdata[1:25]
coefdata[[1]]<-0
erreurmatrix=matrix(smg$se)
erreurmatrix=erreurmatrix[1:25]
erreurmatrix[[1]]<-0
plot(coefdata,pch=16,col="red",ylim=c(min(coefdata-(erreurmatrix)),max(coefdata+(erreurmatrix))),xaxt="n",ylab="",xlab="",main="Variation annulle du CTI par carré STOC") 
axis(1,1:25,labels = c(1989:2013))
abline(h=0,col="grey")
lines(coefdata,col="red")
lines(coefdata-(erreurmatrix),col="blue",lty="dashed")
lines(coefdata+(erreurmatrix),col="blue",lty="dashed")
legend("topleft",legend=c("Variation annuelle du CTI par carré STOC","Indice +/- erreur"),col=c("red","blue"),lty=c(1,2),bty="o",pt.cex=1,cex=0.8,text.col="black",horiz=F,inset=c(0.03,0.03))


anneebase1=annee-1988
mcsi<-gamm(CTI~anneebase1+s(x,y,bs="sos"), data=csi1,random=reStruct(object = ~ 1|carre, pdClass="pdDiag"),correlation=corAR1(form=~anneebase1))
sml<-summary(mcsi$lme)
smg<-summary(mcsi$gam)
sml
smg


mcsi<-gamm(CTI~annee+s(x,y,bs="sos"), data=csi1,random=reStruct(object = ~ 1|carre, pdClass="pdDiag"),correlation=corAR1(form=~annee))


summary(lm(coefdata~c(1:25)))
abline(lm(coefdata~c(1:25)))


mcsi2<-gamm(CTI~s(anneebase1)+s(x,y,bs="sos"), data=csi1,random=reStruct(object = ~ 1|carre, pdClass="pdDiag"),correlation=corAR1(form=~anneebase1))
par(mfrow=c(1,2))
plot(mcsi2$gam)


mcsi3<-gamm(CTI~s(annee)+s(x,y,bs="sos"), data=csi1,random=reStruct(object = ~ 1|carre, pdClass="pdDiag"),correlation=corAR1(form=~anneebase1))
par(mfrow=c(1,2))
plot(mcsi3$gam)

coefficients(mcsi3$gam)
gam.check(mcsi3$gam)


mcsi3<-gamm(CTI~s(annee,k=3,fx=T), data=csi1,random=reStruct(object = ~ 1|carre, pdClass="pdDiag"),correlation=corAR1(form=~anneebase1))
par(mfrow=c(1,2))
plot(mcsi3$gam)





csi1=read.table("CTIcarre1989-2013.txt",h=T)
toto=factor(csi1[,2],levels=c("2001","1989","1990","1991","1992","1993","1994","1995","1996","1997","1998","1999","2000","2002","2003","2004","2005","2006","2007","2008","2009","2010","2011","2012","2013"))    
summary(csi1)
attach(csi1)




toto=hist(CTI,breaks=100)
toto

plot(mcsi$res)

toto=hist(CTI)

summary(CTIab)
summary(CTIab[annee=="1989"])


toto=predict.gam(mcsi$gam)
hist(toto)




###année avec + gros effort d'echantionnage spatial 2008

length(y[annee=="2008"&inf800=="TRUE"])
km=(y[annee=="2008"&inf800=="TRUE"]-1716200)*10^-5*111.115
plot(km,CTI[annee=="2008"&inf800=="TRUE"],xlab="km (gradient Sud/Nord)",ylab="CTI (°C)")
abline(lm(CTI[annee=="2008"&inf800=="TRUE"]~km))
summary(lm(CTI[annee=="2008"&inf800=="TRUE"]~km))

length(y[annee=="2009"&inf800=="TRUE"])
km=(y[annee=="2009"&inf800=="TRUE"]-1716200)*10^-5*111.115
plot(km,CTI[annee=="2009"&inf800=="TRUE"],xlab="km (gradient Sud/Nord)",ylab="CTI (°C)")
abline(lm(CTI[annee=="2009"&inf800=="TRUE"]~km))
summary(lm(CTI[annee=="2009"&inf800=="TRUE"]~km))


#########Distribution temporelle temperature en france

crutem=read.table("crutem5carre.txt",h=T)
summary(crutem)
crutem[,1]=as.factor(crutem[,1])
attach(crutem)

#plot(c(1989:2012),tmean,pch=16,xlab="Année",ylab="Temperature anomaly (°C)")
#lines(c(1989:2012),tmean)
#abline(lm(tmean~c(1989:2012)))
#summary(lm(tmean~c(1:24)))

plot(c(1989:2013),tmean,pch=16,xlab="Année",ylab="Temperature anomaly (°C)")
lines(c(1989:2013),tmean)
abline(lm(tmean~c(1989:2012),weights=1/varmean))
summary(lm(tmean~c(1:24),weights=1/varmean))

summary(lm(tmeancor~c(1:24),weights=1/varmean))



plot(c(1989:2013),meanlisse[5:29],pch=16,xlab="Année",ylab="Temperature anomaly (°C)")
lines(c(1989:2013),meanlisse[5:29])
abline(lm(meanlisse[5:29]~c(1989:2013)))
summary(lm(meanlisse[5:29]~c(1989:2013)))



summary(lm(meanlisse[5:29]~c(1:25)))


#########Distribution temporelle temperature en france
csi1=read.table("CTIcarre1989-2013trifinal.txt",h=T)
summary(csi1)
attach(csi1)

tmean=read.table("tmean.txt",h=T)
summary(tmean)
attach(tmean)
plot(alt,mean)
boxplot(tmean[5:11]/10)



km=(y-6138869.90747)*10^-5*111.115
plot(km,mean,xlab="km (gradient Sud/Nord)",ylab="température moyenne Mars-Septembre(°C)")
abline(lm(mean~km),col="red")
summary(lm(mean~km))




###2 graphes superposés##


plot(c(1989:2013),meanlisse[5:29],pch=16,xlab="Année",ylab="Temperature anomaly (°C)")
lines(c(1989:2013),meanlisse[5:29])
par(new=TRUE) 
plot(coefdata,pch=16, axes = FALSE,col="red",xaxt="n",ylab="",xlab="",main="Variations annulles du CTI et des températures moyennes en France") 
lines(coefdata,col="red")
axis(4)
text(25, mean(coefdata), "Variation du CTI", srt = 270) 
legend("bottomright",legend=c("Variation des températures en France","Variation du CTI par carré STOC"),col=c("black","red"),lty=c(1,1),bty="o",pt.cex=1,cex=0.8,text.col="black",horiz=F,inset=c(0.03,0.03))




















##############  SUR 10 ANS ###########

csi1=read.table("CTIcarre2003-2013trifinal.txt",h=T)
summary(csi1)
attach(csi1)


library(nlme)
library(mgcv)
mcsi<-gamm(CTI~factor(annee)+s(x,y,bs="sos"), data=csi1,random=reStruct(object = ~ 1| carre, pdClass="pdDiag"),correlation=corAR1(form=~annee))
sml<-summary(mcsi$lme)
smg<-summary(mcsi$gam)
sml
smg
plot(mcsi$gam)

coefdata=coefficients(mcsi$gam)
coefdata=matrix(coefdata)
coefdata=coefdata[1:11]
coefdata[[1]]<-0
erreurmatrix=matrix(smg$se)
erreurmatrix=erreurmatrix[1:11]
erreurmatrix[[1]]<-0
plot(coefdata,pch=16,col="red",ylim=c(min(coefdata-(erreurmatrix)),max(coefdata+(erreurmatrix))),xaxt="n",ylab="",xlab="",main="Variation annulle du CTI par carré STOC sur les 10 dernières années") 
axis(1,1:11,labels = c(2003:2013))
abline(h=0,col="grey")
lines(coefdata,col="red")
lines(coefdata-(erreurmatrix),col="blue",lty="dashed")
lines(coefdata+(erreurmatrix),col="blue",lty="dashed")
legend("topright",legend=c("Variation annuelle du CTI par carré STOC","Indice +/- erreur"),col=c("red","blue"),lty=c(1,2),bty="o",pt.cex=1,cex=0.8,text.col="black",horiz=F,inset=c(0.03,0.03))


anneebase1=annee-2002
mcsi<-gamm(CTI~anneebase1+s(x,y,bs="sos"), data=csi1,random=reStruct(object = ~ 1|carre, pdClass="pdDiag"),correlation=corAR1(form=~anneebase1))
sml<-summary(mcsi$lme)
smg<-summary(mcsi$gam)
sml
smg




toto=hist(CTI)
toto

#########Distribution temporelle temperature en france

crutem=read.table("crutem5carre.txt",h=T)
summary(crutem)
crutem[,1]=as.factor(crutem[,1])
attach(crutem)


plot(c(2003:2013),meanlisse[19:29],pch=16,xlab="Année",ylab="Temperature anomaly (°C)")
lines(c(2003:2013),meanlisse[19:29])
abline(lm(meanlisse[19:29]~c(2003:2013)))
summary(lm(meanlisse[19:29]~c(2003:2013)))



summary(lm(meanlisse[19:29]~c(1:11)))



###2 graphes superposés##


plot(c(2003:2013),meanlisse[19:29],pch=16,xlab="Année",ylab="Temperature anomaly (°C)")
lines(c(2003:2013),meanlisse[19:29])
par(new=TRUE) 
plot(coefdata,pch=16,col="red",add=T,xaxt="n",ylab="",xlab="",main="Variations annulles du CTI et des températures moyennes en France") 
lines(coefdata,col="red")

legend("topright",legend=c("Variation annuelle du CTI par carré STOC","Indice +/- erreur"),col=c("red","blue"),lty=c(1,2),bty="o",pt.cex=1,cex=0.8,text.col="black",horiz=F,inset=c(0.03,0.03))


plot(c(2003:2013),meanlisse[19:29],pch=16,xlab="Année",ylab="Temperature anomaly (°C)")
lines(c(2003:2013),meanlisse[19:29])
par(new=TRUE) 
plot(coefdata,pch=16, axes = FALSE,col="red",xaxt="n",ylab="",xlab="",main="Variations annulles du CTI et des températures moyennes en France") 
lines(coefdata,col="red")
axis(4)
text(11.1, mean(coefdata), "Variation du CTI", srt = 270) 
legend("topright",legend=c("Variation des températures en France","Variation du CTI par carré STOC"),col=c("black","red"),lty=c(1,1),bty="o",pt.cex=1,cex=0.8,text.col="black",horiz=F,inset=c(0.03,0.03))






####Coller les coordonnées coresspondantes a chaque carrés :
coord<-read.table("ref.txt",h=T)
test<-read.table("carre.txt",h=T)
lala<-match(test[,1],coord[,1])
toto=coord[lala,]
write.table(toto, "toto.txt")


####mettre par odre alphabétique :
transpo=read.table("atranspo.txt",h=T)
datazero=replace(transpo,is.na(transpo),0)
plouf=t(datazero)
pototos=plouf[order(rownames(plouf)),]
pototos[1:10,1:10]
banane=t(pototos)
write.table(banane,"transpo.txt")







