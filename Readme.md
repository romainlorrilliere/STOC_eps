

# Instalation de la base de données STOC eps


## Installer PostgreSQL

la base de données de Birdlab est ensuite creér sous postgreSQL afin
de bénéficier de l'extension postGIS

### PostgresSQL
https://www.postgresql.org/download/

ATTENTION bien conserver le nom du compte adminitrateur et le
mot de passe enregistré 
ex: 
 - compte: _postgres_ mp: _postgres_
 - compte: _admin_  mp: _admin_


### Installer l'extension postGIS
Dans le Stack builder :
selection la version de l'instalatin (ex: PostgresSQL 12...)
instalation des spatial extension : PostGIS

### Creer une base spatiale

creer une spatial database
nom : __stoc_eps__

## Installer R 
https://cran.r-project.org/

il est recommandé d'utiliser un éditeur de texte adapté à R 
 - le plus utilisé et le plus facile d'utilisation: R-studio https://rstudio.com/
 - le plus puissant (selon moi ;-) ) Emacs https://www.gnu.org/software/emacs/ et ESS https://ess.r-project.org/
 - Notepad++ https://notepad-plus-plus.org/downloads/ et NppToR https://sourceforge.net/projects/npptor/
 - ...
 
 




## Les variable d'environement 
Pour que le script _R_ fonctionne il déclarer quelques variable
d'environnement

### sous windows : 
variable d'environement 
### nouvelle variable utilisateur
nouvelle variable : _PGPASSWORD_ valeur de la variable : _postgres_ ou _MON_MOT_DE_PASSE_
###	modifier path par l'ajout de deux path ou 3 path si intallation de MySQL
ex: avec XX la version du driver de DB
 - C:\Program Files\PostgreSQL\XX\bin
 - C:\Program Files\PostgreSQL\XX\lib



## QGIS
Conseil installer QGIS, il permetra de visualiser directement les
table spatiales de la DB postgreSQL



## github
Se creer un compte github
demander un accès au repositroy :
https://github.com/romainlorrilliere/STOC_eps

installer github desktop 
clone dans dossier git à la racine (ex: "C:/git/")




# Le script d'installation de la base

## sourcer le script
 
 
 
```R
source("scriptPrepaNouvelBaseSTOC.r")
```

## lancer la fonction de preparation de la base

ex: 

à partir des fichiers bruts issues de VigiePlume


```R
laDate <- as.Date(Sys.time())

prepaData(dateExportVP=laDate,nomFileVP="export_stoc_25112019.txt",nomFileVP_ONF="export_stoc_onf_03122019.txt",dateExportFNat="2017-01-04", importACCESS=FALSE, nomFileFNat="FNat_plat_2017-01-04.csv", importationDataBrut=TRUE, constructionPoint=TRUE,constructionCarre=TRUE, constructionInventaire=TRUE,constructionObservation = TRUE, constructionHabitat = TRUE,dateConstruction=NULL,postgresql_import=TRUE,nomDBpostgresql=NULL,postgresql_createAll=TRUE,postgresUser="posgres", postgresPassword="postgres",postGIS_initiation=TRUE,import_shape=FALSE,repertoire=NULL, postgresql_abondanceSeuil=TRUE,seuilAbondance = .99, historiqueCarre=TRUE, pointCarreAnnee=TRUE,importPointCarreAnnee=TRUE,fileTemp=FALSE)




	
```



à partir des fichiers pré-construits
```R

laDate <- ["la date noté dans les noms des fichier"]

prepaData(dateExportVP=laDate, importationDataBrut=FALSE, constructionPoint=FALSE,constructionCarre=FALSE, constructionInventaire=FALSE,constructionObservation = FALSE, constructionHabitat = FALSE,dateConstruction=NULL,postgresql_import=TRUE,nomDBpostgresql=NULL,postgresql_createAll=TRUE,postgresUser="postgres", postgresPassword="postgres",postGIS_initiation=TRUE,import_shape=FALSE,repertoire=NULL, postgresql_abondanceSeuil=TRUE,seuilAbondance = .99, historiqueCarre=TRUE, pointCarreAnnee=TRUE,importPointCarreAnnee=TRUE,fileTemp=FALSE)

```
