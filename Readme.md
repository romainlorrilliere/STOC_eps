

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





# Création de la base de données STOC-eps

## sourcer le script
```R
source("stoc_eps_db_functions.r")
```

 
 
 
## lancer la fonction de preparation de la base

ex: 

Importatoin à partir des fichiers bruts issues de VigiePlume


```R
laDate <- as.Date(Sys.time())

prepaData(dateExportVP=laDate,nomFileVP="export_stoc_25112019.txt",nomFileVP_ONF="export_stoc_onf_03122019.txt",dateExportFNat="2017-01-04", importACCESS=FALSE, nomFileFNat="FNat_plat_2017-01-04.csv", importationDataBrut=TRUE, constructionPoint=TRUE,constructionCarre=TRUE, constructionInventaire=TRUE,constructionObservation = TRUE, constructionHabitat = TRUE,dateConstruction=NULL,postgresql_import=TRUE,nomDBpostgresql=NULL,postgresql_createAll=TRUE,postgresUser="posgres", postgresPassword="postgres",postGIS_initiation=TRUE,import_shape=FALSE,repertoire=NULL, postgresql_abondanceSeuil=TRUE,seuilAbondance = .99, historiqueCarre=TRUE, pointCarreAnnee=TRUE,importPointCarreAnnee=TRUE,fileTemp=FALSE)
	
```



si l'ordinateur n'a pas assez de mémoire RAM (16Go recommandé), voici
comment lancé l'importationà partir des fichiers pré-construits
```R

laDate <- ["la date noté dans les noms des fichier"]

prepaData(dateExportVP=laDate, importationDataBrut=FALSE, constructionPoint=FALSE,constructionCarre=FALSE, constructionInventaire=FALSE,constructionObservation = FALSE, constructionHabitat = FALSE,dateConstruction=NULL,postgresql_import=TRUE,nomDBpostgresql=NULL,postgresql_createAll=TRUE,postgresUser="postgres", postgresPassword="postgres",postGIS_initiation=TRUE,import_shape=FALSE,repertoire=NULL, postgresql_abondanceSeuil=TRUE,seuilAbondance = .99, historiqueCarre=TRUE, pointCarreAnnee=TRUE,importPointCarreAnnee=TRUE,fileTemp=FALSE)

```

tous les parametres: 

- __dateExportVP__ date de l'export des données VP au format YYYY-MM-JJ
- __nomFileVP__ nom du fichier exporté de VP
- __nomFileVP_ONF__ nom du fichier exporté de VP pour les données ONF
- __dateExportFNat__ [=_"2017-01-04"_] date de l'export FNAT par default
- __importACCESS__ [=_FALSE_] valeur booleenne permet l'export des données FNAT directement dans la base access. ATTENTION cette fonctionne uniquement sous windows avec une version 32bit de R (.../bin/i386/Rgui.exe)
- __nomFileFNat__ [= _"FNat_plat_2017-01-04.csv"_] nom du fichier à plat de FNat
- __nomDBFNat__ [= _"Base FNat2000.MDB"_]
- __importationDataBrut__ [= _TRUE_] valeur booleene qui permet d'activé la récupération des données brut
- __constructionPoint__ [= _TRUE_] valeur booleene qui active la création de la table point
- __constructionCarre__ [= _TRUE_] valeur booleene qui active la création de la table carré
- __constructionInventaire__ [=_TRUE_] valeur booleene qui active la création de la table inventaire
- __constructionObservation__ [= _TRUE_] valeur booleene qui active la création de la table observation
- __constructionHabitat__ [= _TRUE_] valeur booleene qui active la création de la table habitat
- __dateConstruction__ [= _NULL_] date de la constuction de la base si null la date du jour si au format "YYYY-MM-JJ"
- __postgresql_import__ [= _TRUE_]  valeur booleene pour importer les tables dans la base postgres
- __nomDBpostgresql__ [= _NULL_] nom de la base postgres si null "stoc_eps"
- __postgresql_createAll__ [= _TRUE_] valeur booleene qui active la création complete de la base de données avec l'import des tables génériques
- __postgresUser__ [= _"postgres"_] nom de l'utilisateur de de la base de données par default "postgres"
- __postgresPassword__ [= _"postgres"_] mot de passe de l'utilisateur de de la base de données par default "postgres"
- __postGIS_initiation__ [= _FALSE_] valeur booleene qui permet d'activé l'initialisation de l'extension posgis de la base de données
- __import_shape__ [= _FALSE_] valeur booleene qui permet d'activé l'importation de shape_file dans la base de donnée
- __repertoire__ [= _NULL_] repertoire de travail du script par default le repertoire courant
- __postgresql_abondanceSeuil__ [= _TRUE_] valeur booleene qui permet d'activé le calcul du seuil des abondances par espèces et par classe de distance
- __seuilAbondance__ [= _0.99_] valeur du seuil par default 0.99
- __historiqueCarre__ [= _TRUE_]  valeur booleene qui permet d'activé le calcul d'un historique des carrés
- __pointCarreAnnee__ [= _TRUE_] valeur booleene qui permet d'activé la création des tables _point\_annee_ et _carre\_annee_
- __importPointCarreAnnee__ [= _TRUE_]  valeur booleene qui permet d'activé l'importation des tables _point\_annee_ et _carre\_annee_
- __fileTemp__ [= _FALSE_]  valeur booleene qui permet d'activé qui de conservé les fichier sql créer lors du processus, par default FALSE




# Utilisation des fonctions d'export


## sourcer le script
```R
source("scriptPrepaNouvelBaseSTOC.r")
```

 
