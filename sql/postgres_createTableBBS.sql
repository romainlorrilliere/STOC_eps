------------------------------------------------------------------------
-- Data initial importation FBBS and species functionnal indicators
------------------------------------------------------------------------

DROP table if exists point;
CREATE TABLE point  
	(pk_point varchar(100) primary key,
	 id_carre varchar(100) NOT NULL,
         commune varchar(50),
         site varchar(50),
         insee varchar(6),
         departement varchar(3),
         nom_point varchar(10),
         num_point varchar(2) NOT NULL,
         altitude int,
         longitude_wgs84 double precision,
         latitude_wgs84 double precision,
         	db varchar(10) NOT NULL,
	 date_export varchar(10) NOT NULL);


DROP table if exists carre;
CREATE TABLE carre
       (pk_carre varchar(100) primary key,
       nom_carre varchar(100),
       nom_carre_fnat varchar(100),
       commune varchar(50),
       site varchar(50),
       etude varchar(25),
       insee varchar(5),
       departement varchar(3),
       altitude int,
       area int,
			 perimeter int,
       longitude_points_wgs84 double precision,
       latitude_points_wgs84 double precision,
       longitude_grid_wgs84 double precision,
       latitude_grid_wgs84 double precision,
       db varchar(10) NOT NULL,
       date_export varchar(10) NOT NULL);




DROP table if exists inventaire;
CREATE TABLE inventaire
       (pk_inventaire varchar(100) primary key,
       unique_inventaire_fnat varchar(100),
       id_carre varchar(100) NOT NULL,
       id_point varchar(100) NOT NULL,
       num_point varchar(2) NOT NULL,
       etude varchar(25) NOT NULL,
       annee int  NOT NULL,
       date varchar(10) NOT NULL,
       jour_julien int NOT NULL,
       passage int,
       info_passage varchar(20),
       passage_stoc int,
       nombre_de_passage int,
       temps_entre_passage int,
       info_entre_passage varchar(20),
       heure_debut varchar(5),
       heure_fin varchar(5),
       duree_minute int,
       observateur varchar(50),
       email varchar(50),
       nuage int,
       pluie int,
       vent int,
       visibilite int,
       neige int,
       db varchar(10) NOT NULL,
       date_export varchar(10) NOT NULL,
       version varchar(10) NOT NULL);


DROP table if exists observation;
CREATE TABLE observation
       (pk_observation varchar(100) primary key,
        id_fnat_unique_citation varchar(100),
        id_inventaire varchar(100) NOT NULL,
        id_point varchar(100)  NOT NULL,
        id_carre varchar(100) NOT NULL,
        num_point int NOT NULL,
        passage int,
        date varchar(10) NOT NULL,
        annee int NOT NULL,
        classe varchar(1),
        espece varchar(9) NOT NULL,
        code_sp varchar(6) NOT NULL,
        abondance int NOT NULL,
        distance_contact varchar(8),
 	db varchar(10) NOT NULL,
	date_export varchar(10) NOT NULL);


DROP table if exists habitat;
create TABLE habitat
       (pk_habitat varchar(100) primary key,
       id_point varchar(100)  NOT NULL,
       passage int NOT NULL,
       date varchar(10) NOT NULL,
       annee int NOT NULL,
       p_milieu varchar(1),
       p_type int,
       p_cat1 int,
       p_cat2 int,
       s_milieu varchar(1),
       s_type int,
       s_cat1 int,
       s_cat2 int,                        
       db varchar(10) NOT NULL,
       date_export varchar(10) NOT NULL);

