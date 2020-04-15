------------------------------------------------------------------------
-- Data initial importation FBBS and species functionnal indicators
------------------------------------------------------------------------

---------------------

DROP table if exists carrenat;
CREATE TABLE carrenat
	(pk_carre character varying(100) primary key,
	carrenat int,
	area int,
	perimeter int,
	long_lamb2e int,
	lat_lamb2e int,
	lat_WGS84	real,
	lon_WGS84 real);

---------------------

DROP table if exists species;
CREATE TABLE species
       (pk_species varchar(9) PRIMARY KEY,
       euring int,
       taxref int,
       scientific_name varchar(150),
       french_name varchar(150),
       english_name varchar(150),
       niveau_taxo varchar(25),
       class_tax varchar(50),
       order_tax varchar(50),
       family_tax varchar(50));
       
 --------------------
 
DROP table if exists species_list_indicateur;
CREATE TABLE species_list_indicateur
       (pk_species varchar(9) PRIMARY KEY,
        euring int,
        taxref int,
       	indicator boolean,
     		habitat_specialisation_f varchar(25),
     	 	habitat_specialisation varchar(25),
     	 	european_fbi boolean,
     	 	french_fbi boolean,
     	 	montane_lehikoinen boolean,
     	 	french_trim boolean,
     	 	ebcc boolean);

       
 --------------------
 
DROP table if exists species_indicateur_fonctionnel;
CREATE TABLE species_indicateur_fonctionnel
				(pk_species varchar(9) PRIMARY KEY,
				ssi real,
				ssi_old real,
				ssi_2007 real,
				sti real,
				sti_europe real,
				thermal_niche_mean real,
				thermal_niche_range real,
				thermal_niche_max real,
				thermal_niche_min real,
				stri real,
				exp_stri real,
				trophic_vegetation smallint,
				trophic_invertebrate smallint,
				trophic_vertebrate smallint);

	
---------------------------------------------

DROP TABLE IF EXISTS habitat_code_ref;

CREATE TABLE habitat_code_ref
       (pk_habitat_code_ref smallint PRIMARY KEY,
       milieu varchar(1),
       level varchar(10),
       value varchar(2),
       nom varchar(50),
       name varchar(55));



---------------------------------------------

DROP TABLE IF EXISTS habitat_cat;

CREATE TABLE habitat_cat
       (pk_habitat_code smallint PRIMARY KEY,
       habitat_code varchar(3),
       habitat_nom varchar(50),
       habitat_name varchar(60),
       habitat_cat_strict varchar(15),
       habitat_cat_strict_eng varchar(10),
       milieu_code varchar(1),
       milieu_nom varchar(30),
       milieu_name varchar(30),
       type_code smallint,
       type_nom varchar(50),
       type_name varchar(60),
       foret_strict boolean,
       foret_large boolean,
       ouvert_nat_strict boolean,
       ouvert_nat_large boolean,
       agricole_strict boolean,
       agricole_large boolean,
       urbain_strict boolean,
       urbain_large boolean,
       aquatique_strict boolean,
       aquatique_large boolean);


-------------------------------------------------


