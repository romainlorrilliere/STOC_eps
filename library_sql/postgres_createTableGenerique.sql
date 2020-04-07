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

	

