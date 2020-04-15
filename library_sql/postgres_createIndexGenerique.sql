--drop INDEX if exists species_sc;
CREATE INDEX species_sc ON species (scientific_name);

--drop INDEX if exists species_fr;
CREATE INDEX species_fr ON species (french_name);

--drop INDEX if exists species_eng;
CREATE INDEX species_eng ON species (english_name);

--drop INDEX if exists species_fam;
CREATE INDEX species_fam ON species (family_tax);

--drop INDEX if exists indic_sp;
CREATE INDEX indic_sp ON species_list_indicateur (indicator);

--drop INDEX if exists indic_hab_fr;
CREATE INDEX indic_hab_fr ON species_list_indicateur (habitat_specialisation_f);

--drop INDEX if exists indic_hab_eng;
CREATE INDEX indic_hab_eng ON species_list_indicateur (habitat_specialisation);

--drop INDEX if exists indic_fbi;
CREATE INDEX indic_fbi ON species_list_indicateur (european_fbi);

--drop INDEX if exists indic_fbi_fr;
CREATE INDEX indic_fbi_fr ON species_list_indicateur (french_fbi);

--drop INDEX if exists indic_montane;
CREATE INDEX indic_montane ON species_list_indicateur (montane_lehikoinen);


--drop INDEX if exists indic_montane;
CREATE INDEX indic_trim ON species_list_indicateur (french_trim);

--- CREATE INDEX indic_ebcc ON species_list_indicateur (french_ebcc);



CREATE INDEX hab_code ON habitat_cat(habitat_code);
CREATE INDEX hab_nom ON habitat_cat(habitat_nom);
CREATE INDEX hab_name ON habitat_cat(habitat_name);
CREATE INDEX hab_foret_s ON habitat_cat(foret_strict);
CREATE INDEX hab_foret_l ON habitat_cat(foret_large);
CREATE INDEX hab_ouvert_nat_s ON habitat_cat(ouvert_nat_strict);
CREATE INDEX hab_ouvert_nat_l ON habitat_cat(ouvert_nat_large);
CREATE INDEX hab_agricole_s ON habitat_cat(agricole_strict);
CREATE INDEX hab_agricole_l ON habitat_cat(agricole_large);
CREATE INDEX hab_urbain_s ON habitat_cat(urbain_strict);
CREATE INDEX hab_urbain_l ON habitat_cat(urbain_large);
CREATE INDEX hab_aquatique_s ON habitat_cat(aquatique_strict);
CREATE INDEX hab_aquatique_l ON habitat_cat(aquatique_large);


CREATE INDEX href_milieu ON habitat_code_ref(milieu);
CREATE INDEX href_value ON habitat_code_ref(value);
CREATE INDEX href_level ON habitat_code_ref(level);
