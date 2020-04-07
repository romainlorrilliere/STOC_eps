-- Creation des index des tables


-- point

create index p_carre on point(id_carre);
create index p_dep on point(departement);
create index p_npoint on point(num_point);

-- carre
create index c_etude on carre(etude);
create index c_dep on carre(departement);
create index c_alt on carre(altitude);

-- inventaire

create index i_carre on inventaire(id_carre);
create index i_point on inventaire(id_point);
create index i_etude on inventaire(etude);
create index i_date on inventaire(date);
create index i_an on inventaire(annee);
create index i_julien on inventaire(jour_julien);
create index i_passage on inventaire(passage);
create index i_pass_stoc on inventaire(passage_stoc);
create index i_ipass on inventaire(info_passage);
create index i_nbpass on inventaire(nombre_de_passage);
create index i_obs on inventaire(observateur);
create index i_nuage on inventaire(nuage);
create index i_pluie on inventaire(pluie);
create index i_vent on inventaire(vent);
create index i_visib on inventaire(visibilite);
create index i_neige on inventaire(neige);

-- Observations

create index o_carre on observation(id_carre);
create index o_point on observation(id_point);
create index o_date on observation(date);
create index o_an on observation(annee);
create index o_passage on observation(passage);
create index o_sp9 on observation(espece);
create index o_sp6 on observation(code_sp);
create index o_dist on observation(distance_contact);

-- Habitat

create index h_point on habitat(id_point);
create index h_an on habitat(annee);
create index h_pm on habitat(p_milieu);
create index h_sm on habitat(s_milieu);
create index h_ptyp on habitat(p_type);
create index h_styp on habitat(s_type);
