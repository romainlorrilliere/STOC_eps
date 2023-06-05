-- Creation des index des tables


-- point

create index p_carre on point(id_carre);
create index p_dep on point(departement);
create index p_npoint on point(num_point);

-- carre
create index c_etude on carre(etude);
create index c_dep on carre(departement);
create index c_alt on carre(altitude_median);

-- inventaire

create index i_carre on inventaire(id_carre);
create index i_point on inventaire(id_point);
create index i_etude on inventaire(etude);
create index i_date on inventaire(date);
create index i_an on inventaire(annee);
create index i_julien on inventaire(jour_julien);
create index i_pass_stoc on inventaire(passage_stoc);
create index i_ipass on inventaire(info_passage);
create index i_ipassan on inventaire(info_passage_an);
create index i_nbpass on inventaire(nombre_passage_stoc_annee);
create index i_ihd on inventaire(info_heure_debut);
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
create index o_sp9 on observation(espece);
create index o_sp6 on observation(code_sp);
create index o_dist on observation(distance_contact);

-- Seuil

create index s_sp6 on seuil_obs(code_sp);

-- Habitat

create index h_point on habitat(id_point);
create index h_an on habitat(annee);
create index h_pm on habitat(p_milieu);
create index h_sm on habitat(s_milieu);
create index h_ptyp on habitat(p_type);
create index h_styp on habitat(s_type);
CREATE INDEX h_habsug ON habitat(habitat_sug);
CREATE INDEX h_habcont ON habitat(habitat_consistent);
CREATE INDEX h_phabsug ON habitat(p_habitat_sug);
CREATE INDEX h_shabsug ON habitat(s_habitat_sug);



-- point_annee

CREATE INDEX pa_point ON point_annee(id_point);
CREATE INDEX pa_carre ON point_annee(id_carre);
CREATE INDEX pa_an ON point_annee(annee);
CREATE INDEX pa_fp ON point_annee(foret_p);
CREATE INDEX pa_op ON point_annee(ouvert_p);
CREATE INDEX pa_ap ON point_annee(agri_p);
CREATE INDEX pa_up ON point_annee(urbain_p);
CREATE INDEX pa_fps ON point_annee(foret_ps);
CREATE INDEX pa_ops ON point_annee(ouvert_ps);
CREATE INDEX pa_aps ON point_annee(agri_ps);
CREATE INDEX pa_ups ON point_annee(urbain_ps);
CREATE INDEX pa_phabpass1 ON point_annee(p_habitat_sug_pass1);
CREATE INDEX pa_shabpass1 ON point_annee(s_habitat_sug_pass1);
CREATE INDEX pa_phabpass2 ON point_annee(p_habitat_sug_pass2);
CREATE INDEX pa_shabpass2 ON point_annee(s_habitat_sug_pass2);

CREATE INDEX pa_pmpass1 ON point_annee(p_milieu_sug_pass1);
CREATE INDEX pa_smpass1 ON point_annee(s_milieu_sug_pass1);
CREATE INDEX pa_pmpass2 ON point_annee(p_milieu_sug_pass2);
CREATE INDEX pa_smpass2 ON point_annee(s_milieu_sug_pass2);

CREATE INDEX pa_ipa ON point_annee(info_passage_an);
CREATE INDEX pa_qis ON point_annee(qualite_inventaire_stoc);



-- carre_annee

CREATE INDEX ca_carre ON carre_annee(id_carre);
CREATE INDEX ca_an ON carre_annee(annee);
CREATE INDEX ca_fp ON carre_annee(foret_p);
CREATE INDEX ca_op ON carre_annee(ouvert_p);
CREATE INDEX ca_ap ON carre_annee(agri_p);
CREATE INDEX ca_up ON carre_annee(urbain_p);
CREATE INDEX ca_fps ON carre_annee(foret_ps);
CREATE INDEX ca_ops ON carre_annee(ouvert_ps);
CREATE INDEX ca_aps ON carre_annee(agri_ps);
CREATE INDEX ca_ups ON carre_annee(urbain_ps);
CREATE INDEX ca_nbfp ON carre_annee(nbp_foret_p);
CREATE INDEX ca_nbfps ON carre_annee(nbp_foret_ps);
CREATE INDEX ca_nbop ON carre_annee(nbp_ouvert_p);
CREATE INDEX ca_nbops ON carre_annee(nbp_ouvert_ps);
CREATE INDEX ca_nbap ON carre_annee(nbp_agri_p);
CREATE INDEX ca_nbaps ON carre_annee(nbp_agri_ps);
CREATE INDEX ca_nbup ON carre_annee(nbp_urbain_p);
CREATE INDEX ca_nbups ON carre_annee(nbp_urbain_ps);
CREATE INDEX ca_qis ON carre_annee(qualite_inventaire_stoc);
CREATE INDEX ca_nbpsa ON carre_annee(nombre_passage_stoc_annee);
CREATE INDEX ca_ipa ON carre_annee(info_passage_an);
