
### Log ex #####

> d44 <- makeTableVP(departement=44,id_output="44")

QUERY: 
  SELECT
  o.id_inventaire as code_inventaire,  i.etude as Etude,
  p.site as Site, FALSE as Pays,
  p.departement as Département, p.insee as INSEE,
  p.commune as Commune, ('CARRE N°'||o.id_carre::varchar) as "N°..Carré.EPS",
  o.date as Date,i.heure_debut as Heure,
  i.heure_fin as "Heure.fin",  i.passage_stoc as "N°..Passage",
  i.observateur as Observateur,  i.email as Email,
  ('Point N°'||o.num_point::varchar) as EXPORT_STOC_TEXT_EPS, p.altitude as Altitude,
  '0'::varchar(1) as Classe, o.espece as Espèce,
  o.abondance as Nombre,o.distance_contact as "Distance.de.contact",
  p.longitude_wgs84 as Longitude, p.latitude_wgs84 as Latitude,
  '2' as "Type.de.coodonnées", 'WGS84' as "Type.de.coordonnées.lambert",
  i.nuage as "EPS.Nuage",  i.pluie as "EPS.Pluie",
  i.vent as "EPS.Vent",  i.visibilite as "EPS.Visibilité",
  i.neige as "EPS.Neige", 'NA' as "EPS.Transport",
  h.p_milieu as "EPS.P.Milieu",  h.p_type as "EPS.P.Type",
  h.p_cat1 as "EPS.P.Cat1", h.p_cat2 as "EPS.P.Cat2",
  h.s_milieu as "EPS.S.Milieu",  h.s_type as "EPS.S.Type",
  h.s_cat1 as "EPS.S.Cat1", h.s_cat2 as "EPS.S.Cat2",
  o.db,  o.date_export as date_import,
  '2018-07-23'::varchar(10) as date_export,
  'Lorrilliere Romain'::varchar(50) as operateur,
  'lorrilliere@mnhn.fr'::varchar(50) as email_operateur
FROM
  public.point as p,   public.point_annee as pa,  public.carre as c, public.carre_annee as ca, public.inventaire as i,  public.observation as o,  public.species as s,  public.habitat as h
WHERE
  o.id_inventaire = i.pk_inventaire AND   o.id_point = p.pk_point AND  o.id_point = pa.id_point AND  o.annee = pa.annee AND  o.id_carre = ca.id_carre AND  o.annee = ca.annee AND  o.id_carre = c.pk_carre AND  o.espece = s.pk_species AND  o.id_inventaire = h.pk_habitat  and i.annee >= 2001  and i.annee <= 2018 and c.etude in ('STOC_EPS', 'STOC_ONF')  and  c.altitude <= 8000 and  c.altitude >= 0

 and p.departement in  ('44')  
 

 -->  export/data_FrenchBBS_VigiePlume_44_2001_2018.csv 

     #      ==> Duree: 6 minutes




> dk100 <- makeTablePoint(id_output="Karine_P_20180705_100",lastYear=2107,distance_contact="100",user="romain",mp=mp)

Requete principale:

 select om.id_point as point, p.id_carre as carre, om.annee as annee,
(om.annee::varchar(4)||om.id_point::varchar(100))::varchar(100) as id_point_annee,
c.etude as etude, p.commune,p.insee,p.departement as departement,
code_sp , e.scientific_name as nom_scientifique, e.french_name as nom_francais, e.english_name as nom_anglais, e.euring as code_espece_euring,e.taxref as code_espece_taxref,
abond_brut as abondance_brut, abond as abondance, pa.qualite_inventaire_stoc as qualite_inventaire_stoc,
p.altitude, longitude_wgs84,  latitude_wgs84,  longitude_grid_wgs84,latitude_grid_wgs84,
pa.p_milieu as habitat_principal, pa.s_milieu as habitat_secondaire, pa.p_dernier_descri as temps_depuis_derniere_description_habitat ,
   pa.foret_p as foret_p, pa.agri_p as agricole_p, pa.urbain_p as urbain_p, pa.ouvert_p as ouvert_p, pa.foret_ps as foret_ps, pa.agri_ps as agricole_ps, pa.urbain_ps as urbain_ps, pa.ouvert_ps as ouvert_ps,
  nbp_foret_p  as carre_nb_pts_foret_p, nbp_ouvert_p as carre_nb_pts_ouvert_p, nbp_agri_p as carre_nb_pts_agricole_p, nbp_urbain_p as carre_nb_pts_urbain_p,
nbp_foret_ps as carre_nb_pts_foret_ps, nbp_ouvert_ps as carre_nb_pts_ouvert_ps, nbp_agri_ps as carre_nb_pts_agricole_ps, nbp_urbain_ps  as carre_nb_pts_urbain_ps,  p.db as data_base_name, '2018-07-05'::varchar(10) as date_export,
'Lorrilliere Romain'::varchar(50) as operateur,
'lorrilliere@mnhn.fr'::varchar(50) as email_operateur

from(
	select id_point, annee,code_sp,max(abond_brut) as abond_brut,max(abond) as abond
		from -- ## begin ## correction abondance obs par seuil
			(select id_inventaire,id_point, passage_stoc, annee, code_sp,abond as abond_brut, abond100_seuil99 as seuil_abondance_sp,LEAST(abond,abond100_seuil99) as abond
			from -- ## begin ## selection classe de distance et different filtre
				(  SELECT id_inventaire, code_sp, sum(abondance) as abond
				FROM
				observation as o, inventaire as i,point as p, point_annee as pa, carre as c
				WHERE
				o.id_inventaire = i.pk_inventaire and
				o.id_carre = c.pk_carre and
				o.id_point = p.pk_point and
				o.id_point = pa.id_point and o.annee=pa.annee and
				pa.qualite_inventaire_stoc > 0 and
                                 code_sp not  in  ('MOTFLA' , 'SAXRUB' , 'ANTPRA' , 'OENOEN' , 'PHYTRO')  and

                                passage_stoc in (1,2) and  distance_contact in ('LESS25','LESS100') and  i.annee >= 2001  and i.annee <= 2107 and c.etude in ('STOC_EPS', 'STOC_ONF')  and  p.altitude <= 8000 and p.altitude >= 0





			GROUP BY
			id_inventaire, code_sp
                                union
                                  --  ajout des especes tardives dont on ne garde que le second passage
             SELECT id_inventaire, code_sp, sum(abondance) as abond
				FROM
				observation as o, inventaire as i,point as p, point_annee as pa, carre as c
				WHERE
				o.id_inventaire = i.pk_inventaire and
				o.id_carre = c.pk_carre and
				o.id_point = p.pk_point and
				o.id_point = pa.id_point and o.annee=pa.annee and
				pa.qualite_inventaire_stoc > 0 and
                                 code_sp in  ('MOTFLA' , 'SAXRUB' , 'ANTPRA' , 'OENOEN' , 'PHYTRO')  and    passage_stoc = 2 and  distance_contact in ('LESS25','LESS100') and  i.annee >= 2001  and i.annee <= 2107 and c.etude in ('STOC_EPS', 'STOC_ONF')  and  p.altitude <= 8000 and p.altitude >= 0





			GROUP BY
			id_inventaire, code_sp
	                        ) --  ## end ## selection classe de distance et different filtre
			as opb, espece_abondance_point_seuil as s, inventaire as i
			WHERE
			opb.code_sp = s.pk_species and opb.id_inventaire = i.pk_inventaire

			) -- ## end ## correction abondance obs par seuil
		as op
		group by id_point,annee,code_sp
		 )-- ## end ## obs max par point sur 2 passages
		 as om , point as p, carre as c, species as e , point_annee as pa , carre_annee as ca
where
om.id_point = p.pk_point and p.id_carre = c.pk_carre and om.code_sp = e.pk_species and om.id_point = pa.id_point and om.annee = pa.annee and p.id_carre = ca.id_carre and om.annee = ca.annee
order by
om.id_point, annee,code_sp;

 -->  export/data_FrenchBBS_point_Karine_P_20180705_100_allSp_2001_2107fr.csv

     #      ==> Duree: 5 minutes
> dk200 <- makeTablePoint(id_output="Karine_P_20180705_200",lastYear=2107,distance_contact="200",user="romain",mp=mp)

Requete principale:

 select om.id_point as point, p.id_carre as carre, om.annee as annee,
(om.annee::varchar(4)||om.id_point::varchar(100))::varchar(100) as id_point_annee,
c.etude as etude, p.commune,p.insee,p.departement as departement,
code_sp , e.scientific_name as nom_scientifique, e.french_name as nom_francais, e.english_name as nom_anglais, e.euring as code_espece_euring,e.taxref as code_espece_taxref,
abond_brut as abondance_brut, abond as abondance, pa.qualite_inventaire_stoc as qualite_inventaire_stoc,
p.altitude, longitude_wgs84,  latitude_wgs84,  longitude_grid_wgs84,latitude_grid_wgs84,
pa.p_milieu as habitat_principal, pa.s_milieu as habitat_secondaire, pa.p_dernier_descri as temps_depuis_derniere_description_habitat ,
   pa.foret_p as foret_p, pa.agri_p as agricole_p, pa.urbain_p as urbain_p, pa.ouvert_p as ouvert_p, pa.foret_ps as foret_ps, pa.agri_ps as agricole_ps, pa.urbain_ps as urbain_ps, pa.ouvert_ps as ouvert_ps,
  nbp_foret_p  as carre_nb_pts_foret_p, nbp_ouvert_p as carre_nb_pts_ouvert_p, nbp_agri_p as carre_nb_pts_agricole_p, nbp_urbain_p as carre_nb_pts_urbain_p,
nbp_foret_ps as carre_nb_pts_foret_ps, nbp_ouvert_ps as carre_nb_pts_ouvert_ps, nbp_agri_ps as carre_nb_pts_agricole_ps, nbp_urbain_ps  as carre_nb_pts_urbain_ps,  p.db as data_base_name, '2018-07-05'::varchar(10) as date_export,
'Lorrilliere Romain'::varchar(50) as operateur,
'lorrilliere@mnhn.fr'::varchar(50) as email_operateur

from(
	select id_point, annee,code_sp,max(abond_brut) as abond_brut,max(abond) as abond
		from -- ## begin ## correction abondance obs par seuil
			(select id_inventaire,id_point, passage_stoc, annee, code_sp,abond as abond_brut, abond200_seuil99 as seuil_abondance_sp,LEAST(abond,abond200_seuil99) as abond
			from -- ## begin ## selection classe de distance et different filtre
				(  SELECT id_inventaire, code_sp, sum(abondance) as abond
				FROM
				observation as o, inventaire as i,point as p, point_annee as pa, carre as c
				WHERE
				o.id_inventaire = i.pk_inventaire and
				o.id_carre = c.pk_carre and
				o.id_point = p.pk_point and
				o.id_point = pa.id_point and o.annee=pa.annee and
				pa.qualite_inventaire_stoc > 0 and
                                 code_sp not  in  ('MOTFLA' , 'SAXRUB' , 'ANTPRA' , 'OENOEN' , 'PHYTRO')  and

                                passage_stoc in (1,2) and  distance_contact in ('LESS25','LESS100','MORE100','LESS200') and  i.annee >= 2001  and i.annee <= 2107 and c.etude in ('STOC_EPS', 'STOC_ONF')  and  p.altitude <= 8000 and p.altitude >= 0





			GROUP BY
			id_inventaire, code_sp
                                union
                                  --  ajout des especes tardives dont on ne garde que le second passage
             SELECT id_inventaire, code_sp, sum(abondance) as abond
				FROM
				observation as o, inventaire as i,point as p, point_annee as pa, carre as c
				WHERE
				o.id_inventaire = i.pk_inventaire and
				o.id_carre = c.pk_carre and
				o.id_point = p.pk_point and
				o.id_point = pa.id_point and o.annee=pa.annee and
				pa.qualite_inventaire_stoc > 0 and
                                 code_sp in  ('MOTFLA' , 'SAXRUB' , 'ANTPRA' , 'OENOEN' , 'PHYTRO')  and    passage_stoc = 2 and  distance_contact in ('LESS25','LESS100','MORE100','LESS200') and  i.annee >= 2001  and i.annee <= 2107 and c.etude in ('STOC_EPS', 'STOC_ONF')  and  p.altitude <= 8000 and p.altitude >= 0





			GROUP BY
			id_inventaire, code_sp
	                        ) --  ## end ## selection classe de distance et different filtre
			as opb, espece_abondance_point_seuil as s, inventaire as i
			WHERE
			opb.code_sp = s.pk_species and opb.id_inventaire = i.pk_inventaire

			) -- ## end ## correction abondance obs par seuil
		as op
		group by id_point,annee,code_sp
		 )-- ## end ## obs max par point sur 2 passages
		 as om , point as p, carre as c, species as e , point_annee as pa , carre_annee as ca
where
om.id_point = p.pk_point and p.id_carre = c.pk_carre and om.code_sp = e.pk_species and om.id_point = pa.id_point and om.annee = pa.annee and p.id_carre = ca.id_carre and om.annee = ca.annee
order by
om.id_point, annee,code_sp;

 -->  export/data_FrenchBBS_point_Karine_P_20180705_200_allSp_2001_2107fr.csv

     #      ==> Duree: 6 minutes
> dkInf <- makeTablePoint(id_output="Karine_P_20180705_Inf",lastYear=2107,distance_contact="inf",user="romain",mp=mp)

Requete principale:

 select om.id_point as point, p.id_carre as carre, om.annee as annee,
(om.annee::varchar(4)||om.id_point::varchar(100))::varchar(100) as id_point_annee,
c.etude as etude, p.commune,p.insee,p.departement as departement,
code_sp , e.scientific_name as nom_scientifique, e.french_name as nom_francais, e.english_name as nom_anglais, e.euring as code_espece_euring,e.taxref as code_espece_taxref,
abond_brut as abondance_brut, abond as abondance, pa.qualite_inventaire_stoc as qualite_inventaire_stoc,
p.altitude, longitude_wgs84,  latitude_wgs84,  longitude_grid_wgs84,latitude_grid_wgs84,
pa.p_milieu as habitat_principal, pa.s_milieu as habitat_secondaire, pa.p_dernier_descri as temps_depuis_derniere_description_habitat ,
   pa.foret_p as foret_p, pa.agri_p as agricole_p, pa.urbain_p as urbain_p, pa.ouvert_p as ouvert_p, pa.foret_ps as foret_ps, pa.agri_ps as agricole_ps, pa.urbain_ps as urbain_ps, pa.ouvert_ps as ouvert_ps,
  nbp_foret_p  as carre_nb_pts_foret_p, nbp_ouvert_p as carre_nb_pts_ouvert_p, nbp_agri_p as carre_nb_pts_agricole_p, nbp_urbain_p as carre_nb_pts_urbain_p,
nbp_foret_ps as carre_nb_pts_foret_ps, nbp_ouvert_ps as carre_nb_pts_ouvert_ps, nbp_agri_ps as carre_nb_pts_agricole_ps, nbp_urbain_ps  as carre_nb_pts_urbain_ps,  p.db as data_base_name, '2018-07-05'::varchar(10) as date_export,
'Lorrilliere Romain'::varchar(50) as operateur,
'lorrilliere@mnhn.fr'::varchar(50) as email_operateur

from(
	select id_point, annee,code_sp,max(abond_brut) as abond_brut,max(abond) as abond
		from -- ## begin ## correction abondance obs par seuil
			(select id_inventaire,id_point, passage_stoc, annee, code_sp,abond as abond_brut, abondAll_seuil99 as seuil_abondance_sp,LEAST(abond,abondAll_seuil99) as abond
			from -- ## begin ## selection classe de distance et different filtre
				(  SELECT id_inventaire, code_sp, sum(abondance) as abond
				FROM
				observation as o, inventaire as i,point as p, point_annee as pa, carre as c
				WHERE
				o.id_inventaire = i.pk_inventaire and
				o.id_carre = c.pk_carre and
				o.id_point = p.pk_point and
				o.id_point = pa.id_point and o.annee=pa.annee and
				pa.qualite_inventaire_stoc > 0 and
                                 code_sp not  in  ('MOTFLA' , 'SAXRUB' , 'ANTPRA' , 'OENOEN' , 'PHYTRO')  and

                                passage_stoc in (1,2) and  distance_contact in ('U','LESS25','LESS100','MORE100','LESS200','MORE200') and  i.annee >= 2001  and i.annee <= 2107 and c.etude in ('STOC_EPS', 'STOC_ONF')  and  p.altitude <= 8000 and p.altitude >= 0





			GROUP BY
			id_inventaire, code_sp
                                union
                                  --  ajout des especes tardives dont on ne garde que le second passage
             SELECT id_inventaire, code_sp, sum(abondance) as abond
				FROM
				observation as o, inventaire as i,point as p, point_annee as pa, carre as c
				WHERE
				o.id_inventaire = i.pk_inventaire and
				o.id_carre = c.pk_carre and
				o.id_point = p.pk_point and
				o.id_point = pa.id_point and o.annee=pa.annee and
				pa.qualite_inventaire_stoc > 0 and
                                 code_sp in  ('MOTFLA' , 'SAXRUB' , 'ANTPRA' , 'OENOEN' , 'PHYTRO')  and    passage_stoc = 2 and  distance_contact in ('U','LESS25','LESS100','MORE100','LESS200','MORE200') and  i.annee >= 2001  and i.annee <= 2107 and c.etude in ('STOC_EPS', 'STOC_ONF')  and  p.altitude <= 8000 and p.altitude >= 0





			GROUP BY
			id_inventaire, code_sp
	                        ) --  ## end ## selection classe de distance et different filtre
			as opb, espece_abondance_point_seuil as s, inventaire as i
			WHERE
			opb.code_sp = s.pk_species and opb.id_inventaire = i.pk_inventaire

			) -- ## end ## correction abondance obs par seuil
		as op
		group by id_point,annee,code_sp
		 )-- ## end ## obs max par point sur 2 passages
		 as om , point as p, carre as c, species as e , point_annee as pa , carre_annee as ca
where
om.id_point = p.pk_point and p.id_carre = c.pk_carre and om.code_sp = e.pk_species and om.id_point = pa.id_point and om.annee = pa.annee and p.id_carre = ca.id_carre and om.annee = ca.annee
order by
om.id_point, annee,code_sp;

 -->  export/data_FrenchBBS_point_Karine_P_20180705_Inf_allSp_2001_2107fr.csv

     #      ==> Duree: 6 minutes
>























d <- makeTableCarre(id_output="L_Mouysset",sp=c("ALAARV","ALERUF","ANTCAM","ANTPRA","BUTBUT","CARCAN","CORFRU","COTCOT","EMBCIR","EMBCIT","FALTIN","GALCRI","LANCOL","LULARB",
"MILCAL","MOTFLA","OENOEN","PERPER","SAXRUB","SAXTOR","SYLCOM","UPUEPO","VANVAN"),altitude_max=800,champsHabitat=TRUE,user="romain",mp="crexCREX44!")
d <- makeTableCarre(id_output="L_Mouysset",sp=c("ALAARV","ALERUF","ANTCAM","ANTPRA","BUTBUT","CARCAN","CORFRU","COTCOT","EMBCIR","EMBCIT","FALTIN","GALCRI","LANCOL","LULARB",
+ "MILCAL","MOTFLA","OENOEN","PERPER","SAXRUB","SAXTOR","SYLCOM","UPUEPO","VANVAN"),altitude_max=800,champsHabitat=TRUE,user="romain",mp="****")

Requete principale:


select oc.id_carre as carre, oc.annee, (oc.annee::varchar(4)||oc.id_carre::varchar(100))::varchar(100) as id_carre_annee, c.etude,
qualite_inventaire_stoc, commune,insee,departement,
code_sp, e.scientific_name, e.french_name, e.english_name as nom_anglais, e.euring,e.taxref,
abond_brut as abondance_brut, abond  as abondance,
altitude, longitude_grid_wgs84,latitude_grid_wgs84,  foret_p, ouvert_p, agri_p, urbain_p,
foret_ps, ouvert_ps, agri_ps, urbain_ps,
nbp_foret_p, nbp_ouvert_p, nbp_agri_p, nbp_urbain_p,
nbp_foret_ps, nbp_ouvert_ps, nbp_agri_ps, nbp_urbain_ps,  c.db as data_base_name, '2018-06-04'::varchar(10) as date_export,
'Lorrilliere Romain'::varchar(50) as operateur,
'lorrilliere@mnhn.fr'::varchar(50) as email_operateur

from( -- ## begin ## somme sur carre des max par points sur 2 passages
	select id_carre, annee,code_sp,sum(abond_brut) as abond_brut,sum(abond) as abond
	from -- ## begin ## obs max par point sur 2 passages
		(select id_point, annee,code_sp,max(abond_brut) as abond_brut,max(abond) as abond
		from -- ## begin ## correction abondance obs par seuil
			(select id_inventaire,id_point, passage_stoc, annee, code_sp,abond as abond_brut, abondAll_seuil99 as seuil_abondance_sp,LEAST(abond, abondAll_seuil99) as abond
			from -- ## begin ## selection classe de distance et different filtre
				( SELECT id_inventaire, code_sp, sum(abondance) as abond
				FROM
				observation as o, inventaire as i,carre_annee as ca, carre as c
				WHERE
				o.id_inventaire = i.pk_inventaire and
				o.id_carre = c.pk_carre and
				o.id_carre = ca.id_carre and o.annee=ca.annee and
				ca.qualite_inventaire_stoc > 0 and
                                  code_sp in  ('ALAARV' , 'ALERUF' , 'ANTCAM' , 'BUTBUT' , 'CARCAN' , 'CORFRU' , 'COTCOT' , 'EMBCIR' , 'EMBCIT' , 'FALTIN' , 'GALCRI' , 'LANCOL' , 'LULARB' , 'MILCAL' , 'PERPER' , 'SAXTOR' , 'SYLCOM' , 'UPUEPO' , 'VANVAN')  and    passage_stoc in (1,2) and  distance_contact in ('U','LESS25','LESS100','MORE100','LESS200','MORE200') and  i.annee >= 2001
and i.annee <= 2018 and c.etude in ('STOC_EPS', 'STOC_ONF')  and  c.altitude <= 800 and  c.altitude >= 0



			GROUP BY
			id_inventaire, code_sp
                                union
                                  --  ## begin ## ajout des especes tardives dont on ne garde que le second passage
             SELECT id_inventaire, code_sp, sum(abondance) as abond
				FROM
				observation as o, inventaire as i,carre_annee as ca, carre as c
				WHERE
				o.id_inventaire = i.pk_inventaire and
				o.id_carre = c.pk_carre and
				o.id_carre = ca.id_carre and o.annee=ca.annee and
				ca.qualite_inventaire_stoc > 0 and
                                  code_sp in  ('ANTPRA' , 'MOTFLA' , 'OENOEN' , 'SAXRUB')  and    passage_stoc = 2 and  distance_contact in ('U','LESS25','LESS100','MORE100','LESS200','MORE200') and  i.annee >= 2001
and i.annee <= 2018 and c.etude in ('STOC_EPS', 'STOC_ONF')  and  c.altitude <= 800 and  c.altitude >= 0



			GROUP BY
			id_inventaire, code_sp
				) -- ## end ## selection classe de distance et different filtre
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
as oc, carre as c, species as e, carre_annee as ca
where
oc.id_carre = c.pk_carre and oc.code_sp = e.pk_species and oc.id_carre = ca.id_carre and oc.annee = ca.annee
order by
oc.id_carre, annee,code_sp;

 -->  export/data_FrenchBBS_squarre_L_Mouysset_23sp_2001_2018.csv
                                        #      ==> Duree: 26 minutes



##################################################


























q <- "select * from species_list_indicateur"
dq <- dbGetQuery(con,q)
sp_montane <- subset(dq,montane_lehikoinen)$pk_species

d.mont <- makeTablePoint(user="romain",mp="crexCREX44!",output=TRUE,sp=sp_montane,champsHabitat=FALSE,lastYear=2017,selectTypeHabitat="ouvert_ps",altitude_min=900,isEnglish=TRUE,id_output="montaneBird_Lehikoinen_dist100_alt900",distance_contact=100)

Requete principale:

 select om.id_point as point, p.id_carre as carre, om.annee as annee,
(om.annee::varchar(4)||om.id_point::varchar(100))::varchar(100) as id_point_annee,
c.etude as etude, p.commune,p.insee,p.departement as departement,
code_sp , e.scientific_name as nom_scientifique, e.french_name as nom_francais, e.english_name as nom_anglais, e.euring as code_espece_euring,e.taxref as code_espece_taxref,
abond_brut as abondance_brut, abond as abondance, pa.qualite_inventaire_stoc as qualite_inventaire_stoc,
p.altitude, longitude_wgs84,  latitude_wgs84,  longitude_grid_wgs84,latitude_grid_wgs84,
 p.db as data_base_name, '2018-05-03'::varchar(10) as date_export,
'Lorrilliere Romain'::varchar(50) as operateur,
'lorrilliere@mnhn.fr'::varchar(50) as email_operateur

from(
	select id_point, annee,code_sp,max(abond_brut) as abond_brut,max(abond) as abond
		from -- ## begin ## correction abondance obs par seuil
			(select id_inventaire,id_point, passage_stoc, annee, code_sp,abond as abond_brut, abond100_seuil99 as seuil_abondance_sp,LEAST(abond,abond100_seuil99) as abond
			from -- ## begin ## selection classe de distance et different filtre
				(  SELECT id_inventaire, code_sp, sum(abondance) as abond
				FROM
				observation as o, inventaire as i,point as p, point_annee as pa, carre as c
				WHERE
				o.id_inventaire = i.pk_inventaire and
				o.id_carre = c.pk_carre and
				o.id_point = p.pk_point and
				o.id_point = pa.id_point and o.annee=pa.annee and
				pa.qualite_inventaire_stoc > 0 and
                                  code_sp in  ('ALAARV' , 'ALEGRA' , 'ANTSPI' , 'AQUCHR' , 'CARCAN' , 'CARFLA' , 'CARFLACAB' , 'EMBCIA' , 'FALTIN' , 'LAGMUT' , 'MONNIV' , 'MONSAX' , 'PHOOCH' , 'PRUCOL' , 'PTYRUP' , 'PYRGRA' , 'PYRRAX' , 'SERCIT' , 'SYLCUR' , 'TETRIX' , 'TICMUR' , 'TURTOR')  and

                                passage_stoc in (1,2) and  distance_contact in ('LESS25','LESS100') and  i.annee >= 2001  and i.annee <= 2017 and c.etude in ('STOC_EPS', 'STOC_ONF')  and  p.altitude <= 8000 and p.altitude >= 900



 AND ( pa.ouvert_ps = TRUE)
			GROUP BY
			id_inventaire, code_sp
                                union
                                  --  ajout des especes tardives dont on ne garde que le second passage
             SELECT id_inventaire, code_sp, sum(abondance) as abond
				FROM
				observation as o, inventaire as i,point as p, point_annee as pa, carre as c
				WHERE
				o.id_inventaire = i.pk_inventaire and
				o.id_carre = c.pk_carre and
				o.id_point = p.pk_point and
				o.id_point = pa.id_point and o.annee=pa.annee and
				pa.qualite_inventaire_stoc > 0 and
                                  code_sp in  ('OENOEN' , 'SAXRUB')  and    passage_stoc = 2 and  distance_contact in ('LESS25','LESS100') and  i.annee >= 2001  and i.annee <= 2017 and c.etude in ('STOC_EPS', 'STOC_ONF')  and  p.altitude <= 8000 and p.altitude >= 900



 AND ( pa.ouvert_ps = TRUE)
			GROUP BY
			id_inventaire, code_sp
	                        ) --  ## end ## selection classe de distance et different filtre
			as opb, espece_abondance_point_seuil as s, inventaire as i
			WHERE
			opb.code_sp = s.pk_species and opb.id_inventaire = i.pk_inventaire

			) -- ## end ## correction abondance obs par seuil
		as op
		group by id_point,annee,code_sp
		 )-- ## end ## obs max par point sur 2 passages
		 as om , point as p, carre as c, species as e , point_annee as pa , carre_annee as ca
where
om.id_point = p.pk_point and p.id_carre = c.pk_carre and om.code_sp = e.pk_species and om.id_point = pa.id_point and om.annee = pa.annee and p.id_carre = ca.id_carre and om.annee = ca.annee
order by
om.id_point, annee,code_sp;

 - Traduction des noms de colonnes
 -->  export/data_FrenchBBS_point_montaneBird_Lehikoinen_dist100_alt900_24sp_2001_2017eng.csv

     #      ==> Duree: 3 minutes

ddep <- read.csv("librairie/dept_montagne.csv")
colnames(ddep)[1] <- "district"

d.mont <- d.mont[,theColumn]
dim(d.mont)
d.mont <- merge(d.mont,ddep,by="district",all=FALSE)
dim(d.mont)

write.csv(d.mont,"export/data_FrenchBBS_point_montaneBird_Lehikoinen_dist100_alt900_24sp_2001_2018eng.csv",row.names=FALSE)

gg <- ggplot(data=d.mont,aes(x=english_name,y=abundance))+geom_violin()+theme(axis.text.x = element_text(angle = 90, hjust = 1,vjust=.1));gg
ggsave("output/abundance_montaneBird.png",gg)

gg <- ggplot(data=d.mont,aes(x=abundance))+geom_histogram(binwidth=1)+facet_wrap(~english_name,scales="free");gg
ggsave("output/abundanceHistogram_montaneBird.png",gg)





> d66 <- makeTableBrut(departement="66",id_output="ABDOLA-TROLLUX-Charlotte")

 QUERY données BRUT:
--------------

 
SELECT 
  o.pk_observation as code_observation, 
  o.id_fnat_unique_citation as code_observation_fnat,
  o.id_inventaire as code_inventaire, 
  i.etude, 
   i.observateur, 
  i.email, 
 p.site as nom_site, 
  p.commune, 
  p.insee, 
  p.departement,
  o.id_carre as code_carre,
    o.id_point as code_point, 
   o.num_point, 
     o.date, 
  o.annee, 
  i.heure_debut, 
  i.heure_fin, 
  i.duree_minute, 
   o.passage, 
  i.info_passage, 
  i.passage_stoc as numero_passage_stoc, 
  i.nombre_de_passage, 
  i.temps_entre_passage, 
  o.espece, 
   s.scientific_name as nom_scientifique, 
  s.french_name as nom_francais,
s.english_name as nom_anglais,
s.euring as code_espece_euring,
s.taxref as code_espece_taxref,
   o.abondance, 
  o.distance_contact, 
 i.nuage, 
  i.pluie, 
  i.vent, 
  i.visibilite, 
  i.neige, 
  p.altitude, 
  p.longitude_wgs84, 
  p.latitude_wgs84,
  c.longitude_grid_wgs84,
  c.latitude_grid_wgs84,
  h.p_milieu, 
  h.p_type, 
  h.p_cat1, 
  h.p_cat2, 
  h.s_milieu, 
  h.s_type, 
  h.s_cat1, 
  h.s_cat2,
   o.db, 
  o.date_export as date_import,
'2018-03-22'::varchar(10) as date_export,
'Lorrilliere Romain'::varchar(50) as operateur,
'lorrilliere@mnhn.fr'::varchar(50) as email_operateur
FROM 
  public.point as p,
  public.carre as c,
  public.inventaire as i, 
  public.observation as o, 
  public.species as s, 
  public.habitat as h
WHERE 
  o.id_inventaire = i.pk_inventaire AND
  o.id_point = p.pk_point AND
  o.id_carre = c.pk_carre AND 
  o.espece = s.pk_species AND
  o.id_inventaire = h.pk_habitat AND
  
 i.annee >= 2001  and i.annee <= 2018 and c.etude in ('STOC_EPS', 'STOC_ONF')  and  p.altitude <= 8000
 and p.departement in  ('66')  
 
 ; 
 -->  export/data_FrenchBBS_BRUT_ABDOLA-TROLLUX-Charlotte_allSp_2001_2018fr.csv 

     #      ==> Duree: 6 minutes




















 donf <- historicCarre(id_carre=c("601391","601092","251093","390135","400585","401035","600971","021542","710948","710949","710948","710949"))

 QUERY historique CARRE:
--------------

 select i.id_carre, c.departement,c.altitude,
i.annee, i.etude,  i.observateur, i.email 
from inventaire as i, carre as c
where i.id_carre = c.pk_carre and   i.annee >= 2001  and i.annee <= 2018 and i.etude in ('STOC_EPS', 'STOC_ONF')  and  c.altitude <= 8000

 and i.id_carre in  ('601391' , '601092' , '251093' , '390135' , '400585' , '401035' , '600971' , '021542' , '710948' , '710949' , '710948' , '710949')  
  
group by i.id_carre, i.etude,c.departement,c.altitude,i.annee,i.observateur,i.email
order by i.id_carre, i.annee; 
 -->  export/Historic_carre__2001_2018fr.csv 

     #      ==> Duree: 0 minutes


















> d <- makeTableBrut(departement = c("91","92","75","77","93","95","94","78"),id_output="NatureParif_IdF")

 QUERY données BRUT:
--------------

 
SELECT 
  o.pk_observation as code_observation, 
  o.id_fnat_unique_citation as code_observation_fnat,
  o.id_inventaire as code_inventaire, 
  i.etude, 
   i.observateur, 
  i.email, 
 p.site as nom_site, 
  p.commune, 
  p.insee, 
  p.departement,
  o.id_carre as code_carre,
    o.id_point as code_point, 
   o.num_point, 
     o.date, 
  o.annee, 
  i.heure_debut, 
  i.heure_fin, 
  i.duree_minute, 
   o.passage, 
  i.info_passage, 
  i.passage_stoc as numero_passage_stoc, 
  i.nombre_de_passage, 
  i.temps_entre_passage, 
  o.espece, 
   s.scientific_name as nom_scientifique, 
  s.french_name as nom_francais,
s.english_name as nom_anglais,
s.euring as code_espece_euring,
s.taxref as code_espece_taxref,
   o.abondance, 
  o.distance_contact, 
 i.nuage, 
  i.pluie, 
  i.vent, 
  i.visibilite, 
  i.neige, 
  p.altitude, 
  p.longitude_wgs84, 
  p.latitude_wgs84,
  c.longitude_grid_wgs84,
  c.latitude_grid_wgs84,
  h.p_milieu, 
  h.p_type, 
  h.p_cat1, 
  h.p_cat2, 
  h.s_milieu, 
  h.s_type, 
  h.s_cat1, 
  h.s_cat2,
   o.db, 
  o.date_export as date_import,
'2018-03-08'::varchar(10) as date_export,
'Lorrilliere Romain'::varchar(50) as operateur,
'lorrilliere@mnhn.fr'::varchar(50) as email_operateur
FROM 
  public.point as p,
  public.carre as c,
  public.inventaire as i, 
  public.observation as o, 
  public.species as s, 
  public.habitat as h
WHERE 
  o.id_inventaire = i.pk_inventaire AND
  o.id_point = p.pk_point AND
  o.id_carre = c.pk_carre AND 
  o.espece = s.pk_species AND
  o.id_inventaire = h.pk_habitat AND
  
 i.annee >= 2001  and i.annee <= 2018 and c.etude in ('STOC_EPS', 'STOC_ONF')  and  p.altitude <= 8000
 and p.departement in  ('91' , '92' , '75' , '77' , '93' , '95' , '94' , '78')  
 
 ; 
 -->  export/data_FrenchBBS_BRUT_NatureParif_IdF_allSp_2001_2018fr.csv 

     #      ==> Duree: 18 minutes
> d <- makeTableBrut(id_output="NatureParif_France")

 QUERY données BRUT:
--------------

 
SELECT 
  o.pk_observation as code_observation, 
  o.id_fnat_unique_citation as code_observation_fnat,
  o.id_inventaire as code_inventaire, 
  i.etude, 
   i.observateur, 
  i.email, 
 p.site as nom_site, 
  p.commune, 
  p.insee, 
  p.departement,
  o.id_carre as code_carre,
    o.id_point as code_point, 
   o.num_point, 
     o.date, 
  o.annee, 
  i.heure_debut, 
  i.heure_fin, 
  i.duree_minute, 
   o.passage, 
  i.info_passage, 
  i.passage_stoc as numero_passage_stoc, 
  i.nombre_de_passage, 
  i.temps_entre_passage, 
  o.espece, 
   s.scientific_name as nom_scientifique, 
  s.french_name as nom_francais,
s.english_name as nom_anglais,
s.euring as code_espece_euring,
s.taxref as code_espece_taxref,
   o.abondance, 
  o.distance_contact, 
 i.nuage, 
  i.pluie, 
  i.vent, 
  i.visibilite, 
  i.neige, 
  p.altitude, 
  p.longitude_wgs84, 
  p.latitude_wgs84,
  c.longitude_grid_wgs84,
  c.latitude_grid_wgs84,
  h.p_milieu, 
  h.p_type, 
  h.p_cat1, 
  h.p_cat2, 
  h.s_milieu, 
  h.s_type, 
  h.s_cat1, 
  h.s_cat2,
   o.db, 
  o.date_export as date_import,
'2018-03-08'::varchar(10) as date_export,
'Lorrilliere Romain'::varchar(50) as operateur,
'lorrilliere@mnhn.fr'::varchar(50) as email_operateur
FROM 
  public.point as p,
  public.carre as c,
  public.inventaire as i, 
  public.observation as o, 
  public.species as s, 
  public.habitat as h
WHERE 
  o.id_inventaire = i.pk_inventaire AND
  o.id_point = p.pk_point AND
  o.id_carre = c.pk_carre AND 
  o.espece = s.pk_species AND
  o.id_inventaire = h.pk_habitat AND
  
 i.annee >= 2001  and i.annee <= 2018 and c.etude in ('STOC_EPS', 'STOC_ONF')  and  p.altitude <= 8000

 
 ; 
 -->  export/data_FrenchBBS_BRUT_NatureParif_France_allSp_2001_2018fr.csv 

     #      ==> Duree: 3 minutes
> 























> dif <- makeTableCarre(departement=c("91","92","75","77","93","95","94","78"), formatTrend = TRUE,id_output="NatureParif")

Requete principale:

 
select oc.id_carre as carre, oc.annee, (oc.annee::varchar(4)||oc.id_carre::varchar(100))::varchar(100) as id_carre_annee, c.etude,
qualite_inventaire_stoc, commune,insee,departement, 
code_sp, e.scientific_name, e.french_name, e.euring,e.taxref,
abond_brut as abondance_brut, abond  as abondance, 
altitude, longitude_grid_wgs84,latitude_grid_wgs84,  foret_p, ouvert_p, agri_p, urbain_p,
foret_ps, ouvert_ps, agri_ps, urbain_ps,
nbp_foret_p, nbp_ouvert_p, nbp_agri_p, nbp_urbain_p,
nbp_foret_ps, nbp_ouvert_ps, nbp_agri_ps, nbp_urbain_ps,  c.db as data_base_name, '2018-02-28'::varchar(10) as date_export,
'Lorrilliere Romain'::varchar(50) as operateur,
'lorrilliere@mnhn.fr'::varchar(50) as email_operateur

from( -- ## begin ## somme sur carre des max par points sur 2 passages
	select id_carre, annee,code_sp,sum(abond_brut) as abond_brut,sum(abond) as abond
	from -- ## begin ## obs max par point sur 2 passages
		(select id_point, annee,code_sp,max(abond_brut) as abond_brut,max(abond) as abond
		from -- ## begin ## correction abondance obs par seuil
			(select id_inventaire,id_point, passage_stoc, annee, code_sp,abond as abond_brut, abondAll_seuil99 as seuil_abondance_sp,LEAST(abond, abondAll_seuil99) as abond
			from -- ## begin ## selection classe de distance et different filtre
				( SELECT id_inventaire, code_sp, sum(abondance) as abond
				FROM 
				observation as o, inventaire as i,carre_annee as ca, carre as c
				WHERE 
				o.id_inventaire = i.pk_inventaire and 
				o.id_carre = c.pk_carre and 
				o.id_carre = ca.id_carre and o.annee=ca.annee and 
				ca.qualite_inventaire_stoc > 0 and
                                 code_sp not  in  ('MOTFLA' , 'SAXRUB' , 'ANTPRA' , 'OENOEN' , 'PHYTRO')  and    passage_stoc in (1,2) and  i.annee >= 2001  and i.annee <= 2018 and c.etude in ('STOC_EPS', 'STOC_ONF')  and  c.altitude <= 8000  and departement in  ('91' , '92' , '75' , '77' , '93' , '95' , '94' , '78')     
			GROUP BY 
			id_inventaire, code_sp 
                                union
                                  --  ## begin ## ajout des especes tardives dont on ne garde que le second passage
             SELECT id_inventaire, code_sp, sum(abondance) as abond
				FROM 
				observation as o, inventaire as i,carre_annee as ca, carre as c
				WHERE 
				o.id_inventaire = i.pk_inventaire and 
				o.id_carre = c.pk_carre and 
				o.id_carre = ca.id_carre and o.annee=ca.annee and 
				ca.qualite_inventaire_stoc > 0 and
                                 code_sp in  ('MOTFLA' , 'SAXRUB' , 'ANTPRA' , 'OENOEN' , 'PHYTRO')  and    passage_stoc = 2 and  i.annee >= 2001  and i.annee <= 2018 and c.etude in ('STOC_EPS', 'STOC_ONF')  and  c.altitude <= 8000  and departement in  ('91' , '92' , '75' , '77' , '93' , '95' , '94' , '78')     
			GROUP BY 
			id_inventaire, code_sp
				) -- ## end ## selection classe de distance et different filtre  
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
as oc, carre as c, species as e, carre_annee as ca 
where 
oc.id_carre = c.pk_carre and oc.code_sp = e.pk_species and oc.id_carre = ca.id_carre and oc.annee = ca.annee
order by 
oc.id_carre, annee,code_sp; 

 -->  export/data_FrenchBBS_squarre_NatureParif_allSp_2001_2018.csv 













> cti_national(query=TRUE,firstYear = 2003,lastYear = 2015,id="France_Aksu",fileName="dataCTI_Aksu_2018-02-07")

Requete cti:

 
select cti.id_carre_annee, ca.id_carre, ca.annee, cti, cti_brut,qualite_inventaire_stoc, commune,insee,departement,
 altitude, longitude_grid_wgs84,latitude_grid_wgs84, '2018-02-07'::varchar(10) as date_export,
'Lorrilliere Romain'::varchar(50) as operateur,
'lorrilliere@mnhn.fr'::varchar(50) as email_operateur
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
     				( SELECT id_inventaire, code_sp, sum(abondance) as abond
				FROM 
				observation as o, inventaire as i,carre_annee as ca, carre as c
				WHERE 
				o.id_inventaire = i.pk_inventaire and 
				o.id_carre = c.pk_carre and 
				o.id_carre = ca.id_carre and o.annee=ca.annee and 
				ca.qualite_inventaire_stoc > 0 and
                                 code_sp not  in  ('MOTFLA' , 'SAXRUB' , 'ANTPRA' , 'OENOEN' , 'PHYTRO')  and    passage_stoc in (1,2) and  i.annee >= 2003  and i.annee <= 2015 and c.etude in ('STOC_EPS', 'STOC_ONF')  and  c.altitude <= 800    
			GROUP BY 
			id_inventaire, code_sp 
                                union
                                  --  ## begin ## ajout des especes tardives dont on ne garde que le second passage
             SELECT id_inventaire, code_sp, sum(abondance) as abond
				FROM 
				observation as o, inventaire as i,carre_annee as ca, carre as c
				WHERE 
				o.id_inventaire = i.pk_inventaire and 
				o.id_carre = c.pk_carre and 
				o.id_carre = ca.id_carre and o.annee=ca.annee and 
				ca.qualite_inventaire_stoc > 0 and
                                 code_sp in  ('MOTFLA' , 'SAXRUB' , 'ANTPRA' , 'OENOEN' , 'PHYTRO')  and    passage_stoc = 2 and  i.annee >= 2003  and i.annee <= 2015 and c.etude in ('STOC_EPS', 'STOC_ONF')  and  c.altitude <= 800    
			GROUP BY 
			id_inventaire, code_sp
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
ca.id_carre, annee ; 


Estimation de la variation annuelle cti~ factor(year)+s(longitude_grid_wgs84,latitude_grid_wgs84,bs='sos')

Gam pour la figure cti~s(year)
Saving 7 x 7 in image

--> Output/figCTI_plotFrance_Aksu.png

Estimation de la tendence  cti~ year+s(longitude_grid_wgs84,latitude_grid_wgs84,bs='sos')

  --> Output/cti_gammPlot_France_Aksu.csv






























> d <- makeTableBrut(departement = c("01","03","07","15","26","38","42","43","63","69","73","74"),id_output = "ARA")

 QUERY données BRUT:
--------------

 
SELECT 
  o.pk_observation as code_observation, 
  o.id_fnat_unique_citation as code_observation_fnat,
  o.id_inventaire as code_inventaire, 
  i.etude, 
   i.observateur, 
  i.email, 
 p.site as nom_site, 
  p.commune, 
  p.insee, 
  p.departement,
  o.id_carre as code_carre,
    o.id_point as code_point, 
   o.num_point, 
     o.date, 
  o.annee, 
  i.heure_debut, 
  i.heure_fin, 
  i.duree_minute, 
   o.passage, 
  i.info_passage, 
  i.passage_stoc as numero_passage_stoc, 
  i.nombre_de_passage, 
  i.temps_entre_passage, 
  o.espece, 
   s.scientific_name as nom_scientifique, 
  s.french_name as nom_francais,
s.english_name as nom_anglais,
s.euring as code_espece_euring,
s.taxref as code_espece_taxref,
   o.abondance, 
  o.distance_contact, 
 i.nuage, 
  i.pluie, 
  i.vent, 
  i.visibilite, 
  i.neige, 
  p.altitude, 
  p.longitude_wgs84, 
  p.latitude_wgs84,
  c.longitude_grid_wgs84,
  c.latitude_grid_wgs84,
  h.p_milieu, 
  h.p_type, 
  h.p_cat1, 
  h.p_cat2, 
  h.s_milieu, 
  h.s_type, 
  h.s_cat1, 
  h.s_cat2,
   o.db, 
  o.date_export as date_import,
'2018-01-17'::varchar(10) as date_export,
'Lorrilliere Romain'::varchar(50) as operateur,
'lorrilliere@mnhn.fr'::varchar(50) as email_operateur
FROM 
  public.point as p,
  public.carre as c,
  public.inventaire as i, 
  public.observation as o, 
  public.species as s, 
  public.habitat as h
WHERE 
  o.id_inventaire = i.pk_inventaire AND
  o.id_point = p.pk_point AND
  o.id_carre = c.pk_carre AND 
  o.espece = s.pk_species AND
  o.id_inventaire = h.pk_habitat AND
  
 i.annee >= 2001  and i.annee <= 2018 and c.etude in ('STOC_EPS', 'STOC_ONF')  and  p.altitude <= 8000
 and p.departement in  ('01' , '03' , '07' , '15' , '26' , '38' , '42' , '43' , '63' , '69' , '73' , '74')  
 
 ; 
 -->  export/data_FrenchBBS_BRUT_ARA_allSp_2001_2018fr.csv 

     #      ==> Duree: 34 minutes
> head(d)
     code_observation code_observation_fnat   code_inventaire    etude
1 20010429380729P0401                260540 20010429380729P04 STOC_EPS
2 20010429380729P0402                260542 20010429380729P04 STOC_EPS
3 20010429380729P0403                260541 20010429380729P04 STOC_EPS
4 20010429380729P0404                260543 20010429380729P04 STOC_EPS
5 20010429380729P0405                260544 20010429380729P04 STOC_EPS
6 20010429380729P0406                260545 20010429380729P04 STOC_EPS
         observateur email nom_site commune insee departement code_carre
1 COQUELET Jean-Marc           <NA>    <NA>  <NA>          38     380729
2 COQUELET Jean-Marc           <NA>    <NA>  <NA>          38     380729
3 COQUELET Jean-Marc           <NA>    <NA>  <NA>          38     380729
4 COQUELET Jean-Marc           <NA>    <NA>  <NA>          38     380729
5 COQUELET Jean-Marc           <NA>    <NA>  <NA>          38     380729
6 COQUELET Jean-Marc           <NA>    <NA>  <NA>          38     380729
  code_point num_point       date annee heure_debut heure_fin duree_minute
1  380729P04         4 2001-04-29  2001       09:00     09:05            5
2  380729P04         4 2001-04-29  2001       09:00     09:05            5
3  380729P04         4 2001-04-29  2001       09:00     09:05            5
4  380729P04         4 2001-04-29  2001       09:00     09:05            5
5  380729P04         4 2001-04-29  2001       09:00     09:05            5
6  380729P04         4 2001-04-29  2001       09:00     09:05            5
  passage info_passage numero_passage_stoc nombre_de_passage
1       1       normal                   1                 2
2       1       normal                   1                 2
3       1       normal                   1                 2
4       1       normal                   1                 2
5       1       normal                   1                 2
6       1       normal                   1                 2
  temps_entre_passage espece      nom_scientifique        nom_francais
1                  44 EMBCIR       Emberiza cirlus         Bruant zizi
2                  44 LUSMEG Luscinia megarhynchos Rossignol philomèle
3                  44 LUSMEG Luscinia megarhynchos Rossignol philomèle
4                  44 NUMARQ      Numenius arquata      Courlis cendré
5                  44 SAXTOR     Saxicola rubicola        Tarier pâtre
6                  44 SAXTOR     Saxicola rubicola        Tarier pâtre
         nom_anglais code_espece_euring code_espece_taxref abondance
1       Cirl Bunting              18580               4659         1
2 Common Nightingale              11040               4013         1
3 Common Nightingale              11040               4013         1
4    Eurasian Curlew               5410               2576         1
5 European Stonechat              11390             199425         1
6 European Stonechat              11390             199425         1
  distance_contact nuage pluie vent visibilite neige altitude longitude_wgs84
1          LESS100     1     1    2          1    NA      400        5.326798
2          LESS100     1     1    2          1    NA      400        5.326798
3           LESS25     1     1    2          1    NA      400        5.326798
4          MORE100     1     1    2          1    NA      400        5.326798
5          LESS100     1     1    2          1    NA      400        5.326798
6          MORE100     1     1    2          1    NA      400        5.326798
  latitude_wgs84 longitude_grid_wgs84 latitude_grid_wgs84 p_milieu p_type
1       45.37888             5.338131            45.37588        D      4
2       45.37888             5.338131            45.37588        D      4
3       45.37888             5.338131            45.37588        D      4
4       45.37888             5.338131            45.37588        D      4
5       45.37888             5.338131            45.37588        D      4
6       45.37888             5.338131            45.37588        D      4
  p_cat1 p_cat2 s_milieu s_type s_cat1 s_cat2   db date_import date_export
1      1      5        D      2      1     NA FNat  2017-01-04  2018-01-17
2      1      5        D      2      1     NA FNat  2017-01-04  2018-01-17
3      1      5        D      2      1     NA FNat  2017-01-04  2018-01-17
4      1      5        D      2      1     NA FNat  2017-01-04  2018-01-17
5      1      5        D      2      1     NA FNat  2017-01-04  2018-01-17
6      1      5        D      2      1     NA FNat  2017-01-04  2018-01-17
           operateur     email_operateur
1 Lorrilliere Romain lorrilliere@mnhn.fr
2 Lorrilliere Romain lorrilliere@mnhn.fr
3 Lorrilliere Romain lorrilliere@mnhn.fr
4 Lorrilliere Romain lorrilliere@mnhn.fr
5 Lorrilliere Romain lorrilliere@mnhn.fr
6 Lorrilliere Romain lorrilliere@mnhn.fr
> du <- unique(subset(d,select=c("code_carre","date")))
> dim(du)
[1] 5683    2
> head(du)
   code_carre       date
1      380729 2001-04-29
10     690663 2001-05-07
34     730601 2001-05-09
42     381082 2001-05-12
51     731323 2001-05-12
61     381220 2001-05-18
> du <- du[order(du$code_carre,du$date),]
> head(du)
      code_carre       date
47074     010100 2007-04-29
8422      010100 2007-05-29
9819      010120 2008-04-29
51128     010120 2008-06-10
52757     010120 2009-05-09
15113     010120 2009-06-21
> write.csv2("export/carre_date_ARA.csv",row.names=FALSE)
"x"
"export/carre_date_ARA.csv"
> write.csv2(du,"export/carre_date_ARA.csv",row.names=FALSE)
























> d <- makeTableCarre(champsHabitat=FALSE,departement = c("01","03","07","15","26","38","42","43","63","69","73","74"),id_output = "ARA")

Requete principale:

 
select oc.id_carre as carre, oc.annee, (oc.annee::varchar(4)||oc.id_carre::varchar(100))::varchar(100) as id_carre_annee, c.etude,
qualite_inventaire_stoc, commune,insee,departement, 
code_sp, e.scientific_name, e.french_name, e.euring,e.taxref,
abond_brut as abondance_brut, abond  as abondance, 
altitude, longitude_grid_wgs84,latitude_grid_wgs84,  c.db as data_base_name, '2018-01-11'::varchar(10) as date_export,
'Lorrilliere Romain'::varchar(50) as operateur,
'lorrilliere@mnhn.fr'::varchar(50) as email_operateur

from( -- ## begin ## somme sur carre des max par points sur 2 passages
	select id_carre, annee,code_sp,sum(abond_brut) as abond_brut,sum(abond) as abond
	from -- ## begin ## obs max par point sur 2 passages
		(select id_point, annee,code_sp,max(abond_brut) as abond_brut,max(abond) as abond
		from -- ## begin ## correction abondance obs par seuil
			(select id_inventaire,id_point, passage_stoc, annee, code_sp,abond as abond_brut, abondAll_seuil99 as seuil_abondance_sp,LEAST(abond, abondAll_seuil99) as abond
			from -- ## begin ## selection classe de distance et different filtre
				( SELECT id_inventaire, code_sp, sum(abondance) as abond
				FROM 
				observation as o, inventaire as i,carre_annee as ca, carre as c
				WHERE 
				o.id_inventaire = i.pk_inventaire and 
				o.id_carre = c.pk_carre and 
				o.id_carre = ca.id_carre and o.annee=ca.annee and 
				ca.qualite_inventaire_stoc > 0 and
                                 code_sp not  in  ('MOTFLA' , 'SAXRUB' , 'ANTPRA' , 'OENOEN' , 'PHYTRO')  and    passage_stoc in (1,2) and  i.annee >= 2001  and i.annee <= 2018 and c.etude in ('STOC_EPS', 'STOC_ONF')  and  c.altitude <= 8000  and departement in  ('01' , '03' , '07' , '15' , '26' , '38' , '42' , '43' , '63' , '69' , '73' , '74')     
			GROUP BY 
			id_inventaire, code_sp 
                                union
                                  --  ## begin ## ajout des especes tardives dont on ne garde que le second passage
             SELECT id_inventaire, code_sp, sum(abondance) as abond
				FROM 
				observation as o, inventaire as i,carre_annee as ca, carre as c
				WHERE 
				o.id_inventaire = i.pk_inventaire and 
				o.id_carre = c.pk_carre and 
				o.id_carre = ca.id_carre and o.annee=ca.annee and 
				ca.qualite_inventaire_stoc > 0 and
                                 code_sp in  ('MOTFLA' , 'SAXRUB' , 'ANTPRA' , 'OENOEN' , 'PHYTRO')  and    passage_stoc = 2 and  i.annee >= 2001  and i.annee <= 2018 and c.etude in ('STOC_EPS', 'STOC_ONF')  and  c.altitude <= 8000  and departement in  ('01' , '03' , '07' , '15' , '26' , '38' , '42' , '43' , '63' , '69' , '73' , '74')     
			GROUP BY 
			id_inventaire, code_sp
				) -- ## end ## selection classe de distance et different filtre  
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
as oc, carre as c, species as e, carre_annee as ca 
where 
oc.id_carre = c.pk_carre and oc.code_sp = e.pk_species and oc.id_carre = ca.id_carre and oc.annee = ca.annee
order by 
oc.id_carre, annee,code_sp; 

 -->  export/data_FrenchBBS_squarre_ARA_allSp_2001_2018.csv 
     #      ==> Duree: 1 minutes
> head(d)
   carre annee id_carre_annee    etude qualite_inventaire_stoc commune insee
1 010100  2007     2007010100 STOC_EPS                       1    <NA> 00000
2 010100  2007     2007010100 STOC_EPS                       1    <NA> 00000
3 010100  2007     2007010100 STOC_EPS                       1    <NA> 00000
4 010100  2007     2007010100 STOC_EPS                       1    <NA> 00000
5 010100  2007     2007010100 STOC_EPS                       1    <NA> 00000
6 010100  2007     2007010100 STOC_EPS                       1    <NA> 00000
  departement code_sp     scientific_name        french_name euring taxref
1          01  ANAPLA  Anas platyrhynchos     Canard colvert   1860   1966
2          01  ANTTRI    Anthus trivialis   Pipit des arbres  10090   3723
3          01  BUBIBI       Bubulcus ibis Héron garde-boeufs   1110   2489
4          01  CARCAN Carduelis cannabina Linotte mélodieuse  16600   4588
5          01  CARCHL   Carduelis chloris   Verdier d'Europe  16490   4580
6          01  COLPAL    Columba palumbus      Pigeon ramier   6700   3424
  abondance_brut abondance altitude longitude_grid_wgs84 latitude_grid_wgs84
1              2         2      243             5.107878             46.3901
2              1         1      243             5.107878             46.3901
3              4         4      243             5.107878             46.3901
4              1         1      243             5.107878             46.3901
5             10        10      243             5.107878             46.3901
6             14        14      243             5.107878             46.3901
  data_base_name date_export          operateur     email_operateur
1           FNat  2018-01-11 Lorrilliere Romain lorrilliere@mnhn.fr
2           FNat  2018-01-11 Lorrilliere Romain lorrilliere@mnhn.fr
3           FNat  2018-01-11 Lorrilliere Romain lorrilliere@mnhn.fr
4           FNat  2018-01-11 Lorrilliere Romain lorrilliere@mnhn.fr
5           FNat  2018-01-11 Lorrilliere Romain lorrilliere@mnhn.fr
6           FNat  2018-01-11 Lorrilliere Romain lorrilliere@mnhn.fr
> dunique <- unique(subset(d,select=c("carre","annee")))
> head(dunique)
     carre annee
1   010100  2007
42  010120  2008
90  010120  2009
130 010120  2010
170 010295  2002
209 010295  2003
> write.csv(dunique,"export/carre_Auvergne_Rhone-Alpes.csv",row.names=FALSE)
> d.brut <- makeTableBrut(firstYear=2017,addAbscence=FALSE,id_output="epoc_2018-01-11")

 QUERY données BRUT:
--------------

 
SELECT 
  o.pk_observation as code_observation, 
  o.id_fnat_unique_citation as code_observation_fnat,
  o.id_inventaire as code_inventaire, 
  i.etude, 
   i.observateur, 
  i.email, 
 p.site as nom_site, 
  p.commune, 
  p.insee, 
  p.departement,
  o.id_carre as code_carre,
    o.id_point as code_point, 
   o.num_point, 
     o.date, 
  o.annee, 
  i.heure_debut, 
  i.heure_fin, 
  i.duree_minute, 
   o.passage, 
  i.info_passage, 
  i.passage_stoc as numero_passage_stoc, 
  i.nombre_de_passage, 
  i.temps_entre_passage, 
  o.espece, 
   s.scientific_name as nom_scientifique, 
  s.french_name as nom_francais,
s.english_name as nom_anglais,
s.euring as code_espece_euring,
s.taxref as code_espece_taxref,
   o.abondance, 
  o.distance_contact, 
 i.nuage, 
  i.pluie, 
  i.vent, 
  i.visibilite, 
  i.neige, 
  p.altitude, 
  p.longitude_wgs84, 
  p.latitude_wgs84,
  c.longitude_grid_wgs84,
  c.latitude_grid_wgs84,
  h.p_milieu, 
  h.p_type, 
  h.p_cat1, 
  h.p_cat2, 
  h.s_milieu, 
  h.s_type, 
  h.s_cat1, 
  h.s_cat2,
   o.db, 
  o.date_export as date_import,
'2018-01-11'::varchar(10) as date_export,
'Lorrilliere Romain'::varchar(50) as operateur,
'lorrilliere@mnhn.fr'::varchar(50) as email_operateur
FROM 
  public.point as p,
  public.carre as c,
  public.inventaire as i, 
  public.observation as o, 
  public.species as s, 
  public.habitat as h
WHERE 
  o.id_inventaire = i.pk_inventaire AND
  o.id_point = p.pk_point AND
  o.id_carre = c.pk_carre AND 
  o.espece = s.pk_species AND
  o.id_inventaire = h.pk_habitat AND
  
 i.annee >= 2017  and i.annee <= 2018 and c.etude in ('STOC_EPS', 'STOC_ONF')  and  p.altitude <= 8000

 
 ; 
 -->  export/data_FrenchBBS_BRUT_epoc_2018-01-11_allSp_2017_2018fr.csv 

     #      ==> Duree: 14 minutes


















> makeTablePoint(sp="PASDOM",distance_contact=NULL,addAbscence=TRUE,id_output="Bertille_DistFULL_180104")

Requete principale:

 select om.id_point as point, p.id_carre as carre, om.annee as annee, 
(om.annee::varchar(4)||om.id_point::varchar(100))::varchar(100) as id_point_annee, 
c.etude as etude, p.commune,p.insee,p.departement as departement, 
code_sp , e.scientific_name as nom_scientifique, e.french_name as nom_francais, e.english_name as nom_anglais, e.euring as code_espece_euring,e.taxref as code_espece_taxref,
abond_brut as abondance_brut, abond as abondance, pa.qualite_inventaire_stoc as qualite_inventaire_stoc,
p.altitude, longitude_wgs84,  latitude_wgs84,  longitude_grid_wgs84,latitude_grid_wgs84,  
pa.p_milieu as habitat_principal, pa.s_milieu as habitat_secondaire, pa.p_dernier_descri as temps_depuis_derniere_description_habitat ,
   pa.foret_p as foret_p, pa.agri_p as agricole_p, pa.urbain_p as urbain_p, pa.foret_ps as foret_ps, pa.agri_ps as agricole_ps, pa.urbain_ps as urbain_ps,
  nbp_foret_p  as carre_nb_pts_foret_p, nbp_ouvert_p as carre_nb_pts_ouvert_p, nbp_agri_p as carre_nb_pts_agricole_p, nbp_urbain_p as carre_nb_pts_urbain_p,
nbp_foret_ps as carre_nb_pts_foret_ps, nbp_ouvert_ps as carre_nb_pts_ouvert_ps, nbp_agri_ps as carre_nb_pts_agricole_ps, nbp_urbain_ps  as carre_nb_pts_urbain_ps,  p.db as data_base_name, '2018-01-04'::varchar(10) as date_export,
'Lorrilliere Romain'::varchar(50) as operateur,
'lorrilliere@mnhn.fr'::varchar(50) as email_operateur

from(
	select id_point, annee,code_sp,max(abond_brut) as abond_brut,max(abond) as abond
		from -- ## begin ## correction abondance obs par seuil
			(select id_inventaire,id_point, passage_stoc, annee, code_sp,abond as abond_brut, abondAll_seuil99 as seuil_abondance_sp,LEAST(abond,
                          abondAll_seuil99) as abond
			from -- ## begin ## selection classe de distance et different filtre
				(  SELECT id_inventaire, code_sp, sum(abondance) as abond
				FROM 
				observation as o, inventaire as i,point as p, point_annee as pa, carre as c
				WHERE 
				o.id_inventaire = i.pk_inventaire and 
				o.id_carre = c.pk_carre and 
				o.id_point = p.pk_point and 
				o.id_point = pa.id_point and o.annee=pa.annee and 
				pa.qualite_inventaire_stoc > 0 and
                                  code_sp in  ('PASDOM')  and
                                 
                                passage_stoc in (1,2) and  i.annee >= 2001  and i.annee <= 2018 and c.etude in ('STOC_EPS', 'STOC_ONF')  and  p.altitude <= 8000

 
    
			GROUP BY 
			id_inventaire, code_sp
	                        ) --  ## end ## selection classe de distance et different filtre  
			as opb, espece_abondance_point_seuil as s, inventaire as i
			WHERE 
			opb.code_sp = s.pk_species and opb.id_inventaire = i.pk_inventaire

			) -- ## end ## correction abondance obs par seuil
		as op
		group by id_point,annee,code_sp
		 )-- ## end ## obs max par point sur 2 passages 
		 as om , point as p, carre as c, species as e , point_annee as pa , carre_annee as ca 
where 
om.id_point = p.pk_point and p.id_carre = c.pk_carre and om.code_sp = e.pk_species and p.altitude <= 800 and 
om.id_point = pa.id_point and om.annee = pa.annee and p.id_carre = ca.id_carre and om.annee = ca.annee  
order by 
om.id_point, annee,code_sp;  


 Ajout des absences: 

Requete inventaire: Recherche de tous les inventaires

 select pa.id_point as point, pa.id_carre as carre, pa.annee as annee, pk_point_annee as id_point_annee, ''::varchar(25) as etude,
 p.commune,p.insee,p.departement as departement,pa.qualite_inventaire_stoc as qualite_inventaire_stoc,p.altitude, longitude_wgs84,  latitude_wgs84,
  longitude_grid_wgs84,latitude_grid_wgs84,  
 pa.p_milieu as habitat_principal, pa.s_milieu as habitat_secondaire, pa.p_dernier_descri as temps_depuis_derniere_description_habitat ,
   pa.foret_p as foret_p, pa.agri_p as agricole_p, pa.urbain_p as urbain_p, pa.foret_ps as foret_ps, pa.agri_ps as agricole_ps, pa.urbain_ps as urbain_ps,
  nbp_foret_p  as carre_nb_pts_foret_p, nbp_ouvert_p as carre_nb_pts_ouvert_p, nbp_agri_p as carre_nb_pts_agricole_p, nbp_urbain_p as carre_nb_pts_urbain_p,
nbp_foret_ps as carre_nb_pts_foret_ps, nbp_ouvert_ps as carre_nb_pts_ouvert_ps, nbp_agri_ps as carre_nb_pts_agricole_ps, nbp_urbain_ps  as carre_nb_pts_urbain_ps,  p.db as data_base_name, '2018-01-04'::varchar(10) as date_export,
'Lorrilliere Romain'::varchar(50) as operateur,
'lorrilliere@mnhn.fr'::varchar(50) as email_operateur
from
point_annee as pa, point as p, carre as c , carre_annee as ca 
where 
pa.id_point = p.pk_point and pa.id_carre = c.pk_carre and p.altitude <= 8000 and  pa.annee >= 2001  and pa.annee <= 2018 and pa.id_carre = ca.id_carre and pa.annee = ca.annee   
order by 
pa.id_point, annee; 

 -->  export/data_FrenchBBS_point_Bertille_DistFULL_180104_PASDOM_2001_2018fr.csv 

     #      ==> Duree: 14 minutes

> makeTablePoint(sp="PASDOM",distance_contact=100,addAbscence=TRUE,id_output="Bertille_Dist100_1870104")

Requete principale:

 select om.id_point as point, p.id_carre as carre, om.annee as annee, 
(om.annee::varchar(4)||om.id_point::varchar(100))::varchar(100) as id_point_annee, 
c.etude as etude, p.commune,p.insee,p.departement as departement, 
code_sp , e.scientific_name as nom_scientifique, e.french_name as nom_francais, e.english_name as nom_anglais, e.euring as code_espece_euring,e.taxref as code_espece_taxref,
abond_brut as abondance_brut, abond as abondance, pa.qualite_inventaire_stoc as qualite_inventaire_stoc,
p.altitude, longitude_wgs84,  latitude_wgs84,  longitude_grid_wgs84,latitude_grid_wgs84,  
pa.p_milieu as habitat_principal, pa.s_milieu as habitat_secondaire, pa.p_dernier_descri as temps_depuis_derniere_description_habitat ,
   pa.foret_p as foret_p, pa.agri_p as agricole_p, pa.urbain_p as urbain_p, pa.foret_ps as foret_ps, pa.agri_ps as agricole_ps, pa.urbain_ps as urbain_ps,
  nbp_foret_p  as carre_nb_pts_foret_p, nbp_ouvert_p as carre_nb_pts_ouvert_p, nbp_agri_p as carre_nb_pts_agricole_p, nbp_urbain_p as carre_nb_pts_urbain_p,
nbp_foret_ps as carre_nb_pts_foret_ps, nbp_ouvert_ps as carre_nb_pts_ouvert_ps, nbp_agri_ps as carre_nb_pts_agricole_ps, nbp_urbain_ps  as carre_nb_pts_urbain_ps,  p.db as data_base_name, '2018-01-04'::varchar(10) as date_export,
'Lorrilliere Romain'::varchar(50) as operateur,
'lorrilliere@mnhn.fr'::varchar(50) as email_operateur

from(
	select id_point, annee,code_sp,max(abond_brut) as abond_brut,max(abond) as abond
		from -- ## begin ## correction abondance obs par seuil
			(select id_inventaire,id_point, passage_stoc, annee, code_sp,abond as abond_brut, abondAll_seuil99 as seuil_abondance_sp,LEAST(abond,
                          abondAll_seuil99) as abond
			from -- ## begin ## selection classe de distance et different filtre
				(  SELECT id_inventaire, code_sp, sum(abondance) as abond
				FROM 
				observation as o, inventaire as i,point as p, point_annee as pa, carre as c
				WHERE 
				o.id_inventaire = i.pk_inventaire and 
				o.id_carre = c.pk_carre and 
				o.id_point = p.pk_point and 
				o.id_point = pa.id_point and o.annee=pa.annee and 
				pa.qualite_inventaire_stoc > 0 and
                                  code_sp in  ('PASDOM')  and
                                 
                                passage_stoc in (1,2) and  distance_contact in ('LESS25','LESS100') and  i.annee >= 2001  and i.annee <= 2018 and c.etude in ('STOC_EPS', 'STOC_ONF')  and  p.altitude <= 8000

 
    
			GROUP BY 
			id_inventaire, code_sp
	                        ) --  ## end ## selection classe de distance et different filtre  
			as opb, espece_abondance_point_seuil as s, inventaire as i
			WHERE 
			opb.code_sp = s.pk_species and opb.id_inventaire = i.pk_inventaire

			) -- ## end ## correction abondance obs par seuil
		as op
		group by id_point,annee,code_sp
		 )-- ## end ## obs max par point sur 2 passages 
		 as om , point as p, carre as c, species as e , point_annee as pa , carre_annee as ca 
where 
om.id_point = p.pk_point and p.id_carre = c.pk_carre and om.code_sp = e.pk_species and p.altitude <= 800 and 
om.id_point = pa.id_point and om.annee = pa.annee and p.id_carre = ca.id_carre and om.annee = ca.annee  
order by 
om.id_point, annee,code_sp;  


 Ajout des absences: 

Requete inventaire: Recherche de tous les inventaires

 select pa.id_point as point, pa.id_carre as carre, pa.annee as annee, pk_point_annee as id_point_annee, ''::varchar(25) as etude,
 p.commune,p.insee,p.departement as departement,pa.qualite_inventaire_stoc as qualite_inventaire_stoc,p.altitude, longitude_wgs84,  latitude_wgs84,
  longitude_grid_wgs84,latitude_grid_wgs84,  
 pa.p_milieu as habitat_principal, pa.s_milieu as habitat_secondaire, pa.p_dernier_descri as temps_depuis_derniere_description_habitat ,
   pa.foret_p as foret_p, pa.agri_p as agricole_p, pa.urbain_p as urbain_p, pa.foret_ps as foret_ps, pa.agri_ps as agricole_ps, pa.urbain_ps as urbain_ps,
  nbp_foret_p  as carre_nb_pts_foret_p, nbp_ouvert_p as carre_nb_pts_ouvert_p, nbp_agri_p as carre_nb_pts_agricole_p, nbp_urbain_p as carre_nb_pts_urbain_p,
nbp_foret_ps as carre_nb_pts_foret_ps, nbp_ouvert_ps as carre_nb_pts_ouvert_ps, nbp_agri_ps as carre_nb_pts_agricole_ps, nbp_urbain_ps  as carre_nb_pts_urbain_ps,  p.db as data_base_name, '2018-01-04'::varchar(10) as date_export,
'Lorrilliere Romain'::varchar(50) as operateur,
'lorrilliere@mnhn.fr'::varchar(50) as email_operateur
from
point_annee as pa, point as p, carre as c , carre_annee as ca 
where 
pa.id_point = p.pk_point and pa.id_carre = c.pk_carre and p.altitude <= 8000 and  pa.annee >= 2001  and pa.annee <= 2018 and pa.id_carre = ca.id_carre and pa.annee = ca.annee   
order by 
pa.id_point, annee; 

 -->  export/data_FrenchBBS_point_Bertille_Dist100_1870104_PASDOM_2001_2018fr.csv 

     #      ==> Duree: 13 minutes
> 























###############################

> dk <- makeTableCarre(dist=NULL,id_output="KarineP_distFULL",isEnglish=TRUE)

Requete principale:

 
select oc.id_carre as carre, oc.annee, (oc.annee::varchar(4)||oc.id_carre::varchar(100))::varchar(100) as id_carre_annee, c.etude,
qualite_inventaire_stoc, commune,insee,departement, 
code_sp, e.scientific_name, e.french_name, e.euring,e.taxref,
abond_brut as abondance_brut, abond  as abondance, 
altitude, longitude_grid_wgs84,latitude_grid_wgs84,  foret_p, ouvert_p, agri_p, urbain_p,
foret_ps, ouvert_ps, agri_ps, urbain_ps,
nbp_foret_p, nbp_ouvert_p, nbp_agri_p, nbp_urbain_p,
nbp_foret_ps, nbp_ouvert_ps, nbp_agri_ps, nbp_urbain_ps,  c.db as data_base_name, '2017-12-12'::varchar(10) as date_export,
'Lorrilliere Romain'::varchar(50) as operateur,
'lorrilliere@mnhn.fr'::varchar(50) as email_operateur

from( -- ## begin ## somme sur carre des max par points sur 2 passages
	select id_carre, annee,code_sp,sum(abond_brut) as abond_brut,sum(abond) as abond
	from -- ## begin ## obs max par point sur 2 passages
		(select id_point, annee,code_sp,max(abond_brut) as abond_brut,max(abond) as abond
		from -- ## begin ## correction abondance obs par seuil
			(select id_inventaire,id_point, passage_stoc, annee, code_sp,abond as abond_brut, abondAll_seuil99 as seuil_abondance_sp,LEAST(abond, abondAll_seuil99) as abond
			from -- ## begin ## selection classe de distance et different filtre
				( SELECT id_inventaire, code_sp, sum(abondance) as abond
				FROM 
				observation as o, inventaire as i,carre_annee as ca, carre as c
				WHERE 
				o.id_inventaire = i.pk_inventaire and 
				o.id_carre = c.pk_carre and 
				o.id_carre = ca.id_carre and o.annee=ca.annee and 
				ca.qualite_inventaire_stoc > 0 and
                                 code_sp not  in  ('MOTFLA' , 'SAXRUB' , 'ANTPRA')  and    passage_stoc in (1,2) and  i.annee >= 2001  and i.annee <= 2017 and c.etude in ('STOC_EPS', 'STOC_ONF')  and  c.altitude <= 8000    
			GROUP BY 
			id_inventaire, code_sp 
                                union
                                  --  ## begin ## ajout des especes tardives dont on ne garde que le second passage
             SELECT id_inventaire, code_sp, sum(abondance) as abond
				FROM 
				observation as o, inventaire as i,carre_annee as ca, carre as c
				WHERE 
				o.id_inventaire = i.pk_inventaire and 
				o.id_carre = c.pk_carre and 
				o.id_carre = ca.id_carre and o.annee=ca.annee and 
				ca.qualite_inventaire_stoc > 0 and
                                 code_sp in  ('MOTFLA' , 'SAXRUB' , 'ANTPRA')  and    passage_stoc = 2 and  i.annee >= 2001  and i.annee <= 2017 and c.etude in ('STOC_EPS', 'STOC_ONF')  and  c.altitude <= 8000    
			GROUP BY 
			id_inventaire, code_sp
				) -- ## end ## selection classe de distance et different filtre  
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
as oc, carre as c, species as e, carre_annee as ca 
where 
oc.id_carre = c.pk_carre and oc.code_sp = e.pk_species and oc.id_carre = ca.id_carre and oc.annee = ca.annee
order by 
oc.id_carre, annee,code_sp; 

 - Traduction des noms de colonnes

 -->  export/data_FrenchBBS_squarreKarineP_distFULL_allSp_2001_2017.csv 
     #      ==> Duree: 2 minutes






> dk <- makeTableCarre(dist="100",id_output="KarineP_dist100",isEnglish=TRUE)

Requete principale:

 
select oc.id_carre as carre, oc.annee, (oc.annee::varchar(4)||oc.id_carre::varchar(100))::varchar(100) as id_carre_annee, c.etude,
qualite_inventaire_stoc, commune,insee,departement, 
code_sp, e.scientific_name, e.french_name, e.euring,e.taxref,
abond_brut as abondance_brut, abond  as abondance, 
altitude, longitude_grid_wgs84,latitude_grid_wgs84,  foret_p, ouvert_p, agri_p, urbain_p,
foret_ps, ouvert_ps, agri_ps, urbain_ps,
nbp_foret_p, nbp_ouvert_p, nbp_agri_p, nbp_urbain_p,
nbp_foret_ps, nbp_ouvert_ps, nbp_agri_ps, nbp_urbain_ps,  c.db as data_base_name, '2017-12-12'::varchar(10) as date_export,
'Lorrilliere Romain'::varchar(50) as operateur,
'lorrilliere@mnhn.fr'::varchar(50) as email_operateur

from( -- ## begin ## somme sur carre des max par points sur 2 passages
	select id_carre, annee,code_sp,sum(abond_brut) as abond_brut,sum(abond) as abond
	from -- ## begin ## obs max par point sur 2 passages
		(select id_point, annee,code_sp,max(abond_brut) as abond_brut,max(abond) as abond
		from -- ## begin ## correction abondance obs par seuil
			(select id_inventaire,id_point, passage_stoc, annee, code_sp,abond as abond_brut, abondAll_seuil99 as seuil_abondance_sp,LEAST(abond, abondAll_seuil99) as abond
			from -- ## begin ## selection classe de distance et different filtre
				( SELECT id_inventaire, code_sp, sum(abondance) as abond
				FROM 
				observation as o, inventaire as i,carre_annee as ca, carre as c
				WHERE 
				o.id_inventaire = i.pk_inventaire and 
				o.id_carre = c.pk_carre and 
				o.id_carre = ca.id_carre and o.annee=ca.annee and 
				ca.qualite_inventaire_stoc > 0 and
                                 code_sp not  in  ('MOTFLA' , 'SAXRUB' , 'ANTPRA')  and    passage_stoc in (1,2) and  distance_contact in ('LESS25','LESS100') and  i.annee >= 2001  and i.annee <= 2017 and c.etude in ('STOC_EPS', 'STOC_ONF')  and  c.altitude <= 8000    
			GROUP BY 
			id_inventaire, code_sp 
                                union
                                  --  ## begin ## ajout des especes tardives dont on ne garde que le second passage
             SELECT id_inventaire, code_sp, sum(abondance) as abond
				FROM 
				observation as o, inventaire as i,carre_annee as ca, carre as c
				WHERE 
				o.id_inventaire = i.pk_inventaire and 
				o.id_carre = c.pk_carre and 
				o.id_carre = ca.id_carre and o.annee=ca.annee and 
				ca.qualite_inventaire_stoc > 0 and
                                 code_sp in  ('MOTFLA' , 'SAXRUB' , 'ANTPRA')  and    passage_stoc = 2 and  distance_contact in ('LESS25','LESS100') and  i.annee >= 2001  and i.annee <= 2017 and c.etude in ('STOC_EPS', 'STOC_ONF')  and  c.altitude <= 8000    
			GROUP BY 
			id_inventaire, code_sp
				) -- ## end ## selection classe de distance et different filtre  
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
as oc, carre as c, species as e, carre_annee as ca 
where 
oc.id_carre = c.pk_carre and oc.code_sp = e.pk_species and oc.id_carre = ca.id_carre and oc.annee = ca.annee
order by 
oc.id_carre, annee,code_sp; 

 - Traduction des noms de colonnes

 -->  export/data_FrenchBBS_squarreKarineP_dist100_allSp_2001_2017.csv 
     #      ==> Duree: 1 minutes
     
     
     
     
     
     
     
     
     
     
     
     
     
     
> dk <- makeTablePoint(dist=NULL,id_output="KarineP_distFULL",isEnglish=TRUE)

Requete principale:

 select om.id_point as point, p.id_carre as carre, om.annee as annee, 
(om.annee::varchar(4)||om.id_point::varchar(100))::varchar(100) as id_point_annee, 
c.etude as etude, p.commune,p.insee,p.departement as departement, 
code_sp , e.scientific_name as nom_scientifique, e.french_name as nom_francais, e.english_name as nom_anglais, e.euring as code_espece_euring,e.taxref as code_espece_taxref,
abond_brut as abondance_brut, abond as abondance, pa.qualite_inventaire_stoc as qualite_inventaire_stoc,
p.altitude, longitude_wgs84,  latitude_wgs84,  
pa.p_milieu as habitat_principal, pa.s_milieu as habitat_secondaire, pa.p_dernier_descri as temps_depuis_derniere_description_habitat ,
   pa.foret_p as foret_p, pa.agri_p as agricole_p, pa.urbain_p as urbain_p, pa.foret_ps as foret_ps, pa.agri_ps as agricole_ps, pa.urbain_ps as urbain_ps,
  nbp_foret_p  as carre_nb_pts_foret_p, nbp_ouvert_p as carre_nb_pts_ouvert_p, nbp_agri_p as carre_nb_pts_agricole_p, nbp_urbain_p as carre_nb_pts_urbain_p,
nbp_foret_ps as carre_nb_pts_foret_ps, nbp_ouvert_ps as carre_nb_pts_ouvert_ps, nbp_agri_ps as carre_nb_pts_agricole_ps, nbp_urbain_ps  as carre_nb_pts_urbain_ps,  p.db as data_base_name, '2017-12-12'::varchar(10) as date_export,
'Lorrilliere Romain'::varchar(50) as operateur,
'lorrilliere@mnhn.fr'::varchar(50) as email_operateur

from(
	select id_point, annee,code_sp,max(abond_brut) as abond_brut,max(abond) as abond
		from -- ## begin ## correction abondance obs par seuil
			(select id_inventaire,id_point, passage_stoc, annee, code_sp,abond as abond_brut, abondAll_seuil99 as seuil_abondance_sp,LEAST(abond,
                          abondAll_seuil99) as abond
			from -- ## begin ## selection classe de distance et different filtre
				(  SELECT id_inventaire, code_sp, sum(abondance) as abond
				FROM 
				observation as o, inventaire as i,point as p, point_annee as pa, carre as c
				WHERE 
				o.id_inventaire = i.pk_inventaire and 
				o.id_carre = c.pk_carre and 
				o.id_point = p.pk_point and 
				o.id_point = pa.id_point and o.annee=pa.annee and 
				pa.qualite_inventaire_stoc > 0 and
                                 code_sp not  in  ('MOTFLA' , 'SAXRUB' , 'ANTPRA')  and
                                 
                                passage_stoc in (1,2) and  i.annee >= 2001  and i.annee <= 2017 and c.etude in ('STOC_EPS', 'STOC_ONF')  and  p.altitude <= 8000

 
    
			GROUP BY 
			id_inventaire, code_sp 
                                union
                                  --  ajout des especes tardives dont on ne garde que le second passage
             SELECT id_inventaire, code_sp, sum(abondance) as abond
				FROM 
				observation as o, inventaire as i,point as p, point_annee as pa, carre as c
				WHERE 
				o.id_inventaire = i.pk_inventaire and 
				o.id_carre = c.pk_carre and 
				o.id_point = p.pk_point and 
				o.id_point = pa.id_point and o.annee=pa.annee and 
				pa.qualite_inventaire_stoc > 0 and
                                 code_sp in  ('MOTFLA' , 'SAXRUB' , 'ANTPRA')  and    passage_stoc = 2 and  i.annee >= 2001  and i.annee <= 2017 and c.etude in ('STOC_EPS', 'STOC_ONF')  and  p.altitude <= 8000

 
    
			GROUP BY 
			id_inventaire, code_sp
	                        ) --  ## end ## selection classe de distance et different filtre  
			as opb, espece_abondance_point_seuil as s, inventaire as i
			WHERE 
			opb.code_sp = s.pk_species and opb.id_inventaire = i.pk_inventaire

			) -- ## end ## correction abondance obs par seuil
		as op
		group by id_point,annee,code_sp
		 )-- ## end ## obs max par point sur 2 passages 
		 as om , point as p, carre as c, species as e , point_annee as pa , carre_annee as ca 
where 
om.id_point = p.pk_point and p.id_carre = c.pk_carre and om.code_sp = e.pk_species and p.altitude <= 800 and 
om.id_point = pa.id_point and om.annee = pa.annee and p.id_carre = ca.id_carre and om.annee = ca.annee  
order by 
om.id_point, annee,code_sp;  

 - Traduction des noms de colonnes
 -->  export/data_FrenchBBS_point_KarineP_distFULL_allSp_2001_2017eng.csv 

     #      ==> Duree: 3 minutes
     
     
     
     
     
     
     
     
     
     
     
     
     
     
     
     
     
     
> dk <- makeTablePoint(dist="100",id_output="KarineP_dist100",isEnglish=TRUE)

Requete principale:

 select om.id_point as point, p.id_carre as carre, om.annee as annee, 
(om.annee::varchar(4)||om.id_point::varchar(100))::varchar(100) as id_point_annee, 
c.etude as etude, p.commune,p.insee,p.departement as departement, 
code_sp , e.scientific_name as nom_scientifique, e.french_name as nom_francais, e.english_name as nom_anglais, e.euring as code_espece_euring,e.taxref as code_espece_taxref,
abond_brut as abondance_brut, abond as abondance, pa.qualite_inventaire_stoc as qualite_inventaire_stoc,
p.altitude, longitude_wgs84,  latitude_wgs84,  
pa.p_milieu as habitat_principal, pa.s_milieu as habitat_secondaire, pa.p_dernier_descri as temps_depuis_derniere_description_habitat ,
   pa.foret_p as foret_p, pa.agri_p as agricole_p, pa.urbain_p as urbain_p, pa.foret_ps as foret_ps, pa.agri_ps as agricole_ps, pa.urbain_ps as urbain_ps,
  nbp_foret_p  as carre_nb_pts_foret_p, nbp_ouvert_p as carre_nb_pts_ouvert_p, nbp_agri_p as carre_nb_pts_agricole_p, nbp_urbain_p as carre_nb_pts_urbain_p,
nbp_foret_ps as carre_nb_pts_foret_ps, nbp_ouvert_ps as carre_nb_pts_ouvert_ps, nbp_agri_ps as carre_nb_pts_agricole_ps, nbp_urbain_ps  as carre_nb_pts_urbain_ps,  p.db as data_base_name, '2017-12-12'::varchar(10) as date_export,
'Lorrilliere Romain'::varchar(50) as operateur,
'lorrilliere@mnhn.fr'::varchar(50) as email_operateur

from(
	select id_point, annee,code_sp,max(abond_brut) as abond_brut,max(abond) as abond
		from -- ## begin ## correction abondance obs par seuil
			(select id_inventaire,id_point, passage_stoc, annee, code_sp,abond as abond_brut, abondAll_seuil99 as seuil_abondance_sp,LEAST(abond,
                          abondAll_seuil99) as abond
			from -- ## begin ## selection classe de distance et different filtre
				(  SELECT id_inventaire, code_sp, sum(abondance) as abond
				FROM 
				observation as o, inventaire as i,point as p, point_annee as pa, carre as c
				WHERE 
				o.id_inventaire = i.pk_inventaire and 
				o.id_carre = c.pk_carre and 
				o.id_point = p.pk_point and 
				o.id_point = pa.id_point and o.annee=pa.annee and 
				pa.qualite_inventaire_stoc > 0 and
                                 code_sp not  in  ('MOTFLA' , 'SAXRUB' , 'ANTPRA')  and
                                 
                                passage_stoc in (1,2) and  distance_contact in ('LESS25','LESS100') and  i.annee >= 2001  and i.annee <= 2017 and c.etude in ('STOC_EPS', 'STOC_ONF')  and  p.altitude <= 8000

 
    
			GROUP BY 
			id_inventaire, code_sp 
                                union
                                  --  ajout des especes tardives dont on ne garde que le second passage
             SELECT id_inventaire, code_sp, sum(abondance) as abond
				FROM 
				observation as o, inventaire as i,point as p, point_annee as pa, carre as c
				WHERE 
				o.id_inventaire = i.pk_inventaire and 
				o.id_carre = c.pk_carre and 
				o.id_point = p.pk_point and 
				o.id_point = pa.id_point and o.annee=pa.annee and 
				pa.qualite_inventaire_stoc > 0 and
                                 code_sp in  ('MOTFLA' , 'SAXRUB' , 'ANTPRA')  and    passage_stoc = 2 and  distance_contact in ('LESS25','LESS100') and  i.annee >= 2001  and i.annee <= 2017 and c.etude in ('STOC_EPS', 'STOC_ONF')  and  p.altitude <= 8000

 
    
			GROUP BY 
			id_inventaire, code_sp
	                        ) --  ## end ## selection classe de distance et different filtre  
			as opb, espece_abondance_point_seuil as s, inventaire as i
			WHERE 
			opb.code_sp = s.pk_species and opb.id_inventaire = i.pk_inventaire

			) -- ## end ## correction abondance obs par seuil
		as op
		group by id_point,annee,code_sp
		 )-- ## end ## obs max par point sur 2 passages 
		 as om , point as p, carre as c, species as e , point_annee as pa , carre_annee as ca 
where 
om.id_point = p.pk_point and p.id_carre = c.pk_carre and om.code_sp = e.pk_species and p.altitude <= 800 and 
om.id_point = pa.id_point and om.annee = pa.annee and p.id_carre = ca.id_carre and om.annee = ca.annee  
order by 
om.id_point, annee,code_sp;  

 - Traduction des noms de colonnes
 -->  export/data_FrenchBBS_point_KarineP_dist100_allSp_2001_2017eng.csv 

     #      ==> Duree: 2 minutes



####################################

> dE <- makeTableEBBA2()

 QUERY EBBA2:
--------------

 select sp.id_carre||to_char(to_date(sp.date,'YYYY-MM-DD'),'YYYYMMDD') as "Survey number",
to_char(to_date(sp.date,'YYYY-MM-DD'),'DD.MM.YYYY') as "Date of survey",nbp * 5 as "Duration of survey", 1 as "Field Method",
c.latitude_points_wgs84 as "Latitude",c.longitude_points_wgs84 as "Longitude", '' as "Square code", 
lpad(euring::text, 5, '0') as "EBBA2 species code", s.scientific_name as "EBBA2 scientific name"
from
     (select o.date,o.id_carre,passage_stoc,code_sp, sum(abondance) as abondance
     from inventaire as i , observation as o
     where
     o.id_inventaire = i.pk_inventaire and
     code_sp not  in  ('MOTFLA' , 'SAXRUB' , 'ANTPRA') and passage_stoc in (1,2) and
     o.annee >=2003 and o.annee <= 2017      
     group by o.date, o.id_carre, passage_stoc,code_sp
     union
     select o.date,o.id_carre,passage_stoc,code_sp, sum(abondance) as abondance
     from inventaire as i , observation as o
     where
     o.id_inventaire = i.pk_inventaire and
     code_sp in  ('MOTFLA' , 'SAXRUB' , 'ANTPRA') and passage_stoc = 2 and
     o.annee >=2003 and o.annee <= 2017      
     group by o.date, o.id_carre, passage_stoc,code_sp
     ) as sp,
     (select p.date, p.id_carre, count(p.id_point) as nbp
     from (
        select o.date, o.id_carre, o.id_point
        from inventaire as i , observation as o
        where o.id_inventaire = i.pk_inventaire and passage_stoc in (1,2) and o.annee >=2003 and o.annee <= 2017
        group by o.date, o.id_carre, o.id_point ) as p
     group by p.date, p.id_carre) as cnbp, carre as c, species as s
where 
sp.id_carre = cnbp.id_carre and sp.date = cnbp.date and sp.id_carre = c.pk_carre and sp.code_sp=s.pk_species and  c.etude in ('STOC_EPS', 'STOC_ONF');
 

















##############################################


> makeTablePoint(sp="PASDOM",distance_contact=NULL,addAbscence=TRUE,id_output="Bertille_DistFULL_171206")

Requete principale:

 select om.id_point as point, p.id_carre as carre, om.annee as annee, 
(om.annee::varchar(4)||om.id_point::varchar(100))::varchar(100) as id_point_annee, 
c.etude as etude, p.commune,p.insee,p.departement as departement, 
code_sp , e.scientific_name as nom_scientifique, e.french_name as nom_francais, e.english_name as nom_anglais, e.euring as code_espece_euring,e.taxref as code_espece_taxref,
abond_brut as abondance_brut, abond as abondance, pa.qualite_inventaire_stoc as qualite_inventaire_stoc,
p.altitude, longitude_wgs84,  latitude_wgs84,  
pa.p_milieu as habitat_principal, pa.s_milieu as habitat_secondaire, pa.p_dernier_descri as temps_depuis_derniere_description_habitat ,
   pa.foret_p as foret_p, pa.agri_p as agricole_p, pa.urbain_p as urbain_p, pa.foret_ps as foret_ps, pa.agri_ps as agricole_ps, pa.urbain_ps as urbain_ps,
  nbp_foret_p  as carre_nb_pts_foret_p, nbp_ouvert_p as carre_nb_pts_ouvert_p, nbp_agri_p as carre_nb_pts_agricole_p, nbp_urbain_p as carre_nb_pts_urbain_p,
nbp_foret_ps as carre_nb_pts_foret_ps, nbp_ouvert_ps as carre_nb_pts_ouvert_ps, nbp_agri_ps as carre_nb_pts_agricole_ps, nbp_urbain_ps  as carre_nb_pts_urbain_ps,  p.db as data_base_name, '2017-12-06'::varchar(10) as date_export,
'Lorrilliere Romain'::varchar(50) as operateur,
'lorrilliere@mnhn.fr'::varchar(50) as email_operateur

from(
	select id_point, annee,code_sp,max(abond_brut) as abond_brut,max(abond) as abond
		from -- ## begin ## correction abondance obs par seuil
			(select id_inventaire,id_point, passage_stoc, annee, code_sp,abond as abond_brut, abondAll_seuil99 as seuil_abondance_sp,LEAST(abond,
                          abondAll_seuil99) as abond
			from -- ## begin ## selection classe de distance et different filtre
				(  SELECT id_inventaire, code_sp, sum(abondance) as abond
				FROM 
				observation as o, inventaire as i,point as p, point_annee as pa, carre as c
				WHERE 
				o.id_inventaire = i.pk_inventaire and 
				o.id_carre = c.pk_carre and 
				o.id_point = p.pk_point and 
				o.id_point = pa.id_point and o.annee=pa.annee and 
				pa.qualite_inventaire_stoc > 0 and
                                  code_sp in  ('PASDOM')  and
                                 
                                passage_stoc in (1,2) and  i.annee >= 2001  and i.annee <= 2017 and c.etude in ('STOC_EPS', 'STOC_ONF')  and  p.altitude <= 8000

 
    
			GROUP BY 
			id_inventaire, code_sp
	                        ) --  ## end ## selection classe de distance et different filtre  
			as opb, espece_abondance_point_seuil as s, inventaire as i
			WHERE 
			opb.code_sp = s.pk_species and opb.id_inventaire = i.pk_inventaire

			) -- ## end ## correction abondance obs par seuil
		as op
		group by id_point,annee,code_sp
		 )-- ## end ## obs max par point sur 2 passages 
		 as om , point as p, carre as c, species as e , point_annee as pa , carre_annee as ca 
where 
om.id_point = p.pk_point and p.id_carre = c.pk_carre and om.code_sp = e.pk_species and p.altitude <= 800 and 
om.id_point = pa.id_point and om.annee = pa.annee and p.id_carre = ca.id_carre and om.annee = ca.annee  
order by 
om.id_point, annee,code_sp;  


 Ajout des absences: 

Requete inventaire: Recherche de tous les inventaires

 select pa.id_point as point, pa.id_carre as carre, pa.annee as annee, pk_point_annee as id_point_annee, ''::varchar(25) as etude,
 p.commune,p.insee,p.departement as departement,pa.qualite_inventaire_stoc as qualite_inventaire_stoc,p.altitude, longitude_wgs84,  latitude_wgs84,
 pa.p_milieu as habitat_principal, pa.s_milieu as habitat_secondaire, pa.p_dernier_descri as temps_depuis_derniere_description_habitat ,
   pa.foret_p as foret_p, pa.agri_p as agricole_p, pa.urbain_p as urbain_p, pa.foret_ps as foret_ps, pa.agri_ps as agricole_ps, pa.urbain_ps as urbain_ps,
  nbp_foret_p  as carre_nb_pts_foret_p, nbp_ouvert_p as carre_nb_pts_ouvert_p, nbp_agri_p as carre_nb_pts_agricole_p, nbp_urbain_p as carre_nb_pts_urbain_p,
nbp_foret_ps as carre_nb_pts_foret_ps, nbp_ouvert_ps as carre_nb_pts_ouvert_ps, nbp_agri_ps as carre_nb_pts_agricole_ps, nbp_urbain_ps  as carre_nb_pts_urbain_ps,  p.db as data_base_name, '2017-12-06'::varchar(10) as date_export,
'Lorrilliere Romain'::varchar(50) as operateur,
'lorrilliere@mnhn.fr'::varchar(50) as email_operateur
from
point_annee as pa, point as p, carre as c , carre_annee as ca 
where 
pa.id_point = p.pk_point and pa.id_carre = c.pk_carre and p.altitude <= 8000 and  pa.annee >= 2001  and pa.annee <= 2017 and pa.id_carre = ca.id_carre and pa.annee = ca.annee   
order by 
pa.id_point, annee; 

 -->  export/data_FrenchBBS_point_Bertille_DistFULL_171206_PASDOM_2001_2017fr.csv 

     #      ==> Duree: 22 minutes
> makeTablePoint(sp="PASDOM",distance_contact=100,addAbscence=TRUE,id_output="Bertille_Dist100_171206")

Requete principale:

 select om.id_point as point, p.id_carre as carre, om.annee as annee, 
(om.annee::varchar(4)||om.id_point::varchar(100))::varchar(100) as id_point_annee, 
c.etude as etude, p.commune,p.insee,p.departement as departement, 
code_sp , e.scientific_name as nom_scientifique, e.french_name as nom_francais, e.english_name as nom_anglais, e.euring as code_espece_euring,e.taxref as code_espece_taxref,
abond_brut as abondance_brut, abond as abondance, pa.qualite_inventaire_stoc as qualite_inventaire_stoc,
p.altitude, longitude_wgs84,  latitude_wgs84,  
pa.p_milieu as habitat_principal, pa.s_milieu as habitat_secondaire, pa.p_dernier_descri as temps_depuis_derniere_description_habitat ,
   pa.foret_p as foret_p, pa.agri_p as agricole_p, pa.urbain_p as urbain_p, pa.foret_ps as foret_ps, pa.agri_ps as agricole_ps, pa.urbain_ps as urbain_ps,
  nbp_foret_p  as carre_nb_pts_foret_p, nbp_ouvert_p as carre_nb_pts_ouvert_p, nbp_agri_p as carre_nb_pts_agricole_p, nbp_urbain_p as carre_nb_pts_urbain_p,
nbp_foret_ps as carre_nb_pts_foret_ps, nbp_ouvert_ps as carre_nb_pts_ouvert_ps, nbp_agri_ps as carre_nb_pts_agricole_ps, nbp_urbain_ps  as carre_nb_pts_urbain_ps,  p.db as data_base_name, '2017-12-06'::varchar(10) as date_export,
'Lorrilliere Romain'::varchar(50) as operateur,
'lorrilliere@mnhn.fr'::varchar(50) as email_operateur

from(
	select id_point, annee,code_sp,max(abond_brut) as abond_brut,max(abond) as abond
		from -- ## begin ## correction abondance obs par seuil
			(select id_inventaire,id_point, passage_stoc, annee, code_sp,abond as abond_brut, abondAll_seuil99 as seuil_abondance_sp,LEAST(abond,
                          abondAll_seuil99) as abond
			from -- ## begin ## selection classe de distance et different filtre
				(  SELECT id_inventaire, code_sp, sum(abondance) as abond
				FROM 
				observation as o, inventaire as i,point as p, point_annee as pa, carre as c
				WHERE 
				o.id_inventaire = i.pk_inventaire and 
				o.id_carre = c.pk_carre and 
				o.id_point = p.pk_point and 
				o.id_point = pa.id_point and o.annee=pa.annee and 
				pa.qualite_inventaire_stoc > 0 and
                                  code_sp in  ('PASDOM')  and
                                 
                                passage_stoc in (1,2) and  distance_contact in ('LESS25','LESS100') and  i.annee >= 2001  and i.annee <= 2017 and c.etude in ('STOC_EPS', 'STOC_ONF')  and  p.altitude <= 8000

 
    
			GROUP BY 
			id_inventaire, code_sp
	                        ) --  ## end ## selection classe de distance et different filtre  
			as opb, espece_abondance_point_seuil as s, inventaire as i
			WHERE 
			opb.code_sp = s.pk_species and opb.id_inventaire = i.pk_inventaire

			) -- ## end ## correction abondance obs par seuil
		as op
		group by id_point,annee,code_sp
		 )-- ## end ## obs max par point sur 2 passages 
		 as om , point as p, carre as c, species as e , point_annee as pa , carre_annee as ca 
where 
om.id_point = p.pk_point and p.id_carre = c.pk_carre and om.code_sp = e.pk_species and p.altitude <= 800 and 
om.id_point = pa.id_point and om.annee = pa.annee and p.id_carre = ca.id_carre and om.annee = ca.annee  
order by 
om.id_point, annee,code_sp;  


 Ajout des absences: 

Requete inventaire: Recherche de tous les inventaires

 select pa.id_point as point, pa.id_carre as carre, pa.annee as annee, pk_point_annee as id_point_annee, ''::varchar(25) as etude,
 p.commune,p.insee,p.departement as departement,pa.qualite_inventaire_stoc as qualite_inventaire_stoc,p.altitude, longitude_wgs84,  latitude_wgs84,
 pa.p_milieu as habitat_principal, pa.s_milieu as habitat_secondaire, pa.p_dernier_descri as temps_depuis_derniere_description_habitat ,
   pa.foret_p as foret_p, pa.agri_p as agricole_p, pa.urbain_p as urbain_p, pa.foret_ps as foret_ps, pa.agri_ps as agricole_ps, pa.urbain_ps as urbain_ps,
  nbp_foret_p  as carre_nb_pts_foret_p, nbp_ouvert_p as carre_nb_pts_ouvert_p, nbp_agri_p as carre_nb_pts_agricole_p, nbp_urbain_p as carre_nb_pts_urbain_p,
nbp_foret_ps as carre_nb_pts_foret_ps, nbp_ouvert_ps as carre_nb_pts_ouvert_ps, nbp_agri_ps as carre_nb_pts_agricole_ps, nbp_urbain_ps  as carre_nb_pts_urbain_ps,  p.db as data_base_name, '2017-12-06'::varchar(10) as date_export,
'Lorrilliere Romain'::varchar(50) as operateur,
'lorrilliere@mnhn.fr'::varchar(50) as email_operateur
from
point_annee as pa, point as p, carre as c , carre_annee as ca 
where 
pa.id_point = p.pk_point and pa.id_carre = c.pk_carre and p.altitude <= 8000 and  pa.annee >= 2001  and pa.annee <= 2017 and pa.id_carre = ca.id_carre and pa.annee = ca.annee   
order by 
pa.id_point, annee; 

 -->  export/data_FrenchBBS_point_Bertille_Dist100_171206_PASDOM_2001_2017fr.csv 

     #      ==> Duree: 12 minutes

########################################



 poney <- makeTablePoint(output=TRUE,sp=c("SYLATR","CARCHL","EMBCIT","EMBCIR","COLPAL","ERIRUB","TURMER","PASDOM","ALAARV","CARCAN","TROTRO"),champsHabitat=TRUE,distance_contact=NULL,id_output="AlexR_distAll_2017-11-24",isEnglish=TRUE)

Requete principale:

 select om.id_point as point, p.id_carre as carre, om.annee as annee, 
(om.annee::varchar(4)||om.id_point::varchar(100))::varchar(100) as id_point_annee, 
c.etude as etude, p.commune,p.insee,p.departement as departement, 
code_sp , e.scientific_name as nom_scientifique, e.french_name as nom_francais, e.english_name as nom_anglais, e.euring as code_espece_euring,e.taxref as code_espece_taxref,
abond_brut as abondance_brut, abond as abondance, pa.qualite_inventaire_stoc as qualite_inventaire_stoc,
p.altitude, longitude_wgs84,  latitude_wgs84,  
pa.p_milieu as habitat_principal, pa.s_milieu as habitat_secondaire, pa.p_dernier_descri as temps_depuis_derniere_description_habitat ,
   pa.foret_p as foret_p, pa.agri_p as agricole_p, pa.urbain_p as urbain_p, pa.foret_ps as foret_ps, pa.agri_ps as agricole_ps, pa.urbain_ps as urbain_ps,
  nbp_foret_p  as carre_nb_pts_foret_p, nbp_ouvert_p as carre_nb_pts_ouvert_p, nbp_agri_p as carre_nb_pts_agricole_p, nbp_urbain_p as carre_nb_pts_urbain_p,
nbp_foret_ps as carre_nb_pts_foret_ps, nbp_ouvert_ps as carre_nb_pts_ouvert_ps, nbp_agri_ps as carre_nb_pts_agricole_ps, nbp_urbain_ps  as carre_nb_pts_urbain_ps,  p.db as data_base_name, '2017-11-24'::varchar(10) as date_export,
'Lorrilliere Romain'::varchar(50) as operateur,
'lorrilliere@mnhn.fr'::varchar(50) as email_operateur

from(
	select id_point, annee,code_sp,max(abond_brut) as abond_brut,max(abond) as abond
		from -- ## begin ## correction abondance obs par seuil
			(select id_inventaire,id_point, passage_stoc, annee, code_sp,abond as abond_brut, abondAll_seuil99 as seuil_abondance_sp,LEAST(abond,
                          abondAll_seuil99) as abond
			from -- ## begin ## selection classe de distance et different filtre
				(  SELECT id_inventaire, code_sp, sum(abondance) as abond
				FROM 
				observation as o, inventaire as i,point as p, point_annee as pa, carre as c
				WHERE 
				o.id_inventaire = i.pk_inventaire and 
				o.id_carre = c.pk_carre and 
				o.id_point = p.pk_point and 
				o.id_point = pa.id_point and o.annee=pa.annee and 
				pa.qualite_inventaire_stoc > 0 and
                                  code_sp in  ('SYLATR' , 'CARCHL' , 'EMBCIT' , 'EMBCIR' , 'COLPAL' , 'ERIRUB' , 'TURMER' , 'PASDOM' , 'ALAARV' , 'CARCAN' , 'TROTRO')  and
                                 
                                passage_stoc in (1,2) and  i.annee >= 2001  and i.annee <= 2017 and c.etude in ('STOC_EPS', 'STOC_ONF')  and  p.altitude <= 8000

 
    
			GROUP BY 
			id_inventaire, code_sp
	                        ) --  ## end ## selection classe de distance et different filtre  
			as opb, espece_abondance_point_seuil as s, inventaire as i
			WHERE 
			opb.code_sp = s.pk_species and opb.id_inventaire = i.pk_inventaire

			) -- ## end ## correction abondance obs par seuil
		as op
		group by id_point,annee,code_sp
		 )-- ## end ## obs max par point sur 2 passages 
		 as om , point as p, carre as c, species as e , point_annee as pa , carre_annee as ca 
where 
om.id_point = p.pk_point and p.id_carre = c.pk_carre and om.code_sp = e.pk_species and p.altitude <= 800 and 
om.id_point = pa.id_point and om.annee = pa.annee and p.id_carre = ca.id_carre and om.annee = ca.annee  
order by 
om.id_point, annee,code_sp;  


 Ajout des absences: 

Requete inventaire: Recherche de tous les inventaires

 select pa.id_point as point, pa.id_carre as carre, pa.annee as annee, pk_point_annee as id_point_annee, ''::varchar(25) as etude,
 p.commune,p.insee,p.departement as departement,pa.qualite_inventaire_stoc as qualite_inventaire_stoc,p.altitude, longitude_wgs84,  latitude_wgs84,
 pa.p_milieu as habitat_principal, pa.s_milieu as habitat_secondaire, pa.p_dernier_descri as temps_depuis_derniere_description_habitat ,
   pa.foret_p as foret_p, pa.agri_p as agricole_p, pa.urbain_p as urbain_p, pa.foret_ps as foret_ps, pa.agri_ps as agricole_ps, pa.urbain_ps as urbain_ps,
  nbp_foret_p  as carre_nb_pts_foret_p, nbp_ouvert_p as carre_nb_pts_ouvert_p, nbp_agri_p as carre_nb_pts_agricole_p, nbp_urbain_p as carre_nb_pts_urbain_p,
nbp_foret_ps as carre_nb_pts_foret_ps, nbp_ouvert_ps as carre_nb_pts_ouvert_ps, nbp_agri_ps as carre_nb_pts_agricole_ps, nbp_urbain_ps  as carre_nb_pts_urbain_ps,  p.db as data_base_name, '2017-11-24'::varchar(10) as date_export,
'Lorrilliere Romain'::varchar(50) as operateur,
'lorrilliere@mnhn.fr'::varchar(50) as email_operateur
from
point_annee as pa, point as p, carre as c , carre_annee as ca 
where 
pa.id_point = p.pk_point and pa.id_carre = c.pk_carre and p.altitude <= 8000 and  pa.annee >= 2001  and pa.annee <= 2017 and pa.id_carre = ca.id_carre and pa.annee = ca.annee   
order by 
pa.id_point, annee; 

 -->  export/data_FrenchBBS_point_AlexR_distAll_2017-11-24_11sp_2001_2017eng.csv 

     #      ==> Duree: 2 minutes
















###---------------------------------------------------------


> poney100 <- makeTablePoint(output=TRUE,sp=c("SYLATR","CARCHL","EMBCIT","EMBCIR","COLPAL","ERIRUB","TURMER","PASDOM","ALAARV","CARCAN","TROTRO"),champsHabitat=TRUE,distance_contact="100",id_output="AlexR_dist100_2017-11-24",isEnglish=TRUE)

Requete principale:

 select om.id_point as point, p.id_carre as carre, om.annee as annee, 
(om.annee::varchar(4)||om.id_point::varchar(100))::varchar(100) as id_point_annee, 
c.etude as etude, p.commune,p.insee,p.departement as departement, 
code_sp , e.scientific_name as nom_scientifique, e.french_name as nom_francais, e.english_name as nom_anglais, e.euring as code_espece_euring,e.taxref as code_espece_taxref,
abond_brut as abondance_brut, abond as abondance, pa.qualite_inventaire_stoc as qualite_inventaire_stoc,
p.altitude, longitude_wgs84,  latitude_wgs84,  
pa.p_milieu as habitat_principal, pa.s_milieu as habitat_secondaire, pa.p_dernier_descri as temps_depuis_derniere_description_habitat ,
   pa.foret_p as foret_p, pa.agri_p as agricole_p, pa.urbain_p as urbain_p, pa.foret_ps as foret_ps, pa.agri_ps as agricole_ps, pa.urbain_ps as urbain_ps,
  nbp_foret_p  as carre_nb_pts_foret_p, nbp_ouvert_p as carre_nb_pts_ouvert_p, nbp_agri_p as carre_nb_pts_agricole_p, nbp_urbain_p as carre_nb_pts_urbain_p,
nbp_foret_ps as carre_nb_pts_foret_ps, nbp_ouvert_ps as carre_nb_pts_ouvert_ps, nbp_agri_ps as carre_nb_pts_agricole_ps, nbp_urbain_ps  as carre_nb_pts_urbain_ps,  p.db as data_base_name, '2017-11-24'::varchar(10) as date_export,
'Lorrilliere Romain'::varchar(50) as operateur,
'lorrilliere@mnhn.fr'::varchar(50) as email_operateur

from(
	select id_point, annee,code_sp,max(abond_brut) as abond_brut,max(abond) as abond
		from -- ## begin ## correction abondance obs par seuil
			(select id_inventaire,id_point, passage_stoc, annee, code_sp,abond as abond_brut, abondAll_seuil99 as seuil_abondance_sp,LEAST(abond,
                          abondAll_seuil99) as abond
			from -- ## begin ## selection classe de distance et different filtre
				(  SELECT id_inventaire, code_sp, sum(abondance) as abond
				FROM 
				observation as o, inventaire as i,point as p, point_annee as pa, carre as c
				WHERE 
				o.id_inventaire = i.pk_inventaire and 
				o.id_carre = c.pk_carre and 
				o.id_point = p.pk_point and 
				o.id_point = pa.id_point and o.annee=pa.annee and 
				pa.qualite_inventaire_stoc > 0 and
                                  code_sp in  ('SYLATR' , 'CARCHL' , 'EMBCIT' , 'EMBCIR' , 'COLPAL' , 'ERIRUB' , 'TURMER' , 'PASDOM' , 'ALAARV' , 'CARCAN' , 'TROTRO')  and
                                 
                                passage_stoc in (1,2) and  distance_contact in ('LESS25','LESS100') and  i.annee >= 2001  and i.annee <= 2017 and c.etude in ('STOC_EPS', 'STOC_ONF')  and  p.altitude <= 8000

 
    
			GROUP BY 
			id_inventaire, code_sp
	                        ) --  ## end ## selection classe de distance et different filtre  
			as opb, espece_abondance_point_seuil as s, inventaire as i
			WHERE 
			opb.code_sp = s.pk_species and opb.id_inventaire = i.pk_inventaire

			) -- ## end ## correction abondance obs par seuil
		as op
		group by id_point,annee,code_sp
		 )-- ## end ## obs max par point sur 2 passages 
		 as om , point as p, carre as c, species as e , point_annee as pa , carre_annee as ca 
where 
om.id_point = p.pk_point and p.id_carre = c.pk_carre and om.code_sp = e.pk_species and p.altitude <= 800 and 
om.id_point = pa.id_point and om.annee = pa.annee and p.id_carre = ca.id_carre and om.annee = ca.annee  
order by 
om.id_point, annee,code_sp;  


 Ajout des absences: 

Requete inventaire: Recherche de tous les inventaires

 select pa.id_point as point, pa.id_carre as carre, pa.annee as annee, pk_point_annee as id_point_annee, ''::varchar(25) as etude,
 p.commune,p.insee,p.departement as departement,pa.qualite_inventaire_stoc as qualite_inventaire_stoc,p.altitude, longitude_wgs84,  latitude_wgs84,
 pa.p_milieu as habitat_principal, pa.s_milieu as habitat_secondaire, pa.p_dernier_descri as temps_depuis_derniere_description_habitat ,
   pa.foret_p as foret_p, pa.agri_p as agricole_p, pa.urbain_p as urbain_p, pa.foret_ps as foret_ps, pa.agri_ps as agricole_ps, pa.urbain_ps as urbain_ps,
  nbp_foret_p  as carre_nb_pts_foret_p, nbp_ouvert_p as carre_nb_pts_ouvert_p, nbp_agri_p as carre_nb_pts_agricole_p, nbp_urbain_p as carre_nb_pts_urbain_p,
nbp_foret_ps as carre_nb_pts_foret_ps, nbp_ouvert_ps as carre_nb_pts_ouvert_ps, nbp_agri_ps as carre_nb_pts_agricole_ps, nbp_urbain_ps  as carre_nb_pts_urbain_ps,  p.db as data_base_name, '2017-11-24'::varchar(10) as date_export,
'Lorrilliere Romain'::varchar(50) as operateur,
'lorrilliere@mnhn.fr'::varchar(50) as email_operateur
from
point_annee as pa, point as p, carre as c , carre_annee as ca 
where 
pa.id_point = p.pk_point and pa.id_carre = c.pk_carre and p.altitude <= 8000 and  pa.annee >= 2001  and pa.annee <= 2017 and pa.id_carre = ca.id_carre and pa.annee = ca.annee   
order by 
pa.id_point, annee; 

 -->  export/data_FrenchBBS_point_AlexR_dist100_2017-11-24_11sp_2001_2017eng.csv 

     #      ==> Duree: 2 minutes


# 2017-11-16
#  Export pour tendence national STOC


 d <- makeTableCarre(sp=NULL,champsHabitat=FALSE,firstYear=2001,lastYear=2017,departement=NULL,onf=TRUE,distance_contact=NULL,formatTrend=TRUE,addAbscence=TRUE,id_output="France")





##-------------------------------------------------------------


Requete principale:

 
select oc.id_carre as carre, oc.annee, (oc.annee::varchar(4)||oc.id_carre::varchar(100))::varchar(100) as id_carre_annee, c.etude,
qualite_inventaire_stoc, commune,insee,departement, 
code_sp, e.scientific_name, e.french_name, e.euring,e.taxref,
abond_brut as abondance_brut, abond  as abondance, 
altitude, longitude_grid_wgs84,latitude_grid_wgs84, '2017-11-16'::varchar(10) as date_export,
'Lorrilliere Romain'::varchar(50) as operateur,
'lorrilliere@mnhn.fr'::varchar(50) as email_operateur

from( -- ## begin ## somme sur carre des max par points sur 2 passages
	select id_carre, annee,code_sp,sum(abond_brut) as abond_brut,sum(abond) as abond
	from -- ## begin ## obs max par point sur 2 passages
		(select id_point, annee,code_sp,max(abond_brut) as abond_brut,max(abond) as abond
		from -- ## begin ## correction abondance obs par seuil
			(select id_inventaire,id_point, passage_stoc, annee, code_sp,abond as abond_brut, abondAll_seuil99 as seuil_abondance_sp,LEAST(abond, abondAll_seuil99) as abond
			from -- ## begin ## selection classe de distance et different filtre
				( SELECT id_inventaire, code_sp, sum(abondance) as abond
				FROM 
				observation as o, inventaire as i,carre_annee as ca, carre as c
				WHERE 
				o.id_inventaire = i.pk_inventaire and 
				o.id_carre = c.pk_carre and 
				o.id_carre = ca.id_carre and o.annee=ca.annee and 
				ca.qualite_inventaire_stoc > 0 and
                                 code_sp not  in  ('MOTFLA' , 'SAXRUB' , 'ANTPRA')  and    passage_stoc in (1,2) and  i.annee >= 2001  and i.annee <= 2017 and c.etude in ('STOC_EPS', 'STOC_ONF')  and  c.altitude <= 10000    
			GROUP BY 
			id_inventaire, code_sp 
                                union
                                  --  ## begin ## ajout des especes tardives dont on ne garde que le second passage
             SELECT id_inventaire, code_sp, sum(abondance) as abond
				FROM 
				observation as o, inventaire as i,carre_annee as ca, carre as c
				WHERE 
				o.id_inventaire = i.pk_inventaire and 
				o.id_carre = c.pk_carre and 
				o.id_carre = ca.id_carre and o.annee=ca.annee and 
				ca.qualite_inventaire_stoc > 0 and
                                 code_sp in  ('MOTFLA' , 'SAXRUB' , 'ANTPRA')  and    passage_stoc = 2 and  i.annee >= 2001  and i.annee <= 2017 and c.etude in ('STOC_EPS', 'STOC_ONF')  and  c.altitude <= 10000    
			GROUP BY 
			id_inventaire, code_sp
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
as oc, carre as c, species as e, carre_annee as ca 
where 
oc.id_carre = c.pk_carre and oc.code_sp = e.pk_species and oc.id_carre = ca.id_carre and oc.annee = ca.annee
order by 
oc.id_carre, annee,code_sp; 


 Ajout des absences: 

Requete inventaire: Recherche de tous les inventaires

 select ca.id_carre as square, ca.annee as year, pk_carre_annee as id_carre_annee, etude as study,
 c.commune,c.insee,c.departement as district, 
c.altitude, longitude_grid_wgs84 ,latitude_grid_wgs84,  c.db as data_base_name, '2017-11-16'::varchar(10) as date_export,
'Lorrilliere Romain'::varchar(50) as operateur,
'lorrilliere@mnhn.fr'::varchar(50) as email_operateur
from
carre as c , carre_annee as ca 
where 
ca.id_carre = c.pk_carre and c.altitude <= 10000 and 
 ca.qualite_inventaire_stoc > 0 and ca.annee >= 2001  and ca.annee <= 2017 and c.etude in ('STOC_EPS', 'STOC_ONF')    
order by 
ca.id_carre, year; 

     #      ==> Duree: 4 minutes
#################################################################
