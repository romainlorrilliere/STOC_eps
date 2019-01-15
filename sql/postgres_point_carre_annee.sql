
DROP table if exists point_annee;
CREATE TABLE point_annee  
	(pk_point_annee varchar(100) primary key,
		id_point varchar(100) NOT NULL,
		id_carre varchar(100) NOT NULL,
		annee integer NOT NULL,
		qualite_inventaire_stoc real,
		p_milieu varchar(1), 
		s_milieu varchar(1), 
		p_dernier_descri integer,
		foret_p boolean NOT NULL, 
		ouvert_p boolean NOT NULL, 
		agri_p boolean NOT NULL, 
		urbain_p boolean NOT NULL,
		foret_ps boolean NOT NULL, 
		ouvert_ps boolean NOT NULL, 
		agri_ps boolean NOT NULL, 
		urbain_ps boolean NOT NULL,
		date_export varchar(10) NOT NULL,
    version varchar(10) NOT NULL);





CREATE INDEX pa_point ON point_annee(id_point);
CREATE INDEX pa_carre ON point_annee(id_carre);
CREATE INDEX pa_an ON point_annee(annee);
CREATE INDEX pa_pm ON point_annee(p_milieu);
CREATE INDEX pa_sm ON point_annee(s_milieu);
CREATE INDEX pa_fp ON point_annee(foret_p);
CREATE INDEX pa_op ON point_annee(ouvert_p);
CREATE INDEX pa_ap ON point_annee(agri_p);
CREATE INDEX pa_up ON point_annee(urbain_p);
CREATE INDEX pa_fps ON point_annee(foret_ps);
CREATE INDEX pa_ops ON point_annee(ouvert_ps);
CREATE INDEX pa_aps ON point_annee(agri_ps);
CREATE INDEX pa_ups ON point_annee(urbain_ps);

DROP table if exists carre_annee;
CREATE TABLE carre_annee  
	(pk_carre_annee varchar(100) primary key,
		id_carre varchar(100) NOT NULL,
		annee integer NOT NULL,
		qualite_inventaire_stoc real,
		foret_p boolean NOT NULL, 
		ouvert_p boolean NOT NULL, 
		agri_p boolean NOT NULL, 
		urbain_p boolean NOT NULL,
		foret_ps boolean NOT NULL, 
		ouvert_ps boolean NOT NULL, 
		agri_ps boolean NOT NULL, 
		urbain_ps boolean NOT NULL,
		nbp_foret_p integer NOT NULL, 
		nbp_ouvert_p integer NOT NULL, 
		nbp_agri_p integer NOT NULL, 
		nbp_urbain_p integer NOT NULL,
		nbp_foret_ps integer NOT NULL, 
		nbp_ouvert_ps integer NOT NULL, 
		nbp_agri_ps integer NOT NULL, 
		nbp_urbain_ps integer NOT NULL,
		date_export varchar(10) NOT NULL,
    version varchar(10) NOT NULL);



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

