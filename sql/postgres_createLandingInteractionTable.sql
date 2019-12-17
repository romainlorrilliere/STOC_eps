
DROP TABLE IF EXISTS interaction_landing; 

CREATE TABLE interaction_landing AS (
SELECT * 
FROM (
SELECT a.id_sample, s.id_first_feederpair,a.feeder as feeder,TRUE as same_feeder,
a.pk_birdsonfeeder as id_birdsonfeeder, a.id_bird as id_bird, a.id_species as id_species,p.id_bird as id_bird_2, p.id_species as id_species_2,
a.duration as time2leaving, p.leaving_time - a.arrival_time as time2leaving_2,
LEAST(p.leaving_time - a.arrival_time,a.duration) as time_interaction,
LEAST(p.leaving_time - a.arrival_time,a.duration)/a.duration as prop_interaction, LEAST(p.leaving_time - a.arrival_time,a.duration)/(p.duration) as prop_interaction_2,
a.leaving_kind_action as leaving_kind_action,p.leaving_kind_action as leaving_kind_action_2
FROM
	(SELECT pk_birdsonfeeder, id_sample, id_bird, name_bird, id_species, feeder, arrival_time,duration,leaving_time, leaving_kind_action
	FROM birdsonfeeders
	WHERE arrival_kind_action LIKE 'arrival') as a,
	(SELECT pk_birdsonfeeder,id_sample, id_bird, name_bird, id_species, feeder, arrival_time,leaving_time,duration,leaving_kind_action
	FROM birdsonfeeders) as p,
    samples as s
WHERE a.id_sample = s.pk_sample AND a.id_sample = p.id_sample AND a.feeder=p.feeder AND a.arrival_time >= p.arrival_time AND a.arrival_time <= p.leaving_time and a.pk_birdsonfeeder NOT LIKE p.pk_birdsonfeeder
UNION
SELECT a.id_sample, s.id_first_feederpair,a.feeder as feeder,FALSE as same_feeder,
a.pk_birdsonfeeder as id_birdsonfeeder,a.id_bird as id_bird, a.id_species as id_species,p.id_bird as id_bird_2, p.id_species as id_species_2,
a.duration as time2leaving, p.leaving_time - a.arrival_time as time2leaving_2,
LEAST(p.leaving_time - a.arrival_time,a.duration) as time_interaction,
LEAST(p.leaving_time - a.arrival_time,a.duration)/a.duration as prop_interaction, LEAST(p.leaving_time - a.arrival_time,a.duration)/(p.duration) as prop_interaction_2,
a.leaving_kind_action as leaving_kind_action,p.leaving_kind_action as leaving_kind_action2
FROM
	(SELECT pk_birdsonfeeder, id_sample, id_bird, name_bird, id_species, feeder, arrival_time,duration,leaving_time, leaving_kind_action
	FROM birdsonfeeders
	WHERE arrival_kind_action LIKE 'arrival') as a,
	(SELECT pk_birdsonfeeder,id_sample, id_bird, name_bird, id_species, feeder, arrival_time,leaving_time,duration,leaving_kind_action
	FROM birdsonfeeders) as p,
    samples as s
WHERE a.id_sample = s.pk_sample AND a.id_sample = p.id_sample AND a.feeder NOT LIKE p.feeder AND a.arrival_time >= p.arrival_time AND a.arrival_time <= p.leaving_time and a.pk_birdsonfeeder NOT LIKE p.pk_birdsonfeeder) t
ORDER BY id_bird);



CREATE INDEX IF NOT EXISTS il_sample ON interaction_landing(id_sample);
CREATE INDEX IF NOT EXISTS il_feederpair ON interaction_landing(id_first_feederpair);
CREATE INDEX IF NOT EXISTS il_species ON interaction_landing(id_species);
CREATE INDEX IF NOT EXISTS il_species2 ON interaction_landing(id_species_2);
CREATE INDEX IF NOT EXISTS il_samefeed ON interaction_landing(same_feeder);
CREATE INDEX IF NOT EXISTS il_birdsonfeeder ON interaction_landing(id_birdsonfeeder);
CREATE INDEX IF NOT EXISTS il_bird ON interaction_landing(id_bird);
CREATE INDEX IF NOT EXISTS il_bird2 ON interaction_landing(id_bird_2);
CREATE INDEX IF NOT EXISTS il_leaving_kind_action ON interaction_landing(leaving_kind_action);
CREATE INDEX IF NOT EXISTS il_leaving_kind_action2 ON interaction_landing(leaving_kind_action_2);