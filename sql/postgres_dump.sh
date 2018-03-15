# sudo -i -u postgres
# /usr/bin/pg_dump -h localhost stoc_eps > /home/romain/1_Recherche/STOCeps/Output/stoc_eps_dump.sql
# sudo -i -u postgres
# /usr/bin/pg_dump -h localhost stoc_eps > /home/romain/1_Recherche/STOCeps/Output/stoc_eps_dump.sql



# les deux bonne ligne de commande
pg_dump stoc_eps > git/STOC_eps/Output/dump_stoc_eps_2018-02-28.sql
pg_dump -E LATIN1 stoc_eps > git/STOC_eps/Output/dump_stoc_eps_LATIN1_2018-02-28.sql

