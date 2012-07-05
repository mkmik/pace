#!/bin/sh 
echo "DROP TABLE IF EXISTS merges_results;"
echo "CREATE TABLE merges_results (merged_id text REFERENCES results (resultid), merges_id text REFERENCES results (resultid));"
#echo "CREATE TABLE merges_results (merged_id text, merges_id text);"
echo "ALTER TABLE merges_results ADD unique (merged_id, merges_id);"
echo "DROP TABLE IF EXISTS merges_results_log;"
echo "CREATE TABLE merges_results_log (_dnet_resource_identifier_ character varying(255), operation character varying(10), date timestamp without time zone default now(),	id serial, status character varying(10) default ''::character varying);"
echo "CREATE OR REPLACE FUNCTION cleanup_results_projects_frommerge() RETURNS void LANGUAGE plpgsql AS  \$\$ DECLARE BEGIN if 0 = (select count(*) from information_schema.columns where table_name = 'results_projects\' and column_name = 'frommerge') then ALTER TABLE results_projects ADD frommerge bool DEFAULT false;  END IF; END \$\$;"
echo "SELECT cleanup_results_projects_frommerge();"
echo "BEGIN;"
echo "DELETE FROM results_projects where frommerge = true;"

mongo --quiet pace  $(dirname $0)/candidates_insert.js  | sort -u

echo "update results set merged = 'f', mergedinresult = NULL where merged = 't';"
echo "update results set merged = 't' from merges_results m where m.merges_id = resultid;"
echo "INSERT INTO results_projects (result,project,frommerge) SELECT m.merged_id,rp.project,'t' FROM merges_results m JOIN results_projects rp on (m.merges_id = rp.result) WHERE project not in (SELECT project FROM results_projects rp2 WHERE rp2.result = m.merged_id);"
echo "COMMIT;"

echo "DROP TABLE IF EXISTS merges_results_log;"
echo "DROP TABLE IF EXISTS merges_results;"

