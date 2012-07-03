// the "candidates" mongodb collection contains groups of ids which are meant to be merged in one 
// This code generates an auxiliary table which generates
print("DROP TABLE IF EXISTS merges_results;");
print("CREATE TABLE merges_results (merged_id text REFERENCES results (resultid), merges_id text REFERENCES results (resultid));");
//print("CREATE TABLE merges_results (merged_id text, merges_id text);");
print("ALTER TABLE merges_results ADD unique (merged_id, merges_id);");
print("DROP TABLE IF EXISTS merges_results_log;");
print("CREATE TABLE merges_results_log (_dnet_resource_identifier_ character varying(255), operation character varying(10), date timestamp without time zone default now(),	id serial, status character varying(10) default ''::character varying);");
print("CREATE OR REPLACE FUNCTION cleanup_results_projects_frommerge() RETURNS void LANGUAGE plpgsql AS  $$ DECLARE BEGIN if 0 = (select count(*) from information_schema.columns where table_name = 'results_projects' and column_name = 'frommerge') then ALTER TABLE results_projects ADD frommerge bool DEFAULT false;  END IF; END $$;");
print("SELECT cleanup_results_projects_frommerge();");
print("BEGIN;");
print("DELETE FROM results_projects where frommerge = true;");
db.candidates.find({}, {'ids': 1}).forEach(function(rec) {
    var ids = rec["ids"].sort().reverse();
    var h = ids[0];
    ids.slice(1).forEach(function(el) {
        print("INSERT into merges_results values ('"+h+"', '"+el+"');")
    });
});
print("update results set merged = 'f' where merged = 't';");
print("update results set merged = 't' from merges_results m where m.merges_id = resultid;");
print("INSERT INTO results_projects (result,project,frommerge) SELECT m.merged_id,rp.project,'t' FROM merges_results m JOIN results_projects rp on (m.merges_id = rp.result) WHERE project not in (SELECT project FROM results_projects rp2 WHERE rp2.result = m.merged_id);");
print("COMMIT;");

print("DROP TABLE IF EXISTS merges_results_log;");
print("DROP TABLE IF EXISTS merges_results;");
