// the "candidates" mongodb collection contains groups of ids which are meant to be merged in one 

// This code generates an auxiliary table which generates
print("BEGIN;");
print("DROP TABLE IF EXISTS merges_results;");
print("CREATE TABLE merges_results (merged_id text REFERENCES results (resultid), merges_id text REFERENCES results (resultid));");
print("ALTER TABLE merges_results ADD unique (merged_id, merges_id);");
print("CREATE OR REPLACE FUNCTION cleanup_results_projects_frommerge() RETURNS void LANGUAGE plpgsql AS  $$ DECLARE BEGIN delete from results_projects where frommerge = true;  END; $$;");
print("SELECT cleanup_results_projects_frommerge");
print("ALTER TABLE results_projects DROP IF EXISTS frommerge;");
print("ALTER TABLE results_projects ADD frommerge bool DEFAULT false;");
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
