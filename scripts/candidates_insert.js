print("BEGIN;");
print("TRUNCATE merges_results;");
db.candidates.find({}, {'ids': 1}).forEach(function(rec) {
    var ids = rec["ids"].sort().reverse();
    var h = ids[0];
    ids.slice(1).forEach(function(el) {
        print("INSERT into merges_results values ('"+h+"', '"+el+"');")
    });
});
print("update results set merged = 'f' where merged = 't';");
print("update results set merged = 't' from merges_results m where m.merges_id = resultid;");
print("COMMIT;");