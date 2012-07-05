// the "candidates" mongodb collection contains groups of ids which are meant to be merged in one 
// This code generates an auxiliary table which generates
db.candidates.find({}, {'ids': 1}).forEach(function(rec) {
    var ids = rec["ids"].sort().reverse();
    var h = ids[0];
    ids.slice(1).forEach(function(el) {
        print("INSERT into merges_results values ('"+h+"', '"+el+"');")
    });
});
