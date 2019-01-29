exports = function(changeEvent) {
    // Test by 
    // exports({ 'fullDocument'  : {  '_id' : 1, 'message' : 'hello'} })
  
    // Access the latest version of the changed document (with Full Document enabled for Insert, Update, and Replace operations):
    var doc = changeEvent.fullDocument;
    var id_str = EJSON.stringify(doc._id);
    id_obj = JSON.parse(id_str);
    doc.mongodb_id = id_obj['$oid'];
    delete doc._id;
    console.log("Got message " + JSON.stringify(doc._id));

    // Log via slack 
    var elastic = context.services.get("elastic");
		var elastic_data = doc;
		var elastic_headers = {"Content-Type": ["application/json"]}; 

    var args = 
    {
        url: context.values.get("elastic-url") + doc.mongodb_id,
        headers: elastic_headers,
        body : JSON.stringify(elastic_data)
    };

    console.log("Sending doc to url " + args.url);
    elastic.post(args);
  
};