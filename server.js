var Db = require('mongodb').Db,
    MongoClient = require('mongodb').MongoClient,
    Server = require('mongodb').Server,
    ReplSetServers = require('mongodb').ReplSetServers,
    ObjectID = require('mongodb').ObjectID,
    Binary = require('mongodb').Binary,
    GridStore = require('mongodb').GridStore,
    Grid = require('mongodb').Grid,
    Code = require('mongodb').Code;
var fs = require('fs');    
var uuidv4 = require('uuid/v4');
var Promise = require('rsvp').Promise;
var createdCtr = 0;
var MongoClient = require('mongodb').MongoClient;
var url = "mongodb://localhost:27017";
var collectionNames = ["association", "audio", "authority", "customer", "event", "pivot", "rule", "setting", "tag", "user"];

//***********************************

function deleteFile(outFile) {
  if (fs.existsSync(outFile)) {
    fs.unlink(outFile, (err) => {
      if (err) throw err;
      console.log('successfully deleted ' + outFile);
    });
  }
}

function writeFile(outFile, data) {
    fs.writeFile(outFile, JSON.stringify(data), 'utf8', function(err) {
    if(err) {
      console.log('error ' + err);
    }
  });
}

function appendFile(outFile, data) {
  fs.appendFile(outFile, data, function(err) {
    if(err){
      console.log("Error Appending FIle: " + err);
    }
  });
}

function mongoInsert(obj) {
  console.log('mongoInsert');
  MongoClient.connect(url, {native_parser:true, useUnifiedTopology: true}, function(err, db) {
    if (err) throw err;
    var dbo = db.db("local");
    dbo.collection("pivot").insertMany(obj, function(err, res) {
      if (err) throw err;
      console.log("Number of documents inserted: " + res.insertedCount);
      db.close();
    });
  });
}

function queryPivot(fieldName, objectName) {
    return new Promise((resolve, reject) => {
     MongoClient.connect(url, {native_parser:true, useUnifiedTopology: true}, function(err, client) {
       const db = client.db('local');
      
       //Step 1: declare promise      
       var myPromise = () => {
         return new Promise((resolve, reject) => {
            db
             .collection('pivot')
             .find({[fieldName]: objectName})
             .toArray(function(err, data) {
                 err 
                    ? reject(err) 
                    : resolve(data[0]);
               });
         });
       };

       //Step 2: async promise handler
       var callMyPromise = async () => {          
          var result = await (myPromise());
          //anything here is executed after result is resolved
          return result;
       };
 
       //Step 3: make the call
       callMyPromise().then(function(result) {
          client.close();
          resolve(result);
       });
    }); //end mongo client

   }); // Promise

}

function queryPivots(fieldName1, objectName1, fieldName2, objectName2) {
    return new Promise((resolve, reject) => {
     MongoClient.connect(url, {native_parser:true, useUnifiedTopology: true}, function(err, client) {
       const db = client.db('local');
      
       //Step 1: declare promise      
       var myPromise = () => {
         return new Promise((resolve, reject) => {
            db
             .collection('pivot')
             .find({[fieldName]: objectName, [fieldName2]: objectName2})
             .toArray(function(err, data) {
                 err 
                    ? reject(err) 
                    : resolve(data[0]);
               });
         });
       };

       //Step 2: async promise handler
       var callMyPromise = async () => {          
          var result = await (myPromise());
          //anything here is executed after result is resolved
          return result;
       };
 
       //Step 3: make the call
       callMyPromise().then(function(result) {
          client.close();
          resolve(result);
       });
    }); //end mongo client

   }); // Promise

}

function dedupe(ary, field){
  ary.sort((a,b) => ( String(a[field]).toLowerCase() > String(b[field]).toLowerCase() ) ? 1 : ( ( String(b[field]).toLowerCase() > String(a[field]).toLowerCase() ) ? -1 : 0));
  var result = [];
  var hold = ary[0];
  for (var i = 0; i < ary.length; i++) {
    if( String(ary[i][field]).toLowerCase() != String(hold[field]).toLowerCase() ) {
      result.push(hold);
      hold = ary[i];
    }
  }  
  result.push(hold);
  return result;
}


//***********************************


// locationRegions();
// locationCountries();
// locationStates();

// mongoCreateCollections();
// locationRegionInserts();
// locationCountryInserts();
locationStateInserts();


//***********************************



function locationRegionInserts() {
  var obj = JSON.parse(fs.readFileSync('/Users/ricksteinberger/Home/git/mongo-data/location-regions.json', 'utf8'));
  mongoInsert(obj);
}

function locationCountryInserts() {
  var obj = JSON.parse(fs.readFileSync('/Users/ricksteinberger/Home/git/mongo-data/location-countries.json', 'utf8'));
  mongoInsert(obj);
}

function locationStateInserts() {
  var obj = JSON.parse(fs.readFileSync('/Users/ricksteinberger/Home/git/mongo-data/location-states.json', 'utf8'));
  mongoInsert(obj);
}


//***********************************


function locationRegions() {
  var locations = JSON.parse(fs.readFileSync('/Users/ricksteinberger/Home/git/mongo-data/locations.json', 'utf8'));
  var outFile = '/Users/ricksteinberger/Home/git/mongo-data/location-regions.json';
  deleteFile(outFile);
  locations = dedupe(locations, "region");

  var results = [];
  locations.forEach((element, index) => { 
    var obj = {"_id": uuidv4(), "_class": "region", "name": element.region };
    results.push(obj);
  });
  writeFile(outFile, results);
}


function locationCountries() {
  var locations = JSON.parse(fs.readFileSync('/Users/ricksteinberger/Home/git/mongo-data/locations.json', 'utf8'));
  var locationRegions = JSON.parse(fs.readFileSync('/Users/ricksteinberger/Home/git/mongo-data/location-regions.json', 'utf8'));
  var outFile = '/Users/ricksteinberger/Home/git/mongo-data/location-countries.json';
  deleteFile(outFile);
  locations = dedupe(locations, "country");

  var result = [];
  locations.forEach((element, index) => { 
    var obj = {"_id": uuidv4(), "_class": "country", "name": element.country };

      locationRegions.forEach((element1, index) => { 
        if( element.region == element1.name ) {
          obj.region_id = element1._id;
          obj.region_name = element1.name;
        }
      });


    if(obj.name != "") {
      result.push(obj);      
    }

  });
  writeFile(outFile, result);
}


function locationStates() {
  var locations = JSON.parse(fs.readFileSync('/Users/ricksteinberger/Home/git/mongo-data/locations.json', 'utf8'));
  var locationCountries = JSON.parse(fs.readFileSync('/Users/ricksteinberger/Home/git/mongo-data/location-countries.json', 'utf8'));
  var outFile = '/Users/ricksteinberger/Home/git/mongo-data/location-states.json';
  deleteFile(outFile);
  locations = dedupe(locations, "state");
  var result = [];
  locations.forEach((element, index) => { 

    var temp = element.state;
    if(temp) {
      temp = temp.replace("'", "");
      temp = temp.replace("`", "");
      temp = temp.replace(" ", "");      
    }

    var obj = {"_id": uuidv4(), "_class": "province", "name": temp };

      locationCountries.forEach((element1, index) => { 
        if( element.country == element1.name ) {

          obj.region_id = element1.region_id;
          obj.region_name = element1.region_name;

          obj.country_id = element1._id;
          obj.country_name = element1.name;


        }
      });



    if(obj.name != "") {
      result.push(obj);      
    }

  });
  writeFile(outFile, result);
}

//***********************************

function mongoCreateCollections() {

  MongoClient.connect(url, {native_parser:true, useUnifiedTopology: true}, function(err, db) {
    if (err) throw err;

    console.log("Creating collections");

    var dbo = db.db("local");
    collectionNames.forEach(name => { 
      dropCollection(db, dbo, name);
    });

  });

}

function dropCollection(db, dbo, collectionName) {
  dbo.collection(collectionName).drop(function(err, success) {
    if (err) {
      console.log("ERROR: " + err);
      createCollection(db, dbo, collectionName);
    };
    if (success) {
      console.log("Collection deleted: " + collectionName);
      createCollection(db, dbo, collectionName);
    }

  });
}

function createCollection(db, dbo, collectionName) {
  dbo.createCollection(collectionName, function(err, success) {
    if (err) {
      console.log("ERROR: " + err);
    };
    if (success) {
      console.log("Collection created: " + collectionName);
      createdCtr++;
      if(createdCtr == collectionNames.length) {
        db.close();
        console.log("Database closed");
      }
    }

  });
}

//***********************************


