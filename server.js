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

// mongoCreateCollections();

// locationRegions();
// locationCountries();
// locationStates();
// locationLocations();
// locationDulles();

// locationRegionInserts();
// locationCountryInserts();
// locationStateInserts();
// locationLocationInserts();
locationDullesInserts();



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

function locationLocationInserts() {
  var obj = JSON.parse(fs.readFileSync('/Users/ricksteinberger/Home/git/mongo-data/location-locations.json', 'utf8'));
  mongoInsert(obj);
}

function locationDullesInserts() {
  var obj = JSON.parse(fs.readFileSync('/Users/ricksteinberger/Home/git/mongo-data/location-dulles.json', 'utf8'));
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
    var obj = {"_id": uuidv4(), "_class": "region", "name": element.region, "has_active": false };
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
    var obj = {"_id": uuidv4(), "_class": "country", "name": element.country, "has_active": false };

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

    var obj = {"_id": uuidv4(), "_class": "province", "name": temp, "has_active": false };

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

function locationLocations() {
  var locationStates = JSON.parse(fs.readFileSync('/Users/ricksteinberger/Home/git/mongo-data/location-states.json', 'utf8'));  
  var locations = JSON.parse(fs.readFileSync('/Users/ricksteinberger/Home/git/mongo-data/locations.json', 'utf8'));  
  var outFile = '/Users/ricksteinberger/Home/git/mongo-data/location-locations.json';
  deleteFile(outFile);

  var results = [];
  var BreakException = {};
  var ctr = 0;

  locations.forEach((element, index) => { 

    try {
      locationStates.forEach((element1, index1) => { 
        var location = {};
        ctr++;
        if( element.state == element1.name ) {
          location._id = uuidv4();
          location._class = "location";
          location.name = element.name;
          location.location_code = element.code;
          location.latitude = element.lat;
          location.longitude = element.lon;
          location.city_name = element.city;
          location.province_name = element.state;
          location.country_name = element.country;
          location.country_code = element.country_code;
          location.region_name = element.region;
          location.region_id = element1.region_id;
          location.country_id = element1.country_id;
          location.province_id = element1._id;
          location.has_active = false;
          results.push(location);
          throw BreakException;
        }
      });

    } catch( e ) {
      console.log('break after ' + ctr);
      ctr = 0;
    }

  });
  writeFile(outFile, results);
}



function locationDulles() {
  var locations = JSON.parse(fs.readFileSync('/Users/ricksteinberger/Home/git/mongo-data/location-locations.json', 'utf8'));  
  var outFile = '/Users/ricksteinberger/Home/git/mongo-data/location-dulles.json';
  deleteFile(outFile);

  var results = [];
  var BreakException = {};
  var ctr = 0;

  var counters = {"area_number":1000, "cell_number":2000, "sensor_number":3000};

  locations.forEach((element, index) => { 

    // try {

        ctr++;


        if( element.location_code == "IAD" ) {

          var site = {};
          site._class = "site";
          site.region_id = element.region_id;
          site.country_id = element.country_id;
          site.province_id = element.province_id;
          site.location_id = element._id;
          site.has_active = false;

          site._id = uuidv4();
          site.latitude = element.lat;
          site.longitude = element.lon;
          site.name = "Main Terminal";
          results.push(site);


              var area = {};
              area._class = "area";
              area.site_id = site._id;          
              area.region_id = site.region_id;
              area.country_id = site.country_id;
              area.province_id = site.province_id;
              area.location_id = site.location_id;
              area.latitude = site.latitude;
              area.longitude = site.longitude;
              area.has_active = false;

              area._id = uuidv4();
              area.number = counters.area_number++;
              area.name = "Main Terminal North Level 1";
              results.push(area);
              results = getCells(area, results, counters);

              var area = {};
              area._class = "area";
              area.site_id = site._id;          
              area.region_id = site.region_id;
              area.country_id = site.country_id;
              area.province_id = site.province_id;
              area.location_id = site.location_id;
              area.latitude = site.latitude;
              area.longitude = site.longitude;
              area.has_active = false;

              area._id = uuidv4();
              area.number = counters.area_number++;
              area.name = "Main Terminal South Level 1";
              results.push(area);
              results = getCells(area, results, counters);

              var area = {};
              area._class = "area";
              area.site_id = site._id;          
              area.region_id = site.region_id;
              area.country_id = site.country_id;
              area.province_id = site.province_id;
              area.location_id = site.location_id;
              area.latitude = site.latitude;
              area.longitude = site.longitude;
              area.has_active = false;

              area._id = uuidv4();
              area.number = counters.area_number++;
              area.name = "Main Terminal South Level 2";
              results.push(area);
              results = getCells(area, results, counters);

          var site = {};
          site._class = "site";
          site.region_id = element.region_id;
          site.country_id = element.country_id;
          site.province_id = element.province_id;
          site.location_id = element._id;
          site.has_active = false;

          site._id = uuidv4();
          site.latitude = element.lat;
          site.longitude = element.lon;
          site.name = "Concourse A";
          results.push(site);


              var area = {};
              area._class = "area";
              area.site_id = site._id;          
              area.region_id = site.region_id;
              area.country_id = site.country_id;
              area.province_id = site.province_id;
              area.location_id = site.location_id;
              area.latitude = site.latitude;
              area.longitude = site.longitude;
              area.has_active = false;

              area._id = uuidv4();
              area.number = counters.area_number++;
              area.name = "Concourse A North";
              results.push(area);
              results = getCells(area, results, counters);

              var area = {};
              area._class = "area";
              area.site_id = site._id;          
              area.region_id = site.region_id;
              area.country_id = site.country_id;
              area.province_id = site.province_id;
              area.location_id = site.location_id;
              area.latitude = site.latitude;
              area.longitude = site.longitude;
              area.has_active = false;

              area._id = uuidv4();
              area.number = counters.area_number++;
              area.name = "Concourse A South";
              results.push(area);
              results = getCells(area, results, counters);

          var site = {};
          site._class = "site";
          site.region_id = element.region_id;
          site.country_id = element.country_id;
          site.province_id = element.province_id;
          site.location_id = element._id;
          site.has_active = false;

          site._id = uuidv4();
          site.latitude = element.lat;
          site.longitude = element.lon;
          site.name = "Concourse B";
          results.push(site);

              var area = {};
              area._class = "area";
              area.site_id = site._id;          
              area.region_id = site.region_id;
              area.country_id = site.country_id;
              area.province_id = site.province_id;
              area.location_id = site.location_id;
              area.latitude = site.latitude;
              area.longitude = site.longitude;
              area.has_active = false;

              area._id = uuidv4();
              area.number = counters.area_number++;
              area.name = "Crew Lounge";
              results.push(area);
              results = getCells(area, results, counters);

              var area = {};
              area._class = "area";
              area.site_id = site._id;          
              area.region_id = site.region_id;
              area.country_id = site.country_id;
              area.province_id = site.province_id;
              area.location_id = site.location_id;
              area.latitude = site.latitude;
              area.longitude = site.longitude;
              area.has_active = false;

              area._id = uuidv4();
              area.number = counters.area_number++;
              area.name = "Concourse B West";
              results.push(area);
              results = getCells(area, results, counters);

              var area = {};
              area._class = "area";
              area.site_id = site._id;          
              area.region_id = site.region_id;
              area.country_id = site.country_id;
              area.province_id = site.province_id;
              area.location_id = site.location_id;
              area.latitude = site.latitude;
              area.longitude = site.longitude;
              area.has_active = false;

              area._id = uuidv4();
              area.number = counters.area_number++;
              area.name = "Concourse B South";
              results.push(area);
              results = getCells(area, results, counters);

          var site = {};
          site._class = "site";
          site.region_id = element.region_id;
          site.country_id = element.country_id;
          site.province_id = element.province_id;
          site.location_id = element._id;
          site.has_active = false;

          site._id = uuidv4();
          site.latitude = element.lat;
          site.longitude = element.lon;
          site.name = "Concourse C";
          results.push(site);

              var area = {};
              area._class = "area";
              area.site_id = site._id;          
              area.region_id = site.region_id;
              area.country_id = site.country_id;
              area.province_id = site.province_id;
              area.location_id = site.location_id;
              area.latitude = site.latitude;
              area.longitude = site.longitude;
              area.has_active = false;

              area._id = uuidv4();
              area.number = counters.area_number++;
              area.name = "Concourse C North";
              results.push(area);
              results = getCells(area, results, counters);

              var area = {};
              area._class = "area";
              area.site_id = site._id;          
              area.region_id = site.region_id;
              area.country_id = site.country_id;
              area.province_id = site.province_id;
              area.location_id = site.location_id;
              area.latitude = site.latitude;
              area.longitude = site.longitude;
              area.has_active = false;

              area._id = uuidv4();
              area.number = counters.area_number++;
              area.name = "Concourse C West";
              results.push(area);
              results = getCells(area, results, counters);

              var area = {};
              area._class = "area";
              area.site_id = site._id;          
              area.region_id = site.region_id;
              area.country_id = site.country_id;
              area.province_id = site.province_id;
              area.location_id = site.location_id;
              area.latitude = site.latitude;
              area.longitude = site.longitude;
              area.has_active = false;

              area._id = uuidv4();
              area.number = counters.area_number++;
              area.name = "Secondary Jetway";
              results.push(area);
              results = getCells(area, results, counters);

          var site = {};
          site._class = "site";
          site.region_id = element.region_id;
          site.country_id = element.country_id;
          site.province_id = element.province_id;
          site.location_id = element._id;
          site.has_active = false;

          site._id = uuidv4();
          site.latitude = element.lat;
          site.longitude = element.lon;
          site.name = "Concourse D";
          results.push(site);

              var area = {};
              area._class = "area";
              area.site_id = site._id;          
              area.region_id = site.region_id;
              area.country_id = site.country_id;
              area.province_id = site.province_id;
              area.location_id = site.location_id;
              area.latitude = site.latitude;
              area.longitude = site.longitude;
              area.has_active = false;

              area._id = uuidv4();
              area.number = counters.area_number++;
              area.name = "Concourse D Level 1";
              results.push(area);
              results = getCells(area, results, counters);

              var area = {};
              area._class = "area";
              area.site_id = site._id;          
              area.region_id = site.region_id;
              area.country_id = site.country_id;
              area.province_id = site.province_id;
              area.location_id = site.location_id;
              area.latitude = site.latitude;
              area.longitude = site.longitude;
              area.has_active = false;

              area._id = uuidv4();
              area.number = counters.area_number++;
              area.name = "Concourse D Level 2";
              results.push(area);
              results = getCells(area, results, counters);

              var area = {};
              area._class = "area";
              area.site_id = site._id;          
              area.region_id = site.region_id;
              area.country_id = site.country_id;
              area.province_id = site.province_id;
              area.location_id = site.location_id;
              area.latitude = site.latitude;
              area.longitude = site.longitude;
              area.has_active = false;
              
              area._id = uuidv4();
              area.number = counters.area_number++;
              area.name = "Concourse D Level 3";
              results.push(area);
              results = getCells(area, results, counters);


          // throw BreakException;
        }

    // } catch( e ) {
    //   console.log('break after ' + ctr + " " + e);
    //   ctr = 0;
    // }

  });
  console.log('write');
  writeFile(outFile, results);
}

function getCells(area, results, counters) {

  for (var i = 0; i < 3; i++) {
    var cell = {};
    cell._class = "cell";
    cell._id = uuidv4();
    cell.number = counters.cell_number++;
    cell.area_id = area._id;          
    cell.site_id = area.site_id;          
    cell.region_id = area.region_id;
    cell.country_id = area.country_id;
    cell.province_id = area.province_id;
    cell.location_id = area.location_id;
    cell.has_active = false;
    cell.name = area.number + "." + cell.number;
    results.push(cell);
    results = getSensors(area, cell, results, counters);
  }
  return results;
}

function getSensors(area, cell, results, counters) {

  for (var i = 0; i < 3; i++) {
    var sensor = {};
    sensor._class = "sensor";
    sensor._id = uuidv4();
    sensor.number = counters.sensor_number++;
    sensor.cell_id = cell._id;
    sensor.area_id = cell.area_id;          
    sensor.site_id = cell.site_id;          
    sensor.region_id = cell.region_id;
    sensor.country_id = cell.country_id;
    sensor.province_id = cell.province_id;
    sensor.location_id = cell.location_id;
    sensor.has_active = false;
    sensor.name = area.number + "." + cell.number + "." + sensor.number;
    results.push(sensor);
  }
  return results;


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


