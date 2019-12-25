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

function airportsToList() {
  var outFile = '/Users/ricksteinberger/Home/git/mongo-data/airports-list.json';
  var airports = JSON.parse(fs.readFileSync('/Users/ricksteinberger/Home/git/mongo-data/airports.json', 'utf8'));
  var airportsList = [];
  for(var key in airports) {
      if(airports.hasOwnProperty(key)) {
        airportsList.push(airports[key]);
      }
  }
  writeFile(outFile, airportsList);
}

function airportsListToProvinceList() {  
  var outFile = '/Users/ricksteinberger/Home/git/mongo-data/airports-province-list.json';
  var airportsList = JSON.parse(fs.readFileSync('/Users/ricksteinberger/Home/git/mongo-data/airports-list.json', 'utf8'));
  airportsList.sort((a,b) => (a.state > b.state) ? 1 : ((b.state > a.state) ? -1 : 0));

  deleteFile(outFile);
  setTimeout(function(){ console.log('continue'); }, 2000);

  var provinceList = [];
  var holdState = "";
  airportsList.forEach((element, index) => { 
    if(element.state != holdState){
      provinceList.push(element);
      holdState = element.state;
    } 
  });

  console.log(provinceList.length);
  writeFile(outFile, provinceList);

}

function locationRegion() {

  var outFile = '/Users/ricksteinberger/Home/git/mongo-data/locations-regions.json';
  var locations = JSON.parse(fs.readFileSync('/Users/ricksteinberger/Home/git/mongo-data/locations.json', 'utf8'));
  var countryRegions = JSON.parse(fs.readFileSync('/Users/ricksteinberger/Home/git/mongo-data/country-region.json', 'utf8'));

  var count = locations.length;
  countryRegions.sort((a,b) => (a.country > b.country) ? 1 : ((b.country > a.country) ? -1 : 0)); 

  deleteFile(outFile);

  locations.forEach(element => { 

    countryRegions.forEach(cr => { 
      if(cr.country == element.country) {
        element.region = cr.region;
      }
    }); 

  }); 

  writeFile(outFile, locations);

}

function locationCountryCode() {

  var outFile = '/Users/ricksteinberger/Home/git/mongo-data/locations-regions-cc.json';
  var locations = JSON.parse(fs.readFileSync('/Users/ricksteinberger/Home/git/mongo-data/locations-regions.json', 'utf8'));
  var countries = JSON.parse(fs.readFileSync('/Users/ricksteinberger/Home/git/mongo-data/countries.json', 'utf8'));

  var count = locations.length;

  deleteFile(outFile);

  locations.forEach(element => { 

    countries.forEach(cc => { 
      if(cc.name == element.country) {
        element.country_code = cc.country_code;
      }
    }); 

  }); 

  writeFile(outFile, locations);

}

function location


//***********************************

// airportsToList();
// airportsListToProvinceList();
// locationRegion();
// locationCountryCode();

// mongoCreateCollections();
// pivotRegion();
pivotCountry();
// pivotProvince();
// pivotLocation();


//***********************************

function pivotLocation() {

  var outFile = '/Users/ricksteinberger/Home/git/mongo-data/pivot-location.json';
  var airportsList = JSON.parse(fs.readFileSync('/Users/ricksteinberger/Home/git/mongo-data/airports-list.json', 'utf8'));

  var count = airportsList.length;
  console.log(count);

  deleteFile(outFile);
  appendFile(outFile, "[");

    var ctr = 0;

  airportsList.forEach((element, index) => { 

      var province = {};


      if(element.state == "Alaska") {
        ctr++;
        console.log('HIT ' + ctr + " " + element.city);


    // queryPivot("countrycode", element.country).then(function(pivot) {

    //   var countryId = pivot._id;
    //   console.log(pivot);

    //   // queryPivots("countryId", countryId, "_class", "province").then(function(pivot) {

    //   //   console.log(JSON.stringify(pivot));

    //   // }, function(err) {
    //   //   console.error('Error Getting Pivot: ', err);
    //   // });



    // }, function(err) {
    //   console.error('Error Getting Pivot: ', err);
    // });

    } // if


  });





}

//***********************************

function pivotProvince() {

  var outFile = '/Users/ricksteinberger/Home/git/mongo-data/pivot-province.json';
  var provinceList = JSON.parse(fs.readFileSync('/Users/ricksteinberger/Home/git/mongo-data/airports-province-list.json', 'utf8'));
  // provinceList.sort((a,b) => (a.state > b.state) ? 1 : ((b.state > a.state) ? -1 : 0));

  // console.log(provinceList.length);

  var shortList = [];
  for (var i = 0; i < provinceList.length; i++) {
    // if( i < 50) {
      shortList.push( provinceList[i] );      
    // }

  }
  provinceList = shortList;

  var count = provinceList.length;
  console.log(count);

  deleteFile(outFile);
  appendFile(outFile, "[");

  provinceList.forEach((element, index) => { 

    var province = {};

    queryPivot("countrycode", element.country).then(function(pivot) {

      if(pivot) {

        console.log(pivot.name);

        province = {"_class": "province"};
        province.regionId = pivot.regionId;
        province.countryId = pivot._id;
        province.name = element.state;


        count--;
        if(count == 0) {
          appendFile(outFile, JSON.stringify(province) + "]" );
          setTimeout(function(){
              console.log('Writing1...'); 
              var obj = JSON.parse(fs.readFileSync(outFile, 'utf8'));
              console.log(obj.length);
              mongoInsert(obj);
          }, 2000);
        } else {
          if(province.name){
            appendFile(outFile, JSON.stringify(province) + "," );          
          }

        }

      } else {
        count--;
        if(count == 0) {
          appendFile(outFile, JSON.stringify(province) + "]" );
          setTimeout(function(){
              console.log('Writing2...'); 
              var obj = JSON.parse(fs.readFileSync(outFile, 'utf8'));
              console.log(obj.length);
              mongoInsert(obj);
          }, 2000);
        }
      } 


    }, function(err) {
      console.error('Error Getting Pivot: ', err);
    });
 
  }); // forEach 
}

//***********************************


function pivotCountry() {

  var outFile = '/Users/ricksteinberger/Home/git/mongo-data/pivot-country.json';
  var countryList = JSON.parse(fs.readFileSync('/Users/ricksteinberger/Home/git/mongo-data/countries.json', 'utf8'));
  var locations = JSON.parse(fs.readFileSync('/Users/ricksteinberger/Home/git/mongo-data/locations-regions-cc.json', 'utf8'));
  var count = locations.length;
  locations.sort((a,b) => (a.country > b.country) ? 1 : ((b.country > a.country) ? -1 : 0)); 

  console.log(count);

  deleteFile(outFile);
  appendFile(outFile, "[");

  var holdCountry = "";
  locations.forEach(element => { 

    var country = {};

    if(element.country !== holdCountry){

      queryPivot("name", element.region).then(function(pivot) {

        if(pivot) {

          console.log(element.region);
          // console.log(JSON.stringify(pivot));
          country = {"_class": "country"};
          country.regionId = pivot._id;
          country.name = element.country;

          countryListItem = getCountryFromList(element.country, countryList);
          country.countrycode = countryListItem.country_code;
          if (countryListItem.latlng) country.latitude = JSON.stringify( countryListItem.latlng[0] );
          if (countryListItem.latlng) country.longitude = JSON.stringify( countryListItem.latlng[1] );
          country.timezones = countryListItem.timezones;      

          count--;
          console.log(count);
          if(count == 0) {
            appendFile(outFile, JSON.stringify(country) + "]" );
            setTimeout(function(){ 
                var obj = JSON.parse(fs.readFileSync(outFile, 'utf8'));
                mongoInsert(obj);
            }, 2000);
          }else{
            appendFile(outFile, JSON.stringify(country) + "," );          
          }



        } else {
          count--;
          if(count == 0) {
            appendFile(outFile, JSON.stringify(country) + "]" );
            setTimeout(function(){ 
                var obj = JSON.parse(fs.readFileSync(outFile, 'utf8'));
                mongoInsert(obj);
            }, 2000);
          }

        } // if pivot





      }, function(err) {
        console.error('Error Getting Pivot: ', err);
      });

      holdCountry = element.country;

    } // if 

  }); 

}


// function pivotCountry() {

//   var outFile = '/Users/ricksteinberger/Home/git/mongo-data/pivot-country.json';
//   var countryList = JSON.parse(fs.readFileSync('/Users/ricksteinberger/Home/git/mongo-data/countries.json', 'utf8'));
//   var countryRegions = JSON.parse(fs.readFileSync('/Users/ricksteinberger/Home/git/mongo-data/country-region.json', 'utf8'));
//   var count = countryRegions.length;
//   countryRegions.sort((a,b) => (a.country > b.country) ? 1 : ((b.country > a.country) ? -1 : 0)); 

//   deleteFile(outFile);
//   setTimeout(function(){ console.log('continue'); }, 2000);
//   appendFile(outFile, "[");

//   var holdCountry = "";
//   countryRegions.forEach(element => { 

//     var country = {};

//     if(element.country !== holdCountry){

//       queryPivot("name", element.region).then(function(pivot) {
//         // console.log(element.region);
//         // console.log(JSON.stringify(pivot));
//         country = {"_class": "country"};
//         country.regionId = pivot._id;
//         country.name = element.country;

//         countryListItem = getCountryFromList(element.country, countryList);
//         country.countrycode = countryListItem.country_code;
//         if (countryListItem.latlng) country.latitude = JSON.stringify( countryListItem.latlng[0] );
//         if (countryListItem.latlng) country.longitude = JSON.stringify( countryListItem.latlng[1] );
//         country.timezones = countryListItem.timezones;      

//         count--;
//         if(count == 0) {
//           appendFile(outFile, JSON.stringify(country) + "]" );
//           setTimeout(function(){ 
//               var obj = JSON.parse(fs.readFileSync(outFile, 'utf8'));
//               mongoInsert(obj);
//           }, 2000);
//         }else{
//           appendFile(outFile, JSON.stringify(country) + "," );          
//         }

//       }, function(err) {
//         console.error('Error Getting Pivot: ', err);
//       });

//       holdCountry = element.country;

//     } // if 

//   }); 

// }



function getCountryFromList(name, countryList) {
  var result = {};
  countryList.forEach(element => { 
    if(element.name === name) {
      result = element;

    }
  });
  return result;
}


//***********************************

function pivotRegion() {
  var outFile = '/Users/ricksteinberger/Home/git/mongo-data/pivot-region.json';
  var obj = JSON.parse(fs.readFileSync('/Users/ricksteinberger/Home/git/mongo-data/country-region.json', 'utf8'));
  obj.sort((a,b) => (a.region > b.region) ? 1 : ((b.region > a.region) ? -1 : 0)); 
  var regions = [];

  var region1 = {"_class": "region", "name": "", "longitude": -77.3619, "latitude": 38.9440};;
  var hold = obj[0].region;
  region1.name = hold;

  regions.push(region1);

  obj.forEach(element => { 
    var region = {"_class": "region", "name": "", "longitude": -77.3619, "latitude": 38.9440};;
    if(element.region === hold){
      // na
    } else {
      hold = element.region;
      console.log(JSON.stringify(hold));
      region.name = hold
      regions.push(region);


    } 
  }); 

  writeFile(outFile, regions);
  mongoInsert(regions);

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


