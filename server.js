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

// mongoCreateCollections();
// pivotRegion();
pivotCountry();


function deleteFile(outFile) {
  fs.unlink(outFile, (err) => {
  if (err) throw err;
  console.log('successfully deleted ' + outFile);
});
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

//***********************************

function pivotCountry() {

  var outFile = '/Users/ricksteinberger/Home/git/mongo-data/pivot-country.json';
  var countryList = JSON.parse(fs.readFileSync('/Users/ricksteinberger/Home/git/mongo-data/countries.json', 'utf8'));
  var countryRegions = JSON.parse(fs.readFileSync('/Users/ricksteinberger/Home/git/mongo-data/country-region.json', 'utf8'));
  var count = countryRegions.length;
  countryRegions.sort((a,b) => (a.country > b.country) ? 1 : ((b.country > a.country) ? -1 : 0)); 

  deleteFile(outFile);
  setTimeout(function(){ console.log('continue'); }, 2000);
  appendFile(outFile, "[");

  countryRegions.forEach(element => { 

    var country = {};
    var holdCountry = "";

    if(element.country !== holdCountry){

      queryPivot("name", element.region).then(function(pivot) {
        // console.log(element.region);
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
        if(count == 0) {
          appendFile(outFile, JSON.stringify(country) + "]" );
          setTimeout(function(){ 
              var obj = JSON.parse(fs.readFileSync(file, 'utf8'));
              mongoInsert(obj);
          }, 2000);
        }else{
          appendFile(outFile, JSON.stringify(country) + "," );          
        }

      }, function(err) {
        console.error('Error Getting Pivot: ', err);
      });

      hold = element.country;

    } // if 

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

function getCountryFromList(name, countryList) {
  var result = {};
  countryList.forEach(element => { 
    if(element.name === name) {
      result = element;

    }
  });
  return result;
}

// function insertCountries(file) {
//   var obj = JSON.parse(fs.readFileSync(file, 'utf8'));
//   mongoInsert(obj);
// }

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

  // console.log(regions); 
  writeFile(outFile, regions);

  // fs.writeFile('/Users/ricksteinberger/Home/git/mongo-data/pivot-region.json', JSON.stringify(regions), 'utf8', function(err) {
  //   if(err) {
  //     console.log('error ' + err);
  //   }
  //   console.log('Done');
  // });

  mongoInsert(regions);

}





//***********************************

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


