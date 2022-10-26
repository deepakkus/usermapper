const AWS = require('aws-sdk');
const documentClient = new AWS.DynamoDB.DocumentClient();
const axios = require('axios');
const turf = require('@turf/turf');
const MongoClient = require('mongodb').MongoClient;

const updateMapperDB = async (userId, farm) => {
  const params = {
    TableName: 'user-farm-devices',
    Key: {
        'userId': userId,
        'farmId': farm.farmId.toString()
    },
    UpdateExpression: 'SET devices=:devices, #loc=:l, soilType=:s, terrainType=:t, waterSource=:w',
    ExpressionAttributeNames: {
      '#loc': 'location'
    },
    ExpressionAttributeValues: {
        ':devices': farm.devices,
        ':l': farm.location,
        ':s': farm.soilType,
        ':t': farm.terrainType,
        ':w': farm.waterSource,
    }
  };          
  const res = await documentClient.update(params).promise();
  return res;
}

const isInsideFarm = (farmLoc, deviceLoc) => {
  const pt = turf.point(deviceLoc);
  const poly = turf.polygon([farmLoc]);  
  return turf.booleanPointInPolygon(pt, poly);
}

const getIotRecords = async (deviceIds) => {
  try {
    return await axios.post('https://api.sensegrass.com/devices/records', {ids: deviceIds})
  } catch (error) {
    console.error(error);
    throw error;
  }
}

const getDBData = async (db, collection, filter) => {
  return await db
  .collection(collection)
  .find(filter).toArray();  
}

exports.handler = async (event, context, callback) => {  
  const userId = (event.pathParameters || {}).userId;
  if(!userId){
    console.log('No userId specififed, exit...')
    const response = {
      statusCode: 200,
      body: JSON.stringify({
        message: 'There is no user id specified, please specify one',
      }),
    };
    callback(null, response);
    return;
  }
  const client = new MongoClient(process.env.MONGO_URI || 'mongodb://localhost:27017/SGCropMgtDB', {
    useNewUrlParser: true,
    useUnifiedTopology: true
  });
  try {
    await client.connect(); 
    const db = client.db('SGCropMgtDB');  
    const devices = await getDBData(db, 'userdevices', { userId });
    const deviceTypes = await getDBData(db, 'devicetypes', { });
    const soilTypes = await getDBData(db, 'soiltypes', { });
    const terrainTypes = await getDBData(db, 'terraintypes', { });
    const waterSources = await getDBData(db, 'watersources', { });
    const sensors = devices.filter(d => d.deviceTypeId.toString() === (deviceTypes.find(device => device.name === 'Soil Sensor')._id).toString());    
    const farmsdb = await getDBData(db, 'userfarms', { userId });   
    
    const farms = farmsdb.map(f => {
      const soilType = soilTypes.find(st => st._id.toString() === f.soilTypeId.toString()).name;
      const terrainType = terrainTypes.find(tt => tt._id.toString() === f.terrainTypeId.toString()).name;
      const waterSource = waterSources.find(ws => ws._id.toString() === f.waterSourceId.toString()).name;
      return {...f, soilType, terrainType, waterSource};
    }); 
    const deviceIds = sensors.length ? sensors.map(device => device.deviceId).join() : '';    
    const deviceRecords =  deviceIds ? await getIotRecords(deviceIds) : null;
    
    const records = deviceRecords ? deviceRecords.data : [];
    const mappedFarms = [];
    for (const rec of records) {      
      const deviceLoc = [Number(rec.location.latitude), Number(rec.location.longitude)];
      for (const farm of farms) {
        if (isInsideFarm(farm.location, deviceLoc)) {
          if (!mappedFarms.length) {
            mappedFarms.push({
              farmId: farm._id,
              devices: [rec.deviceId],
              location: farm.location,
              soilType: farm.soilType,
              terrainType: farm.terrainType,
              waterSource: farm.waterSource
            });
          } else {
            const farmMapped = mappedFarms.find(f => f.farmId.toString() === farm._id.toString());
            if (farmMapped) {
              const devices = (farmMapped.devices || []).push(rec.deviceId);
              farmMapped.devices = devices;
            } else {
              mappedFarms.push({
                farmId: farm._id,
                devices: [rec.deviceId],
                location: farm.location,
                soilType: farm.soilType,
                terrainType: farm.terrainType,
                waterSource: farm.waterSource
              });
            }
          }          
        }
      }
    } 
    // for a farm that does not have device yet
    for (const farm of farms) {
      if (!mappedFarms.find(f => f.farmId.toString() === farm._id.toString())) {        
        mappedFarms.push({
          farmId: farm._id,
          devices: [],
          location: farm.location,
          soilType: farm.soilType,
          terrainType: farm.terrainType,
          waterSource: farm.waterSource
        });                   
      }
    }
    // update dynamo db with mapping   
    for (const farm of mappedFarms) {
      await updateMapperDB(userId, farm);
    }
    const response = {
        statusCode: 200,
        headers: {
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Credentials': true,
        },
        body: JSON.stringify({"success": "true"}),
    };
    client.close();   
    callback(null, response);
  } catch (err) {
    console.log('error...', err);
    if (client && client.close) {
      client.close();
    }   
    callback(err, null);
  }    
};