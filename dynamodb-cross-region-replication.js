/*
 * Based on the work of Daniela Miao
 * http://www.meetup.com/Palo-Alto-AWS-BigData-Meetup/events/229638493/?eventId=229638493
 * The Why and How of DynamoDB Cross-region Replication
 *
 * This file is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. 
 * 
 * Pull requests are most welcome
 */
var startTime = Date.now();
console.log("Start Time: " + startTime);
var http = require('http');
var https = require('https');
http.globalAgent.maxSockets = 500;
https.globalAgent.maxSockets = 500;

// Update these 2 lines with destination region and table name.
var destinationRegion = 'us-west-1';
var destinationTable = 'test-table-replica';

//Set up AWS client
var AWS = require('aws-sdk');

//Update AWS configuration to set region
AWS.config.update({region : destinationRegion});

// Set up DynamoDB client
var dynamodb = new AWS.DynamoDB();

exports.handler = function(event, context) {
    var handlerStart = Date.now();
    console.log("Handler Start Time: " + handlerStart);
    //Keep track of how may requests are in flight
    var inflightRequests = 0;

    //Deduplicate updates to the same key
    var buffer = {};
    event.Records.forEach(function(record){
        buffer[JSON.stringify(record.dynamodb.Keys)] = record.dynamodb;
    });

    // Make a callback function to execute once the request completes
    var handleResponse = function(err, data) {
        if (err) {
            //log errors
            console.error(err, err.stack);
        } else {
            //check if all requests are finished, if so, end the function
            inflightRequests--;
            if (inflightRequests === 0) {
                context.succeed("Successfully processed" + event.Records.length + " records. ");
                console.log("Total Time: ", Date.now()-handlerStart, "ms");
            }
        }
    };

    for (var key in buffer) {
        if (!buffer.hasOwnProperty(key)) continue;

        // Get the new image of the DynamoDB stream record
        var oldItemImage = buffer[key].OldImage;
        var newItemImage = buffer[key].NewImage;

        // Figure out the what type of request to send
        if (validate(oldItemImage) && !validate(newItemImage)) {
            dynamodb.deleteItem({Key : buffer[key].Keys, TableName : destinationTable}, handleResponse);
        } else if (validate(newItemImage)) {
            dynamodb.putItem({Item : newItemImage, TableName : destinationTable}, handleResponse);
        } else {
            console.error("Both old and new images are not valid.");
        }

        // Increase count for number of requests in flight
        inflightRequests++;
    }

    console.log("Sent all request took: ", Date.now()-handlerStart, "ms");

    // if there are no more requests pending, end the function
    if (inflightRequests === 0) {
        context.succeed("Successfully processed " + event.Records.length + " records.");
    }
};

// validate the given image of record, return true only if the image is valid
var validate = function(image) {
  if (typeof image !== 'undefined' && image) {
      return true;
  }
  return false;
};
