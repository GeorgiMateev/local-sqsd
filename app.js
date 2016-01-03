#! /usr/bin/env node

var config = require('config');
var chalk = require('chalk');
var os = require('os');
var Q = require('q');
var request = require('request');
var AWS = require('aws-sdk');

var accessKey = config.get('sqsd.accessKey');
var secretAccessKey = config.get('sqsd.secretAccessKey');
var region = config.has('sqsd.region') ? config.get('sqsd.region') : "us-west-1";
var queueUrl = config.get('sqsd.queueUrl');
var maxNumberOfMessages = config.has('sqsd.maxNumberOfMessages') ? config.get('sqsd.maxNumberOfMessages') : 10;
var visibilityTimeout = config.has('sqsd.visibilityTimeoutSec') ? config.get('sqsd.visibilityTimeoutSec') : 30;
var inactivityTimeout = config.has('sqsd.inactivityTimeoutSec') ? config.get('sqsd.inactivityTimeoutSec') : 30;
var pingInterval = config.has('sqsd.pingIntervalMs') ? config.get('sqsd.pingIntervalMs') : 1000;
var waitTime = config.has('sqsd.waitTimeSec') ? config.get('sqsd.waitTimeSec') : 1;
var workerUrl = config.get('sqsd.workerUrl');

var sqs = new AWS.SQS({
    accessKeyId: accessKey,
    secretAccessKey: secretAccessKey,
    region: region,
    apiVersion: '2012-11-05'
});

var argv = require('minimist')(process.argv.slice(2));

if(argv.d) {
    inactivityTimeout = 120 * 60;
    console.log(chalk.green('Local SQSD started in debug mode.'));
    console.log(chalk.yellow('You will have 2 hours to debug a message before timeout.'))
}
else {
    console.log(chalk.green('Starting to send messages to '+ workerUrl));
}

console.log(os.EOL);

ping(function () {
    var deferred = Q.defer();

    var params = {
        QueueUrl: queueUrl,
        AttributeNames: ['All'],
        MaxNumberOfMessages: maxNumberOfMessages,
        MessageAttributeNames: ['All'],
        VisibilityTimeout: visibilityTimeout,
        WaitTimeSeconds: waitTime
    };

    sqs.receiveMessage(params, function(err, data) {
        if (err) {
            console.log(chalk.red("Error when recieving a message from the queue:"));
            console.log(chalk.red(err));
            console.log(os.EOL);
            
            deferred.resolve();
        }
        else if(data.Messages) {
            var messagesPromises = [];

            if(argv.d) {
                console.log('Recieved ' + data.Messages.length + ' messages.');
                console.log(os.EOL);
            }

            for (var i = 0; i < data.Messages.length; i++) {
                var message = data.Messages[i];
                messagesPromises.push(sendMessageToWorker(message));
            }

            Q.all(messagesPromises).then(function () {
                deferred.resolve();
            });
        }
    });

    return deferred.promise;
}, pingInterval, argv.d);

function ping(cb, pingInterval, inDebug) {
    if (inDebug) {
        var readline = require('readline');

        var rl = readline.createInterface({
          input: process.stdin,
          output: process.stdout
        });

        console.log(chalk.green("Press Enter to send " + maxNumberOfMessages + " messages to "+ workerUrl));

        rl.on('line', function () {
            cb();
            console.log(chalk.green("Press Enter to send " + maxNumberOfMessages + " messages to "+ workerUrl));
        });
    }
    else {
        setInterval(cb, pingInterval);
    }
}

function sendMessageToWorker (message) {
    var headers = getHeaders(message);
    var body = getBody(message);

    var options = {
        url: workerUrl,
        method: 'POST',
        headers: headers,
        json: true,
        body: body,
        timeout: inactivityTimeout * 1000
    }

    var receiptHandle = message.ReceiptHandle;

    var deferred = Q.defer();
    request(options, function (err, response, body) {
        sendMessageCallback(err, response, body, receiptHandle);
        deferred.resolve();
    });

    return deferred.promise;
}

function getHeaders (message) {
    var headers = {
        'Content-Type': 'application/json'
    };

    var attributeHeaderPrefix = 'X-Aws-Sqsd-Attr-';
    for (var key in message.MessageAttributes) {
        var attribute = message.MessageAttributes[key];

        var value;
        if (attribute.DataType === 'String') {
            value = attribute.StringValue;
        }
        else if (attribute.DataType === 'Binary'){
            value = attribute.BinaryValue;
        }

        var headerName = attributeHeaderPrefix + key;
        headers[headerName] = value;
    }

    return headers;
}

function getBody (message) {
    return JSON.parse(message.Body);
}

function sendMessageCallback (err, response, body, receiptHandle) {
    if (err) {
        console.log(chalk.red('Error when sending request to the worker:'));
        console.log(chalk.red(err, err.stack.replace("\n", os.EOL)));
        console.log(os.EOL);
        return;
    }

    if (response.statusCode === 200) {
        if(argv.d) {
            console.log('Message processed by the worker.');
        }
        deleteMessage(receiptHandle);
    }
    else {
        // Do nothing. Message will be returned to the queue and eventually sent to the dead messaged queue.
        console.log(chalk.yellow('The worker responded with status code: ' + response.statusCode + ' - ' + response.statusMessage));
        console.log(os.EOL);
    }
}

function deleteMessage (receiptHandle) {
    var params = {
        QueueUrl: queueUrl,
        ReceiptHandle: receiptHandle
    };
    sqs.deleteMessage(params, function(err, data) {
        if (err) {
            console.log(chalk.red('Error when deleting a message from the queue:'));
            console.log(chalk.red(err));
            console.log(os.EOL);
            return;
        }
    });
}