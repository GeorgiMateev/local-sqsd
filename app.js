var config = require('config');
var chalk = require('chalk');
var os = require("os");
var request = require('request');
var AWS = require('aws-sdk');

var accessKey = config.get('sqsd.accessKey');
var secretAccessKey = config.get('sqsd.secretAccessKey');
var region = config.has('sqsd.region') ? config.get('sqsd.region') : "us-west-1";
var queueUrl = config.get('sqsd.queueUrl');
var maxNumberOfMessages = config.has('sqsd.maxNumberOfMessages') ? config.get('sqsd.maxNumberOfMessages') : 10;
var visibilityTimeout = config.has('sqsd.visibilityTimeout') ? config.get('sqsd.visibilityTimeout') : 30;
var inactivityTimeout = config.has('sqsd.inactivityTimeout') ? config.get('sqsd.inactivityTimeout') : 30;
var pingInterval = config.has('sqsd.pingInterval') ? config.get('sqsd.pingInterval') : 1000;
var waitTime = config.has('sqsd.waitTime') ? config.get('sqsd.waitTime') : 1;
var workerUrl = config.get('sqsd.workerUrl');

var sqs = new AWS.SQS({
    accessKeyId: accessKey,
    secretAccessKey: secretAccessKey,
    region: region,
    apiVersion: '2012-11-05'
});

console.log(chalk.green('Started to send messages.'));
console.log(os.EOL);

setInterval(function () {
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
        }
        else if(data.Messages) {
            for (var i = 0; i < data.Messages.length; i++) {
                var message = data.Messages[i];
                sendMessageToWorker(message);
            }
        }
    });
}, pingInterval);

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

    request(options, function (err, response, body) {
        sendMessageCallback(err, response, body, receiptHandle);
    });
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