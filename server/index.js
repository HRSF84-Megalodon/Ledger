if (process.env.NODE_ENV !== 'production') {
  require('dotenv').load();
}
const express = require('express');
const bodyParser = require('body-parser');
const path = require('path');
const db = require('../database/index.js')
// const sqs = require('sqs');
const app = express();
const aws = require('aws-sdk');
const faker = require('faker');



aws.config.loadFromPath('/Users/ifiok/Downloads/git/work/HRSF84/Sprints/hrsf84-thesis/config.json');
const sqs = new aws.SQS();
const queueUrl = 'https://sqs.us-east-2.amazonaws.com/425761756181/megalodon';
const resQueUrl = 'https://sqs.us-east-2.amazonaws.com/425761756181/megalodon-response';
const cliQueUrl = 'https://sqs.us-east-2.amazonaws.com/425761756181/megalodon-client'; 

let data = {
  payer: { 
    userId: 3,
    firstName: "Todd",
    lastName: "Baker"
  },
  payee: { 
    userId: 4,
    firstName: "Tamey", 
    lastName: "Mamey",
  },
  amount: 100,
  transactionId: 10000123,
  transactionKind: "external",
  action: "payment",
  timestamp: new Date() 
}


const balanceQueue = [];
const failedBalanceQueue = [];

const sendMessage = (data, url, cb) => {
  let params = {
      MessageBody: JSON.stringify(data),
      QueueUrl: url,
      DelaySeconds: 0
  };

  sqs.sendMessage(params, function(err, data) {
      if(err) {
          console.log('SEND ERR', err);
      }
      else {
          // console.log(data);
          if (cb) {
            cb()
          }
      }
  });
};


const receiveMessage = (url) => {
  var params = {
      QueueUrl: url,
      VisibilityTimeout: 60, // 1 min wait time for anyone else to process.
      MaxNumberOfMessages: 1
  };

  sqs.receiveMessage(params, async function(err, data) {
      if(err) {
          console.log('RECEIVE ERROR', err);
      }
      else {
        if (data.Messages) {
          for (let message of data.Messages) {
            let messageBody = JSON.parse(message.Body);
            let ledgerUpdated = await db.write(messageBody);

            if (ledgerUpdated) { 
              let payeeUpdated = await db.update(messageBody.payee.userId, messageBody.amount);
              let transactionInfo = [{
                  transactionId: messageBody.transactionId,
                  userId: payeeUpdated.userId,
                  balance: payeeUpdated.balance || "pending"                  
              }]
              //if transaction is a payment add payer data
              let payerUpdated;
              if (messageBody.payer.userId) {
                payerUpdated = await db.update(messageBody.payer.userId, -1 * messageBody.amount);
                transactionInfo.push({
                  transactionId: messageBody.transactionId, 
                  userId: payerUpdated.userId,
                  balance: payerUpdated.balance || "pending"
                })
              }

              sendMessage(transactionInfo, resQueUrl, () => { deleteMessage(message.ReceiptHandle)  });

              if (messageBody.payer.userId) {
                if (!payerUpdated || !payeeUpdated) {
                  db.addFails(messageBody.payee.userId);
                  db.addFails(messageBody.payer.userId);
                }
              } else {
                if(!payeeUpdated) {
                  db.addFails(messageBody.payee.userId);
                }
              }
            }
          }
        }
      }
  });
};





const deleteMessage = (id) => {
  var params = {
    QueueUrl: queueUrl,
    ReceiptHandle: id
  };
    
  sqs.deleteMessage(params, function(err, data) {
    if(err) {
      console.log('DELETE ERROR', err);
    } 
    else {
      // console.log('DELETED');
    } 
  });
}

const sendBalances =  async () => {
  let userArr = [];
  let balanceArr = [];
  for (let i = 0; i < 10; i++) {
    userArr.push(balanceQueue.shift())
  }
  let result = await db.find(userArr);
  // console.log('**********',result);
  for (let balance of result) {
    balanceArr.push(balance.dataValues);
  }
  sendMessage(balanceArr, cliQueUrl);
}

const script = () => {
  for (let i = 0; i < 1; i++) {
    // sendMessage(data, queueUrl);
    receiveMessage(queueUrl);
    // setTimeout( () => { sendBalances() }, 5000)
  }
}

// db.update(1, 1000);


script();


// const createBalanceTable =  async () => {
//   console.log('doing results lookup');
//   let dataArr = [];
//   let result = await db.findAll();
//   for (let i of result) {
//     dataArr.push([i.dataValues.balance, i.dataValues.user_id]);
//   }
//   db.storeBalances(dataArr);
// }

// createBalanceTable();

app.get('/messages', function (req, res) {
  // console.time('SCRIPT');
  script();
  // console.timeEnd('SCRIPT');
  res.send();
});



app.use(bodyParser.json());
app.use(bodyParser.urlencoded({ extended: false }));



app.get('/', function (req, res) {
    res.send("hellsdo");
});



const port = process.env.PORT || 4001;

app.listen(port, function() {
  console.log(`listening on port ${port}!`);
});

const addToBalanceQues = (user) => {
  balanceQueue.push(user.dataValues)
}

module.exports.addToBalanceQues


// queue.push('megalodon', {
//     some:'data'
// }, function (response) {
//     console.log(response);
//     console.log('done') 
// });

