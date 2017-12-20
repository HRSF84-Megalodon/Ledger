const Sequelize = require('sequelize');
const sequelize = new Sequelize("postgres://ifiok:hhhggg@localhost:5432/megalodondb");
const faker = require('faker');
const Chance = require('chance');
const fs = require('fs');
const moment = require('moment');
const server = require('../server/index.js')


const chance = new Chance()

console.log()

sequelize.authenticate()
  .then(() => {
    console.log('Connection has been established successfully.');
  })
  .catch(err => {
    console.error('Unable to connect to the database:', err);
  });


const User = sequelize.define('user', {
  id: {
    type: Sequelize.INTEGER,
    autoIncrement: true,
    primaryKey: true
  },
  transaction_id: {
    type: Sequelize.INTEGER
  }, 
  first_name: {
    type: Sequelize.STRING
  },
  last_name: {
    type: Sequelize.STRING
  },
  user_id: {
    type: Sequelize.FLOAT
  },
  transaction_type: {
    type: Sequelize.STRING
  },
  amount: {
    type: Sequelize.FLOAT
  },
  transaction_kind: {
    type: Sequelize.STRING
  },
  original_time: {
    type: Sequelize.DATE
  },
  completion_time: {
    type: Sequelize.DATE
  },
  status: {
    type: Sequelize.STRING
  },
  transaction_kind_2: {
    type: Sequelize.STRING
  }
});

const Balance = sequelize.define('balance', {
  id: {
    type: Sequelize.INTEGER,
    autoIncrement: true,
    primaryKey: true
  },
  user_id: {
    type: Sequelize.FLOAT
  },
  balance: {
    type: Sequelize.FLOAT
  }
});

const Fails = sequelize.define('fails', {
  id: {
    type: Sequelize.INTEGER,
    autoIncrement: true,
    primaryKey: true
  },
  user_id: {
    type: Sequelize.FLOAT
  }
});


const write = (obj) => {
  return sequelize.transaction(function (t) {
  //handles payment and cashout
  if (obj.payer.userId) {
    return User.create({
        transaction_id: obj.transactionId,
        first_name: obj.payer.firstName,
        last_name: obj.payer.lastName,
        user_id: obj.payer.userId,
        transaction_type: "debit",
        amount: obj.amount,
        transaction_kind: obj.transactionKind,
        original_time: obj.timestamp,
        completion_time: moment().format(),
        status: obj.status
      }, {transaction: t}).then(function (user) {
      return User.create({
        transaction_id: obj.transactionId,
        first_name: obj.payee.firstName,
        last_name: obj.payee.lastName,
        user_id: obj.payee.userId,
        transaction_type: "credit",
        amount: obj.amount,
        transaction_kind: obj.transactionKind,
        original_time: obj.timestamp,
        completion_time: moment().format(),
        status: obj.status
      }, {transaction: t});
    });
  } else {
    return User.create({
      transaction_id: obj.transactionId,
      first_name: obj.payer.firstName,
      last_name: obj.payer.lastName,
      user_id: obj.payer.userId,
      transaction_type: "debit",
      amount: obj.amount,
      transaction_kind: obj.transactionKind,
      original_time: obj.timestamp,
      completion_time: moment().format(),
      status: obj.status
    })
  }
  }).then(function (result) {
    return true
  }).catch(function (err) {
    console.log(err);
    return false
  });
}

const find = (userId) => {
  if (userId.length > 0) {
    return Balance.findAll({
      attributes: [[sequelize.col('balance'), 'balance'],[sequelize.col('user_id'), 'user_id']],
      where: {
        user_id: {$in: userId}
      }
      // group: ['user_id']
    })
  }
}

const findAll = () => {
    return User.findAll({
      attributes: [[sequelize.fn('SUM', sequelize.col('amount')), 'balance'],[sequelize.col('user_id'), 'user_id']],
      group: ['user_id']
  })
}

const update = (userId, amount) => {
  return Balance.find({ 
      where: { user_id: userId } 
    })
      .then( async (user) => { return user.updateAttributes({
          balance: user.dataValues.balance + amount
        }) 
      })
      .then( async (user) => {
        await user;
        return user.dataValues
      })
      .catch((err) => {return false })
}

const addFails = (id) => {
  Fails.create({
    user_id: id
  })
}




/*  THE BELOW CODE IS FOR GENERATING FAKE DATA INTO A FILE. USE "COPY users FROM '/Users/ifiok/Downloads/transactions5.txt' (DELIMITER('|'));"
    TO COPY INTO POSTGRES DATABASE


User.sync({force: true});
const writer = fs.createWriteStream('/Users/ifiok/Downloads/transactions5.txt');
let t = 0;
let z = 0;



writeOneMillionTimes = function(writer, data, encoding, callback) {
  let i = 5000000;
  write();
  function write() {
    let ok = true;
    let ok2 = true;
    do {
      i--;
      let transactionId = t;
      let payerFirstName = faker.name.firstName();
      let payerLastName = faker.name.lastName();
      let payeeLastName = faker.name.lastName();
      let payeeFirstName = faker.name.firstName();
      let originalTime = faker.date.between('2017-09-14', '2017-12-14');
      let completionTime = faker.date.future(1, originalTime);
      let payeeAmount = chance.floating({min: 0, max: 2999.99, fixed: 2});
      let payerAmount = -1* payeeAmount;
      let payerUserId = faker.random.number({min:0, max:500000});
      let payeeUserId = faker.random.number({min:0, max:500000});
      while ( payeeUserId === payerUserId ) {
        payeeUserId = faker.random.number({min:0, max:500000});
      }
      let payerTransactionType = "debit";
      let payeeTransactionType = "credit";

      let transactionKind;
      if ( faker.random.boolean() ) {
        transactionKind = "internal"
      } else {
        transactionKind = "external"
      }


      let status;
      if (faker.random.number({min:0, max:8002}) > 8000) {
        status = "reversal";
      } else {
        status = "opened";
      }

      t += 1;
      z += 1
      let payerData = `${z.toString()}|${t.toString()}|${payerFirstName}|${payerLastName}|${payerUserId}|${payerTransactionType}|${payerAmount}|${transactionKind}|${moment(originalTime).format()}|${moment(completionTime).format()}|${status}|payment|${moment().format()}|${moment().format()}\n`
      z += 1
      let payeeData = `${z.toString()}|${t.toString()}|${payeeFirstName}|${payeeLastName}|${payeeUserId}|${payeeTransactionType}|${payeeAmount}|${transactionKind}|${moment(originalTime).format()}|${moment(completionTime).format()}|${status}|payment|${moment().format()}|${moment().format()}\n`

      if (i === 0) {
        // last time!
        writer.write(payerData, encoding, callback);
        writer.write(payeeData, encoding, callback);
      } else {
        // see if we should continue, or wait
        // don't pass the callback, because we're not done yet.
        ok = writer.write(payerData, encoding);
        ok2 = writer.write(payeeData, encoding);
      }
    } while (i > 0 && ok, ok2);
    if (i > 0) {
      // had to stop early!
      // write some more once it drains
      writer.once('drain', write);
    }
  }
}

writeOneMillionTimes(writer, null, () => { null } )
*/

/* USED TO CREATE FAKE BALANCE TABLE FROM EXISITING USER TABLE
writeBalances = function(writer, data, encoding, callback, arr) {
  let t = 0;
  let i = arr.length;
  write();
  function write() {
    let ok = true;
    do {
      i--;
      let transactionId = t;
      let userId = arr[t][1];
      let balance = arr[t][0];
      t += 1

      let payerData = `${t.toString()}|${userId}|${balance}|${moment().format()}|${moment().format()}\n`
      
      if (i === 0) {
        // last time!
        writer.write(payerData, encoding, callback);
      } else {
        // see if we should continue, or wait
        // don't pass the callback, because we're not done yet.
        ok = writer.write(payerData, encoding);
      }
    } while (i > 0 && ok);
    if (i > 0) {
      // had to stop early!
      // write some more once it drains
      writer.once('drain', write);
    }
  }
}

const storeBalances = (arr) => {
  console.log('ARRRRR', arr[0]);
  Balance.sync({force: true});
  const writer = fs.createWriteStream('/Users/ifiok/Downloads/balances.txt');
  let t = 0;  
  writeBalances(writer, null, 'utf-8',() => { null }, arr)
}
*/



module.exports.write = write;
module.exports.find = find;
module.exports.update = update;
module.exports.addFails = addFails;

// module.exports.findAll = findAll;
// module.exports.storeBalances = storeBalances;





