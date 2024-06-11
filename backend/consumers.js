const mysql = require('mysql2');
const pubSub = require('./pubsub');

const db = mysql.createConnection({
  host: 'localhost',
  user: 'root',
  password: 'passwordisroot',
  database: 'crm_db'
});

db.connect((err) => {
  if (err) {
    throw err;
  }
  console.log('MySQL Connected...');
});

pubSub.on('createAudience', (rules) => {
  let query = 'SELECT c.id, c.name, c.email FROM customers c JOIN orders o ON c.id=o.customer_id WHERE ';
  const conditions = rules.map(rule => `${rule.field}${rule.condition}${rule.value}`).join(' AND ');
  query += conditions;

  db.query(query, (err, results) => {
    if (err) throw err;
    results.forEach(customer => {
      console.log(`Customer: ${customer.name}, Email: ${customer.email}`);
    });
  });
});

pubSub.on('sendCampaign', (logEntry) => {
  const { logId, customer, message } = logEntry;
  const deliveryStatus = Math.random() > 0.1 ? 'SENT' : 'FAILED';
  pubSub.emit('deliveryReceipt', { logId, status: deliveryStatus });
});

pubSub.on('deliveryReceipt', (receipt) => {
  const { logId, status } = receipt;
  const sql = 'UPDATE communications_log SET status = ? WHERE id = ?';
  db.query(sql, [status, logId], (err) => {
    if (err) throw err;
    console.log(`Log ID: ${logId} updated with status: ${status}`);
  });
});