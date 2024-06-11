const express = require('express');
const bodyParser = require('body-parser');
const cors = require('cors');
const mysql = require('mysql2');
const pubSub = require('./pubsub');

const app = express();
const port = 5000;

app.use(cors());
app.use(bodyParser.json());

// Database connection
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

// APIs
app.post('/api/customers', (req, res) => {
  const { name, email } = req.body;
  const sql = 'INSERT INTO customers (name, email) VALUES (?, ?)';
  db.query(sql, [name, email], (err, result) => {
    if (err) throw err;
    res.json({ id: result.insertId, name, email });
  });
});

app.get('/api/customers', (req, res) => {
  const sql = 'SELECT * FROM customers';
  db.query(sql, (err, results) => {
    if (err) throw err;
    res.json(results);
  });
});

app.put('/api/customers/:id', (req, res) => {
  const { id } = req.params;
  const { name, email } = req.body;
  const sql = 'UPDATE customers SET name = ?, email = ? WHERE id = ?';
  db.query(sql, [name, email, id], (err, result) => {
    if (err) throw err;
    res.json({ id, name, email });
  });
});

app.post('/api/orders', (req, res) => {
  const { customer_id, amount, date } = req.body;
  const sql = 'INSERT INTO orders (customer_id, amount, date) VALUES (?, ?, ?)';
  db.query(sql, [customer_id, amount, date], (err, result) => {
    if (err) throw err;
    res.json({ id: result.insertId, customer_id, amount, date });
  });
});

app.post('/api/audience/size', (req, res) => {
  const { rules } = req.body;
  let query = 'SELECT COUNT(*) AS size FROM customers c JOIN orders o ON c.id=o.customer_id WHERE ';
  const conditions = rules.map(rule => `${rule.field}${rule.condition}${rule.value}`).join(' AND ');
  query += conditions;
  
  db.query(query, (err, results) => {
    if (err) throw err;
    res.json({ size: results[0].size });
  });
});

app.get('/api/campaigns', (req, res) => {
  const sql = 'SELECT * FROM communications_log ORDER BY id DESC';
  db.query(sql, (err, results) => {
    if (err) throw err;
    res.json(results);
  });
});

app.post('/api/delivery-receipt', (req, res) => {
  const { logId, status } = req.body;
  const sql = 'UPDATE communications_log SET status = ? WHERE id = ?';
  db.query(sql, [status, logId], (err) => {
    if (err) throw err;
    res.status(200).send('Delivery status updated.');
  });
});

app.post('/api/send-campaign', (req, res) => {
  const { audience, message } = req.body;
  audience.forEach(customer => {
    const commLogSql = 'INSERT INTO communications_log (customer_id, message, status) VALUES (?, ?, ?)';
    db.query(commLogSql, [customer.id, message, 'PENDING'], (err, result) => {
      if (err) throw err;
      const logEntry = {
        logId: result.insertId,
        customer,
        message
      };
      pubSub.emit('sendCampaign', logEntry);
    });
  });
  res.status(200).send('Campaign sending initiated.');
});

app.listen(port, () => {
  console.log(`Server running on port ${port}`);
});