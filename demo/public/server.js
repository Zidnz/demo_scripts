// server.js
const express = require('express');
const http = require('http');
const { MongoClient, ObjectId } = require('mongodb');
const socketio = require('socket.io');
const bodyParser = require('body-parser');
require('dotenv').config();

const MONGO_URI = process.env.MONGO_URI || 'mongodb://localhost:27017';
const DB_NAME = process.env.DB_NAME || 'fraud_demo';
const PORT = process.env.PORT || 3000;

async function main(){
  const app = express();
  app.use(bodyParser.json());
  const server = http.createServer(app);
  const io = socketio(server, { cors: { origin: "*" } });

  const client = new MongoClient(MONGO_URI, { useNewUrlParser: true, useUnifiedTopology: true });
  await client.connect();
  console.log('Connected to MongoDB', MONGO_URI);
  const db = client.db(DB_NAME);
  const txCol = db.collection('transactions');
  const alertsCol = db.collection('alerts');
  const configCol = db.collection('config');

  // Ensure basic config doc exists
  let config = await configCol.findOne({ name: "demo_config" });
  if(!config){
    await configCol.insertOne({ name: "demo_config", threshold: 0.5, window_minutes: 1 });
    config = await configCol.findOne({ name: "demo_config" });
  }

  // API: get KPIs (simple aggregates)
  app.get('/api/kpis', async (req,res) => {
    try {
      const totalTx = await txCol.countDocuments();
      const totalAlerts = await alertsCol.countDocuments();
      const avgAmountAgg = await txCol.aggregate([
        { $group: { _id: null, avgAmount: { $avg: "$amount" } } }
      ]).toArray();
      const avgAmount = (avgAmountAgg[0] && avgAmountAgg[0].avgAmount) || 0;
      // fraud rate = alerts / tx
      const fraudRate = totalTx > 0 ? (totalAlerts / totalTx) : 0;
      res.json({ totalTx, totalAlerts, avgAmount, fraudRate, config });
    } catch(e){
      console.error(e);
      res.status(500).json({error: e.message});
    }
  });

  // API: get recent transactions / alerts
  app.get('/api/recent', async (req,res) => {
    try {
      const tx = await txCol.find().sort({_id: -1}).limit(50).toArray();
      const alerts = await alertsCol.find().sort({_id: -1}).limit(50).toArray();
      res.json({ tx, alerts });
    } catch(e){ res.status(500).json({error: e.message}); }
  });

  // API: update threshold and config
  app.post('/api/config', async (req,res) => {
    try {
      const body = req.body;
      await configCol.updateOne({ name: "demo_config" }, { $set: body }, { upsert: true });
      const newConfig = await configCol.findOne({ name: "demo_config" });
      io.emit('config:update', newConfig);
      res.json({ ok: true, config: newConfig });
    } catch(e){ res.status(500).json({error: e.message}); }
  });

  // Socket.IO connection
  io.on('connection', socket => {
    console.log('Client connected', socket.id);
    // Optionally client can request full current KPIs
    socket.on('get_kpis', async () => {
      const totalTx = await txCol.countDocuments();
      const totalAlerts = await alertsCol.countDocuments();
      const avgAmountAgg = await txCol.aggregate([{ $group:{_id:null, avgAmount:{$avg:"$amount"}}}]).toArray();
      const avgAmount = (avgAmountAgg[0] && avgAmountAgg[0].avgAmount) || 0;
      const config = await configCol.findOne({ name: "demo_config" });
      socket.emit('kpis', { totalTx, totalAlerts, avgAmount, config });
    });
  });

  // Change stream for transactions
  const txStream = txCol.watch([], { fullDocument: 'updateLookup' });
  txStream.on('change', change => {
    // send created transactions to frontend
    if(change.operationType === 'insert'){
      const doc = change.fullDocument;
      io.emit('tx:new', doc);
    }
  });

  // Change stream for alerts
  const aStream = alertsCol.watch([], { fullDocument: 'updateLookup' });
  aStream.on('change', change => {
    if(change.operationType === 'insert'){
      const doc = change.fullDocument;
      io.emit('alert:new', doc);
    }
  });

  server.listen(PORT, () => console.log(`Server listening on ${PORT}`));
}

main().catch(err => {
  console.error(err);
  process.exit(1);
});
