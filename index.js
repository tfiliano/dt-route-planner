const express = require('express');
const { Pool } = require('pg');

const app = express();
app.use(express.json());

const pool = new Pool({
  // Configure your PostgreSQL connection details here
  connectionString: process.env.DATABASE_URL,
});

// API endpoint to get all manifests
app.get('/api/manifests', async (req, res) => {
  try {
    const result = await pool.query('SELECT * FROM manifests');
    res.json(result.rows);
  } catch (error) {
    console.error('Error retrieving manifests:', error);
    res.status(500).json({ error: 'Internal server error' });
  }
});

// API endpoint para consultar manifestos por data
app.get('/api/manifests/date/:date', async (req, res) => {
  const { date } = req.params;
  try {
    const result = await pool.query('SELECT * FROM manifests WHERE planned_delivery_date = $1', [date]);
    res.json(result.rows);
  } catch (error) {
    console.error('Error retrieving manifests by date:', error);
    res.status(500).json({ error: 'Internal server error' });
  }
});

// API endpoint para consultar um manifesto por número do manifesto
app.get('/api/manifests/:manifestId', async (req, res) => {
  const { manifestId } = req.params;
  try {
    const result = await pool.query('SELECT * FROM manifests WHERE manifest_id = $1', [manifestId]);
    if (result.rows.length === 0) {
      res.status(404).json({ error: 'Manifest not found' });
    } else {
      res.json(result.rows[0]);
    }
  } catch (error) {
    console.error('Error retrieving manifest by ID:', error);
    res.status(500).json({ error: 'Internal server error' });
  }
});

// API endpoint to create a new manifest
app.post('/api/manifests', async (req, res) => {
  const { manifest_id, planned_delivery_date, vehicle_driver, raw_manifest_data } = req.body;
  try {
    const result = await pool.query(
      'INSERT INTO manifests (manifest_id, planned_delivery_date, vehicle_driver, raw_manifest_data) VALUES ($1, $2, $3, $4) RETURNING *',
      [manifest_id, planned_delivery_date, vehicle_driver, raw_manifest_data]
    );
    res.status(201).json(result.rows[0]);
  } catch (error) {
    console.error('Error creating manifest:', error);
    res.status(500).json({ error: 'Internal server error' });
  }
});

// Add more API endpoints for updating and deleting manifests as needed

const port = process.env.PORT || 3000;
app.listen(port, () => {
  console.log(`Server is running on port ${port}`);
});