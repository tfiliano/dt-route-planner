// database-service.js
// Database Service for PDF Delivery Data Extractor - Node.js Version
// Handles storage and retrieval of manifest data in PostgreSQL

const { Pool } = require('pg');
const { v4: uuidv4 } = require('uuid');

class DatabaseService {
  constructor(config) {
    this.config = config;
    this.pool = null;
  }

  // Initialize database connection pool
  async initialize() {
    try {
      this.pool = new Pool({
        host: this.config.host,
        port: this.config.port,
        database: this.config.database,
        user: this.config.user,
        password: this.config.password,
        ssl: this.config.ssl || { rejectUnauthorized: false }, // For Neon.tech
        max: this.config.maxConnections || 10,
        idleTimeoutMillis: 30000,
        connectionTimeoutMillis: 2000,
      });

      // Test connection
      const client = await this.pool.connect();
      await client.query('SELECT NOW()');
      client.release();
      
      console.log('Database connection pool initialized');
    } catch (error) {
      console.error('Failed to initialize database pool:', error);
      throw error;
    }
  }

  // Close database connection pool
  async close() {
    if (this.pool) {
      await this.pool.end();
      console.log('Database connection pool closed');
    }
  }

  // Store a complete manifest with all deliveries
  async storeManifest(manifestData, processingInfo) {
    const client = await this.pool.connect();
    
    try {
      await client.query('BEGIN');

      // Extract collection depot info
      const collectionDepot = manifestData.collection_depot || {};
      
      // Parse planned delivery date
      let plannedDate = null;
      if (manifestData.planned_delivery_date) {
        try {
          const [day, month, year] = manifestData.planned_delivery_date.split('/');
          plannedDate = new Date(year, month - 1, day);
        } catch (error) {
          console.warn('Could not parse date:', manifestData.planned_delivery_date);
        }
      }

      // Parse report time
      let reportTime = null;
      if (manifestData.report_time_loading) {
        reportTime = manifestData.report_time_loading + ':00'; // Add seconds
      }

      // Insert manifest
      const manifestResult = await client.query(`
        INSERT INTO manifests (
          manifest_id, planned_delivery_date, vehicle_driver,
          report_time_loading, collection_depot_name, 
          collection_depot_postcode, original_filename,
          file_size_bytes, delivery_count, raw_manifest_data
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
        RETURNING id
      `, [
        manifestData.manifest_id || '',
        plannedDate,
        manifestData.vehicle_driver || '',
        reportTime,
        collectionDepot.name || '',
        collectionDepot.postcode || '',
        processingInfo.filename || '',
        processingInfo.file_size_bytes || 0,
        (manifestData.deliveries || []).length,
        JSON.stringify(manifestData)
      ]);

      const manifestId = manifestResult.rows[0].id;

      // Insert deliveries
      const deliveries = manifestData.deliveries || [];
      for (let order = 0; order < deliveries.length; order++) {
        await this.storeDelivery(client, manifestId, deliveries[order], order + 1);
      }

      await client.query('COMMIT');
      
      console.log(`Stored manifest ${manifestData.manifest_id} with ${deliveries.length} deliveries`);
      return manifestId;

    } catch (error) {
      await client.query('ROLLBACK');
      console.error('Error storing manifest:', error);
      throw error;
    } finally {
      client.release();
    }
  }

  // Store a single delivery
  async storeDelivery(client, manifestId, delivery, order) {
    // Parse time window
    const timeWindow = delivery.time_window || {};
    let startTime = null;
    let endTime = null;

    if (timeWindow.start) {
      startTime = timeWindow.start + ':00'; // Add seconds
    }
    if (timeWindow.end) {
      endTime = timeWindow.end + ':00'; // Add seconds
    }

    await client.query(`
      INSERT INTO deliveries (
        manifest_id, contact_name, address, postcode, booking_ref,
        arc_number, contact_phone, est_weight_kg, total_cases,
        time_window_start, time_window_end, delivery_instructions,
        delivery_type, delivery_order, raw_delivery_data
      ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15)
    `, [
      manifestId,
      delivery.contact_name || '',
      delivery.address || '',
      delivery.postcode || '',
      delivery.booking_ref || '',
      delivery.arc_number || '',
      delivery.contact_phone || null,
      parseFloat(delivery.est_weight_kg || 0),
      parseInt(delivery.total_cases || 0),
      startTime,
      endTime,
      delivery.delivery_instructions || '',
      delivery.delivery_type || '',
      order,
      JSON.stringify(delivery)
    ]);
  }

  // Get manifest by internal ID
  async getManifestById(manifestId) {
    const client = await this.pool.connect();
    
    try {
      // Get manifest data
      const manifestResult = await client.query(
        'SELECT * FROM manifests WHERE id = $1',
        [manifestId]
      );

      if (manifestResult.rows.length === 0) {
        return null;
      }

      // Get deliveries
      const deliveriesResult = await client.query(
        'SELECT * FROM deliveries WHERE manifest_id = $1 ORDER BY delivery_order',
        [manifestId]
      );

      const manifest = manifestResult.rows[0];
      manifest.deliveries = deliveriesResult.rows;

      return manifest;

    } finally {
      client.release();
    }
  }

  // Get manifest by manifest reference number
  async getManifestByRef(manifestRef) {
    const client = await this.pool.connect();
    
    try {
      const manifestResult = await client.query(
        'SELECT * FROM manifests WHERE manifest_id = $1 ORDER BY created_at DESC LIMIT 1',
        [manifestRef]
      );

      if (manifestResult.rows.length === 0) {
        return null;
      }

      const deliveriesResult = await client.query(
        'SELECT * FROM deliveries WHERE manifest_id = $1 ORDER BY delivery_order',
        [manifestResult.rows[0].id]
      );

      const manifest = manifestResult.rows[0];
      manifest.deliveries = deliveriesResult.rows;

      return manifest;

    } finally {
      client.release();
    }
  }

  // Search manifests with filters
  async searchManifests(filters = {}) {
    const client = await this.pool.connect();
    
    try {
      const conditions = [];
      const params = [];
      let paramCount = 0;

      if (filters.dateFrom) {
        paramCount++;
        conditions.push(`planned_delivery_date >= $${paramCount}`);
        params.push(filters.dateFrom);
      }

      if (filters.dateTo) {
        paramCount++;
        conditions.push(`planned_delivery_date <= $${paramCount}`);
        params.push(filters.dateTo);
      }

      if (filters.driver) {
        paramCount++;
        conditions.push(`vehicle_driver ILIKE $${paramCount}`);
        params.push(`%${filters.driver}%`);
      }

      if (filters.depotPostcode) {
        paramCount++;
        conditions.push(`collection_depot_postcode = $${paramCount}`);
        params.push(filters.depotPostcode);
      }

      const whereClause = conditions.length > 0 ? 'WHERE ' + conditions.join(' AND ') : '';
      
      paramCount++;
      params.push(filters.limit || 100);
      paramCount++;
      params.push(filters.offset || 0);

      const query = `
        SELECT * FROM manifest_summary 
        ${whereClause}
        ORDER BY planned_delivery_date DESC, created_at DESC
        LIMIT $${paramCount - 1} OFFSET $${paramCount}
      `;

      const result = await client.query(query, params);
      return result.rows;

    } finally {
      client.release();
    }
  }

  // Search deliveries with filters
  async searchDeliveries(filters = {}) {
    const client = await this.pool.connect();
    
    try {
      const conditions = [];
      const params = [];
      let paramCount = 0;

      if (filters.postcode) {
        paramCount++;
        if (filters.postcode.length <= 4) {
          // Area search like "SW1"
          conditions.push(`postcode LIKE $${paramCount}`);
          params.push(`${filters.postcode}%`);
        } else {
          // Exact postcode
          conditions.push(`postcode = $${paramCount}`);
          params.push(filters.postcode);
        }
      }

      if (filters.contactName) {
        paramCount++;
        conditions.push(`contact_name ILIKE $${paramCount}`);
        params.push(`%${filters.contactName}%`);
      }

      if (filters.bookingRef) {
        paramCount++;
        conditions.push(`booking_ref = $${paramCount}`);
        params.push(filters.bookingRef);
      }

      const whereClause = conditions.length > 0 ? 'WHERE ' + conditions.join(' AND ') : '';
      
      paramCount++;
      params.push(filters.limit || 100);
      paramCount++;
      params.push(filters.offset || 0);

      const query = `
        SELECT * FROM deliveries_with_manifest 
        ${whereClause}
        ORDER BY planned_delivery_date DESC, delivery_order
        LIMIT $${paramCount - 1} OFFSET $${paramCount}
      `;

      const result = await client.query(query, params);
      return result.rows;

    } finally {
      client.release();
    }
  }

  // Create a new batch job record
  async createBatchJob(jobId, totalFiles) {
    const client = await this.pool.connect();
    
    try {
      const result = await client.query(`
        INSERT INTO batch_jobs (job_id, total_files)
        VALUES ($1, $2)
        RETURNING id
      `, [jobId, totalFiles]);

      return result.rows[0].id;

    } finally {
      client.release();
    }
  }

  // Update batch job status
  async updateBatchJob(jobId, updates) {
    const client = await this.pool.connect();
    
    try {
      const setClause = [];
      const params = [];
      let paramCount = 0;

      if (updates.status !== undefined) {
        paramCount++;
        setClause.push(`status = $${paramCount}`);
        params.push(updates.status);

        if (updates.status === 'completed') {
          paramCount++;
          setClause.push(`completed_at = $${paramCount}`);
          params.push(new Date());
        }
      }

      if (updates.processedFiles !== undefined) {
        paramCount++;
        setClause.push(`processed_files = $${paramCount}`);
        params.push(updates.processedFiles);
      }

      if (updates.successfulFiles !== undefined) {
        paramCount++;
        setClause.push(`successful_files = $${paramCount}`);
        params.push(updates.successfulFiles);
      }

      if (updates.failedFiles !== undefined) {
        paramCount++;
        setClause.push(`failed_files = $${paramCount}`);
        params.push(updates.failedFiles);
      }

      if (updates.results) {
        paramCount++;
        setClause.push(`results = $${paramCount}`);
        params.push(JSON.stringify(updates.results));
      }

      if (updates.errors) {
        paramCount++;
        setClause.push(`errors = $${paramCount}`);
        params.push(JSON.stringify(updates.errors));
      }

      if (updates.errorMessage) {
        paramCount++;
        setClause.push(`error_message = $${paramCount}`);
        params.push(updates.errorMessage);
      }

      if (setClause.length > 0) {
        paramCount++;
        params.push(jobId);

        const query = `
          UPDATE batch_jobs 
          SET ${setClause.join(', ')}
          WHERE job_id = $${paramCount}
        `;

        await client.query(query, params);
      }

    } finally {
      client.release();
    }
  }

  // Link a manifest to a batch job
  async linkBatchManifest(jobId, manifestId, order) {
    const client = await this.pool.connect();
    
    try {
      await client.query(`
        INSERT INTO batch_job_manifests (batch_job_id, manifest_id, processing_order)
        SELECT bj.id, $2, $3
        FROM batch_jobs bj
        WHERE bj.job_id = $1
      `, [jobId, manifestId, order]);

    } finally {
      client.release();
    }
  }

  // Get batch job details
  async getBatchJob(jobId) {
    const client = await this.pool.connect();
    
    try {
      const result = await client.query(
        'SELECT * FROM batch_jobs WHERE job_id = $1',
        [jobId]
      );

      return result.rows.length > 0 ? result.rows[0] : null;

    } finally {
      client.release();
    }
  }

  // Get database statistics
  async getStatistics() {
    const client = await this.pool.connect();
    
    try {
      const manifestStats = await client.query(`
        SELECT 
          COUNT(*) as total_manifests,
          COUNT(DISTINCT planned_delivery_date) as unique_dates,
          COUNT(DISTINCT vehicle_driver) as unique_drivers,
          SUM(delivery_count) as total_deliveries,
          AVG(delivery_count) as avg_deliveries_per_manifest
        FROM manifests
        WHERE planned_delivery_date IS NOT NULL
      `);

      const recentStats = await client.query(`
        SELECT 
          COUNT(*) as recent_manifests,
          MAX(processed_at) as last_processed
        FROM manifests
        WHERE processed_at > NOW() - INTERVAL '24 hours'
      `);

      const batchStats = await client.query(`
        SELECT 
          COUNT(*) as total_batch_jobs,
          COUNT(*) FILTER (WHERE status = 'completed') as completed_jobs,
          COUNT(*) FILTER (WHERE status = 'failed') as failed_jobs,
          COUNT(*) FILTER (WHERE status = 'processing') as processing_jobs
        FROM batch_jobs
      `);

      return {
        manifests: manifestStats.rows[0],
        recent_activity: recentStats.rows[0],
        batch_processing: batchStats.rows[0]
      };

    } finally {
      client.release();
    }
  }
}

// Database configuration helper
class DatabaseConfig {
  constructor(options = {}) {
    this.host = options.host || process.env.DB_HOST || 'localhost';
    this.port = options.port || process.env.DB_PORT || 5432;
    this.database = options.database || process.env.DB_NAME || 'manifest_db';
    this.user = options.user || process.env.DB_USER || 'postgres';
    this.password = options.password || process.env.DB_PASSWORD || '';
    this.ssl = options.ssl || (process.env.NODE_ENV === 'production' ? { rejectUnauthorized: false } : false);
    this.maxConnections = options.maxConnections || process.env.DB_MAX_CONNECTIONS || 10;
  }

  static fromEnv() {
    return new DatabaseConfig();
  }
}

module.exports = {
  DatabaseService,
  DatabaseConfig
};