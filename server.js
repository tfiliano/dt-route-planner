// server.js
// Express.js Microservice for PDF Delivery Data Extraction with Database Integration

const express = require('express');
const multer = require('multer');
const cors = require('cors');
const { v4: uuidv4 } = require('uuid');
const fs = require('fs').promises;
const path = require('path');
const { DatabaseService, DatabaseConfig } = require('./database-service');

// For PDF processing - you'll need to integrate with your PDF extractor
// This is a placeholder - replace with your actual PDF processing logic
// const { processPDF } = require('./pdf-processor'); // Your PDF processing module

const app = express();
const PORT = process.env.PORT || 8000;

// Middleware
app.use(cors());
app.use(express.json({ limit: '50mb' }));
app.use(express.urlencoded({ extended: true, limit: '50mb' }));

// Configure multer for file uploads
const upload = multer({
  dest: 'uploads/',
  limits: {
    fileSize: 50 * 1024 * 1024, // 50MB limit
  },
  fileFilter: (req, file, cb) => {
    if (file.mimetype === 'application/pdf') {
      cb(null, true);
    } else {
      cb(new Error('Only PDF files are allowed'), false);
    }
  }
});

// Initialize database service
const dbConfig = DatabaseConfig.fromEnv();
const dbService = new DatabaseService(dbConfig);

// In-memory store for batch jobs (for backward compatibility)
const batchJobs = new Map();

// Initialize database connection on startup
async function initializeServices() {
  try {
    await dbService.initialize();
    console.log('✅ Database service initialized');
  } catch (error) {
    console.error('❌ Error searching manifests:', error);
    res.status(500).json({ error: 'Error searching manifests' });
  }
};

// Get specific manifest
app.get('/manifests/:manifestId', async (req, res) => {
  try {
    const { manifestId } = req.params;
    let manifest = null;

    // Try as UUID first (internal ID)
    if (manifestId.length === 36 && manifestId.includes('-')) {
      manifest = await dbService.getManifestById(manifestId);
    } else {
      // Try as manifest reference
      manifest = await dbService.getManifestByRef(manifestId);
    }

    if (!manifest) {
      return res.status(404).json({ error: 'Manifest not found' });
    }

    res.json(manifest);

  } catch (error) {
    console.error('❌ Error retrieving manifest:', error);
    res.status(500).json({ error: 'Error retrieving manifest' });
  }
});

// Search deliveries
app.get('/deliveries', async (req, res) => {
  try {
    const filters = {
      postcode: req.query.postcode,
      contactName: req.query.contact_name,
      bookingRef: req.query.booking_ref,
      limit: parseInt(req.query.limit) || 100,
      offset: parseInt(req.query.offset) || 0
    };

    const results = await dbService.searchDeliveries(filters);

    res.json({
      deliveries: results,
      total_returned: results.length,
      filters: {
        postcode: filters.postcode,
        contact_name: filters.contactName,
        booking_ref: filters.bookingRef
      },
      pagination: {
        limit: filters.limit,
        offset: filters.offset
      }
    });

  } catch (error) {
    console.error('❌ Error searching deliveries:', error);
    res.status(500).json({ error: 'Error searching deliveries' });
  }
});

// Get comprehensive service statistics
app.get('/stats', async (req, res) => {
  try {
    // Get database statistics
    const dbStats = await dbService.getStatistics();

    // Get in-memory batch job statistics
    const memoryJobs = Array.from(batchJobs.values());
    const memoryStats = {
      total_jobs: memoryJobs.length,
      completed_jobs: memoryJobs.filter(job => job.status === 'completed').length,
      failed_jobs: memoryJobs.filter(job => job.status === 'failed').length,
      processing_jobs: memoryJobs.filter(job => job.status === 'processing').length
    };
    memoryStats.success_rate = memoryStats.total_jobs > 0 
      ? (memoryStats.completed_jobs / memoryStats.total_jobs * 100) 
      : 0;

    res.json({
      database_stats: dbStats,
      memory_batch_jobs: memoryStats,
      timestamp: new Date().toISOString()
    });

  } catch (error) {
    console.error('❌ Error retrieving statistics:', error);
    res.status(500).json({ error: 'Error retrieving statistics' });
  }
});

// Delete batch job (memory only for backward compatibility)
app.delete('/batch/:jobId', (req, res) => {
  const { jobId } = req.params;
  
  if (batchJobs.has(jobId)) {
    batchJobs.delete(jobId);
    res.json({ message: `Job ${jobId} deleted from memory` });
  } else {
    res.status(404).json({ error: 'Job ID not found in memory' });
  }
});

// Validate PDF file
app.post('/validate/pdf', upload.single('file'), async (req, res) => {
  try {
    if (!req.file) {
      return res.json({
        valid: false,
        reason: 'No file provided'
      });
    }

    if (!req.file.originalname.toLowerCase().endsWith('.pdf')) {
      return res.json({
        valid: false,
        reason: 'File is not a PDF'
      });
    }

    // Read first few bytes to check PDF signature
    try {
      const fileBuffer = await fs.readFile(req.file.path);
      
      if (!fileBuffer.toString('ascii', 0, 4).startsWith('%PDF')) {
        return res.json({
          valid: false,
          reason: 'File is not a valid PDF'
        });
      }

      res.json({
        valid: true,
        file_size_bytes: req.file.size,
        filename: req.file.originalname
      });

    } finally {
      // Clean up uploaded file
      try {
        await fs.unlink(req.file.path);
      } catch (cleanupError) {
        console.warn('⚠️ Failed to clean up validation file:', cleanupError);
      }
    }

  } catch (error) {
    // Clean up file on error
    if (req.file?.path) {
      try {
        await fs.unlink(req.file.path);
      } catch (cleanupError) {
        console.warn('⚠️ Failed to clean up validation file on error:', cleanupError);
      }
    }

    res.json({
      valid: false,
      reason: `Error reading file: ${error.message}`
    });
  }
});

// Error handling middleware
app.use((error, req, res, next) => {
  console.error('❌ Unhandled error:', error);
  
  if (error instanceof multer.MulterError) {
    if (error.code === 'LIMIT_FILE_SIZE') {
      return res.status(400).json({ error: 'File too large. Maximum size is 50MB.' });
    }
    if (error.code === 'LIMIT_UNEXPECTED_FILE') {
      return res.status(400).json({ error: 'Too many files or unexpected field name.' });
    }
  }

  res.status(500).json({ error: 'Internal server error' });
});

// 404 handler
app.use((req, res) => {
  res.status(404).json({ error: 'Endpoint not found' });
});

// Start server
async function startServer() {
  await initializeServices();
  
  app.listen(PORT, () => {
    console.log(`🚀 PDF Delivery Data Extractor API running on port ${PORT}`);
    console.log(`📚 API Documentation available at http://localhost:${PORT}/`);
    console.log(`🔍 Health check: http://localhost:${PORT}/`);
  });
}

startServer().catch(error => {
  console.error('❌ Failed to start server:', error);
  process.exit(1);
});

module.exports = app;
  

// Graceful shutdown
async function shutdown() {
  console.log('🔄 Shutting down gracefully...');
  await dbService.close();
  process.exit(0);
}

process.on('SIGTERM', shutdown);
process.on('SIGINT', shutdown);

// Health check endpoint
app.get('/', (req, res) => {
  res.json({
    service: 'PDF Delivery Data Extractor',
    status: 'healthy',
    version: '2.0.0',
    database: 'connected',
    timestamp: new Date().toISOString()
  });
});

// Extract from single PDF
app.post('/extract/single', upload.single('file'), async (req, res) => {
  try {
    if (!req.file) {
      return res.status(400).json({ error: 'No PDF file provided' });
    }

    const storeInDb = req.query.store_in_db !== 'false'; // Default to true
    const filePath = req.file.path;
    const filename = req.file.originalname;
    const fileSize = req.file.size;

    try {
      // Process PDF - replace this with your actual PDF processing logic
      const extractedData = await processPDF(filePath);

      // Add processing metadata
      const processingInfo = {
        filename: filename,
        file_size_bytes: fileSize,
        processed_at: new Date().toISOString(),
        delivery_count: (extractedData.deliveries || []).length
      };

      extractedData.processing_info = processingInfo;

      // Store in database if requested
      let databaseId = null;
      if (storeInDb) {
        try {
          databaseId = await dbService.storeManifest(extractedData, processingInfo);
          extractedData.database_id = databaseId;
          console.log(`✅ Stored manifest ${extractedData.manifest_id} in database with ID ${databaseId}`);
        } catch (dbError) {
          console.error('❌ Failed to store manifest in database:', dbError);
          extractedData.database_error = dbError.message;
        }
      }

      res.json(extractedData);

    } finally {
      // Clean up uploaded file
      try {
        await fs.unlink(filePath);
      } catch (cleanupError) {
        console.warn('⚠️ Failed to clean up uploaded file:', cleanupError);
      }
    }

  } catch (error) {
    console.error('❌ Error processing single PDF:', error);
    
    // Clean up file on error
    if (req.file?.path) {
      try {
        await fs.unlink(req.file.path);
      } catch (cleanupError) {
        console.warn('⚠️ Failed to clean up uploaded file on error:', cleanupError);
      }
    }

    res.status(500).json({ error: `Error processing PDF: ${error.message}` });
  }
});

// Extract from multiple PDFs (batch)
app.post('/extract/batch', upload.array('files'), async (req, res) => {
  try {
    if (!req.files || req.files.length === 0) {
      return res.status(400).json({ error: 'No PDF files provided' });
    }

    const storeInDb = req.query.store_in_db !== 'false'; // Default to true
    const jobId = uuidv4();

    // Initialize job status
    const jobStatus = {
      status: 'processing',
      total_files: req.files.length,
      processed_files: 0,
      results: [],
      errors: [],
      started_at: new Date().toISOString(),
      store_in_db: storeInDb
    };

    batchJobs.set(jobId, jobStatus);

    // Create database batch job record if storing in DB
    if (storeInDb) {
      try {
        await dbService.createBatchJob(jobId, req.files.length);
      } catch (dbError) {
        console.error('❌ Failed to create batch job in database:', dbError);
      }
    }

    // Process files in background
    processBatchInBackground(jobId, req.files, storeInDb);

    res.json({
      job_id: jobId,
      status: 'processing',
      total_files: req.files.length,
      message: 'Batch processing started',
      store_in_db: storeInDb
    });

  } catch (error) {
    console.error('❌ Error starting batch processing:', error);
    res.status(500).json({ error: `Error starting batch processing: ${error.message}` });
  }
});

// Background batch processing function
async function processBatchInBackground(jobId, files, storeInDb) {
  const job = batchJobs.get(jobId);
  let successfulFiles = 0;
  let failedFiles = 0;

  try {
    for (let order = 0; order < files.length; order++) {
      const file = files[order];
      
      try {
        // Process PDF
        const extractedData = await processPDF(file.path);

        // Add processing metadata
        const processingInfo = {
          filename: file.originalname,
          file_size_bytes: file.size,
          processed_at: new Date().toISOString(),
          delivery_count: (extractedData.deliveries || []).length
        };

        extractedData.processing_info = processingInfo;

        // Store in database if requested
        if (storeInDb) {
          try {
            const databaseId = await dbService.storeManifest(extractedData, processingInfo);
            extractedData.database_id = databaseId;

            // Link to batch job
            await dbService.linkBatchManifest(jobId, databaseId, order + 1);
            
            console.log(`✅ Stored manifest ${extractedData.manifest_id} in database`);
          } catch (dbError) {
            console.error('❌ Failed to store manifest in database:', dbError);
            extractedData.database_error = dbError.message;
          }
        }

        job.results.push(extractedData);
        successfulFiles++;

      } catch (processingError) {
        const errorInfo = {
          filename: file.originalname,
          error: processingError.message,
          timestamp: new Date().toISOString()
        };
        job.errors.push(errorInfo);
        failedFiles++;
        console.error(`❌ Error processing ${file.originalname}:`, processingError);
      }

      // Clean up file
      try {
        await fs.unlink(file.path);
      } catch (cleanupError) {
        console.warn(`⚠️ Failed to clean up ${file.originalname}:`, cleanupError);
      }

      job.processed_files++;
    }

    // Mark job as completed
    job.status = 'completed';
    job.completed_at = new Date().toISOString();

    // Update database batch job
    if (storeInDb) {
      try {
        await dbService.updateBatchJob(jobId, {
          status: 'completed',
          processedFiles: files.length,
          successfulFiles: successfulFiles,
          failedFiles: failedFiles,
          results: job.results,
          errors: job.errors
        });
      } catch (dbError) {
        console.error('❌ Failed to update batch job in database:', dbError);
      }
    }

    console.log(`✅ Batch job ${jobId} completed: ${successfulFiles} successful, ${failedFiles} failed`);

  } catch (error) {
    job.status = 'failed';
    job.error = error.message;
    job.failed_at = new Date().toISOString();
    console.error(`❌ Batch job ${jobId} failed:`, error);

    // Update database batch job
    if (storeInDb) {
      try {
        await dbService.updateBatchJob(jobId, {
          status: 'failed',
          errorMessage: error.message
        });
      } catch (dbError) {
        console.error('❌ Failed to update failed batch job in database:', dbError);
      }
    }
  }
}

// Get batch job status
app.get('/batch/status/:jobId', async (req, res) => {
  try {
    const { jobId } = req.params;

    // First check in-memory store
    if (batchJobs.has(jobId)) {
      return res.json(batchJobs.get(jobId));
    }

    // Check database
    try {
      const dbJob = await dbService.getBatchJob(jobId);
      if (dbJob) {
        return res.json({
          job_id: jobId,
          status: dbJob.status,
          total_files: dbJob.total_files,
          processed_files: dbJob.processed_files,
          successful_files: dbJob.successful_files,
          failed_files: dbJob.failed_files,
          started_at: dbJob.started_at,
          completed_at: dbJob.completed_at,
          source: 'database'
        });
      }
    } catch (dbError) {
      console.error('❌ Error retrieving batch job from database:', dbError);
    }

    res.status(404).json({ error: 'Job ID not found' });

  } catch (error) {
    console.error('❌ Error getting batch status:', error);
    res.status(500).json({ error: 'Error retrieving batch status' });
  }
});

// Get batch job results
app.get('/batch/results/:jobId', async (req, res) => {
  try {
    const { jobId } = req.params;

    // First check in-memory store
    if (batchJobs.has(jobId)) {
      const job = batchJobs.get(jobId);
      if (job.status !== 'completed') {
        return res.status(400).json({
          error: `Job is not completed. Current status: ${job.status}`
        });
      }
      return res.json({
        job_id: jobId,
        results: job.results,
        summary: {
          total_files: job.total_files,
          successful: job.results.length,
          failed: job.errors.length,
          errors: job.errors
        }
      });
    }

    // Check database
    try {
      const dbJob = await dbService.getBatchJob(jobId);
      if (!dbJob) {
        return res.status(404).json({ error: 'Job ID not found' });
      }

      if (dbJob.status !== 'completed') {
        return res.status(400).json({
          error: `Job is not completed. Current status: ${dbJob.status}`
        });
      }

      return res.json({
        job_id: jobId,
        results: dbJob.results || [],
        summary: {
          total_files: dbJob.total_files,
          successful: dbJob.successful_files,
          failed: dbJob.failed_files,
          errors: dbJob.errors || []
        },
        source: 'database'
      });

    } catch (dbError) {
      console.error('❌ Error retrieving batch results from database:', dbError);
      return res.status(500).json({ error: 'Error retrieving batch results' });
    }

  } catch (error) {
    console.error('❌ Error getting batch results:', error);
    res.status(500).json({ error: 'Error retrieving batch results' });
  }
});

// Search manifests
app.get('/manifests', async (req, res) => {
  try {
    const filters = {
      dateFrom: req.query.date_from,
      dateTo: req.query.date_to,
      driver: req.query.driver,
      depotPostcode: req.query.depot_postcode,
      limit: parseInt(req.query.limit) || 100,
      offset: parseInt(req.query.offset) || 0
    };

    const results = await dbService.searchManifests(filters);

    res.json({
      manifests: results,
      total_returned: results.length,
      filters: {
        date_from: filters.dateFrom,
        date_to: filters.dateTo,
        driver: filters.driver,
        depot_postcode: filters.depotPostcode
      },
      pagination: {
        limit: filters.limit,
        offset: filters.offset
      }
    });

  } catch (error) {
    console.error('❌ Error retrieving manifest:', error);
    res.status(500).json({ error: 'Error retrieving manifest' });
  }
});