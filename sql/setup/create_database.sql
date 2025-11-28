-- Snowflake Database Setup Script
-- This script creates the database, schemas, and warehouses for the data engineering project

-- ============================================
-- STEP 1: Create Database
-- ============================================
CREATE DATABASE IF NOT EXISTS DATA_ENGINEERING_DB;

USE DATABASE DATA_ENGINEERING_DB;

-- ============================================
-- STEP 2: Create Schemas
-- ============================================

-- Raw data layer (landing zone)
CREATE SCHEMA IF NOT EXISTS RAW;

-- Staging layer (cleaned data)
CREATE SCHEMA IF NOT EXISTS STAGING;

-- Intermediate layer (business logic)
CREATE SCHEMA IF NOT EXISTS INTERMEDIATE;

-- Analytics layer (final tables for reporting)
CREATE SCHEMA IF NOT EXISTS ANALYTICS;

-- ============================================
-- STEP 3: Create Warehouse
-- ============================================
CREATE WAREHOUSE IF NOT EXISTS DATA_ENGINEERING_WH
    WITH WAREHOUSE_SIZE = 'X-SMALL'
    AUTO_SUSPEND = 60
    AUTO_RESUME = TRUE
    INITIALLY_SUSPENDED = TRUE;

-- ============================================
-- STEP 4: Grant Permissions (adjust as needed)
-- ============================================
-- GRANT USAGE ON DATABASE DATA_ENGINEERING_DB TO ROLE <your_role>;
-- GRANT ALL ON SCHEMA RAW TO ROLE <your_role>;
-- GRANT ALL ON SCHEMA STAGING TO ROLE <your_role>;
-- GRANT ALL ON SCHEMA INTERMEDIATE TO ROLE <your_role>;
-- GRANT ALL ON SCHEMA ANALYTICS TO ROLE <your_role>;
