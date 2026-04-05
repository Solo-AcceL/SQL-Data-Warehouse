-- Create the 'data warehouse' database
CREATE DATABASE IF NOT EXISTS data_warehouse;

-- Initialize the database to be used
USE data_warehouse;

-- Creating Schemas
CREATE SCHEMA Bronze;

CREATE SCHEMA Silver;

CREATE SCHEMA Gold;
