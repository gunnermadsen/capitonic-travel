\set ON_ERROR_STOP on
-- Infrastructure-owned roles/databases only. Application tables belong to migrations.
CREATE ROLE airline_migrator LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION;
CREATE ROLE airline_runtime LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION;
CREATE ROLE polaris_owner LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION;
CREATE ROLE polaris_runtime LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION;
CREATE DATABASE airline;
CREATE DATABASE polaris;
REVOKE ALL ON DATABASE airline FROM PUBLIC;
REVOKE ALL ON DATABASE polaris FROM PUBLIC;
GRANT CONNECT ON DATABASE airline TO airline_migrator, airline_runtime;
GRANT CONNECT ON DATABASE polaris TO polaris_owner, polaris_runtime;
\connect airline
REVOKE ALL ON SCHEMA public FROM PUBLIC;
CREATE EXTENSION timescaledb;
CREATE SCHEMA airline AUTHORIZATION airline_migrator;
GRANT USAGE ON SCHEMA airline TO airline_runtime;
ALTER ROLE airline_migrator IN DATABASE airline SET search_path = airline, public;
ALTER ROLE airline_runtime IN DATABASE airline SET search_path = airline, public;
-- Table-specific runtime grants are the responsibility of later migrations.
\connect polaris
REVOKE ALL ON SCHEMA public FROM PUBLIC;
CREATE SCHEMA polaris_schema AUTHORIZATION polaris_owner;
GRANT USAGE ON SCHEMA polaris_schema TO polaris_runtime;
ALTER ROLE polaris_owner IN DATABASE polaris SET search_path = polaris_schema;
ALTER ROLE polaris_runtime IN DATABASE polaris SET search_path = polaris_schema;
ALTER DEFAULT PRIVILEGES FOR ROLE polaris_owner IN SCHEMA polaris_schema
    GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO polaris_runtime;
ALTER DEFAULT PRIVILEGES FOR ROLE polaris_owner IN SCHEMA polaris_schema
    GRANT USAGE, SELECT ON SEQUENCES TO polaris_runtime;
