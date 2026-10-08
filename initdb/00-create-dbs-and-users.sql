-- Databases
CREATE DATABASE nessie_db;
CREATE DATABASE airflow;

-- Users (roles)
CREATE USER nessie_user   WITH ENCRYPTED PASSWORD 'password';
CREATE USER airflow       WITH ENCRYPTED PASSWORD 'airflow';

-- Make each user own its DB (so they can create schemas, etc.)
ALTER DATABASE nessie_db     OWNER TO nessie_user;
ALTER DATABASE airflow  OWNER TO airflow;

-- Optional: default privileges inside each DB
\connect nessie_db
GRANT CREATE, TEMP ON DATABASE nessie_db TO nessie_user;
GRANT ALL ON SCHEMA public TO nessie_user;

\connect airflow
GRANT CREATE, TEMP ON DATABASE airflow TO airflow;
GRANT ALL ON SCHEMA public TO airflow;
