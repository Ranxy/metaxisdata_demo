-- Airflow keeps its own metadata in its own database on the platform's store server.
--
-- This runs only when the store's data directory is empty, so a fresh volume needs no manual
-- step while an existing volume keeps whatever it already has.
-- `\gexec` builds the statement from the connection's own user, so overriding STORE_USER in
-- .env does not break it.
SELECT format('CREATE DATABASE airflow OWNER %I', current_user) \gexec
