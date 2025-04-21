-- ============================================================
-- Check and clean OFFLINE Prefect workers from the database
--
-- ⚠️ WARNING: This will permanently delete worker records from
-- the Prefect database. Ensure these are not intended to
-- reconnect or be used in health checks before deletion.
-- ============================================================

-- View all OFFLINE workers
SELECT *
FROM prefect.public.worker
WHERE status = 'OFFLINE';

-- Delete all OFFLINE workers
DELETE FROM prefect.public.worker
WHERE status = 'OFFLINE';
