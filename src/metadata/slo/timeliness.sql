SELECT *
FROM system.lakeflow.job_run_timeline
WHERE job_id = :full_medallion_job_id
