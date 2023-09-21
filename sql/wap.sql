CREATE TABLE IF NOT EXISTS local.projects (
       creator string,
       projectname string)
USING iceberg
PARTITIONED BY (creator);
ALTER TABLE local.projects SET TBLPROPERTIES (
    'write.wap.enabled''true'
);
ALTER TABLE local.projects CREATE BRANCH IF NOT EXISTS `audit`;
SET spark.wap.branch = 'branch';
INSERT INTO local.projects VALUES("krisnova", "aurae");
SELECT count(*) FROM local.projects VERSION AS OF 'audit' WHERE creator is NULL;
-- This does not work until we upgrade to 3.5 + Iceberg 1.4.
-- CALL local.system.fastForward("local.projects", "main", "audit-branch");
