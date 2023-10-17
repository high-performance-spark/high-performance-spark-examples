DROP TABLE IF EXISTS local.wap_projects;
CREATE TABLE local.wap_projects (
       creator string,
       projectname string)
USING iceberg
PARTITIONED BY (creator);
ALTER TABLE local.projects SET TBLPROPERTIES (
    'write.wap.enabled''true'
);
-- We need a first commit, see https://github.com/apache/iceberg/issues/8849
INSERT INTO local.wap_projects VALUES("holdenk", "spark");
ALTER TABLE local.wap_projects DROP BRANCH IF EXISTS `audit-branch`;
ALTER TABLE local.wap_projects CREATE BRANCH `audit-branch`;
SET spark.wap.branch = 'audit-branch';
INSERT INTO local.projects VALUES("krisnova", "aurae");
SELECT count(*) FROM local.wap_projects VERSION AS OF 'audit-branch' WHERE creator is NULL;
SELECT count(*) FROM local.wap_projects VERSION AS OF 'audit-branch' WHERE creator == "krisnova";
CALL local.system.remove_orphan_files(table => 'local.wap_projects');
CALL local.system.fast_forward("local.wap_projects", "main", "audit-branch");
