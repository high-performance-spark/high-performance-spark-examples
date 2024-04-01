CREATE TABLE IF NOT EXISTS local.udevelopers (
       username string,
       firstname string,
       lastname string)
USING iceberg;
CREATE TABLE IF NOT EXISTS local.uprojects (
       creator string,
       uprojectname string)
USING iceberg;
INSERT INTO local.udevelopers VALUES("krisnova", "Kris", "Nova");
INSERT INTO local.uprojects VALUES("krisnova", "aurae");
SELECT * FROM local.udevelopers INNER JOIN local.uprojects ON local.uprojects.creator = local.udevelopers.username;
