CREATE TABLE IF NOT EXISTS local.developers (
       username string,
       firstname string,
       lastname string)
USING iceberg
PARTITIONED BY (username);
CREATE TABLE IF NOT EXISTS local.projects (
       creator string,
       projectname string)
USING iceberg
PARTITIONED BY (creator);
INSERT INTO local.developers VALUES("krisnova", "Kris", "Nova");
INSERT INTO local.projects VALUES("krisnova", "aurae");
SELECT * FROM local.developers INNER JOIN local.projects ON local.projects.creator = local.developers.username;
