DROP TABLE IF EXISTS local.udevelopers_sorted;
CREATE TABLE IF NOT EXISTS local.udevelopers_sorted (
       username string,
       firstname string,
       lastname string)
USING ICEBERG;
ALTER TABLE local.udevelopers_sorted WRITE ORDERED BY lastname;
INSERT INTO local.udevelopers_sorted VALUES("krisnova", "Kris", "Nova");
ALTER TABLE local.udevelopers_sorted RENAME COLUMN lastname TO deprecated_lastname;
SELECT * FROM local.udevelopers_sorted;
ALTER TABLE local.udevelopers_sorted DROP COLUMN lastname;
SELECT * FROM local.udevelopers_sorted;

