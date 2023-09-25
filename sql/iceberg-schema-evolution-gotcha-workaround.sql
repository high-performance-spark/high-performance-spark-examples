DROP TABLE IF EXISTS local.udevelopers_sorted;
CREATE TABLE IF NOT EXISTS local.udevelopers_sorted (
       username string,
       firstname string,
       lastname string)
USING ICEBERG;
ALTER TABLE local.udevelopers_sorted WRITE ORDERED BY lastname;
INSERT INTO local.udevelopers_sorted VALUES("krisnova", "Kris", "Nova");
SELECT * FROM local.udevelopers_sorted;
ALTER TABLE local.udevelopers_sorted WRITE ORDERED BY username;
-- Hack, add it to identifier fields so we can do a "partial" drop where it stays in the schema and we don't
-- corrupt the metadata.
ALTER TABLE local.udevelopers_sorted ADD PARTITION FIELD lastname;
ALTER TABLE local.udevelopers_sorted DROP PARTITION FIELD lastname;
SELECT * FROM local.udevelopers_sorted;
