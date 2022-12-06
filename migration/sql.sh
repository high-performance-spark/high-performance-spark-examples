pip install sqlfluff
python -m pip install 'sqlfluff-plugin-sparksql-upgrade @ git+https://github.com/holdenk/spark-upgrade#subdirectory=sql'

sqlfluff rules |grep -i spark
sqlfluff fix --dialect sparksql  farts.sql
