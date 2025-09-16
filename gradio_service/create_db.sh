export db_name='omnias_db'
export db_user='admin'
export db_password='12345'

echo "Start creating DB user $db_user..."
sudo -u postgres psql -c "CREATE USER $db_user WITH PASSWORD '$db_password';"
echo "User $db_user successfully created"
sleep 2

echo "Start creating DB $db_name..."
sudo -u postgres psql -c "CREATE DATABASE $db_name OWNER $db_user;"
echo "DB $db_name successfully created"
sleep 2

export create_documents_table_query="
CREATE TABLE documents (
    user_id SERIAL PRIMARY KEY,
    user_name VARCHAR NOT NULL,
    user_password VARCHAR NOT NULL,
    video_path TEXT,
    doc_contents TEXT,
    doc_summaries TEXT
);"
export create_summary_table_query="
CREATE TABLE summaries (
    doc_id SERIAL PRIMARY KEY,
    user_name VARCHAR NOT NULL,
    doc_name VARCHAR NOT NULL,
    doc_title TEXT,
    doc_summary TEXT
);"

export CREATE_QUERIES=("$create_documents_table_query" "$create_summary_table_query")
export TABLES=("documents" "summaries")
export PRIMARY_KEYS=("user_id" "doc_id")

for (( i=0; i<2; i++ ))
do
echo "Start creating ${TABLES[i]} table..."
sudo -u postgres psql -d $db_name -c "${CREATE_QUERIES[i]}"
sudo -u postgres psql -d $db_name -c "GRANT SELECT, INSERT, UPDATE ON ${TABLES[i]} TO $db_user;"
sudo -u postgres psql -d $db_name -c "GRANT USAGE, SELECT ON SEQUENCE ${TABLES[i]}_${PRIMARY_KEYS[i]}_seq TO $db_user;"
echo "Table ${TABLES[i]} successfully created"
sleep 2
done

echo "Start test... Enter password $db_password for user $db_user."
# Тестовый запрос
psql -h localhost -p 5432 -d $db_name -U admin -c "SELECT * FROM documents"
psql -h localhost -p 5432 -d $db_name -U admin -c "SELECT * FROM summaries"
echo "The DB $db_name has been successfully created and configured."
