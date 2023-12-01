from PGConn import PGConn, TableManager
from dotenv import dotenv_values
from datetime import datetime
import json

# 
config = dotenv_values('.env')
host = config.get('POSTGRES_HOST')
database = config.get('POSTGRES_DATABASE')
user = config.get('POSTGRES_USER')
password = config.get('POSTGRES_PASSWORD')

# 
pg_conn = PGConn(host=host, database=database, user=user, password=password)

# #
# postgres_conn.create_audit_table()

# Check and create tables if they don't exist
TableManager.create_missing_tables(pg_conn, 'backup_history', PGConn.backup_history)
TableManager.create_missing_tables(pg_conn, 'audit_table', PGConn.audit_history)
TableManager.create_missing_tables(pg_conn, 'pre_refresh_checks', PGConn.pre_refresh_checks)
TableManager.create_missing_tables(pg_conn, 'post_refresh_checks', PGConn.post_refresh_checks)


# Inserting into backup_history
data = (
    '123456',
    'user123',
    'backup',
    'MySQL',
    'my_database_backup',
    datetime.now(),
    datetime.now(),
    1024,
    '/backup/my_database_backup.sql',
    '/restore',
    'Success',
    '',
    json.dumps({'info': 'backup details'})  # Convert the dict to a JSON string
)

pg_conn.insert_backup_history(data)

# Close the connection when done
pg_conn.close()
