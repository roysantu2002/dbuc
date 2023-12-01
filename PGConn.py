# PGConn.py

import psycopg2
from dotenv import dotenv_values
from typing import Optional
from typing import Callable, Optional
from typing import Optional, Tuple
import json

class PGConn:
    def __init__(self, host: str, database: str, user: str, password: Optional[str] = None):
        self.connection = None
        self.host = host
        self.database = database
        self.user = user
        self.password = password

        try:
            self.connect()
        except Exception as e:
            print(f"Error: {e}")

    def connect(self):
        try:
            self.connection = psycopg2.connect(
                host=self.host,
                database=self.database,
                user=self.user,
                password=self.password
            )
            print("Connected to PostgreSQL!")
        except psycopg2.Error as e:
            print(f"Error connecting to PostgreSQL: {e}")

    #backup
    def backup_history(self):
        try:
            cursor = self.connection.cursor()
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS backup_history (
                    uuid VARCHAR(100),
                    user_id VARCHAR(50),
                    action VARCHAR(100),
                    db_type VARCHAR(20),
                    backup_name VARCHAR(100),
                    backup_start_time TIMESTAMP,
                    backup_end_time TIMESTAMP,
                    backup_size BIGINT,
                    backup_path TEXT,
                    restore_path VARCHAR(100),
                    backup_status VARCHAR(100),
                    backup_error_message TEXT,
                    backup_details JSONB,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
            ''')
            self.connection.commit()
            print("Backup history table created.")
        except psycopg2.Error as e:
            print(f"Error creating backup_history table: {e}")

    #Audit     
    def audit_history(self):
        try:
            cursor = self.connection.cursor()
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS audit_table (
                    uuid VARCHAR(100),
                    user_id VARCHAR(50),
                    action VARCHAR(100),
                    action_timestamp TIMESTAMP,
                    success BOOLEAN,
                    details JSONB
                );
            ''')
            self.connection.commit()
            print("Audit table created.")
        except psycopg2.Error as e:
            print(f"Error creating audit_table: {e}")

    #pre_refresh_checks
    def pre_refresh_checks(self):
        try:
            cursor = self.connection.cursor()
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS pre_refresh_checks (
                    uuid VARCHAR(100),
                    user_id VARCHAR(50),
                    action VARCHAR(100),
                    action_timestamp TIMESTAMP,
                    success BOOLEAN,
                    details JSONB
                );
            ''')
            self.connection.commit()
            print("pre_refresh_checks table created.")
        except psycopg2.Error as e:
            print(f"Error creating audit_table: {e}")


    #
    def post_refresh_checks(self):
        try:
            cursor = self.connection.cursor()
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS post_refresh_checks (
                    uuid VARCHAR(100),
                    user_id VARCHAR(50),
                    action VARCHAR(100),
                    action_timestamp TIMESTAMP,
                    success BOOLEAN,
                    details JSONB
                );
            ''')
            self.connection.commit()
            print("post_refresh_checks table created.")
        except psycopg2.Error as e:
            print(f"Error creating audit_table: {e}")

    #insert
    def insert_backup_history(self, data: Tuple):
        try:
            cursor = self.connection.cursor()
            cursor.execute('''
                INSERT INTO backup_history
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ''', data)
            self.connection.commit()
            print("Inserted into backup_history.")
        except psycopg2.Error as e:
            print(f"Error inserting into backup_history: {e}")

    def insert_audit_history(self, data: Tuple):
        try:
            cursor = self.connection.cursor()
            cursor.execute('''
                INSERT INTO audit_table
                VALUES (%s, %s, %s, %s, %s)
            ''', data)
            self.connection.commit()
            print("Inserted into audit_table.")
        except psycopg2.Error as e:
            print(f"Error inserting into audit_table: {e}")

    def insert_pre_refresh_checks(self, data: Tuple):
        try:
            cursor = self.connection.cursor()
            cursor.execute('''
                INSERT INTO pre_refresh_checks
                VALUES (%s, %s, %s, %s, %s)
            ''', data)
            self.connection.commit()
            print("Inserted into pre_refresh_checks.")
        except psycopg2.Error as e:
            print(f"Error inserting into pre_refresh_checks: {e}")

    def insert_post_refresh_checks(self, data: Tuple):
        try:
            cursor = self.connection.cursor()
            cursor.execute('''
                INSERT INTO post_refresh_checks
                VALUES (%s, %s, %s, %s, %s)
            ''', data)
            self.connection.commit()
            print("Inserted into post_refresh_checks.")
        except psycopg2.Error as e:
            print(f"Error inserting into post_refresh_checks: {e}")

    def close(self):
        if self.connection:
            self.connection.close()
            print("PostgreSQL connection closed.")

#table manager class

class TableManager:
    @staticmethod
    def create_missing_tables(pg_conn_instance: PGConn, table_name: str, ddl_function: Callable):
        try:
            cursor = pg_conn_instance.connection.cursor()
            cursor.execute(f"SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = '{table_name}')")
            table_exists = cursor.fetchone()[0]
            
            if not table_exists:
                ddl_function(pg_conn_instance)
        except psycopg2.Error as e:
            print(f"Error checking table existence: {e}")