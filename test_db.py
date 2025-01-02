import psycopg2
import os

# Your Supabase credentials with direct-connection string
db_params = {
    'host': 'db.joutwesolkfsyogtvpji.supabase.co',
    'database': 'postgres',
    'user': 'postgres',
    'password': 't^6$1dVGgYH8',
    'port': '5432',
    'sslmode': 'require'
}

try:
    conn = psycopg2.connect(**db_params)
    print("Successfully connected to database!")
    conn.close()
except Exception as e:
    print(f"Connection failed: {str(e)}") 