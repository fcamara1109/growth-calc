import os
import uuid
from dotenv import load_dotenv
import psycopg2
from psycopg2.extras import RealDictCursor

load_dotenv()

class SessionManager:
    def __init__(self):
        self.session_id = str(uuid.uuid4()).replace('-', '_')

def get_db_connection():
    """Get database connection using environment variables"""
    # Try Supabase/production connection first
    if os.getenv('DB_HOST'):
        conn = psycopg2.connect(
            host=os.getenv('DB_HOST'),
            database=os.getenv('DB_NAME'),
            user=os.getenv('DB_USER'),
            password=os.getenv('DB_PASSWORD'),
            port=os.getenv('DB_PORT'),
            sslmode='require',
            cursor_factory=RealDictCursor
        )
        return conn
    
    # Fallback to local development connection
    try:
        return psycopg2.connect(
            host='localhost',
            database='database',  # Changed from fcamara to database
            user='fcamara',
            port=5432,
            cursor_factory=RealDictCursor
        )
    except psycopg2.Error:
        # Final fallback with default PostgreSQL settings
        return psycopg2.connect(
            host='localhost',
            database='postgres',  # Using default postgres database
            user='postgres',
            port=5432,
            cursor_factory=RealDictCursor
        )

def init_session_tables(session_id):
    """Initialize temporary tables for this session"""
    conn = get_db_connection()
    cur = conn.cursor()
    
    try:
        # Create temporary revenue table for this session
        cur.execute(f"""
            DROP TABLE IF EXISTS revenue_data_{session_id};
            
            CREATE UNLOGGED TABLE revenue_data_{session_id} (
                id SERIAL PRIMARY KEY,
                transaction_date DATE NOT NULL,
                transaction_id VARCHAR(255) NOT NULL UNIQUE,
                revenue DECIMAL(10,2) NOT NULL,
                user_id VARCHAR(255) NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            
            CREATE INDEX IF NOT EXISTS idx_revenue_date_{session_id} 
                ON revenue_data_{session_id}(transaction_date);
            CREATE INDEX IF NOT EXISTS idx_revenue_user_{session_id} 
                ON revenue_data_{session_id}(user_id);
        """)
        conn.commit()
    finally:
        cur.close()
        conn.close()

def load_query(filename, session_id):
    """Load SQL query from file and replace table names with session-specific ones"""
    with open(f'sql/{filename}.sql', 'r') as f:
        query = f.read()
        return query.replace('revenue_data', f'revenue_data_{session_id}')

def table_exists(cur, table_name):
    """Check if a temporary table exists"""
    cur.execute("""
        SELECT EXISTS (
            SELECT FROM pg_tables 
            WHERE tablename = %s
        );
    """, (table_name,))
    return cur.fetchone()['exists']

def get_mau_data(session_id, start_date, end_date):
    """Get Monthly Active Users data"""
    conn = get_db_connection()
    cur = conn.cursor()
    
    try:
        table_name = f'revenue_data_{session_id}'
        if not table_exists(cur, table_name):
            return []
            
        # First create daily revenue data
        cur.execute(f"""
        CREATE TEMP TABLE daily_rev_{session_id} AS
        """ + load_query('daily_rev', session_id))
        conn.commit()
        
        # Then run MAU query with date filter
        mau_query = load_query('mau', session_id).replace('daily_rev', f'daily_rev_{session_id}')
        mau_query = mau_query.replace(
            'where month <=',
            'where month between %s and %s and month <='
        )
        
        cur.execute(mau_query, (start_date, end_date))
        result = cur.fetchall()
        
        # Clean up temp table
        cur.execute(f"DROP TABLE IF EXISTS daily_rev_{session_id}")
        conn.commit()
        
        return result
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        cur.close()
        conn.close()

def get_wau_data(session_id, start_date, end_date):
    """Get Weekly Active Users data"""
    conn = get_db_connection()
    cur = conn.cursor()
    
    try:
        table_name = f'revenue_data_{session_id}'
        if not table_exists(cur, table_name):
            return []
            
        # First create daily revenue data
        cur.execute(f"""
        CREATE TEMP TABLE daily_rev_{session_id} AS
        """ + load_query('daily_rev', session_id))
        conn.commit()
        
        # Then run WAU query with date filter
        wau_query = load_query('wau', session_id).replace('daily_rev', f'daily_rev_{session_id}')
        wau_query = wau_query.replace(
            'where week <=',
            'where week between %s and %s and week <='
        )
        
        cur.execute(wau_query, (start_date, end_date))
        result = cur.fetchall()
        
        # Clean up temp table
        cur.execute(f"DROP TABLE IF EXISTS daily_rev_{session_id}")
        conn.commit()
        
        return result
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        cur.close()
        conn.close()

def get_dau_data(session_id, start_date, end_date):
    """Get Daily Active Users data"""
    conn = get_db_connection()
    cur = conn.cursor()
    
    try:
        table_name = f'revenue_data_{session_id}'
        if not table_exists(cur, table_name):
            return []
            
        # First create daily revenue data
        cur.execute(f"""
        CREATE TEMP TABLE daily_rev_{session_id} AS
        """ + load_query('daily_rev', session_id))
        conn.commit()
        
        # Then run DAU query with date filter
        dau_query = load_query('dau', session_id).replace('daily_rev', f'daily_rev_{session_id}')
        dau_query = dau_query.replace(
            'where day <=',
            'where day between %s and %s and day <='
        )
        
        cur.execute(dau_query, (start_date, end_date))
        result = cur.fetchall()
        
        # Clean up temp table
        cur.execute(f"DROP TABLE IF EXISTS daily_rev_{session_id}")
        conn.commit()
        
        return result
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        cur.close()
        conn.close()

def get_monthly_retention_rates(session_id, start_date, end_date):
    """Get monthly retention rates data"""
    conn = get_db_connection()
    cur = conn.cursor()
    
    try:
        table_name = f'revenue_data_{session_id}'
        if not table_exists(cur, table_name):
            return []
            
        # First create daily revenue data
        cur.execute(f"""
        CREATE TEMP TABLE daily_rev_{session_id} AS
        """ + load_query('daily_rev', session_id))
        conn.commit()
        
        # Get MAU query
        mau_query = load_query('mau', session_id).replace('daily_rev', f'daily_rev_{session_id}')
        
        # Then run retention rates query with MAU data
        retention_query = load_query('monthly_retention_timeseries', session_id)
        retention_query = retention_query.format(mau_query=mau_query)
        retention_query = retention_query.replace(
            'WHERE month <=',
            'WHERE month BETWEEN %s AND %s AND month <='
        )
        
        cur.execute(retention_query, (start_date, end_date))
        result = cur.fetchall()
        
        # Clean up temp table
        cur.execute(f"DROP TABLE IF EXISTS daily_rev_{session_id}")
        conn.commit()
        
        return result
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        cur.close()
        conn.close()

def get_weekly_retention_rates(session_id, start_date, end_date):
    """Get weekly retention rates data"""
    conn = get_db_connection()
    cur = conn.cursor()
    
    try:
        table_name = f'revenue_data_{session_id}'
        if not table_exists(cur, table_name):
            return []
            
        # First create daily revenue data
        cur.execute(f"""
        CREATE TEMP TABLE daily_rev_{session_id} AS
        """ + load_query('daily_rev', session_id))
        conn.commit()
        
        # Get WAU query
        wau_query = load_query('wau', session_id).replace('daily_rev', f'daily_rev_{session_id}')
        
        # Then run retention rates query with WAU data
        retention_query = load_query('weekly_retention_timeseries', session_id)
        retention_query = retention_query.format(wau_query=wau_query)
        retention_query = retention_query.replace(
            'WHERE week <=',
            'WHERE week BETWEEN %s AND %s AND week <='
        )
        
        cur.execute(retention_query, (start_date, end_date))
        result = cur.fetchall()
        
        # Clean up temp table
        cur.execute(f"DROP TABLE IF EXISTS daily_rev_{session_id}")
        conn.commit()
        
        return result
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        cur.close()
        conn.close()

def get_daily_retention_rates(session_id, start_date, end_date):
    """Get daily retention rates data"""
    conn = get_db_connection()
    cur = conn.cursor()
    
    try:
        table_name = f'revenue_data_{session_id}'
        if not table_exists(cur, table_name):
            return []
        
        # First create daily revenue data
        cur.execute(f"""
        CREATE TEMP TABLE daily_rev_{session_id} AS
        """ + load_query('daily_rev', session_id))
        conn.commit()
        
        # Get DAU query
        dau_query = load_query('dau', session_id).replace('daily_rev', f'daily_rev_{session_id}')
        
        # Then run retention rates query with DAU data
        retention_query = load_query('daily_retention_timeseries', session_id)
        retention_query = retention_query.format(dau_query=dau_query)
        retention_query = retention_query.replace(
            'WHERE day <=',
            'WHERE day BETWEEN %s AND %s AND day <='
        )
        
        cur.execute(retention_query, (start_date, end_date))
        result = cur.fetchall()
        
        # Clean up temp table
        cur.execute(f"DROP TABLE IF EXISTS daily_rev_{session_id}")
        conn.commit()
        
        return result
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        cur.close()
        conn.close()

def get_monthly_quick_ratio(session_id, start_date, end_date):
    """Get monthly quick ratio data"""
    conn = get_db_connection()
    cur = conn.cursor()
    
    try:
        table_name = f'revenue_data_{session_id}'
        if not table_exists(cur, table_name):
            return []
            
        # First create daily revenue data
        cur.execute(f"""
        CREATE TEMP TABLE daily_rev_{session_id} AS
        """ + load_query('daily_rev', session_id))
        conn.commit()
        
        # Get MAU query
        mau_query = load_query('mau', session_id).replace('daily_rev', f'daily_rev_{session_id}')
        
        # Then run quick ratio query with MAU data
        quick_ratio_query = load_query('monthly_quick_ratio', session_id)
        quick_ratio_query = quick_ratio_query.format(mau_query=mau_query)
        quick_ratio_query = quick_ratio_query.replace(
            'WHERE month <=',
            'WHERE month BETWEEN %s AND %s AND month <='
        )
        
        cur.execute(quick_ratio_query, (start_date, end_date))
        result = cur.fetchall()
        
        # Clean up temp table
        cur.execute(f"DROP TABLE IF EXISTS daily_rev_{session_id}")
        conn.commit()
        
        return result
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        cur.close()
        conn.close()

def get_weekly_quick_ratio(session_id, start_date, end_date):
    """Get weekly quick ratio data"""
    conn = get_db_connection()
    cur = conn.cursor()
    
    try:
        table_name = f'revenue_data_{session_id}'
        if not table_exists(cur, table_name):
            return []
            
        # First create daily revenue data
        cur.execute(f"""
        CREATE TEMP TABLE daily_rev_{session_id} AS
        """ + load_query('daily_rev', session_id))
        conn.commit()
        
        # Get WAU query
        wau_query = load_query('wau', session_id).replace('daily_rev', f'daily_rev_{session_id}')
        
        # Then run quick ratio query with WAU data
        quick_ratio_query = load_query('weekly_quick_ratio', session_id)
        quick_ratio_query = quick_ratio_query.format(wau_query=wau_query)
        quick_ratio_query = quick_ratio_query.replace(
            'WHERE week <=',
            'WHERE week BETWEEN %s AND %s AND week <='
        )
        
        cur.execute(quick_ratio_query, (start_date, end_date))
        result = cur.fetchall()
        
        # Clean up temp table
        cur.execute(f"DROP TABLE IF EXISTS daily_rev_{session_id}")
        conn.commit()
        
        return result
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        cur.close()
        conn.close()

def get_daily_quick_ratio(session_id, start_date, end_date):
    """Get daily quick ratio data"""
    conn = get_db_connection()
    cur = conn.cursor()
    
    try:
        table_name = f'revenue_data_{session_id}'
        if not table_exists(cur, table_name):
            return []
            
        # First create daily revenue data
        cur.execute(f"""
        CREATE TEMP TABLE daily_rev_{session_id} AS
        """ + load_query('daily_rev', session_id))
        conn.commit()
        
        # Get DAU query
        dau_query = load_query('dau', session_id).replace('daily_rev', f'daily_rev_{session_id}')
        
        # Then run quick ratio query with DAU data
        quick_ratio_query = load_query('daily_quick_ratio', session_id)
        quick_ratio_query = quick_ratio_query.format(dau_query=dau_query)
        quick_ratio_query = quick_ratio_query.replace(
            'WHERE day <=',
            'WHERE day BETWEEN %s AND %s AND day <='
        )
        
        cur.execute(quick_ratio_query, (start_date, end_date))
        result = cur.fetchall()
        
        # Clean up temp table
        cur.execute(f"DROP TABLE IF EXISTS daily_rev_{session_id}")
        conn.commit()
        
        return result
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        cur.close()
        conn.close()

def get_mrr_data(session_id, start_date, end_date):
    """Get Monthly Recurring Revenue data"""
    conn = get_db_connection()
    cur = conn.cursor()
    
    try:
        table_name = f'revenue_data_{session_id}'
        if not table_exists(cur, table_name):
            return []
            
        # First create daily revenue data
        cur.execute(f"""
        CREATE TEMP TABLE daily_rev_{session_id} AS
        """ + load_query('daily_rev', session_id))
        conn.commit()
        
        # Then run MRR query with date filter
        mrr_query = load_query('mrr', session_id).replace('daily_rev', f'daily_rev_{session_id}')
        mrr_query = mrr_query.replace(
            'where month <=',
            'where month between %s and %s and month <='
        )
        
        cur.execute(mrr_query, (start_date, end_date))
        result = cur.fetchall()
        
        # Clean up temp table
        cur.execute(f"DROP TABLE IF EXISTS daily_rev_{session_id}")
        conn.commit()
        
        return result
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        cur.close()
        conn.close()

def get_wrr_data(session_id, start_date, end_date):
    """Get Weekly Recurring Revenue data"""
    conn = get_db_connection()
    cur = conn.cursor()
    
    try:
        table_name = f'revenue_data_{session_id}'
        if not table_exists(cur, table_name):
            return []
            
        # First create daily revenue data
        cur.execute(f"""
        CREATE TEMP TABLE daily_rev_{session_id} AS
        """ + load_query('daily_rev', session_id))
        conn.commit()
        
        # Then run WRR query with date filter
        wrr_query = load_query('wrr', session_id).replace('daily_rev', f'daily_rev_{session_id}')
        wrr_query = wrr_query.replace(
            'where week <=',
            'where week between %s and %s and week <='
        )
        
        cur.execute(wrr_query, (start_date, end_date))
        result = cur.fetchall()
        
        # Clean up temp table
        cur.execute(f"DROP TABLE IF EXISTS daily_rev_{session_id}")
        conn.commit()
        
        return result
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        cur.close()
        conn.close()

def get_drr_data(session_id, start_date, end_date):
    """Get Daily Recurring Revenue data"""
    conn = get_db_connection()
    cur = conn.cursor()
    
    try:
        table_name = f'revenue_data_{session_id}'
        if not table_exists(cur, table_name):
            return []
            
        # First create daily revenue data
        cur.execute(f"""
        CREATE TEMP TABLE daily_rev_{session_id} AS
        """ + load_query('daily_rev', session_id))
        conn.commit()
        
        # Then run DRR query with date filter
        drr_query = load_query('drr', session_id).replace('daily_rev', f'daily_rev_{session_id}')
        drr_query = drr_query.replace(
            'where day <=',
            'where day between %s and %s and day <='
        )
        
        cur.execute(drr_query, (start_date, end_date))
        result = cur.fetchall()
        
        # Clean up temp table
        cur.execute(f"DROP TABLE IF EXISTS daily_rev_{session_id}")
        conn.commit()
        
        return result
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        cur.close()
        conn.close()

def get_monthly_revenue_retention_rates(session_id, start_date, end_date):
    """Get monthly revenue retention rates data"""
    conn = get_db_connection()
    cur = conn.cursor()
    
    try:
        table_name = f'revenue_data_{session_id}'
        if not table_exists(cur, table_name):
            return []
            
        # First create daily revenue data
        cur.execute(f"""
        CREATE TEMP TABLE daily_rev_{session_id} AS
        """ + load_query('daily_rev', session_id))
        conn.commit()
        
        # Get MRR query
        mrr_query = load_query('mrr', session_id).replace('daily_rev', f'daily_rev_{session_id}')
        
        # Then run retention rates query with MRR data
        retention_query = load_query('monthly_revenue_retention_timeseries', session_id)
        retention_query = retention_query.format(mrr_query=mrr_query)
        retention_query = retention_query.replace(
            'WHERE month <=',
            'WHERE month BETWEEN %s AND %s AND month <='
        )
        
        cur.execute(retention_query, (start_date, end_date))
        result = cur.fetchall()
        
        # Clean up temp table
        cur.execute(f"DROP TABLE IF EXISTS daily_rev_{session_id}")
        conn.commit()
        
        return result
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        cur.close()
        conn.close()

def get_weekly_revenue_retention_rates(session_id, start_date, end_date):
    """Get weekly revenue retention rates data"""
    conn = get_db_connection()
    cur = conn.cursor()
    
    try:
        table_name = f'revenue_data_{session_id}'
        if not table_exists(cur, table_name):
            return []
            
        # First create daily revenue data
        cur.execute(f"""
        CREATE TEMP TABLE daily_rev_{session_id} AS
        """ + load_query('daily_rev', session_id))
        conn.commit()
        
        # Get WRR query
        wrr_query = load_query('wrr', session_id).replace('daily_rev', f'daily_rev_{session_id}')
        
        # Then run retention rates query with WRR data
        retention_query = load_query('weekly_revenue_retention_timeseries', session_id)
        retention_query = retention_query.format(wrr_query=wrr_query)
        retention_query = retention_query.replace(
            'WHERE week <=',
            'WHERE week BETWEEN %s AND %s AND week <='
        )
        
        cur.execute(retention_query, (start_date, end_date))
        result = cur.fetchall()
        
        # Clean up temp table
        cur.execute(f"DROP TABLE IF EXISTS daily_rev_{session_id}")
        conn.commit()
        
        return result
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        cur.close()
        conn.close()

def get_daily_revenue_retention_rates(session_id, start_date, end_date):
    """Get daily revenue retention rates data"""
    conn = get_db_connection()
    cur = conn.cursor()
    
    try:
        table_name = f'revenue_data_{session_id}'
        if not table_exists(cur, table_name):
            return []
            
        # First create daily revenue data
        cur.execute(f"""
        CREATE TEMP TABLE daily_rev_{session_id} AS
        """ + load_query('daily_rev', session_id))
        conn.commit()
        
        # Get DRR query
        drr_query = load_query('drr', session_id).replace('daily_rev', f'daily_rev_{session_id}')
        
        # Then run retention rates query with DRR data
        retention_query = load_query('daily_revenue_retention_timeseries', session_id)
        retention_query = retention_query.format(drr_query=drr_query)
        retention_query = retention_query.replace(
            'WHERE day <=',
            'WHERE day BETWEEN %s AND %s AND day <='
        )
        
        cur.execute(retention_query, (start_date, end_date))
        result = cur.fetchall()
        
        # Clean up temp table
        cur.execute(f"DROP TABLE IF EXISTS daily_rev_{session_id}")
        conn.commit()
        
        return result
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        cur.close()
        conn.close()

def get_monthly_revenue_quick_ratio(session_id, start_date, end_date):
    """Get monthly revenue quick ratio data"""
    conn = get_db_connection()
    cur = conn.cursor()
    
    try:
        table_name = f'revenue_data_{session_id}'
        if not table_exists(cur, table_name):
            return []
            
        # First create daily revenue data
        cur.execute(f"""
        CREATE TEMP TABLE daily_rev_{session_id} AS
        """ + load_query('daily_rev', session_id))
        conn.commit()
        
        # Get MRR query
        mrr_query = load_query('mrr', session_id).replace('daily_rev', f'daily_rev_{session_id}')
        
        # Then run quick ratio query with MRR data
        quick_ratio_query = load_query('monthly_revenue_quick_ratio', session_id)
        quick_ratio_query = quick_ratio_query.format(mrr_query=mrr_query)
        quick_ratio_query = quick_ratio_query.replace(
            'WHERE month <=',
            'WHERE month BETWEEN %s AND %s AND month <='
        )
        
        cur.execute(quick_ratio_query, (start_date, end_date))
        result = cur.fetchall()
        
        # Clean up temp table
        cur.execute(f"DROP TABLE IF EXISTS daily_rev_{session_id}")
        conn.commit()
        
        return result
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        cur.close()
        conn.close()

def get_weekly_revenue_quick_ratio(session_id, start_date, end_date):
    """Get weekly revenue quick ratio data"""
    conn = get_db_connection()
    cur = conn.cursor()
    
    try:
        table_name = f'revenue_data_{session_id}'
        if not table_exists(cur, table_name):
            return []
            
        # First create daily revenue data
        cur.execute(f"""
        CREATE TEMP TABLE daily_rev_{session_id} AS
        """ + load_query('daily_rev', session_id))
        conn.commit()
        
        # Get WRR query
        wrr_query = load_query('wrr', session_id).replace('daily_rev', f'daily_rev_{session_id}')
        
        # Then run quick ratio query with WRR data
        quick_ratio_query = load_query('weekly_revenue_quick_ratio', session_id)
        quick_ratio_query = quick_ratio_query.format(wrr_query=wrr_query)
        quick_ratio_query = quick_ratio_query.replace(
            'WHERE week <=',
            'WHERE week BETWEEN %s AND %s AND week <='
        )
        
        cur.execute(quick_ratio_query, (start_date, end_date))
        result = cur.fetchall()
        
        # Clean up temp table
        cur.execute(f"DROP TABLE IF EXISTS daily_rev_{session_id}")
        conn.commit()
        
        return result
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        cur.close()
        conn.close()

def get_daily_revenue_quick_ratio(session_id, start_date, end_date):
    """Get daily revenue quick ratio data"""
    conn = get_db_connection()
    cur = conn.cursor()
    
    try:
        table_name = f'revenue_data_{session_id}'
        if not table_exists(cur, table_name):
            return []
            
        # First create daily revenue data
        cur.execute(f"""
        CREATE TEMP TABLE daily_rev_{session_id} AS
        """ + load_query('daily_rev', session_id))
        conn.commit()
        
        # Get DRR query
        drr_query = load_query('drr', session_id).replace('daily_rev', f'daily_rev_{session_id}')
        
        # Then run quick ratio query with DRR data
        quick_ratio_query = load_query('daily_revenue_quick_ratio', session_id)
        quick_ratio_query = quick_ratio_query.format(drr_query=drr_query)
        quick_ratio_query = quick_ratio_query.replace(
            'WHERE day <=',
            'WHERE day BETWEEN %s AND %s AND day <='
        )
        
        cur.execute(quick_ratio_query, (start_date, end_date))
        result = cur.fetchall()
        
        # Clean up temp table
        cur.execute(f"DROP TABLE IF EXISTS daily_rev_{session_id}")
        conn.commit()
        
        return result
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        cur.close()
        conn.close()

def get_monthly_cohorts(session_id, start_date, end_date):
    """Get monthly cohort data"""
    conn = get_db_connection()
    cur = conn.cursor()
    
    try:
        table_name = f'revenue_data_{session_id}'
        if not table_exists(cur, table_name):
            return []
            
        # First create daily revenue data
        cur.execute(f"""
        CREATE TEMP TABLE daily_rev_{session_id} AS
        """ + load_query('daily_rev', session_id))
        conn.commit()
        
        # Then run cohorts query with date filter
        cohorts_query = load_query('cohorts_monthly', session_id).replace('daily_rev', f'daily_rev_{session_id}')
        
        # Add date filter to the final WHERE clause
        cohorts_query = cohorts_query.replace(
            'and first_month <= current_date',
            'and first_month between %s and %s and first_month <= current_date'
        )
        
        cur.execute(cohorts_query, (start_date, end_date))
        result = cur.fetchall()
        
        # Clean up temp table
        cur.execute(f"DROP TABLE IF EXISTS daily_rev_{session_id}")
        conn.commit()
        
        return result
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        cur.close()
        conn.close()

def get_weekly_cohorts(session_id, start_date, end_date):
    """Get weekly cohort data"""
    conn = get_db_connection()
    cur = conn.cursor()
    
    try:
        table_name = f'revenue_data_{session_id}'
        if not table_exists(cur, table_name):
            return []
            
        # First create daily revenue data
        cur.execute(f"""
        CREATE TEMP TABLE daily_rev_{session_id} AS
        """ + load_query('daily_rev', session_id))
        conn.commit()
        
        # Then run cohorts query with date filter
        cohorts_query = load_query('cohorts_weekly', session_id).replace('daily_rev', f'daily_rev_{session_id}')
        
        # Add date filter to the final WHERE clause
        cohorts_query = cohorts_query.replace(
            'and first_week <= current_date',
            'and first_week between %s and %s and first_week <= current_date'
        )
        
        cur.execute(cohorts_query, (start_date, end_date))
        result = cur.fetchall()
        
        # Clean up temp table
        cur.execute(f"DROP TABLE IF EXISTS daily_rev_{session_id}")
        conn.commit()
        
        return result
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        cur.close()
        conn.close()

def get_daily_rev_data(session_id, start_date, end_date):
    """Get raw daily revenue data"""
    conn = get_db_connection()
    cur = conn.cursor()
    
    try:
        table_name = f'revenue_data_{session_id}'
        if not table_exists(cur, table_name):
            return []
            
        # Create and execute daily_rev query
        cur.execute(f"""
        CREATE TEMP TABLE daily_rev_{session_id} AS
        """ + load_query('daily_rev', session_id))
        conn.commit()
        
        # Get all columns from daily_rev
        cur.execute(f"""
        SELECT * FROM daily_rev_{session_id}
        WHERE dt BETWEEN %s AND %s
        ORDER BY user_id, dt
        """, (start_date, end_date))
        
        result = cur.fetchall()
        
        # Clean up temp table
        cur.execute(f"DROP TABLE IF EXISTS daily_rev_{session_id}")
        conn.commit()
        
        return result
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        cur.close()
        conn.close()

def get_daily_cohorts(session_id, start_date, end_date):
    """Get daily cohort data"""
    conn = get_db_connection()
    cur = conn.cursor()
    
    try:
        table_name = f'revenue_data_{session_id}'
        if not table_exists(cur, table_name):
            return []
            
        # First create daily revenue data
        cur.execute(f"""
        CREATE TEMP TABLE daily_rev_{session_id} AS
        """ + load_query('daily_rev', session_id))
        conn.commit()
        
        # Then run cohorts query with date filter
        cohorts_query = load_query('cohorts_daily', session_id).replace('daily_rev', f'daily_rev_{session_id}')
        
        # Add date filter to the final WHERE clause
        cohorts_query = cohorts_query.replace(
            'and first_dt <= current_date',
            'and first_dt between %s and %s and first_dt <= current_date'
        )
        
        cur.execute(cohorts_query, (start_date, end_date))
        result = cur.fetchall()
        
        # Clean up temp table
        cur.execute(f"DROP TABLE IF EXISTS daily_rev_{session_id}")
        conn.commit()
        
        return result
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        cur.close()
        conn.close()

def clear_all_data(session_id):
    """Clear all data from the session-specific revenue_data table"""
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(f"TRUNCATE TABLE revenue_data_{session_id};")
        conn.commit()

def store_data(df):
    """Store validated data in PostgreSQL"""
    conn = get_db_connection()
    cur = conn.cursor()
    
    try:
        values = [
            (row['date'], row['id'], float(row['revenue']), row['user_id'])
            for _, row in df.iterrows()
        ]
        
        from psycopg2.extras import execute_values
        execute_values(
            cur,
            f"""
            INSERT INTO revenue_data_{st.session_state.session_id} 
            (transaction_date, transaction_id, revenue, user_id)
            VALUES %s
            """,
            values
        )
        
        conn.commit()
        return True, f"Successfully inserted {len(values)} records"
    except Exception as e:
        conn.rollback()
        return False, f"Error storing data: {str(e)}"
    finally:
        cur.close()
        conn.close()

if __name__ == "__main__":
    init_session_tables()
