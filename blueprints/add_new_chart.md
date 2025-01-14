# Adding a New Chart to Visualize Tab

## 1. Create SQL View
1. Create a new SQL file in `sql/supabase/create_[metric]_view.sql`
2. Define the view with proper session isolation using `session_id`
3. Include all necessary window functions, joins, and aggregations
4. Test the view directly in Supabase SQL editor
5. Execute the view creation script in Supabase

## 2. Create Database Function
1. Add new function in `src/database.py`: `get_[metric]_data()`
2. Follow the pattern:
   - Get filter dates from session state
   - Create query with session isolation
   - Apply date filters if they exist and are enabled
   - Return with `execute_query` and `ttl=0`

## 3. Create Visualization Component
1. Create new file `src/visuals/[metric].py`
2. Implement `plot_[metric]()` function with:
   - Empty data handling
   - Plotly figure creation
   - Layout customization
   - Raw data expander with formatted columns
   - Session ID removal from display

## 4. Update App Integration
1. Add import in `src/app.py`:
   - Import database function
   - Import visualization component
2. Add visualization to appropriate period section
3. If it's a side-by-side visualization (like Users/Revenue):
   - Create columns using `st.columns(2)`
   - Add appropriate subtitles
   - Handle data fetching and visualization in each column

## 5. For Monthly Charts (Default View)
1. Update "Generate Charts" button code to:
   - Fetch both user and revenue data
   - Store both in session state under appropriate keys
   - Example: `'revenue_results': mrr_results`
2. Update visualization section to use stored data instead of fetching again

## Key Steps
1. Create SQL View
2. Create Database Function
3. Update App Integration
4. Update Visualization Section
   - Add side-by-side columns for retention charts
   - Add clear subtitles for each type
   - Use consistent empty data handling
   - Match existing period section structure
5. Add Safe Data Access
   - Use `data.get()` to safely access potentially missing keys
   - Check both key existence and data presence:
     ```python
     if (data.get('key_name') and data['key_name'].data):
         df = pd.DataFrame(data['key_name'].data)
         plot_chart(df)
     else:
         st.info("No data available")
     ```

## Key Learnings
- Complex SQL logic goes in views, not Python
- Use simple `.table("view_name").select("*").eq()` in Python
- Always maintain session isolation
- Handle empty DataFrames consistently
- Use `data.empty` instead of `if not data` for DataFrame checks
- Use safe data access with `.get()` for session state data
- Keep visualizations for specific periods inside their respective period blocks
- When adding default view data, update both:
  - "Generate Charts" button initialization
  - "Apply filters" button data fetching
- Drop session_id before displaying
- Store default view data in session state
- Only fetch data on filter apply for non-default views

## Common Patterns
- Views handle: window functions, joins, date calculations, aggregations
- Python handles: filters, session isolation, data formatting
- Visualization handles: empty states, styling, raw data display
- Period-specific visualizations should:
  - Be nested inside their period condition
  - Use stored data from session state
  - Have consistent empty data handling 

## Additional Technical Requirements

### 1. Materialized Views
- Use materialized views for better performance
- Add indexes for session_id and date columns
- Example:
```sql
CREATE MATERIALIZED VIEW your_view_name AS
SELECT -- your view logic here
FROM your_table;

CREATE INDEX idx_your_view_name_session_id ON your_view_name(session_id);
```

### 2. View Refresher
- Add new views to the refresh trigger
- Use security definer for proper permissions
```sql
CREATE OR REPLACE FUNCTION refresh_views()
RETURNS trigger
SECURITY DEFINER
SET search_path = public
AS $$
BEGIN
    REFRESH MATERIALIZED VIEW your_view_name;
    -- other views...
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

GRANT SELECT ON your_view_name TO postgres, authenticated, anon;
```

### 3. Data Cleanup
- Add cleanup cron job in Supabase
```sql
CREATE OR REPLACE FUNCTION cleanup_old_data()
RETURNS void
LANGUAGE plpgsql
SECURITY DEFINER
AS $$
BEGIN
    DELETE FROM refresh_trigger WHERE created_at < NOW() - INTERVAL '24 hours';
END;
$$;

-- Add in Supabase dashboard:
-- Schedule: 0 * * * * (every hour)
-- Command: SELECT cleanup_old_data();
```

### 4. Pagination Handling
- Handle PostgREST's 1000-row limit using pagination
- Example database function:
```python
def get_your_data():
    """Get data with pagination handling"""
    conn = init_connection()
    query = conn.table("your_view_name").select("*").eq('session_id', st.session_state.session_id)
    
    # Initialize pagination
    all_data = []
    page_size = 1000
    current_range = 0
    
    while True:
        result = execute_query(
            query.range(current_range, current_range + page_size - 1)
            .order('your_date_column'),
            ttl=0
        )
        
        if not result.data:
            break
            
        all_data.extend(result.data)
        
        if len(result.data) < page_size:
            break
            
        current_range += page_size
    
    result.data = all_data
    return result
```

### 5. Performance Considerations
- Monitor materialized view refresh times
- Add appropriate indexes for common queries
- Test with large datasets (>1000 rows)
- Consider cleanup frequency based on data volume
- Use pagination for all large dataset queries 