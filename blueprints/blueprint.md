# Revenue Analysis Dashboard Project Blueprint

## 1. Project Setup
- ✅ Create virtual environment
- ✅ Setup project directory structure
- ✅ Install required dependencies
- ✅ Configure git repository
- ✅ Setup environment variables

## 2. Database Setup
- ✅ Create PostgreSQL database schema
- ✅ Design tables for storing uploaded CSV data
- ✅ Create indexes for optimizing query performance
- ✅ Set up data validation rules

## 3. Core Functionality
- ✅ Implement CSV file upload and validation
- ✅ Parse CSV data and store in database
- ✅ Add error handling for malformed data
- ✅ Create data cleaning/preprocessing pipeline

## 4. SQL Queries Development
- ✅ Write test query for first analysis
- ✅ Create timeline analysis queries
- ✅ Validate it works with the test query

- ✅ Create MAU chart
- ✅ Create WAU chart
- ✅ Create DAU chart

- ✅ Create retention rate and quick ratio per month chart, add benchmarks
- ✅ Create retention rate and quick ratio per week chart
- ✅ Create retention rate and quick ratio per day chart

- ✅ Create MRR chart
- ✅ Create WRR chart
- ✅ Create DRR chart

- ✅ Create retention rate and quick ratio per month chart, add benchmarks
- ✅ Create retention rate and quick ratio per week chart
- ✅ Create retention rate and quick ratio per day chart

- ✅ Create monthly retention cohort chart 
- ✅ Create weekly retention cohort chart
- ✅ Create daily retention cohort chart

- ✅ Create monthly ltv cohort chart 
- ✅ Create weekly ltv cohort chart
- ✅ Create daily ltv cohort chart 

## 4.1. Data Management
- ✅ Before storing new data, clear the database
- ✅ Limit file upload size to 20MB

## 4.2 Database Management & Migration
- ✅ Define which database tool to use
- ✅ Figure out how to create complex visual using Supabase
- ✅ Migrate to Supabase
    - ✅ Growth
    - ✅ Retention
    - ✅ Quick ratio 
    - ✅ Cohorts
    - ✅ Try to optimize views queries by using the same daily_rev_view 
        - ✅ Create daily_rev_view
        - ✅ Update queries
        - ✅ Update on Supabase
    - ✅ Test all next steps first with monthly cohorts view
    - ✅ Change views to be materialized -- decided to do only with cohorts
    - ✅ Add new views to refresh trigger -- decided to do only with cohorts
    - ✅ Add pagination to PostgREST calls
    - ✅ Add cron job to clean refresh trigger
- ✅ Test deployment, if it works delete old projects and files

## 4.3 Optimizations
- ✅ Make sections and chart titles static -- FAILED, not worth it
- ✅ Reuse same chart for same type of data, delete not needed

## 5. UI/UX Implementation
- ✅ Add loading states and progress indicators
- ✅ Add a template CSV and instructions on how to use it
- ✅ Add a button to apply filter and don't apply it by default
- ✅ Adapt design to have a little personal touch
- ✅ Make it one pager with pdf export
    - ✅ Cohorts
    - ✅ Quick ratio 
    - ✅ Ret. over Period
    - ✅ Growth
    - ✅ Growth and Ret.
    - ✅ Cohorts and Quick ratio
    - ✅ Merge it all
    - ❌ Add PDF export -- FAILED, not worth it
- ✅ Add info component in revenue retention explaining that it is just revenue retained over month, it doens't include contraction and expansion
- ✅ Add notes to cohort charts with their limits
- ✅ Fix chart data point hover, show only relevant information
- ✅ Add credits
- ✅ Test title colors
- ✅ Bug fixes: 
    - ✅ Chart filter is not showing january datapoints
    - ✅ Takes too much time to store data
    - ✅ Clearing data loading state persists throughout the whole data storing process
    - ✅ Generate chart success message going away after filters are applied
    - ❌ Flash of loading state when buttons are clicked -- FAILED, not worth it, too complicated
    - ✅ Filter start date is not applying when first generating charts
- ✅ Add reference to Github repo

## 6. Product Analytics 
- ✅ Create log of product usage and errors

## 7. Deployment
- Deploy application through streamlit
- Pay Supabase first tier plan
- Test deployment, stress test too
