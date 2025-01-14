# Revenue Analysis Dashboard Project Blueprint

## 1. Project Setup
- [x] Create virtual environment
- [x] Setup project directory structure
- [x] Install required dependencies
- [x] Configure git repository
- [x] Setup environment variables

## 2. Database Setup
- [x] Create PostgreSQL database schema
- [x] Design tables for storing uploaded CSV data
- [x] Create indexes for optimizing query performance
- [x] Set up data validation rules

## 3. Core Functionality
- [x] Implement CSV file upload and validation
- [x] Parse CSV data and store in database
- [x] Add error handling for malformed data
- [x] Create data cleaning/preprocessing pipeline

## 4. SQL Queries Development
- [x] Write test query for first analysis
- [x] Create timeline analysis queries
- [x] Validate it works with the test query

- [x] Create MAU chart
- [x] Create WAU chart
- [x] Create DAU chart

- [x] Create retention rate and quick ratio per month chart, add benchmarks
- [x] Create retention rate and quick ratio per week chart
- [x] Create retention rate and quick ratio per day chart

- [x] Create MRR chart
- [x] Create WRR chart
- [x] Create DRR chart

- [x] Create retention rate and quick ratio per month chart, add benchmarks
- [x] Create retention rate and quick ratio per week chart
- [x] Create retention rate and quick ratio per day chart

- [x] Create monthly retention cohort chart 
- [x] Create weekly retention cohort chart
- [x] Create daily retention cohort chart

- [x] Create monthly ltv cohort chart 
- [x] Create weekly ltv cohort chart
- [x] Create daily ltv cohort chart 

## 4.1. Data Management
- [x] Before storing new data, clear the database
- [x] Limit file upload size to 20MB

## 4.2 Database Management & Migration
- [x] Define which database tool to use
- [x] Figure out how to create complex visual using Supabase
- [ ] Migrate to Supabase
    - [x] Growth
    - [x] Retention
    - [x] Quick ratio 
    - [x] Cohorts
    - [x] Try to optimize views queries by using the same daily_rev_view 
        - [x] Create daily_rev_view
        - [x] Update queries
        - [x] Update on Supabase
    - [x] Test all next steps first with monthly cohorts view
    - [x] Change views to be materialized -- decided to do only with cohorts
    - [x] Add new views to refresh trigger -- decided to do only with cohorts
    - [x] Add pagination to PostgREST calls
    - [x] Add cron job to clean refresh trigger
- [ ] Test deployment, if it works delete old projects and files

## 4.3 Optimizations
- [ ] Make sections and chart titles static
- [x] Reuse same chart for same type of data, delete not needed

## 5. UI/UX Implementation
- [x] Add loading states and progress indicators
- [x] Add a template CSV and instructions on how to use it
- [x] Add a button to apply filter and don't apply it by default
- [x] Adapt design to have a little personal touch
- [x] Make it one pager with pdf export
    - [x] Cohorts
    - [x] Quick ratio 
    - [x] Ret. over Period
    - [x] Growth
    - [x] Growth and Ret.
    - [x] Cohorts and Quick ratio
    - [x] Merge it all
    - [ ] Add PDF export
- [ ] Add info component in revenue retention explaining that it is just revenue retained over month, it doens't include contraction and expansion
- [ ] Add notes
- [ ] Fix chart data point hover, show only relevant information 
- [ ] Add whitespace laterally to raw data dropdowns
- [ ] Add footer with credits
- [ ] Test title colors
- [ ] Bug fixes: 
    - [x] Chart filter is not showing january datapoints
    - [ ] Clearing data loading state persists throughout the whole data storing process
    - [ ] Generate chart success message going away after filters are applied
    - [ ] Flash of loading state buttons are clicked
    - [ ] Filter start date is not applying when first generating charts
- [x] Add reference to Github repo

## 6. Deployment
- [ ] Deploy application through streamlit
- [ ] Test deployment, stress test too
