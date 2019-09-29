from sql_lite_mock import Mock_Sql_conn
import pandas as pd
# This is a test that removes a function from a sql lite like database based on a url column that is offlinee
# Mock database with an url
    # Create a SQL lite database 
    # Populate with fake data
    # Create a function that maps the bad urls to requests
# Mock function of schema

# Mock database deletion


json_sql_database = [{'url':'http://test-fail-url.com'},{'url':'http://test-sucess-url.com'}]
def request_replacer(url,sample_page):
    if url == 'http://test-fail-url.com':
        request.status_code = 400
    elif url == 'http://test-sucess-url.com':
        request.data = open(sample_page+'pagination_page.html')
    elif url == 'http://test-500-url.com':
        request.data = open(sample_page+'error_500.html')

def test_removing_urls(sample_page):
    mock_sql = Mock_Sql_conn(sample_page+'mock_data.db')
    mock_sql.load_mock_data_parquet(sample_page+'mock_rental_data.parquet')
    df = pd.read_parquet(sample_page+'mock_rental_data.parquet')
    error_page = df.loc[df['url'] == 'http://test-500-url.com' ,'url'].count()
    error_status_code = df.loc[df['url'] == 'http://test-fail-url.com' ,'url'].count()
    success = df.loc[df['url'] == 'http://test-sucess-url.com' ,'url'].count()