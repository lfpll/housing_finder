from turbodbc import connect
from tests.mock_dbs import Mock_Sql_conn

# Test to check if it's able to connect
def test_get_cursor():
    cursor = get_cursor(username="test",password="test",db="postgres",ip="127.0.0.1")
    assert cursor == connect


def test_execute_query():
    query = "SELECT * FROM mock_table"
    
    pass

def test_get_next_vals():
    pass

def test_check_url_live():
    pass

def test_check_batch_urls():
    pass