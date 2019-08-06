from functions.pagination import main as module_test
import os
import base64
import json
# This is for testing the pagination lambda function4
# 1 - Test for sucesfull
# 2 - Test for errors on http code
# 3 - Test for incorrect schema on html page

config = json.load(open("pagination_test.json"))

# Populating enviroment variables for the test
for key,val in config.keys(mock_request_200):
    os.environ[key] = val

def test_pagination_success(mock_request_200):
    data_path = './sample/pagination_page.html'
    b64encoded = base64.b64encode({'data':data_path}).decode('utf-8')
    context = None
    
    


    