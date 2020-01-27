import sys
import os
from os.path import dirname
import requests
import pytest
from unittest.mock import MagicMock
from unittest.mock import patch
from mock_gcloud import *
PARENT_PATH = os.path.abspath(os.path.join(dirname(__file__), os.pardir))
SAMPLES_FOLDER = PARENT_PATH + '/tests/samples/'
TMP_FOLDER = PARENT_PATH + "/tmp/"
sys.path.append(PARENT_PATH)

os.environ["TMP_FOLDER"] = TMP_FOLDER

@pytest.fixture()
def current_folder():
    return PARENT_PATH

@pytest.fixture()
def sample_folder():
    return SAMPLES_FOLDER



