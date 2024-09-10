from core.db.standalone import setup_django
from pathlib import Path

import pytest

###
### This test file concentrates on the issues around variable import and querying
###

@pytest.fixture(scope="session", autouse=True)
def setup_test_db(tmp_path_factory):
    """ 
    Get ourselves a db to work with. Note that this database is progressively
    modified by all the tests that follow. So if you're debugging tests, you 
    have to work though them consecutively. 
    """
    tmp_path = tmp_path_factory.mktemp('testing_interface')
    dbfile = str(Path(tmp_path)/'test.db')
    migrations_location = str(Path(tmp_path)/'migrations')
    setup_django(db_file=dbfile,  migrations_location=migrations_location)

@pytest.fixture
def test_db():
    """ 
    This database (and it's contents) is used in all the following
    tests, and is progressively modified as the tests proceed.
    """
    from core.db.interface import CollectionDB
    return CollectionDB()


def test_simple_variable(test_db):
    properties = {'identity':'test var 1','atomic_origin':'imaginary','temporal_resolution':'daily',
                  'domain':{'name':'N216','region':'global','nominal_resolution':'10km',
                            'size':1000,'coordinates':'longitude,latitude,pressure'}}
    test_db.variable_retrieve_or_make(properties)



