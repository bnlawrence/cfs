import cf
import pytest
import numpy as np

@pytest.fixture
def django_dependencies():
    """ 
    The database (and it's contents) is used in all tests
    tests, and is progressively modified as the tests proceed.
    Posix imports some django dependent stuff as well. 
    """
    from cfs.db.interface import CollectionDB
    from cfs.plugins.posix import Posix, get_parent_paths
    db = CollectionDB()
    return db, Posix(db,'vftesting'), get_parent_paths

@pytest.fixture
def inputfield():
    """ 
    Create a field to use for testing. This is nearly straight from the CF documentation.
    """
    f = cf.example_field(2)
    f.set_properties({'project': 'testing', 'institution':'NCAS'})
    f.set_construct(
        cf.CellMethod(axes=f.domain_axis('T', key=True), method='maximum')
    )
    return f

