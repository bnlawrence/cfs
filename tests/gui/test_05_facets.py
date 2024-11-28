from cfs.db.project_config import ProjectInfo
from pathlib import Path

CONFIGDIR = Path(__file__).parent.resolve()/'config'

def test_read_facets():
    assert CONFIGDIR.exists()
    info = ProjectInfo(CONFIGDIR)
    facets = info.get_facets('CANARI')
    assert facets[0] == 'institution'
    assert set(info.projects) == {'CANARI','RAMIP'}
    atomics = info.get_atomic_params('RAMIP')
    assert set(atomics) == set('experiment, source-id, variant_label, realm'.split(','))
