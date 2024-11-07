from cfs.db.interface import VariableInterface

def _filterview(selections):
    """ Common filtering operations"""
    properties_sname = set(selections['dd-sname'])
    properties_lname = set(selections['dd-lname'])
    properties_tave = set(selections['dd-tave'])
    properties_ens = set(selections['dd-ens'])
    collections = set(selections['dd-col'])
    results = VariableInterface.filter_by_property_keys(
        [properties_sname, properties_lname, properties_tave, properties_ens]
        )
    if collections:
        results = results.filter(contained_in__in=collections)
    return results

def _summary(sdata, n, nvariants):
    """Generates summary HTML for the selection."""
    shtml = f'<p>Total Results {n}. Includes <ul>'
    shtml += f"<li>{sdata['nspatial']} unique spatial domains, and </li>"
    shtml += f"<li>{sdata['ntime']} unique time domain(s),</li>"
    shtml += f"<li>from {nvariants} ensemble member(s).</li>"
    shtml += "</ul>"
    return shtml