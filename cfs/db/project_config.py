from pathlib import Path
import yaml
import os

class ProjectInfo:
    """ 
    Holds all the layout information to populate the GUI
    """
    def __init__(self,configdir=None):
        """ 
        Instantiate with the location of all the configuration yaml and markdown
        """
        if configdir is None:
            configdir = os.getenv('CFS_DBDIR',None)
            if configdir is not None:
                configdir = Path(configdir)/'config'
                if not configdir.exists():
                    raise ValueError(f'No configuration directory exists at {configdir}')

        if configdir is None:
            raise ValueError('No configuration directory found for ProjectInfo')

        self.configdir = configdir
        self._projects={}
        self._add_projects()
    
    @property
    def projects(self):
        return self._projects.keys()

    def _add_projects(self):
        """ 
        All yaml files in the configdir are project descriptions
        """
        yamls = Path(self.configdir).glob('*.yaml')
        for yamlf in yamls:
            with open(yamlf,'r') as f:
                project = yaml.safe_load(f)
                for facet in project['facets']:
                    if 'url' not in facet:
                        facet['url']=None
                    if facet['url']=='None':
                        facet['url']=None
                name = project.pop('Project')
                self._projects[name] = project

    def get_facets(self, project):
        """ 
        Return the facets which are in play for a given project. Used
        at input time to help in parsing, but not directly used by 
        the database, which organises it's search view based on the
        facets that were actually extracted (which ought to conform
        to these).
        """
        facets = [f['key'] for f in self._projects[project]['facets']]
        return facets
    
    def get_atomic_params(self, project):
        return self._projects[project]['atomic_facets'].split(',')

    def get_description(self,project):
        return self._projects[project]['description']


