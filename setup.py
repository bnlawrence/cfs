from setuptools import setup, find_packages
setup(
    name='cfs',
    version='0.1',
    author='Bryan Lawrence',
    author_email='bryan.lawrence@ncas.ac.uk',
    description='cfstore: lightweight tool for storing CF file information in a database',
    packages=find_packages(),
    scripts=[], 
    install_requires = [
        'django',
        'click',
        'rich',
        'tqdm'
        ]
    )