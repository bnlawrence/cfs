from setuptools import setup, find_packages

# Get dependencies
requirements = open("requirements.txt", "r")
install_requires = requirements.read().splitlines()

setup(
    name='cfs',
    version='0.1',
    author='Bryan Lawrence',
    author_email='bryan.lawrence@ncas.ac.uk',
    description='cfstore: lightweight tool for storing CF file information in a database',
    packages=find_packages(),
    include_package_data=True,
    scripts=[], 
    install_requires=install_requires,
)
