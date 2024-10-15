
# CFS

This python package provides the CFS store command line tools and django gui.

It is a complete refactor of an earlier code, but this version has not yet reached the functionality of the previous code. This version has unit tests for the internal functionality, and quite a lot of the necessary
Django functionality to search data.

There is one mini-real world tests which is in the data directory, canari.py. You will need a dataset to run that.
It has been tested with a couple of complex real-world CFA files.

You can get the dynamic documentation by running `mkdocs serve` in the root directory (next to the mkdocs.yml file) and going to `localhost:8000`

`pytest` should run the tests, and currently all are passing. Talk to me if you want to do anything with the GUI. 
GUI tests are some way down the runway ... that said, it does run with some functionality you can explore
by using `python manage.py runserver 8001` in the project root directory (8001, because you might have a
mkdocs server running on port 8000). More details in the django piece of the documentation.

You will likely have needed to set up some data though, and that's the bit you may need to talk to me about (for now). 
