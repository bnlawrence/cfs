# Project layout

    cfs/
       core/              # holds the reusable logic
            db/           # django models.py and a range of cf and interface utilities
            plugins/      # the interfaces to storage (e.g. posix, S3, tape)
            uml/          # tools for documenting the o-o layout of the database
       data/              # integration tests and data 
       docs/              # this documentation
       gui/               # django app: this is the gui interface to the CFS content
       scripts/           # CFS command line tools
       tests/
            core/         # tests for the core infrastructure
            web/          # django testing
            cli/          # command line testing
       web/               # django project: the main django projecdt config
       setup.py           # normal python project setup.py
       requirements.txt   # all the gubbins this project needs
       mkdocs.yml         # Documentation configuration file.
