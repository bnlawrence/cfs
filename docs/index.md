# CFStore

Provides a lightweight semantic view of data held in CF compliant data files.

Data files are ingested using command lines tools, but the user interface is all about the variables held in those files, not the files themselves. Variables
can be organised into (mulltiple) collections, and documented and tagged
as desired.

Variables can be searched for, and organised by, their internal metadata, both
that known to CF, and the more generic "B-metadata" held in file properties.

There are three modes of interaction: user and system.

## User Mode

In user mode, cfstore is used to keep track of a users own files, either 
by command line tools, or (if you have your own machine) via a browser interface.

## Embedded Mode

cfstore can be embedded in other tools to provide a semantic view of data
when the file numbers get too large for other techniques (e.g it is embeded in the NCAS s3view tool).

## System Mode

For large projects, cfstore can be installed as a system tool, with a web server
interface, allowing multiple users to work with the content. In this mode, there
is likely a "data manager" who can keep track of which data is where, and use
this tool to go from "a variable view" to the necessary files which may need
to be migrated between storage locations. One example of this is the CANARI website, found [here](https://NOT YET).
