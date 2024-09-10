
# Testing Notes

## Issues encountered while building the new tests


1. (240906) I had to remove the size option on the collection, that looks like a hack GOB introduced but he never tested it. I imagine I'll have to come back to (size support) in the collections (at `db.create_collection`).
2. I am really not sure where George got to with facets and properties (i.e whether the use of _proxied or properties is the way to go.)
3. Replicants seems to be borked. I have done some work on it
4. At the moment we can create files and not put them in collections. That shouldn't be allowed.
5. I am in the middle of re-imagining the use of domains and cell methods. The next step is to get out of the database formalism and get a clear idea of how we want to separate the notions of domain and cell methods, respecting CF itself. However, I would want to search on common domains, and then find variable with certain cell methods, so I think this needs a bit more thought. Quickly.
6. 