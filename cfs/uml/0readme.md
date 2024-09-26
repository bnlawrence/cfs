These two files lifted from

 - https://github.com/sen-den/django-model2puml/tree/dev  (MIT License)

on the 16th of September, 2024. Thanks to Denis Senchishen.

Modified to fit into this 'non-standard' Django setup and for better UML compliance (it's not true UML, but it's closer I think). Key differences
are that foreign_keys with cascade are now shown as composition, and
other foreign_keys as composition. THe direction isn't ideal. Also showing
many-to-many as a collapsed many-to-many in both directions.
