# Testing

The core cfs store functionality lies in the cfs module, and is accessed by the tests in the `test/cfs` directory.

In normal operation the tests are run sequentially, with each test file doing a series of tests which build up to a cresdendo which involves explict deletion
(which tests db deletion, which is why we do it explicitly rather than hiding it in tear down routines). Each test file should be independent of the other test files.

My normal mode of testing is to run `pytest -s -x` so that I see the output of the tests, and stop at the first one that fails.

The tests are all run with a special cut-down django setting, which is set up in `cfs/db/standalone.py`. These are the settings which will be used when cfs is used in standalone mode (without the django GUI).

As of the 241014, we have the following classes of tests in `test/cfs`:

- level 0; core testing and database functionality,
- level 1; mixin structure and basic files,
- level 2; the database interface,
- level 3; tests for the variable and variable interface,
- level 4; cf parsing,
- level 5; CFA parsing,
- level 6, CFA importing,
- level 7, uploading content to the database.
