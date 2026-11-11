Dataset composition prototype
=============================

These unfinished composition modules were found in the ignored ``example/``
folder during release consolidation. They and their pre-existing design tests
are preserved here for follow-up work. They are not installed SHYBOX APIs.
The prototype still has inconsistent names, missing factories and logger setup;
its tests reference the intended future package locations. They do not run as
part of the supported dataset planner suite. Complete the implementation and
move the modules/tests into the package before enabling composition examples.

SHYBOX 1.7.0 provides DatasetPlanner and dataset workflow configuration. Existing
converter, merger and disaggregation workflows remain the supported preparation
steps. The Sphinx forcing composition guide describes the intended design.
