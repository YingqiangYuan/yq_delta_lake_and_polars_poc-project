
.. .. image:: https://readthedocs.org/projects/yq-delta-lake-and-polars-poc/badge/?version=latest
    :target: https://yq-delta-lake-and-polars-poc.readthedocs.io/en/latest/
    :alt: Documentation Status

.. .. image:: https://github.com/YingqiangYuan/yq_delta_lake_and_polars_poc-project/actions/workflows/main.yml/badge.svg
    :target: https://github.com/YingqiangYuan/yq_delta_lake_and_polars_poc-project/actions?query=workflow:CI

.. .. image:: https://codecov.io/gh/YingqiangYuan/yq_delta_lake_and_polars_poc-project/branch/main/graph/badge.svg
    :target: https://codecov.io/gh/YingqiangYuan/yq_delta_lake_and_polars_poc-project

.. .. image:: https://img.shields.io/pypi/v/yq-delta-lake-and-polars-poc.svg
    :target: https://pypi.python.org/pypi/yq-delta-lake-and-polars-poc

.. .. image:: https://img.shields.io/pypi/l/yq-delta-lake-and-polars-poc.svg
    :target: https://pypi.python.org/pypi/yq-delta-lake-and-polars-poc

.. .. image:: https://img.shields.io/pypi/pyversions/yq-delta-lake-and-polars-poc.svg
    :target: https://pypi.python.org/pypi/yq-delta-lake-and-polars-poc

.. .. image:: https://img.shields.io/badge/✍️_Release_History!--None.svg?style=social&logo=github
    :target: https://github.com/YingqiangYuan/yq_delta_lake_and_polars_poc-project/blob/main/release-history.rst

.. image:: https://img.shields.io/badge/⭐_Star_me_on_GitHub!--None.svg?style=social&logo=github
    :target: https://github.com/YingqiangYuan/yq_delta_lake_and_polars_poc-project

------

.. .. image:: https://img.shields.io/badge/Link-API-blue.svg
    :target: https://yq-delta-lake-and-polars-poc.readthedocs.io/en/latest/py-modindex.html

.. .. image:: https://img.shields.io/badge/Link-Install-blue.svg
    :target: `install`_

.. image:: https://img.shields.io/badge/Link-GitHub-blue.svg
    :target: https://github.com/YingqiangYuan/yq_delta_lake_and_polars_poc-project

.. image:: https://img.shields.io/badge/Link-Submit_Issue-blue.svg
    :target: https://github.com/YingqiangYuan/yq_delta_lake_and_polars_poc-project/issues

.. image:: https://img.shields.io/badge/Link-Request_Feature-blue.svg
    :target: https://github.com/YingqiangYuan/yq_delta_lake_and_polars_poc-project/issues

.. .. image:: https://img.shields.io/badge/Link-Download-blue.svg
    :target: https://pypi.org/pypi/yq-delta-lake-and-polars-poc#files


Welcome to ``yq_delta_lake_and_polars_poc`` Documentation
==============================================================================
..  .. image:: https://yq-delta-lake-and-polars-poc.readthedocs.io/en/latest/_static/yq_delta_lake_and_polars_poc-logo.png
    :target: https://yq-delta-lake-and-polars-poc.readthedocs.io/en/latest/

About
------------------------------------------------------------------------------

This project is a hands-on learning repository created while working on a
data modernization initiative at a financial organization. The production
system relies on a modern data lake architecture that supports incremental
upserts, schema evolution, and audit-friendly time travel — all built on
**Delta Lake** and **Polars** (without Spark).

To deepen my understanding of how these libraries work together, I extracted
the core data-wrangling patterns from the project and turned them into a
collection of small, self-contained proof-of-concept scripts. Each script
isolates a single concept — reading/writing Delta tables on S3, Polars
transformations, merge/upsert, vacuum, schema evolution — and can be run
independently with no real data involved.

See `examples/README.md <examples/README.md>`_ for the full index of POC
scripts and what each one demonstrates.
