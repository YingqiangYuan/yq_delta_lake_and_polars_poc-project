# -*- coding: utf-8 -*-

if __name__ == "__main__":
    from yq_delta_lake_and_polars_poc.tests import run_cov_test

    run_cov_test(
        __file__,
        "yq_delta_lake_and_polars_poc",
        is_folder=True,
        preview=False,
    )
