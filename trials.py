"""
Queryset tests

This script tests pub / fetch of various querysets, to ensure that views 3 is working correctly.

"""

import logging
from viewser import Queryset, Column

logging.basicConfig(level = logging.DEBUG)

# ### Baseline Query:
#
# This is just a simple query to check that the system is working, not doing any disagg/agg

baseline_queryset = (Queryset("mihai_simple_sys_up", "country_month")
            .with_column(Column("sb_count_cm", from_table = "ged2_cm", from_column = "ged_sb_best_count_nokgi"))
            .with_column(Column("sb_acled_cm", from_table = 'acled2_cm', from_column = "acled_sb_count"))
            )

def baseline_assertions(dataset):
    assert dataset.loc[501].loc[60].sb_count_cm ==  27
    assert dataset.loc[501].loc[60].sb_acled_cm == 333

# ### Example query 1:
#
# What this intends to acheive: Take event counts and fatality counts from both PGM and CM levels and create a PGM dataset copying all CM observations into PGM for comparison purposes.
#
#
# Equivalent SQL that should be run is:
#
# ```sql
# WITH
# pgm_level_left
# AS
#     (
#         SELECT master.id as priogrid_month_id,
#                master.priogrid_gid,
#                master.month_id,
#                master.country_month_id,
#                slave1.ged_sb_best_count_nokgi as sb_count_pgm,
#                slave1.ged_sb_best_sum_nokgi as sb_sum_pgm
#         FROM
#              prod.priogrid_month master,
#              prod.ged2_pgm slave1
#         WHERE
#              master.id = slave1.priogrid_month_id
#     )
# SELECT
#     pgm_level_left.*,
#     ged2_cm.ged_sb_best_count_nokgi AS sb_count_cm,
#     ged2_cm.ged_sb_best_sum_nokgi AS sb_sum_cm
# FROM
#     pgm_level_left
# LEFT JOIN
#     prod.ged2_cm ged2_cm
# ON
#     (pgm_level_left.country_month_id = ged2_cm.country_month_id)
# ```

queryset_alpha = (Queryset("mihai_pgm_cm_comparison2", "priogrid_month")
            .with_column(Column("sb_count_cm", from_table = "ged2_cm", from_column = "ged_sb_best_count_nokgi")
                        )
            .with_column(Column("sb_count_pgm", from_table = "ged2_pgm", from_column = "ged_sb_best_count_nokgi")
                        )
            .with_column(Column("sb_sum_cm", from_table = "ged2_cm", from_column = "ged_sb_best_sum_nokgi")
                        )
            .with_column(Column("sb_sum_pgm", from_table = "ged2_pgm", from_column = "ged_sb_best_sum_nokgi")
                        )
           )

def assertions_alpha(dataset):
    assert dataset.shape[0] == 55224936
    assert dataset.loc[494][175452].sb_count_pgm == 28
    assert dataset.loc[494][175452].sb_sum_cm == 1917

# ### Example Query 3
#
# What this intends to acheive: Take some fatality data from various levels and mix-in with some yearly data to obtain some yearly data. This is the sql below, works perfecly fine:
#
#
# ```sql
# WITH
# pg2cm AS (
#     SELECT country_month_id,
#            sum(ged_sb_best_sum_nokgi) as ged_pgm
#     FROM prod.priogrid_month base,
#          prod.ged2_pgm slave
#     WHERE base.id = slave.priogrid_month_id
#     GROUP BY country_month_id
# ),
# cm2cy AS (
#     SELECT country_year_id,
#            sum(ged_sb_best_sum_nokgi) AS ged_cm,
#            sum(ged_pgm)               AS ged_pgm
#     FROM prod.country_month base,
#          prod.ged2_cm slave,
#          pg2cm slave2
#     WHERE base.id = slave.country_month_id
#       AND base.id = slave2.country_month_id
#     GROUP BY country_year_id
# )
# SELECT country_id, year_id, avg_fatsupply as fat_supply, ged_cm, ged_pgm
# FROM prod.country_year master
#     INNER JOIN prod.faostat_fsec_cy slave ON (master.id = slave.country_year_id)
#     LEFT JOIN cm2cy slave2 ON (master.id = slave2.country_year_id);
# ```

queryset_beta = (Queryset("mihai_pgm_cm_cy_comparison5","country_year")
      .with_column(Column("ged_pgm",  from_table = "ged2_pgm", from_column = "ged_sb_best_sum_nokgi").aggregate("sum"))
      .with_column(Column("fat_supply",  from_table = "faostat_fsec_cy", from_column = "avg_fatsupply"))
      .with_column(Column("ged_cm", from_table = "ged2_cm", from_column = "ged_sb_best_sum_nokgi").aggregate("sum"))
           )

def assertions_beta(dataset):
    assert dataset.shape[0] == 13510
    assert dataset.loc[2002].loc[28].ged_cm == 2268
    assert dataset.loc[2002].loc[28].fat_supply == 73

# ### Example Query 4
#
# Weirdly enough this works...
#
# This would equate to
#
# ```sql
# with
# pg2cm AS (
#     SELECT country_month_id,
#            sum(ged_sb_best_sum_nokgi) as ged_pgm
#     FROM prod.priogrid_month base,
#          prod.ged2_pgm slave
#     WHERE base.id = slave.priogrid_month_id
#     GROUP BY country_month_id
# )
# SELECT month_id, country_id,
#            ged_sb_best_sum_nokgi      AS ged_cm,
#            ged_pgm                    AS ged_pgm
# FROM
# prod.country_month base
# LEFT JOIN prod.ged2_cm slave ON (base.id = slave.country_month_id)
# LEFT JOIN pg2cm slave2 ON (base.id = slave2.country_month_id)
# ```

queryset_gamma = (Queryset("mihai_pgm_cm_cy_comparison5","country_month")
      .with_column(Column("ged_pgm",  from_table = "ged2_pgm", from_column = "ged_sb_best_sum_nokgi").aggregate("sum"))
      .with_column(Column("ged_cm", from_table = "ged2_cm", from_column = "ged_sb_best_sum_nokgi").aggregate("sum"))
           )

def assertions_gamma(dataset):
    assert dataset.loc[365].loc[59].ged_pgm == 426
    assert dataset.loc[365].loc[59].ged_cm == 440

if __name__ == "__main__":
    trials = [
            (baseline_queryset, baseline_assertions, "Baseline data worked!"),
            (queryset_alpha, assertions_alpha, "Alpha worked"),
            (queryset_beta, assertions_beta, "Beta worked"),
            (queryset_gamma, assertions_gamma, "Gamma worked"),
        ]

    for queryset, assertions, message in trials:
        data = queryset.publish().fetch()
        assertions(data)
        print(message)
