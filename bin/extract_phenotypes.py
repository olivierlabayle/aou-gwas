#!/usr/bin/env -S uv run --script

import pandas
import pandas_gbq
import os


person_sql = """
SELECT 
    date_of_birth, 
    ethnicity, 
    T_DISP_ethnicity,
    ethnicity_concept_id,
    gender,
    T_DISP_gender,
    gender_concept_id,
    person_id,race,
    T_DISP_race,
    race_concept_id,
    self_reported_category,
    T_DISP_self_reported_category,
    self_reported_category_concept_id,
    sex_at_birth,
    T_DISP_sex_at_birth,
    sex_at_birth_concept_id
    FROM `wb-silky-artichoke-2408.C2024Q3R8_index_111825`.T_ENT_person
    WHERE id IN (
        SELECT 
            person_id AS primary_id
            FROM `wb-silky-artichoke-2408.C2024Q3R8_index_111825`.T_ENT_conditionOccurrence
            WHERE
            (condition_concept_id IN (
                SELECT descendant 
                FROM `wb-silky-artichoke-2408.C2024Q3R8_index_111825`.T_HAD_conditionConcept_default
                WHERE ancestor = 4266367 
                UNION ALL SELECT 4266367)
                )
            AND (visit_type IN (8668, 38004207, 38004218, 38004222, 38004228, 38004238, 38004251, 38004267, 38004269, 38004268, 38004262, 38004249, 38004246, 38004245, 38004250, 38004258, 581479, 38004259, 8883, 8870, 9203, 32037, 8782, 8717, 38004515)))
"""

# We attempt to use the BigQuery Storage API for high-speed data retrieval.
# If the BQ Storage API is not enabled in the environment or permissions 
# are missing (which may occur if running outside of the Workbench environment), 
# it will fall back to the standard (slower) REST API automatically.
try:
    person_df = pandas_gbq.read_gbq(
        person_sql,
        dialect="standard",
        use_bqstorage_api=True,
        progress_bar_type="tqdm_notebook")
except Exception:
    # Fallback execution if the Storage API is unavailable or unauthorized.
    person_df = pandas_gbq.read_gbq(
        person_sql,
        dialect="standard",
        use_bqstorage_api=False,
        progress_bar_type="tqdm_notebook")

person_df.head(5)

