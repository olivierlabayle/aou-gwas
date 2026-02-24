#!/usr/bin/env -S uv run --script

import pandas
import pandas_gbq
import os

# This is the set of infections we are interested in, add more here to define new phenotypes
INFLUENZA_SNOMED_ID = 4266367
SNOMED_IDS = [INFLUENZA_SNOMED_ID]
SNOMED_IDS = ", ".join(str(x) for x in SNOMED_IDS)

# This defines what we consider to be extreme cases
EXTREME_VISTI_TYPES_NAMES = ["Urgent Care Facility", "Intensive Care", "Emergency department patient visit", "Emergency Room - Hospital", "Emergency Room and Inpatient Visit", "Hospital", "Emergency Room Visit", "Ambulance - Land"]
EXTREME_VISIT_TYPES = [8782, 32037, 4163685, 8870, 262, 38004515, 9203, 8668]
EXTREME_VISIT_TYPES = ", ".join(str(x) for x in EXTREME_VISIT_TYPES)

query = f"""SELECT 
            PARENT_CONDITION_CONCEPT_ID AS PARENT_CONDITION_CONCEPT_ID,
            T_DISP_standard_concept_name AS CONDITION_CONCEPT_NAME,
            T_DISP_condition AS CONDITION_CONCEPT_NAME_BIS,
            DATE_DIFF(CURRENT_DATE(), CAST(date_of_birth AS DATE), YEAR) AS AGE,
            visit_occurrence_concept_name AS VISIT_TYPE,
            T_DISP_condition_type_concept_name AS CONDITION_TYPE,
            T_DISP_sex_at_birth AS SEX_AT_BIRTH,
            T_DISP_gender AS GENDER,
            age_at_occurrence AS AGE_AT_OCCURENCE
            FROM
            (SELECT ancestor AS PARENT_CONDITION_CONCEPT_ID, descendant AS CHILD_CONDITION_CONCEPT_ID
                FROM `wb-silky-artichoke-2408.C2024Q3R8_index_111825`.T_HAD_conditionConcept_default
                WHERE ancestor IN ({SNOMED_IDS}) 
            UNION ALL 
            SELECT DISTINCT ancestor AS PARENT_CONDITION_CONCEPT_ID, ancestor AS CHILD_CONDITION_CONCEPT_ID
                FROM `wb-silky-artichoke-2408.C2024Q3R8_index_111825`.T_HAD_conditionConcept_default
                WHERE ancestor IN ({SNOMED_IDS})) AS CONDITION_IDS
            INNER JOIN
            (SELECT *
                FROM `wb-silky-artichoke-2408.C2024Q3R8_index_111825`.T_ENT_conditionOccurrence
                WHERE visit_type IN ({EXTREME_VISIT_TYPES})
            ) AS EXTREME_VISITS
            ON CONDITION_IDS.CHILD_CONDITION_CONCEPT_ID = EXTREME_VISITS.condition_concept_id
            INNER JOIN
            (SELECT
                id,
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
            ) AS PERSON_DETAILS
            ON EXTREME_VISITS.person_id = PERSON_DETAILS.id
"""

try:
    df = pandas_gbq.read_gbq(
        query,
        dialect="standard",
        use_bqstorage_api=True,
        progress_bar_type="tqdm_notebook")
except Exception:
    # Fallback execution if the Storage API is unavailable or unauthorized.
    df = pandas_gbq.read_gbq(
        query,
        dialect="standard",
        use_bqstorage_api=False,
        progress_bar_type="tqdm_notebook")

df.to_csv("covariates.tsv", sep="\t")