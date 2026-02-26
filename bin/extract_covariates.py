#!/usr/bin/env -S uv run --script

import pandas
import pandas_gbq
import os

# This is the set of infections we are interested in, add more here to define new phenotypes
INFECTION_MAP = {4183609: "ACUTE_INFLUENZA", 199074: "ACUTE_PANCREATITIS", 255848: "PNEUMONIA"}
INFECTION_CODES = ", ".join(str(x) for x in INFECTION_MAP.keys())

# This defines what we consider to be extreme visits
EXTREME_VISIT_TYPE_NAMES = ["Urgent Care Facility", "Intensive Care", "Emergency department patient visit", "Emergency Room - Hospital", "Emergency Room and Inpatient Visit", "Hospital", "Emergency Room Visit", "Ambulance - Land"]
EXTREME_VISIT_TYPES = [8782, 32037, 4163685, 8870, 262, 38004515, 9203, 8668]
EXTREME_VISIT_TYPES = ", ".join(str(x) for x in EXTREME_VISIT_TYPES)

def sex_is_concordant(sab, gender):
    if sab == "Male" and gender == "Man":
        return True
    elif sab == "Female" and gender == "Woman":
        return True
    else:
        return False
        
def load_query(query):
    try:
        df = pandas_gbq.read_gbq(
            query,
            dialect="standard",
            use_bqstorage_api=True)
    except Exception:
        # Fallback execution if the Storage API is unavailable or unauthorized.
        df = pandas_gbq.read_gbq(
            query,
            dialect="standard",
            use_bqstorage_api=False)
    return df


def load_person_df():
    """
    Returns a Dataframe with columns: PERSON_ID, AGE, SEX_AT_BIRTH, GENDER
    Only individuals who :
        - consented EHRs
        - Have concordant sex and gender
    are returned.
    """
    person_df = load_query("""
    SELECT
        id AS PERSON_ID,
        DATE_DIFF(CURRENT_DATE(), CAST(date_of_birth AS DATE), YEAR) AS AGE,
        T_DISP_sex_at_birth AS SEX_AT_BIRTH,
        T_DISP_gender AS GENDER
    FROM `wb-silky-artichoke-2408.C2024Q3R8_index_111825`.T_ENT_person
    WHERE has_ehr_data = true
    """)
    person_df["SEX_IS_CONCORDANT"] = [sex_is_concordant(sab, gender) for sab, gender in zip(person_df.SEX_AT_BIRTH, person_df.GENDER)]
    person_df = person_df[person_df["SEX_IS_CONCORDANT"]][["PERSON_ID",	"AGE", "SEX_AT_BIRTH"]]
    person_df["SEX_AT_BIRTH"] = [1 if x == "Male" else 0 for x in person_df["SEX_AT_BIRTH"]]
    return person_df

def load_extreme_visits():
    extreme_visits_df = load_query(f"""
    SELECT 
        person_id AS PERSON_ID,  
        PARENT_CONDITION_CONCEPT_ID AS PARENT_CONDITION_CONCEPT_ID,
        T_DISP_standard_concept_name AS CONDITION_CONCEPT_NAME,
        visit_occurrence_concept_name AS VISIT_TYPE,
        T_DISP_condition_type_concept_name AS CONDITION_TYPE,
        age_at_occurrence AS AGE_AT_OCCURENCE
    FROM 
        (SELECT ancestor AS PARENT_CONDITION_CONCEPT_ID, descendant AS CHILD_CONDITION_CONCEPT_ID
            FROM `wb-silky-artichoke-2408.C2024Q3R8_index_111825`.T_HAD_conditionConcept_default
            WHERE ancestor IN ({INFECTION_CODES}) 
        UNION ALL 
        SELECT DISTINCT ancestor AS PARENT_CONDITION_CONCEPT_ID, ancestor AS CHILD_CONDITION_CONCEPT_ID
            FROM `wb-silky-artichoke-2408.C2024Q3R8_index_111825`.T_HAD_conditionConcept_default
            WHERE ancestor IN ({INFECTION_CODES})
        ) AS CONDITION_IDS
    INNER JOIN
        (SELECT *
            FROM `wb-silky-artichoke-2408.C2024Q3R8_index_111825`.T_ENT_conditionOccurrence
            WHERE visit_type IN ({EXTREME_VISIT_TYPES})
        ) AS EXTREME_VISITS
    ON CONDITION_IDS.CHILD_CONDITION_CONCEPT_ID = EXTREME_VISITS.condition_concept_id
    """)
    # Make a SEVERE column for each infection
    for infection_code, infection_name in INFECTION_MAP.items():
        extreme_visits_df[infection_name] = extreme_visits_df.PARENT_CONDITION_CONCEPT_ID == infection_code
    # Just record whether an individual had an extreme infection
    person_has_extreme_infection = extreme_visits_df[["PERSON_ID", *INFECTION_MAP.values()]].groupby("PERSON_ID").any()
    # Record the min age at infection that will be used for age
    person_min_age_at_extreme_infection = extreme_visits_df[["PERSON_ID", "AGE_AT_OCCURENCE"]].groupby("PERSON_ID").min()
    return person_has_extreme_infection.join(person_min_age_at_extreme_infection, on="PERSON_ID").reset_index()

def load_conditions():
    parent_condition_string = "\n".join([f"UNION ALL SELECT {code} AS PARENT_CONDITION_CONCEPT_ID, {code} AS CHILD_CONDITION_CONCEPT_ID" for code in INFECTION_CODES.split(", ")])
    conditions_df = load_query(f"""
    SELECT 
        person_id AS PERSON_ID,  
        PARENT_CONDITION_CONCEPT_ID AS PARENT_CONDITION_CONCEPT_ID,
        T_DISP_standard_concept_name AS CONDITION_CONCEPT_NAME,
        visit_occurrence_concept_name AS VISIT_TYPE,
        T_DISP_condition_type_concept_name AS CONDITION_TYPE,
        age_at_occurrence AS AGE_AT_OCCURENCE
    FROM 
        (SELECT ancestor AS PARENT_CONDITION_CONCEPT_ID, descendant AS CHILD_CONDITION_CONCEPT_ID
            FROM `wb-silky-artichoke-2408.C2024Q3R8_index_111825`.T_HAD_conditionConcept_default
            WHERE ancestor IN ({INFECTION_CODES}) 
        {parent_condition_string}
        ) AS CONDITION_IDS
    INNER JOIN
        (SELECT *
            FROM `wb-silky-artichoke-2408.C2024Q3R8_index_111825`.T_ENT_conditionOccurrence
        ) CONDITION_OCCURENCE
    ON CONDITION_IDS.CHILD_CONDITION_CONCEPT_ID = CONDITION_OCCURENCE.condition_concept_id
    """)
    # Make a SEVERE column for each infection
    for infection_code, infection_name in INFECTION_MAP.items():
        conditions_df[infection_name] = conditions_df.PARENT_CONDITION_CONCEPT_ID == infection_code
    # Just record whether an individual had an extreme infection
    unique_person_to_conditions_df = conditions_df[["PERSON_ID", *INFECTION_MAP.values()]].groupby("PERSON_ID").any()
    # Record the min age at infection that will be used for age
    person_min_age_at_conditions_df = conditions_df[["PERSON_ID", "AGE_AT_OCCURENCE"]].groupby("PERSON_ID").min()
    return unique_person_to_conditions_df.join(person_min_age_at_conditions_df, on="PERSON_ID").reset_index()

def main():
    print("Loading person data.")
    person_df = load_person_df()
    print("Loading condtions matching: ", INFECTION_CODES)
    conditions_df = load_conditions()
    covariates = person_df.merge(conditions_df, on="PERSON_ID", how="left")
    print("Consolidating dataset.")
    covariates["AGE"] = [age if pandas.isna(age_at_occ) else age_at_occ for age, age_at_occ in zip(covariates["AGE"], covariates["AGE_AT_OCCURENCE"])]
    for infection_name in INFECTION_MAP.values():
        covariates[infection_name] = covariates[infection_name].fillna(False)
    covariates.drop(columns=["AGE_AT_OCCURENCE"], inplace=True)
    covariates.to_csv("covariates.csv", index=False)
    print("Done.")
    return 0

if __name__ == "__main__":
    main()