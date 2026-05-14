import pandas
import pandas_gbq
import os

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

def load_person_df(db_name):
    """
    Returns a Dataframe with columns: PERSON_ID, AGE, SEX_AT_BIRTH, GENDER
    Only individuals who :
        - consented EHRs
        - Have concordant sex and gender
    are returned.
    """
    person_df = load_query(f"""
    SELECT
        id AS PERSON_ID,
        DATE_DIFF(CURRENT_DATE(), CAST(date_of_birth AS DATE), YEAR) AS AGE,
        T_DISP_sex_at_birth AS SEX_AT_BIRTH,
        T_DISP_gender AS GENDER
    FROM {db_name}.T_ENT_person
    WHERE has_ehr_data = true
    """)
    person_df["SEX_IS_CONCORDANT"] = [sex_is_concordant(sab, gender) for sab, gender in zip(person_df.SEX_AT_BIRTH, person_df.GENDER)]
    person_df = person_df[person_df["SEX_IS_CONCORDANT"]][["PERSON_ID",	"AGE", "SEX_AT_BIRTH"]]
    person_df["SEX_AT_BIRTH"] = [1 if x == "Male" else 0 for x in person_df["SEX_AT_BIRTH"]]
    return person_df

def load_conditions_data(db_name, conditions_concept_ids):
    unnest_string = ",".join(f"STRUCT({cid} AS condition_concept_id, {cid} AS master_condition_concept_id)" for cid in conditions_concept_ids)
    conditions_query = f"""
    SELECT DISTINCT 
        person_id, 
        T_DISP_standard_concept_name, 
        condition_concepts.condition_concept_id, 
        condition_concepts.master_condition_concept_id, 
        age_at_occurrence, 
        condition_start_datetime
    FROM {db_name}.T_ENT_conditionOccurrence
    INNER JOIN 
    (SELECT
        descendant as condition_concept_id, ancestor as master_condition_concept_id
    FROM
        {db_name}.T_HAD_conditionConcept_default 
    WHERE
        ancestor IN ({", ".join(conditions_concept_ids)})
    UNION DISTINCT
    SELECT * FROM UNNEST([{unnest_string}])
    ) AS condition_concepts
    ON T_ENT_conditionOccurrence.condition_concept_id = condition_concepts.condition_concept_id
    """
    return load_query(conditions_query)

def get_severe_events_from_conditions(db_name, severe_conditions_concept_ids=["132797"]):
    """
    Currently severe conditions are defined as "Sepsis" which is identified by the 132797 concept id.
    """
    severe_conditions_df = load_conditions_data(db_name, severe_conditions_concept_ids)
    return severe_conditions_df[["person_id", "condition_start_datetime"]].rename(columns={"condition_start_datetime": "severe_start_datetime"}).drop_duplicates()

def get_severe_events_from_procedures(db_name, severe_procedure_concept_ids=["4145647", "40487536"]):
    severe_procedures_query = f"""
    SELECT DISTINCT 
        person_id, 
        T_DISP_standard_concept_name, 
        procedure_concepts.procedure_concept_id, 
        age_at_occurrence, 
        procedure_datetime
    FROM {db_name}.T_ENT_procedureOccurrence
    INNER JOIN 
    (SELECT
        descendant as procedure_concept_id, ancestor as master_procedure_concept_id
    FROM
        {db_name}.T_HAD_procedureConcept_default 
    WHERE
        ancestor IN ({", ".join(severe_procedure_concept_ids)})
    UNION DISTINCT
    SELECT * FROM UNNEST([{", ".join(f"STRUCT({cid} AS procedure_concept_id, {cid} AS master_procedure_concept_id)" for cid in severe_procedure_concept_ids)}])
    ) AS procedure_concepts
    ON T_ENT_procedureOccurrence.procedure_concept_id = procedure_concepts.procedure_concept_id
    """
    severe_procedures_df = load_query(severe_procedures_query)
    return severe_procedures_df[["person_id", "procedure_datetime"]].rename(columns={"procedure_datetime": "severe_start_datetime"}).drop_duplicates()

def get_severe_events_from_observations(db_name, severe_observation_concept_ids=["4148981", "4046295"]):
    severe_observations_query = f"""
    SELECT DISTINCT 
        person_id, 
        observation_datetime
    FROM {db_name}.T_ENT_observationOccurrence 
    WHERE
        observation_concept_id IN ({", ".join(severe_observation_concept_ids)})
    """
    severe_observations_df = load_query(severe_observations_query)
    return severe_observations_df[["person_id", "observation_datetime"]].rename(columns={"observation_datetime": "severe_start_datetime"}).drop_duplicates()

def get_severe_events_from_visits(db_name, severe_visit_concept_ids=["32037"]):
    severe_visits_query = f"""
    SELECT DISTINCT 
        person_id, 
        visit_start_datetime
    FROM {db_name}.T_ENT_visitOccurrence 
    WHERE
        visit_concept_id IN ({", ".join(severe_visit_concept_ids)})
    """
    severe_visits_df = load_query(severe_visits_query)
    return severe_visits_df[["person_id", "visit_start_datetime"]].rename(columns={"visit_start_datetime": "severe_start_datetime"}).drop_duplicates()

def get_conditions_of_interest_events(db_name, conditions_of_interest = {"4266367": "Influenza", "4120302": "Streptococcus pyogenes infection", "4192640": "Pancreatitis", "255848": "Pneumonia"}):  
    conditions_of_interest_df = load_conditions_data(db_name, list(conditions_of_interest.keys()))
    conditions_of_interest_df["condition"] = [conditions_of_interest[str(x)] for x in conditions_of_interest_df["master_condition_concept_id"]]
    return conditions_of_interest_df

def find_overlap_between_conditions_and_severe_events(conditions_df, severity_source_df, days_threshold=7):
    merged_df = conditions_df.merge(severity_source_df, on="person_id", how="inner")
    merged_df["events_time_diff"] = (merged_df["condition_start_datetime"] - merged_df["severe_start_datetime"]).dt.days.abs()
    merged_df = merged_df[merged_df["events_time_diff"] < days_threshold]
    return merged_df.groupby(["person_id", "condition"], as_index=False)["age_at_occurrence"].min()


def main():
    # Hardcode variables for now but these could be easily converted to command line arguments or config variables in the future.
    db_name = "`wb-silky-artichoke-2408.C2024Q3R8_index_111825`"
    days_threshold = 7
    conditions_of_interest = {"4266367": "INFLUENZA", "4120302": "GROUP_A_STREPTOCOCCAL_INFECTION", "4192640": "PANCREATITIS", "255848": "PNEUMONIA"}
    severe_procedure_concept_ids = {"4145647": "Assisted breathing", "40487536": "Intubation of respiratory tract", "4013354": "Insertion of endotracheal tube", "4230167": "Artificial respiration", "44783799": "Exteriorization of trachea"}
    severe_conditions_concept_ids = {"132797": "Sepsis"}
    severe_observation_concept_ids = {"4148981": "Intensive care unit", "4046295": "Care of intensive care unit patient"}
    severe_visit_concept_ids = {"32037": "Intensive Care"}
    # Load the severe events that we want to compare against. Severe events are defined from any of the following sources:
    # - Severe conditions (defined by severe_procedure_concept_ids)
    # - Severe procedures (defined by severe_procedure_concept_ids)
    # - Severe observations (defined by severe_observation_concept_ids)
    # - Severe visits (defined by severe_visit_concept_ids)
    print("Loading severe events from conditions, procedures, observations, and visits.")
    severe_events_from_conditions_df = get_severe_events_from_conditions(db_name, severe_conditions_concept_ids=list(severe_conditions_concept_ids.keys()))
    severe_events_from_procedure_df = get_severe_events_from_procedures(db_name, severe_procedure_concept_ids=list(severe_procedure_concept_ids.keys()))
    severe_events_from_observations_df = get_severe_events_from_observations(db_name, severe_observation_concept_ids=list(severe_observation_concept_ids.keys()))
    severe_events_from_visits_df = get_severe_events_from_visits(db_name, severe_visit_concept_ids=list(severe_visit_concept_ids.keys()))
    
    # Load the conditions of interest which will be used to determine the infection and the timing of the infection relative to the severe event.
    print("Loading conditions of interest: ", conditions_of_interest.values())
    conditions_of_interest_df = get_conditions_of_interest_events(db_name, conditions_of_interest=conditions_of_interest)
    
    # For each source of severe events, find the conditions of interest that occur within a certain time threshold of the severe event and record the minimum age at occurrence for each condition of interest for each individual.
    print("Finding conditions of interest that occur within ", days_threshold, " days of severe events.")
    severe_conditions_dfs = []
    for severity_source_df in [severe_events_from_conditions_df, severe_events_from_procedure_df, severe_events_from_observations_df, severe_events_from_visits_df]:
        severe_conditions_from_severity_source_df = find_overlap_between_conditions_and_severe_events(conditions_of_interest_df, severity_source_df, days_threshold=days_threshold)
        severe_conditions_dfs.append(severe_conditions_from_severity_source_df)

    severe_conditions_df = pandas.concat(severe_conditions_dfs, ignore_index=True)

    # Load person info and merge with severe conditions to create covariates dataset. 
    # For each condition, create a binary column indicating whether the person had that condition within the specified time threshold of a severe event. 
    person_to_severe_condition_df = severe_conditions_df[["person_id", "condition"]].drop_duplicates()
    person_df = load_person_df(db_name)
    for condition in severe_conditions_df["condition"].unique():
        severe_condition_df = person_to_severe_condition_df.loc[severe_conditions_df["condition"] == condition, ["person_id"]]
        severe_condition_df[condition] = 1
        person_df = pandas.merge(person_df, severe_condition_df[["person_id", condition]], on="person_id", how="left")
        person_df[condition] = person_df[condition].fillna(0)

    # Extract the first age of occurence of any severe condition (otherwise AGE would depend on the condition which would require different covariate datasets for each condition)
    print("Extracting age at occurrence for severe conditions.")
    severe_person_age_at_occurence = severe_conditions_df.groupby("person_id").min("age_at_occurence").to_dict()['age_at_occurrence']
    person_df["AGE"] = [severe_person_age_at_occurence[pid] if pid in severe_person_age_at_occurence else age for pid, age in zip(person_df["person_id"], person_df["AGE"])]

    # Remove severe individuals that do not have one of the desired conditions to avoid contamination of the control group
    print("Removing severe individuals that do not have one of the desired conditions to avoid contamination of the control group.")
    all_severe_individuals = set(severe_events_from_conditions_df["person_id"]).union(set(severe_events_from_procedure_df["person_id"])).union(set(severe_events_from_observations_df["person_id"])).union(set(severe_events_from_visits_df["person_id"]))
    severe_individuals_with_conditions_of_interest = set(severe_conditions_df["person_id"])
    severe_individuals_without_conditions_of_interest = all_severe_individuals - severe_individuals_with_conditions_of_interest
    person_df = person_df[~person_df["person_id"].isin(severe_individuals_without_conditions_of_interest)]

    # Save the covariates dataset
    print("Saving covariates dataset.")
    person_df.insert(0, "FID", person_df["person_id"])
    person_df.rename(columns={"person_id": "IID"}, inplace=True)
    person_df.to_csv("covariates.csv", index=False, sep="\t", na_rep="NA")

    print("Done.")
    return 0

if __name__ == "__main__":
    main()