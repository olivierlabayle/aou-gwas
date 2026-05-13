import pandas
import pandas_gbq
import os

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

def get_severe_events_from_visits(db_name, severe_visit_concept_ids=["9201", "262"]):
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
    db_name = "`wb-silky-artichoke-2408.C2024Q3R8_index_111825`"
    days_threshold = 7
    # Define the conditions of interest and the severe events of interest (both conditions and procedures) that will be used to determine whether an individual had a severe event within a certain time threshold of having a condition of interest.
    conditions_of_interest = {"4266367": "Influenza", "4120302": "Streptococcus pyogenes infection", "4192640": "Pancreatitis", "255848": "Pneumonia"}
    severe_procedure_concept_ids = {"4145647": "Assisted breathing", "40487536": "Intubation of respiratory tract", "4013354": "Insertion of endotracheal tube", "4230167": "Artificial respiration", "44783799": "Exteriorization of trachea"}
    severe_conditions_concept_ids = {"132797": "Sepsis"}
    severe_observation_concept_ids = {"4148981": "Intensive care unit", "4046295": "Care of intensive care unit patient"}
    # Load the severe events that we want to compare against. Severe events are defined from any of the following sources:
    # - Severe conditions (defined by severe_procedure_concept_ids)
    # - Severe procedures (defined by severe_procedure_concept_ids)
    severe_events_from_conditions_df = get_severe_events_from_conditions(db_name, severe_conditions_concept_ids=list(severe_conditions_concept_ids.keys()))
    severe_events_from_procedure_df = get_severe_events_from_procedures(db_name, severe_procedure_concept_ids=list(severe_procedure_concept_ids.keys()))
    severe_events_from_observations_df = get_severe_events_from_observations(db_name, severe_observation_concept_ids=list(severe_observation_concept_ids.keys()))

    # Load the conditions of interest which will be used to determine the infection and the timing of the infection relative to the severe event.
    conditions_of_interest_df = get_conditions_of_interest_events(db_name, conditions_of_interest=conditions_of_interest)
    
    # For each source of severe events, find the conditions of interest that occur within a certain time threshold of the severe event and record the minimum age at occurrence for each condition of interest for each individual.
    severe_conditions_dfs = []

    for severity_source_df in [severe_events_from_conditions_df, severe_events_from_procedure_df, severe_events_from_observations_df]:
        severe_conditions_from_severity_source_df = find_overlap_between_conditions_and_severe_events(conditions_of_interest_df, severity_source_df, days_threshold=days_threshold)
        severe_conditions_dfs.append(severe_conditions_from_severity_source_df)

    severe_conditions_df = pandas.concat(severe_conditions_dfs, ignore_index=True)

if __name__ == "__main__":
    main()