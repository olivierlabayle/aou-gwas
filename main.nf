process ExtractPhenotypes {
    input:
        path ancestry_file

    output:
        path "covariates.tsv"

    script: 
        """
        ./bin/extract_phenotypes.py \
            --ancestry_filepath ${ancestry_file} \
            --db_name ${params.DB_NAME} \
            --days_threshold ${params.N_DAYS_THRESHOLD}
        """
}

workflow {
    ancestry_file = channel.fromPath(params.ANCESTRY_FILEPATH)
    covariates = ExtractPhenotypes(ancestry_file)
}