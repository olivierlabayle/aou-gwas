process ExtractPhenotypes {
    cpus 2
    memory '12 GB'

    input:
        path ancestry_file

    output:
        path "covariates.tsv"

    script: 
        """
        python /opt/aou-gwas/bin/extract_phenotypes.py \
            --ancestry_filepath ${ancestry_file} \
            --db_name ${params.DB_NAME} \
            --days_threshold ${params.N_DAYS_THRESHOLD}
        """
}

workflow {
    ancestry_file = channel.fromPath(params.ANCESTRY_FILEPATH)
    covariates = ExtractPhenotypes(ancestry_file)
}