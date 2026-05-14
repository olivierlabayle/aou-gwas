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
            ${ancestry_file} \
            ${params.DB_NAME} \
            -d ${params.N_DAYS_THRESHOLD}
        """
}

workflow {
    ancestry_file = channel.fromPath(params.ANCESTRY_FILEPATH)
    covariates = ExtractPhenotypes(ancestry_file)
}