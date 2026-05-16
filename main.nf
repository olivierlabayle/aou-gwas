process ExtractPhenotypes {
    cpus 2
    memory '12 GB'

    input:
        path ancestry_file

    output:
        path "covariates.tsv", emit: covariates
        path "iids.txt", emit: iids

    script: 
        """
        python /opt/aou-gwas/bin/extract_phenotypes.py \
            ${ancestry_file} \
            ${params.DB_NAME} \
            -d ${params.N_DAYS_THRESHOLD}
        """
}

process QCPGENFile {
    cpus 8
    memory '16 GB'

    input:
        tuple val(chr), path(pgen_file), path(pvar_file), path(psam_file)
        path sample_ids

    output:
        tuple val(chr), path("${output_prefix}.pgen"), path("${output_prefix}.pvar"), path("${output_prefix}.psam")

    script:
        input_prefix = pgen_file.baseName
        output_prefix = "chr${chr}.qced"
        """
        plink2 \
            --pfile ${input_prefix} \
            --max-alleles 2 \
            --min-alleles 2 \
            --keep ${sample_ids} \
            --snps-only \
            --set-all-var-ids @:#:\$r:\$a \
            --make-pgen \
            --out ${output_prefix}
        # Add a FID column to the psam file which is required for downstream analyses.
        awk 'BEGIN {OFS="\t"}
            NR==1 {
                print "#FID", "IID", $2
                next
            }
            {
                print $1, $1, $2
            }' ${output_prefix}.psam > ${output_prefix}.temp.psam
        mv ${output_prefix}.temp.psam ${output_prefix}.psam
        """
}

process QCBEDFile {
    cpus 8
    memory '16 GB'

    input:
        tuple path(bed_file), path(bim_file), path(fam_file)
        path sample_ids

    output:
        tuple path("${output_prefix}.bed"), path("${output_prefix}.bim"), path("${output_prefix}.fam")

    script:
        input_prefix = bed_file.baseName
        output_prefix = "${input_prefix}.qced"
        """
        plink2 \
            --bfile ${input_prefix} \
            --keep ${sample_ids} \
            --geno ${params.GENO_MISSINGNESS} \
            --maf ${params.MAF} \
            --hwe ${params.HWE} \
            --make-bed \
            --out ${output_prefix}
        # Add a FID column to the fam file which is required for downstream analyses.
        awk 'BEGIN {OFS="\t"}
            {
                $1 = $2
                print
            }' ${output_prefix}.fam > ${output_prefix}.temp.fam
        mv ${output_prefix}.temp.fam ${output_prefix}.fam
        """
}

workflow {
    main:
        // Create the covariates dataset
        ancestry_file = file(params.ANCESTRY_FILEPATH)
        phenotypes_ch = ExtractPhenotypes(ancestry_file)
        //  QC the PGEN files
        pgen_ch = channel.of(params.PGEN_FILES)
            .flatMap { csv -> csv.splitCsv() }
            .map { row ->
                record(chr: row[0], pgen: file(row[1]), pvar: file(row[2]), psam: file(row[3]))
            }
        QCPGENFile(pgen_ch, phenotypes_ch.iids)

        //QC BED files
        bed_file = channel.fromPath(params.BED_PREFIX).collect()
        QCBEDFile(bed_file, phenotypes_ch.iids)

    publish:
        phenotypes_ch
}

output {
    phenotypes_ch {
        path { f -> "covariates/{f.name}" }
        mode 'copy'
    }
}