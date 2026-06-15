# aou-gwas

## Generating GWAS Data

### Parameters

I believe only the `DB_NAME` parameter will be project specific.

- `DB_NAME (default: wb-silky-artichoke-2408.C2024Q3R8_index_111825)`: Name of your main phenotypes database.
- `ANCESTRY_FILEPATH (default: gs://vwb-aou-datasets-controlled/v8/wgs/short_read/snpindel/aux/ancestry/echo_v4_r2.ancestry_preds.tsv)`: Path to the ancestry file estimated by AoU.
- `N_DAYS_THRESHOLD (default: 7)`: A condition is considered severe if a critical illness phenotype is also defined within this time window.
- `PGEN_FILES (default: config/pgen_files.csv)`: A csv file pointing to the location of pgen files, see `config/pgen_files.csv` for the default. 
- `BED_PREFIX (default: gs://vwb-aou-datasets-controlled/v8/microarray/plink/arrays)`: Prefix to genotyping microarray data.
- `HWE (default 1e-30)`: Hardy-Weinberg QC parameter (this parameter together with the following QC parameters result in about 450,000 variants)
- `MAF (default: 0.01)`: Minor allele freuency QC parameter.
- `GENO_MISSINGNESS (default: 0.02)`: Genotype missingness filter.

### Running the Workflow

Login to a jupyter notebook instance, and run:

```bash
wb nextflow run main.nf -c config/run.config -profile google-batch
```

You should probably do this from within a tmux or screen session to make sure the pipelines completes.

## Running the GWAS

In order to run the GWAS you can use the [WDL-GWAS](https://github.com/olivierlabayle/WDL-GWAS) workflow. To set it up, you can follow the instructions presented in the [Verily docs](https://support.workbench.verily.com/docs/guides/workflows/cromwell/). Briefly, in your Workbench Workspace, go to the workflow page and add the workflow to it. Then run it and provide the inputs as required. 

Unfortunately, the workbench currently requires you to provide the inputs manually instead of via a json file. An example file from which the inputs can be copy/pasted is in `config/gwas.config`. Refer to the [WDL-GWAS documentation](https://olivierlabayle.github.io/WDL-GWAS/stable/) for more information on the parameters.