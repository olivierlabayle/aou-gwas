# aou-gwas


## Make GWAS Data

### Parameters

- `DB_NAME (default: wb-silky-artichoke-2408.C2024Q3R8_index_111825)`: Name of your main phenotypes database.
- `ANCESTRY_FILEPATH (default: gs://vwb-aou-datasets-controlled/v8/wgs/short_read/snpindel/aux/ancestry/echo_v4_r2.ancestry_preds.tsv)`: Path to the ancestry file estimated by AoU.
- `N_DAYS_THRESHOLD (default: 7)`: A condition is considered severe if a critical illness phenotype is defined within that time window.

### Running the Workflow

Login to a jupyter notebook instance, and run:

```bash
wb nextflow run main.nf -c config/run.config -profile google-batch -process.container="${ARTIFACT_REGISTRY_DOCKER_REPO}/olivierlabayle/aou-gwas:main"
```