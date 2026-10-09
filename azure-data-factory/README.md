# Azure Data Factory: `adf-beehaven-dev`

Exported from Azure Data Factory (**Manage → ARM template → Export ARM template**).
All account keys and connection strings are `SecureString` parameters with **empty values**, so no secrets are stored here.

| File | What it is |
|---|---|
| `pipeline_BronzeNew_To_BronzeArchive.json` | The main end-to-end pipeline (bronze → silver → gold) |
| `pipeline_Lake_old_to_broze_new.json` | Helper pipeline: copies the raw CSVs from the source storage into `bronze/new` |
| `trigger_DailyRun.json` | Schedule trigger: runs the main pipeline once a day |
| `arm-template/` | Full ARM template of the factory (pipelines, datasets, linked services, trigger). Can be redeployed to recreate the factory |

## Main pipeline: `BronzeNew_To_BronzeArchive`

![Pipeline canvas](../images/azure/03b_adf_pipeline_canvas.png)

| # | Activity | Type | Runs after | What it does |
|---|---|---|---|---|
| 1 | `Copy_Lake_Old_to_Bronze_New` | Copy | start | Raw CSVs from the source storage (`hive-uploads`) → `bronze/new` |
| 2 | `Delete_Lake_Old` | Delete | 1 | Clears the source drop zone |
| 3 | `GetMetadata_BronzeNew` | Get Metadata | 1 | Lists the files in `bronze/new` (`childItems`) |
| 4 | `ForEachEachFileInBrozeNew` → `CopyBronzeNew_To_Archive` | ForEach + Copy | 3 | Archives every file with a timestamp (see below) |
| 5 | `Copy_Bronze_New_to_Silver_processing` | Copy | 4 | Working copy → `silver/processing` |
| 6 | `Delete_Bronze_New` | Delete | 5 | Empties `bronze/new` (files are safe in the archive) |
| 7 | `Notebook_Silver_to_goldProcessing` | Synapse Notebook | 5 | Runs notebook **04**: clean → `silver/` + hand-over to `gold/processing` |
| 8 | `Delete_SilverProcessing` | Delete | 7 | Empties the silver in-tray **only if 04 succeeded** |
| 9 | `Notebook_Gold_1DF` | Synapse Notebook | 8 | Runs notebook **05**: builds the gold tables |
| 10 | `Delete_GoldPrcessing` | Delete | 9 | Empties the gold in-tray |

Both notebook activities run on Spark pool `PoolDemo1` with **Small** driver and executor sizes (4 vCores / 28 GB), matching the pool's node size.

### Dynamic archive naming (parameterised dataset `Bronze_Archive`)
```text
folder_name = @concat('bronze/archive/', split(item().name, '_')[0])
file_name   = @concat(split(split(item().name, '_')[1], '.')[0], '_',
                      utcNow('yyyy-MM-ddTHH''h''mm''m''ss''s'''), '.csv')
```
`flow_schwartau.csv` → `bronze/archive/flow/schwartau_2026-10-09T10h17m49s.csv` (UTC time)

### Trigger
`DailyRun`: schedule trigger, every 1 day, time zone W. Europe (Berlin).
