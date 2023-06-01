Make sure your docker for mac has > 16gb of memory
Note: maybe just one small data file locally

1. Add the query_history_schema.json and query_history_offlineSpec.json to the trino/plugin/trino-pinot/src/test/resources directory
2. Download the data from s3://galaxy-data-lake/data-lake/tmp_pinot_0429_0530/ (use prod aws account)
3. For each file untar (i.e. tar -zxf <file>) and put into the following directory:
$HOME/Downloads/hackathon/input

Then go to io.trino.plugin.pinot.BasePinotConnectorSmokeTest, run testSleep for the TestPinotLatestConnectorSmokeTest

it will generate segments to $HOME/Downloads/hackathon/output and upload to the 


