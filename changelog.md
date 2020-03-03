# Change log Parse Imoveis

## 0.1 - First version of functions

The functions pipeline is working for extracting the data
* download_html - download html from url passed into GCS
* parse_retal - parse downloaded html into json file
* pagination - do the pagination of the topics


* treat_bucket_to_sql.py - adding function that loads json from bucket into sql