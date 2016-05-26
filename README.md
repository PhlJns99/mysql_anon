# DB Anonymizer

This script will anonymize a MySQL backup file. Config is in config.csv. The config file has three columns: table name, column name and data type.  Add entry to the file for each column that needs anonymizing.  Valid data types are address, companyname, contactname, email, phone, url and random.  Anon data is pulled from the CSV files, only 5000 entries are loaded, and it will loop through to beginning of the list when the 5000 have been used.

### Limitations

Currently only works with lower case table names (simple change to the regex would fix that). Probably won't work if you have schema names(again, update the regex!).  Anonymised data will be truncated to the length of the original data to ensure the insert line doesn't exceed the mysql max allowed packet. May throw up the odd warning about invalid utf8 bytes. 

### Install require modules
```shel
sudo apt-get install libtext-csv-xs-perl libtext-csv-perl
```


### Usage
```shell
./anon.pl <name_of_backup_file.sql>
```
