Usage:

# csv_query --query "select * from ./xxxx.csv where res_code in (200, 201) and cs_byte>=1000 and url in-file-regex ./URLS.lst"
cat original.csv | csv_query --query "res_code in (200, 201)"
            | csv_query --query "cs_byte >= 1000"
            | csv_query --query "url in-file-with-regex ./URLS.lst"
            | tee filtered.csv
