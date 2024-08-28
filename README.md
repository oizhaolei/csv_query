# Query csv steam with sql `where-like` language.

## Usage:

``` shell
cat original.csv | csv_query "res_code in (200, 201)"
                 | csv_query "cs_byte >= 1000"
                 | csv_query "url match_any_in_file ./url.regex.txt"
                 | cut -d , -f 1-4,8
                 > tee filtered.csv
```

``` shell
cat ./original.csv 
                | csv_query "resp_size > 116000" 
                | csv_query "req_method = CONNECT"
```

``` shell
cat ./original.csv 
                | csv_query "resp_size >= 1000" 
                | csv_query "req_method not_in (CONNECT,GET)" 
                | csv_query "remote_host match http://10\." 
                | csv_query "remote_host match_any_in_file ./url.regex.txt"
```

## Query Specification

- Three parts: Field Name, Operator, Value, MUST be separated with `SPACE`.

For example:
- `req_method = GET`
- `req_method != GET`
- `req_method <> GET`
- `cs_byte > 1000`
- `cs_byte >= 1000`
- `cs_byte < 1000`
- `cs_byte <= 1000`
- `res_code in (200, 201)`
- `res_code not_in (400, 404)`
- `remote_host match ^https://github.com`
- `remote_host match_any_in_file ./url.regex.txt`
