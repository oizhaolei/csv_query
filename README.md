## Usage:

``` shell
-- csv_query "select * from ./xxxx.csv where res_code in (200, 201) and cs_byte>=1000 and url in-file-regex ./URLS.lst"
cat original.csv | csv_query "res_code in (\"200\", \"201\")"
            | csv_query "cs_byte >= 1000"
            | csv_query "url match_any_in_file ./url.regex.txt"
            | tee filtered.csv

```

## Query Specification

`Field_Name Operator Value`

- Three parts, separated with SPACE.
- Keyword `OR`, `AND` is not supported yet.

For example:
- `req_method = 'GET'`
- `req_method != "GET"`
- `req_method <> 'GET'`
- `cs_byte > 1000`
- `cs_byte >= 1000`
- `cs_byte < 1000`
- `cs_byte <= 1000`
- `res_code in ('200', '201')`
- `res_code not_in (400, 404)`
- `remote_host match ^https://github.com`
- `remote_host match_any_in_file ./url.regex.txt`




## TODO
- [X] parse query string with `nom`
- [ ] numeric field compare
- [X] in-file-with-regex


``` shell

head ./sample.csv | ./target/release/csv_query "resp_size > 116000" | ./target/release/csv_query "req_method = 'CONNECT'"
  
```

``` shell
 head -n 10000 ./sample.csv | ./target/release/csv_query "resp_size > 1000" | ./target/release/csv_query "req_method not_in (CONNECT,GET)" | ./target/release/csv_query "remote_host match http://10\." | ./target/release/csv_query "remote_host match_any_in_file ./url.regex.txt"

```
