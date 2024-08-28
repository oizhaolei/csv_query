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

- Only 'STRING' type compare is supported currently.
- Keyword `OR`, `AND` is not supported.



## TODO
- [X] parse query string with `nom`
- [ ] numeric field compare
- [ ] in-file-with-regex
