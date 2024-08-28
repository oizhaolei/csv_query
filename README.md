## Usage:

``` shell
-- csv_query "select * from ./xxxx.csv where res_code in (200, 201) and cs_byte>=1000 and url in-file-regex ./URLS.lst"
cat original.csv | csv_query "res_code in (\"200\", \"201\")"
            | csv_query "cs_byte >= 1000"
            | csv_query "url match_by_file ./URLS.regex.lst"
            | tee filtered.csv

```

## Query Specification
Only 'STRING' type is supported currently.
- `=`
- `>`
- `>=`
- `<`
- `<=`
- `in`
- `match`
- `match-by-file`




## TODO
- [ ] parse query string with `nom`
- [ ] numeric field compare
- [ ] in-file-with-regex
