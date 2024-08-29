use std::fs::File;
use std::io::{BufRead, BufReader};
use std::{env, error::Error, io, process};

use nom::branch::alt;
use nom::bytes::complete::{tag_no_case, take_till, take_while};
use nom::character::complete::{char, multispace0, multispace1, space0};
use nom::combinator::map;
use nom::sequence::tuple;
use nom::IResult;
use nom::{multi::separated_list0, sequence::delimited};
use regex::Regex;

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub enum Operator {
    Equal(String),
    NotEqual(String),
    Greater(String),
    GreaterOrEqual(String),
    Less(String),
    LessOrEqual(String),
    In(Vec<String>),
    NotIn(Vec<String>),
    Match(String),
    MatchAnyInFile(Vec<String>),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Condition {
    pub field: String,
    pub operate: Operator,
}

fn till_space(s: &str) -> IResult<&str, &str> {
    take_till(|c| c == ' ')(s)
}

fn binary_comparison_operator(i: &str, value: String) -> IResult<&str, Operator> {
    alt((
        map(tag_no_case("!="), |_| Operator::NotEqual(value.clone())),
        map(tag_no_case("<>"), |_| Operator::NotEqual(value.clone())),
        map(tag_no_case(">="), |_| {
            Operator::GreaterOrEqual(value.clone())
        }),
        map(tag_no_case("<="), |_| Operator::LessOrEqual(value.clone())),
        map(tag_no_case("="), |_| Operator::Equal(value.clone())),
        map(tag_no_case("<"), |_| Operator::Less(value.clone())),
        map(tag_no_case(">"), |_| Operator::Greater(value.clone())),
        map(tag_no_case("in"), |_| {
            let (_, vec) = parse_query_value_to_vec(&value).unwrap();
            Operator::In(vec)
        }),
        map(tag_no_case("not_in"), |_| {
            let (_, vec) = parse_query_value_to_vec(&value).unwrap();
            Operator::NotIn(vec)
        }),
        map(tag_no_case("match_any_in_file"), |_| {
            Operator::MatchAnyInFile(read_file_to_vec(&value).unwrap())
        }),
        map(tag_no_case("match"), |_| Operator::Match(value.clone())),
    ))(i)
}

fn parse_query(i: &str) -> IResult<&str, Condition> {
    let (remain, (_, field, _, op, _)) = tuple((
        multispace0,
        till_space,
        multispace1,
        till_space,
        multispace1,
    ))(i)?;
    let value = remain.trim().to_string();
    let (_, operate) = binary_comparison_operator(op, value.clone())?;
    Ok((
        "",
        Condition {
            field: field.to_string(),
            operate,
        },
    ))
}

// Parser to extract the list inside parentheses
fn parse_query_value_to_vec(input: &str) -> IResult<&str, Vec<String>> {
    // Helper parser to recognize digits
    fn is_word_or_with_quote(c: char) -> bool {
        c == '\'' || c == '"' || c.is_alphanumeric()
    }

    // Parse a single number as a string
    let parse_number = map(take_while(is_word_or_with_quote), |s: &str| s.to_string());

    // Parse a comma-separated list of numbers
    let parse_list = separated_list0(delimited(space0, char(','), space0), parse_number);

    // Parse the entire list enclosed in parentheses
    delimited(char('('), parse_list, char(')'))(input)
}

fn read_file_to_vec(filename: &str) -> Result<Vec<String>, Box<dyn Error>> {
    // Open the file in read-only mode (returns io::Result<File>)
    let file = File::open(filename)?;

    // Create a BufReader to efficiently read the file line by line
    let reader = BufReader::new(file);

    // Read lines into a Vec<String>
    let lines: Vec<String> = reader
        .lines() // returns an iterator over io::Result<String>
        .collect::<Result<_, _>>()?; // collect into Vec<String>, handling errors

    Ok(lines)
}

fn run() -> Result<(), Box<dyn Error>> {
    // Get the query from the positional arguments.
    // If one doesn't exist, return an error.
    let query = match env::args().nth(1) {
        None => return Err(From::from("Usage: PIPELINE | csv_query \"QUERY_STRING\"")),
        Some(query) => query,
    };
    eprintln!("query: {}", query);

    let (_, condition) = parse_query(&query).unwrap();
    eprintln!("condition: {:?}", condition);

    // Build CSV readers and writers to stdin and stdout, respectively.
    let mut rdr = csv::Reader::from_reader(io::stdin());
    let mut wtr = csv::WriterBuilder::new()
        .delimiter(b',')
        .quote_style(csv::QuoteStyle::Always)
        .from_writer(io::stdout());

    // Before reading our data records, we should write the header record.
    wtr.write_record(rdr.headers()?)?;

    let field_index = rdr
        .headers()?
        .iter()
        .position(|h| h == condition.field)
        .unwrap();

    // Iterate over all the records in `rdr`, and write only records containing
    // `query` to `wtr`.
    for result in rdr.records() {
        let record = result?;

        let field_value = &record[field_index];
        let is_match = match &condition.operate {
            Operator::Equal(value) => {
                if Regex::new(r#"['|"][^'"]+['|"]"#).unwrap().is_match(value) {
                    //compare after ignore quota
                    field_value == &value[1..value.len() - 1]
                } else {
                    field_value == value
                }
            }
            Operator::NotEqual(value) => {
                if Regex::new(r#"['|"][^'"]+['|"]"#).unwrap().is_match(value) {
                    //compare after ignore quota
                    field_value != &value[1..value.len() - 1]
                } else {
                    field_value != value
                }
            }
            Operator::Greater(value) => {
                if Regex::new(r#"['|"][^'"]+['|"]"#).unwrap().is_match(value) {
                    //compare by stirng style
                    field_value > &value[1..value.len() - 1]
                } else {
                    //compare by number style
                    field_value.parse::<i64>().unwrap() > value.parse::<i64>().unwrap()
                }
            }
            Operator::GreaterOrEqual(value) => {
                if Regex::new(r#"['|"][^'"]+['|"]"#).unwrap().is_match(value) {
                    //compare by stirng style
                    field_value >= &value[1..value.len() - 1]
                } else {
                    //compare by number style
                    field_value.parse::<i64>().unwrap() >= value.parse::<i64>().unwrap()
                }
            }
            Operator::Less(value) => {
                if Regex::new(r#"['|"][^'"]+['|"]"#).unwrap().is_match(value) {
                    //compare by stirng style
                    field_value < &value[1..value.len() - 1]
                } else {
                    //compare by number style
                    field_value.parse::<i64>().unwrap() < value.parse::<i64>().unwrap()
                }
            }
            Operator::LessOrEqual(value) => {
                if Regex::new(r#"['|"][^'"]+['|"]"#).unwrap().is_match(value) {
                    //compare by stirng style
                    field_value <= &value[1..value.len() - 1]
                } else {
                    //compare by number style
                    field_value.parse::<i64>().unwrap() <= value.parse::<i64>().unwrap()
                }
            }
            Operator::In(vec) => vec.contains(&field_value.to_string()),
            Operator::NotIn(vec) => !vec.contains(&field_value.to_string()),
            Operator::Match(regex_str) => Regex::new(regex_str).unwrap().is_match(field_value),
            Operator::MatchAnyInFile(regex_vec) => regex_vec
                .iter()
                .map(|r| Regex::new(r).unwrap())
                .any(|re| re.is_match(field_value)),
        };

        if is_match {
            wtr.write_record(&record)?;
        }
    }

    // CSV writers use an internal buffer, so we should always flush when done.
    wtr.flush()?;
    Ok(())
}

fn main() {
    if let Err(err) = run() {
        eprintln!("{}", err);
        process::exit(1);
    }
}

#[cfg(test)]
mod tests {
    use regex::Regex;

    use crate::{parse_query, parse_query_value_to_vec, read_file_to_vec, Condition};

    #[test]
    fn test_is_match_regex_file() {
        let re_list = read_file_to_vec("./url.regex.txt")
            .unwrap()
            .iter()
            .map(|r| Regex::new(r).unwrap())
            .collect::<Vec<Regex>>();

        let hay = "https://www.youtube.com/watch?v=TLpufG9s0QY";

        let match_any = re_list.iter().any(|re| re.is_match(hay));

        assert!(match_any);
    }
    #[test]
    fn test_parse_query_equal() {
        assert_eq!(
            parse_query("req_method = 'GET'"),
            Ok((
                "",
                Condition {
                    field: "req_method".to_string(),
                    operate: crate::Operator::Equal("'GET'".to_string()),
                }
            ))
        );
    }

    #[test]
    fn test_parse_query_value_to_vec() {
        assert_eq!(
            parse_query_value_to_vec("(200, 201)"),
            Ok(("", vec!["200".to_string(), "201".to_string()]))
        );
        assert_eq!(
            parse_query_value_to_vec("('200', '201')"),
            Ok(("", vec!["'200'".to_string(), "'201'".to_string()]))
        );
    }

    #[test]
    fn test_parse_query_match() {
        assert_eq!(
            parse_query("url match ^https://github.com"),
            Ok((
                "",
                Condition {
                    field: "url".to_string(),
                    operate: crate::Operator::Match("^https://github.com".to_string()),
                }
            ))
        );
    }

    #[test]
    fn test_parse_query_match_any_in_file() {
        assert_eq!(
            parse_query("url match_any_in_file ./url.regex.txt"),
            Ok((
                "",
                Condition {
                    field: "url".to_string(),
                    operate: crate::Operator::MatchAnyInFile(vec![
                        "^https://www.youtube.com/".to_string(),
                        "^https://github.com".to_string()
                    ]),
                }
            ))
        );
    }

    #[test]
    fn test_parse_query_in() {
        let str = "res_code in (200, 201)";

        assert_eq!(
            parse_query(str),
            Ok((
                "",
                Condition {
                    field: "res_code".to_string(),
                    operate: crate::Operator::In(vec!["200".to_string(), "201".to_string()]),
                }
            ))
        );
        let str = "res_code IN (200, 201)";

        assert_eq!(
            parse_query(str),
            Ok((
                "",
                Condition {
                    field: "res_code".to_string(),
                    operate: crate::Operator::In(vec!["200".to_string(), "201".to_string()]),
                }
            ))
        );
    }
}
