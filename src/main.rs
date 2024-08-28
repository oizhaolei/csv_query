use std::fmt::{self, Display};
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::{env, error::Error, io, process};

use nom::combinator::map;

use nom::branch::alt;
use nom::bytes::complete::{tag_no_case, take_till};
use nom::character::complete::{multispace0, multispace1};
use nom::sequence::tuple;
use nom::IResult;

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub enum Operator {
    Not,
    And,
    Or,
    Like,
    NotLike,
    Equal,
    NotEqual,
    Greater,
    GreaterOrEqual,
    Less,
    LessOrEqual,
    In,
    NotIn,
    Is,
    Match,
    MatchByFile,
}

impl Display for Operator {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let op = match *self {
            Operator::Not => "NOT",
            Operator::And => "AND",
            Operator::Or => "OR",
            Operator::Like => "LIKE",
            Operator::NotLike => "NOT_LIKE",
            Operator::Equal => "=",
            Operator::NotEqual => "!=",
            Operator::Greater => ">",
            Operator::GreaterOrEqual => ">=",
            Operator::Less => "<",
            Operator::LessOrEqual => "<=",
            Operator::In => "IN",
            Operator::NotIn => "NOT_IN",
            Operator::Is => "IS",
            Operator::Match => "MATCH",
            Operator::MatchByFile => "MATCH_BY_FILE",
        };
        write!(f, "{}", op)
    }
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Condition {
    pub field: String,
    pub operate: Operator,
    pub value: String,
}

fn till_space(s: &str) -> IResult<&str, &str> {
    take_till(|c| c == ' ')(s)
}
fn binary_comparison_operator(i: &str) -> IResult<&str, Operator> {
    alt((
        map(tag_no_case("not_like"), |_| Operator::NotLike),
        map(tag_no_case("like"), |_| Operator::Like),
        map(tag_no_case("!="), |_| Operator::NotEqual),
        map(tag_no_case("<>"), |_| Operator::NotEqual),
        map(tag_no_case(">="), |_| Operator::GreaterOrEqual),
        map(tag_no_case("<="), |_| Operator::LessOrEqual),
        map(tag_no_case("="), |_| Operator::Equal),
        map(tag_no_case("<"), |_| Operator::Less),
        map(tag_no_case(">"), |_| Operator::Greater),
        map(tag_no_case("in"), |_| Operator::In),
        map(tag_no_case("not_in"), |_| Operator::NotIn),
        map(tag_no_case("match"), |_| Operator::Match),
        map(tag_no_case("match_by_file"), |_| Operator::MatchByFile),
    ))(i)
}

fn parse_query(i: &str) -> IResult<&str, Condition> {
    let (remain, (_, field, _, operate)) = tuple((
        multispace0,
        till_space,
        multispace1,
        binary_comparison_operator,
    ))(i)?;

    Ok((
        "",
        Condition {
            field: field.to_string(),
            operate,
            value: remain.trim().to_string(),
        },
    ))
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
    println!("query: {}", query);

    //TODO parse query

    // Build CSV readers and writers to stdin and stdout, respectively.
    let mut rdr = csv::Reader::from_reader(io::stdin());
    let mut wtr = csv::WriterBuilder::new()
        .delimiter(b',')
        .quote_style(csv::QuoteStyle::Always)
        .from_writer(io::stdout());

    // Before reading our data records, we should write the header record.
    wtr.write_record(rdr.headers()?)?;

    // Iterate over all the records in `rdr`, and write only records containing
    // `query` to `wtr`.
    for result in rdr.records() {
        let record = result?;
        if record.iter().any(|field| field == &query) {
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

    use crate::{parse_query, read_file_to_vec, Condition};
    #[test]
    fn test_is_match() {
        let re = Regex::new(r"\b\w{13}\b").unwrap();
        let hay = "I categorically deny having triskaidekaphobia.";
        assert!(re.is_match(hay));
    }
    #[test]
    fn test_is_match_any() {
        let regex_list = ["^https://www.youtube.com", "^github.com"];
        let re_list = regex_list
            .iter()
            .map(|r| Regex::new(r).unwrap())
            .collect::<Vec<Regex>>();

        let hay = "https://www.youtube.com/watch?v=TLpufG9s0QY";

        let match_any = re_list.iter().any(|re| re.is_match(hay));

        assert!(match_any);
    }
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
    fn test_parse_query() {
        let str = "res_code in (\"200\", \"201\")";

        assert_eq!(
            parse_query(str),
            Ok((
                "",
                Condition {
                    field: "res_code".to_string(),
                    operate: crate::Operator::In,
                    value: "(\"200\", \"201\")".to_string(),
                }
            ))
        );
    }
}
