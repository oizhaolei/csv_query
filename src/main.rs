mod query;

use crate::query::{parse_query, Operator};
use regex::Regex;
use std::{env, error::Error, io, process};

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
