mod query;

use crate::query::{parse_query, Operator};
use once_cell::sync::Lazy;
use query::Condition;
use regex::Regex;
use std::collections::HashMap;
use std::sync::Mutex;
use std::{env, error::Error, io, process};

static REGEX_CACHE: Lazy<Mutex<HashMap<String, Regex>>> = Lazy::new(|| Mutex::new(HashMap::new()));

static RE_QUOTED: Lazy<Regex> = Lazy::new(|| Regex::new(r#"['|"][^'"]+['|"]"#).unwrap());

fn get_or_compile_regex(pattern: &str) -> Regex {
    let mut cache = REGEX_CACHE.lock().unwrap();

    // Check if the regex is already in the cache
    if let Some(regex) = cache.get(pattern) {
        return regex.clone();
    }

    // Compile the regex and store it in the cache
    let regex = Regex::new(pattern).unwrap();
    cache.insert(pattern.to_string(), regex.clone());

    regex
}

fn is_match_value(field_value: &str, condition: Condition) -> bool {
    match condition.operate {
        Operator::Equal(value) => {
            if RE_QUOTED.is_match(&value) {
                //compare after ignore quota
                field_value == &value[1..value.len() - 1]
            } else {
                field_value == &value
            }
        }
        Operator::NotEqual(value) => {
            if RE_QUOTED.is_match(&value) {
                //compare after ignore quota
                field_value != &value[1..value.len() - 1]
            } else {
                field_value != &value
            }
        }
        Operator::Greater(value) => {
            if RE_QUOTED.is_match(&value) {
                //compare by stirng style
                field_value > &value[1..value.len() - 1]
            } else {
                //compare by number style
                field_value.parse::<i64>().unwrap() > value.parse::<i64>().unwrap()
            }
        }
        Operator::GreaterOrEqual(value) => {
            if RE_QUOTED.is_match(&value) {
                //compare by stirng style
                field_value >= &value[1..value.len() - 1]
            } else {
                //compare by number style
                field_value.parse::<i64>().unwrap() >= value.parse::<i64>().unwrap()
            }
        }
        Operator::Less(value) => {
            if RE_QUOTED.is_match(&value) {
                //compare by stirng style
                field_value < &value[1..value.len() - 1]
            } else {
                //compare by number style
                field_value.parse::<i64>().unwrap() < value.parse::<i64>().unwrap()
            }
        }
        Operator::LessOrEqual(value) => {
            if RE_QUOTED.is_match(&value) {
                //compare by stirng style
                field_value <= &value[1..value.len() - 1]
            } else {
                //compare by number style
                field_value.parse::<i64>().unwrap() <= value.parse::<i64>().unwrap()
            }
        }
        Operator::In(vec) => vec.contains(&field_value.to_string()),
        Operator::NotIn(vec) => !vec.contains(&field_value.to_string()),
        Operator::Match(regex_str) => get_or_compile_regex(&regex_str).is_match(field_value),
        Operator::MatchAnyInFile(regex_vec) => regex_vec
            .iter()
            .any(|r| get_or_compile_regex(r).is_match(field_value)),
    }
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
        let is_match = is_match_value(field_value, condition.clone());

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
    use crate::{
        is_match_value,
        query::{Condition, Operator},
    };

    #[test]
    fn test_is_match_value() {
        assert!(is_match_value(
            "GET",
            Condition {
                field: '-'.to_string(),
                operate: Operator::Equal("GET".to_string()),
            }
        ));
        assert!(is_match_value(
            "GET",
            Condition {
                field: '-'.to_string(),
                operate: Operator::NotEqual("PUT".to_string()),
            }
        ));
        assert!(is_match_value(
            "300",
            Condition {
                field: '-'.to_string(),
                operate: Operator::Greater("100".to_string()),
            }
        ));
        assert!(!is_match_value(
            "90",
            Condition {
                field: '-'.to_string(),
                operate: Operator::Greater("100".to_string()),
            }
        ));
        assert!(is_match_value(
            "300",
            Condition {
                field: '-'.to_string(),
                operate: Operator::GreaterOrEqual("100".to_string()),
            }
        ));
        assert!(!is_match_value(
            "90",
            Condition {
                field: '-'.to_string(),
                operate: Operator::GreaterOrEqual("100".to_string()),
            }
        ));
        assert!(is_match_value(
            "90",
            Condition {
                field: '-'.to_string(),
                operate: Operator::GreaterOrEqual("90".to_string()),
            }
        ));
        assert!(is_match_value(
            "200",
            Condition {
                field: '-'.to_string(),
                operate: Operator::In(vec!["200".to_string()]),
            }
        ));
        assert!(is_match_value(
            "200",
            Condition {
                field: '-'.to_string(),
                operate: Operator::In(vec!["200".to_string(), "201".to_string()]),
            }
        ));
        assert!(is_match_value(
            "300",
            Condition {
                field: '-'.to_string(),
                operate: Operator::NotIn(vec!["200".to_string(), "201".to_string()]),
            }
        ));
        assert!(is_match_value(
            "300",
            Condition {
                field: '-'.to_string(),
                operate: Operator::Match("3..".to_string()),
            }
        ));
        assert!(is_match_value(
            "300",
            Condition {
                field: '-'.to_string(),
                operate: Operator::MatchAnyInFile(vec!["2..".to_string(), "3..".to_string()]),
            }
        ));
    }
}
