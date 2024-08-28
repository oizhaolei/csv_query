use std::fs::File;
use std::io::{BufRead, BufReader};
use std::{env, error::Error, io, process};

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
        None => return Err(From::from("expected query-string, but got none")),
        Some(query) => query,
    };

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

    use crate::read_file_to_vec;
    #[test]
    fn test_is_match() {
        let re = Regex::new(r"\b\w{13}\b").unwrap();
        let hay = "I categorically deny having triskaidekaphobia.";
        assert!(re.is_match(hay));
    }
    #[test]
    fn test_is_match_any() {
        let regex_list = vec!["^https://www.youtube.com", "^github.com"];
        let re_list = regex_list
            .iter()
            .map(|r| {
                let re = Regex::new(r).unwrap();
                re
            })
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
            .map(|r| {
                let re = Regex::new(r).unwrap();
                re
            })
            .collect::<Vec<Regex>>();

        let hay = "https://www.youtube.com/watch?v=TLpufG9s0QY";

        let match_any = re_list.iter().any(|re| re.is_match(hay));

        assert!(match_any);
    }
}
