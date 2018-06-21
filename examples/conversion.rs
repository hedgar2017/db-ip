extern crate dbip;

fn main() {
    let _ = dbip::Converter::csv_to_db(
        "database.csv",
        "database.db",
    ).map_err(|error| println!("{}", error.to_string()));
}