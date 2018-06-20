extern crate csv;

use std::net::IpAddr;
use std::fs::File;
use std::str::FromStr;
use std::io::{Write, BufReader};
use csv::Reader;

fn main() {
    let mut reader = Reader::from_reader(BufReader::new(File::open("database.csv").unwrap()));

    let mut sql_file = File::create("database.sql").unwrap();
    sql_file.write("CREATE TABLE ip_location (\n\
        ip_start            BLOB NOT NULL,\n\
        ip_end              BLOB NOT NULL UNIQUE,\n\
        country             TEXT NOT NULL,\n\
        stateprov           TEXT NOT NULL,\n\
        district            TEXT NOT NULL,\n\
        city                TEXT NOT NULL,\n\
        zipcode             TEXT NOT NULL,\n\
        latitude            REAL NOT NULL,\n\
        longitude           REAL NOT NULL,\n\
        geoname_id          INTEGER DEFAULT NULL,\n\
        timezone_offset     REAL NOT NULL,\n\
        timezone_name       TEXT NOT NULL,\n\
        isp_name            TEXT NOT NULL,\n\
        connection_type     TEXT DEFAULT NULL,\n\
        organization_name   TEXT NOT NULL\n\
    );".as_bytes()).unwrap();
    sql_file.write("INSERT INTO ip_location VALUES\n".as_bytes()).unwrap();

    let mut index = 0usize;
    let chunk_size = 10000;
    for result in reader.records() {
        let mut record = result.unwrap();

        let ip_start = match IpAddr::from_str(record.get(0).unwrap()).unwrap() {
            IpAddr::V4(start) => format!("{:08x}", u32::from(start)),
            IpAddr::V6(start) => format!("{:032x}", u128::from(start)),
        };
        let ip_end = match IpAddr::from_str(record.get(1).unwrap()).unwrap() {
            IpAddr::V4(end) => format!("{:08x}", u32::from(end)),
            IpAddr::V6(end) => format!("{:032x}", u128::from(end)),
        };

        if index > 0 && index % chunk_size != 0 {
            sql_file.write(",\n".as_bytes()).unwrap();
        }
        sql_file.write(format!(
            "(x'{}',x'{}','{}','{}','{}','{}','{}',{},{},{},{},'{}','{}','{}','{}')",
            ip_start.as_str(),
            ip_end.as_str(),
            record.get(2).unwrap().replace("'", "''"),
            record.get(3).unwrap().replace("'", "''"),
            record.get(4).unwrap().replace("'", "''"),
            record.get(5).unwrap().replace("'", "''"),
            record.get(6).unwrap().replace("'", "''"),
            record.get(7).unwrap().parse::<f64>().unwrap_or(0.0),
            record.get(8).unwrap().parse::<f64>().unwrap_or(0.0),
            record.get(9).unwrap().parse::<i64>().unwrap_or(0),
            record.get(10).unwrap().parse::<f64>().unwrap_or(0.0),
            record.get(11).unwrap().replace("'", "''"),
            record.get(12).unwrap().replace("'", "''"),
            record.get(13).unwrap().replace("'", "''"),
            record.get(14).unwrap().replace("'", "''"),
        ).as_bytes()).unwrap();
        index += 1;
        if index % chunk_size == 0 {
            println!("{} records processed...", index);
            sql_file.write(";\nINSERT INTO ip_location VALUES\n".as_bytes()).unwrap();
        }
    }
    println!("{} records processed...", index);

    sql_file.write(";\n".as_bytes()).unwrap();
}