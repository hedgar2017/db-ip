use std::fs::File;
use std::str::FromStr;
use std::io::{Write, BufReader};
use std::process::{Command, Stdio};
use std::net::IpAddr;
use csv::Reader;

use super::error::ConverterError;

pub struct Converter;

impl Converter {

    pub fn csv_to_db(csv_path: &str, db_path: &str) -> Result<(), ConverterError> {
        use ConverterError::*;

        let inserts_per_statement = 100000;

        let csv_file = File::open(csv_path)?;
        let mut csv_reader = Reader::from_reader(BufReader::new(csv_file));

        let _ = File::create(db_path)?;
        Self::run_sqlite(db_path, "\
            CREATE TABLE ip_location (\
                ip_start            BLOB NOT NULL,\
                ip_end              BLOB NOT NULL UNIQUE,\
                country             TEXT NOT NULL,\
                stateprov           TEXT NOT NULL,\
                district            TEXT NOT NULL,\
                city                TEXT NOT NULL,\
                zipcode             TEXT NOT NULL,\
                latitude            REAL NOT NULL,\
                longitude           REAL NOT NULL,\
                geoname_id          INTEGER DEFAULT NULL,\
                timezone_offset     REAL NOT NULL,\
                timezone_name       TEXT NOT NULL,\
                isp_name            TEXT NOT NULL,\
                connection_type     TEXT DEFAULT NULL,\
                organization_name   TEXT NOT NULL\
            );"
        )?;
        let mut statement = String::with_capacity(inserts_per_statement * 1000);

        let mut index = 0usize;
        for record in csv_reader.records() {
            let record = record?;
            let mut values = record.iter();

            let ip_start = values.next().ok_or(CsvValueGetting)?;
            let ip_start = IpAddr::from_str(ip_start)?;
            let ip_start = match ip_start {
                IpAddr::V4(start) => format!("{:08x}", u32::from(start)),
                IpAddr::V6(start) => format!("{:032x}", u128::from(start)),
            };
            let ip_end = values.next().ok_or(CsvValueGetting)?;
            let ip_end = IpAddr::from_str(ip_end)?;
            let ip_end = match ip_end {
                IpAddr::V4(end) => format!("{:08x}", u32::from(end)),
                IpAddr::V6(end) => format!("{:032x}", u128::from(end)),
            };

            if index % inserts_per_statement == 0 {
                statement.push_str("INSERT INTO ip_location VALUES ");
            }
            if index > 0 && index % inserts_per_statement != 0 {
                statement.push_str(",");
            }
            statement.push_str(format!(
                "(x'{}',x'{}','{}','{}','{}','{}','{}',{},{},{},{},'{}','{}','{}','{}')",
                ip_start.as_str(),
                ip_end.as_str(),
                values.next().ok_or(CsvValueGetting)?.replace("'", "''"),
                values.next().ok_or(CsvValueGetting)?.replace("'", "''"),
                values.next().ok_or(CsvValueGetting)?.replace("'", "''"),
                values.next().ok_or(CsvValueGetting)?.replace("'", "''"),
                values.next().ok_or(CsvValueGetting)?.replace("'", "''"),
                values.next().ok_or(CsvValueGetting)?.parse::<f64>().unwrap_or_default(),
                values.next().ok_or(CsvValueGetting)?.parse::<f64>().unwrap_or_default(),
                values.next().ok_or(CsvValueGetting)?.parse::<i64>().unwrap_or_default(),
                values.next().ok_or(CsvValueGetting)?.parse::<f64>().unwrap_or_default(),
                values.next().ok_or(CsvValueGetting)?.replace("'", "''"),
                values.next().ok_or(CsvValueGetting)?.replace("'", "''"),
                values.next().ok_or(CsvValueGetting)?.replace("'", "''"),
                values.next().ok_or(CsvValueGetting)?.replace("'", "''"),
            ).as_str());
            index += 1;
            if index % inserts_per_statement == 0 {
                statement.push_str(";");
                Self::run_sqlite(db_path, &statement)?;
                statement.clear();
            }
        }
        if statement.len() > 0 {
            statement.push_str(";");
            Self::run_sqlite(db_path, &statement)?;
        }

        Ok(())
    }

    fn run_sqlite(db_path: &str, query: &str) -> Result<(), ConverterError> {
        use ConverterError::*;

        let mut command = Command::new("sqlite3")
            .arg(db_path)
            .stdin(Stdio::piped())
            .spawn()?;
        {
            let stdin = command.stdin.as_mut().ok_or(SqlitePiping)?;
            stdin.write_all(query.as_bytes())?;
        }
        let status = command.wait()?;
        if !status.success() {
            Err(SqliteProcess(status))
        } else {
            Ok(())
        }
    }
}