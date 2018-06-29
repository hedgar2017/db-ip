use std::fs::File;
use std::str::FromStr;
use std::io::{Write, Read};
use std::process::{Command, Stdio};
use std::net::IpAddr;
use csv;
use std::path::Path;

use super::error::ConverterError;

pub struct Converter;
const INSERTS_PER_STATEMENT: usize = 100000;

impl Converter {
    pub fn csv_to_db<R: Read, F: FnMut(usize)>(plain_csv_reader: R, db_path: &Path, mut status_fn: F) -> Result<(), ConverterError> {
        use ConverterError::*;

        let mut csv_reader = csv::Reader::from_reader(plain_csv_reader);

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
        let mut statement = String::with_capacity(INSERTS_PER_STATEMENT * 1000);

        let mut index = 0usize;
        for record in csv_reader.records() {
            let record = record?;
            let mut values = record.iter();

            let ip_start = values.next().ok_or(CsvValueGetting("ip_start"))?;
            let ip_start = IpAddr::from_str(ip_start)?;
            let ip_start = match ip_start {
                IpAddr::V4(start) => format!("{:08x}", u32::from(start)),
                IpAddr::V6(start) => format!("{:032x}", u128::from(start)),
            };
            let ip_end = values.next().ok_or(CsvValueGetting("ip_end"))?;
            let ip_end = IpAddr::from_str(ip_end)?;
            let ip_end = match ip_end {
                IpAddr::V4(end) => format!("{:08x}", u32::from(end)),
                IpAddr::V6(end) => format!("{:032x}", u128::from(end)),
            };

            statement.push_str(if index % INSERTS_PER_STATEMENT == 0 {"INSERT INTO ip_location VALUES "} else {","});
            statement.push_str(format!(
                "(x'{}',x'{}','{}','{}','{}','{}','{}',{},{},{},{},'{}','{}','{}','{}')",
                ip_start.as_str(),
                ip_end.as_str(),
                values.next().ok_or(CsvValueGetting("country"))?.replace("'", "''"),
                values.next().ok_or(CsvValueGetting("stateprov"))?.replace("'", "''"),
                values.next().ok_or(CsvValueGetting("district"))?.replace("'", "''"),
                values.next().ok_or(CsvValueGetting("city"))?.replace("'", "''"),
                values.next().ok_or(CsvValueGetting("zipcode"))?.replace("'", "''"),
                values.next().ok_or(CsvValueGetting("latitude"))?.parse::<f64>().unwrap_or_default(),
                values.next().ok_or(CsvValueGetting("longitude"))?.parse::<f64>().unwrap_or_default(),
                values.next().ok_or(CsvValueGetting("geoname_id"))?.parse::<i64>().unwrap_or_default(),
                values.next().ok_or(CsvValueGetting("timezone_offset"))?.parse::<f64>().unwrap_or_default(),
                values.next().ok_or(CsvValueGetting("timezone_name"))?.replace("'", "''"),
                values.next().ok_or(CsvValueGetting("isp_name"))?.replace("'", "''"),
                values.next().ok_or(CsvValueGetting("connection_type"))?.replace("'", "''"),
                values.next().ok_or(CsvValueGetting("organization_name"))?.replace("'", "''"),
            ).as_str());
            index += 1;
            if index % INSERTS_PER_STATEMENT == 0 {
                (status_fn)(index);
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

    fn run_sqlite(db_path: &Path, query: &str) -> Result<(), ConverterError> {
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