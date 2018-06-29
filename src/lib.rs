mod converter;
mod error;

pub use self::converter::Converter;
pub use self::error::ConverterError;
pub use self::error::Error;

extern crate csv;
extern crate rusqlite;
#[macro_use]
extern crate failure;

use rusqlite::Connection;
use std::net::IpAddr;

pub struct DBIP {
    connection: Connection,
}

#[derive(Debug)]
pub struct Location {
    pub country: String,
    pub stateprov: String,
    pub district: String,
    pub city: String,
    pub zipcode: String,
    pub latitude: f64,
    pub longitude: f64,
    pub geoname_id: i64,
    pub timezone_offset: f64,
    pub timezone_name: String,
    pub isp_name: String,
    pub connection_type: String,
    pub organization_name: String,
}

impl DBIP {
    pub fn new(path: &str) -> Result<DBIP, Error> {
        let connection = Connection::open(path)?;

        Ok(DBIP { connection })
    }

    pub fn location_by_ip(&self, ip: &IpAddr) -> Result<Location, Error> {
        use Error::*;

        let mut statement = self.connection.prepare_cached(
            "\
             SELECT \
             country, stateprov, district, city, zipcode, \
             latitude, longitude, geoname_id, timezone_offset, timezone_name, \
             isp_name, connection_type, organization_name \
             FROM ip_location \
             WHERE ? BETWEEN ip_start AND ip_end \
             ORDER BY ip_end \
             LIMIT 1 \
             ;",
        )?;

        let key = match ip {
            IpAddr::V4(ip) => ip.octets().to_vec(),
            IpAddr::V6(ip) => ip.octets().to_vec(),
        };
        let mut rows = statement.query(&[&key])?;
        if let Some(Ok(row)) = rows.next() {
            let mut iter = 0..row.column_count();
            Ok(Location {
                country: row.get_checked(iter.next().ok_or(Unknown)?)?,
                stateprov: row.get_checked(iter.next().ok_or(Unknown)?)?,
                district: row.get_checked(iter.next().ok_or(Unknown)?)?,
                city: row.get_checked(iter.next().ok_or(Unknown)?)?,
                zipcode: row.get_checked(iter.next().ok_or(Unknown)?)?,
                latitude: row.get_checked(iter.next().ok_or(Unknown)?)?,
                longitude: row.get_checked(iter.next().ok_or(Unknown)?)?,
                geoname_id: row.get_checked(iter.next().ok_or(Unknown)?)?,
                timezone_offset: row.get_checked(iter.next().ok_or(Unknown)?)?,
                timezone_name: row.get_checked(iter.next().ok_or(Unknown)?)?,
                isp_name: row.get_checked(iter.next().ok_or(Unknown)?)?,
                connection_type: row.get_checked(iter.next().ok_or(Unknown)?)?,
                organization_name: row.get_checked(iter.next().ok_or(Unknown)?)?,
            })
        } else {
            Err(Error::NotFound)
        }
    }
}
