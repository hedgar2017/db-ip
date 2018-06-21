mod error;
mod converter;

pub use self::error::Error;
pub use self::converter::Converter;
pub use self::error::ConverterError;

extern crate rusqlite;
extern crate csv;
#[macro_use] extern crate failure;

use std::net::IpAddr;
use rusqlite::Connection;

pub struct DBIP {
    connection              : Connection,
}

#[derive(Debug)]
pub struct Location {
    pub country             : String,
    pub stateprov           : String,
    pub district            : String,
    pub city                : String,
    pub zipcode             : String,
    pub latitude            : f64,
    pub longitude           : f64,
    pub geoname_id          : i64,
    pub timezone_offset     : f64,
    pub timezone_name       : String,
    pub isp_name            : String,
    pub connection_type     : String,
    pub organization_name   : String,
}

impl DBIP {

    pub fn new(path: &str) -> Result<DBIP, Error> {
        let connection = Connection::open(path)?;

        Ok(DBIP{
            connection,
        })
    }

    pub fn location_by_ip(&self, ip: &IpAddr) -> Result<Location, Error> {
        use Error::*;

        let mut statement = self.connection.prepare_cached("\
            SELECT \
            country, stateprov, district, city, zipcode, \
            latitude, longitude, geoname_id, timezone_offset, timezone_name, \
            isp_name, connection_type, organization_name \
            FROM ip_location \
            WHERE ? BETWEEN ip_start AND ip_end \
            ORDER BY ip_end \
            LIMIT 1 \
        ;")?;

        let key = match ip {
            IpAddr::V4(ip) => ip.octets().to_vec(),
            IpAddr::V6(ip) => ip.octets().to_vec(),
        };
        let mut rows = statement.query(&[&key])?;
        if let Some(Ok(row)) = rows.next() {
            let mut iter = 0..row.column_count();
            Ok(Location{
                country             : row.get_checked(iter.next().ok_or(RecordNotFound)?)?,
                stateprov           : row.get_checked(iter.next().ok_or(RecordNotFound)?)?,
                district            : row.get_checked(iter.next().ok_or(RecordNotFound)?)?,
                city                : row.get_checked(iter.next().ok_or(RecordNotFound)?)?,
                zipcode             : row.get_checked(iter.next().ok_or(RecordNotFound)?)?,
                latitude            : row.get_checked(iter.next().ok_or(RecordNotFound)?)?,
                longitude           : row.get_checked(iter.next().ok_or(RecordNotFound)?)?,
                geoname_id          : row.get_checked(iter.next().ok_or(RecordNotFound)?)?,
                timezone_offset     : row.get_checked(iter.next().ok_or(RecordNotFound)?)?,
                timezone_name       : row.get_checked(iter.next().ok_or(RecordNotFound)?)?,
                isp_name            : row.get_checked(iter.next().ok_or(RecordNotFound)?)?,
                connection_type     : row.get_checked(iter.next().ok_or(RecordNotFound)?)?,
                organization_name   : row.get_checked(iter.next().ok_or(RecordNotFound)?)?,
            })
        } else {
            Err(Error::RecordNotFound)
        }
    }
}