mod error;

pub use self::error::Error;

extern crate rusqlite;
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
            Ok(Location{
                country             : row.get_checked(0).unwrap_or(String::new()),
                stateprov           : row.get_checked(1).unwrap_or(String::new()),
                district            : row.get_checked(2).unwrap_or(String::new()),
                city                : row.get_checked(3).unwrap_or(String::new()),
                zipcode             : row.get_checked(4).unwrap_or(String::new()),
                latitude            : row.get_checked(5).unwrap_or(0.0),
                longitude           : row.get_checked(6).unwrap_or(0.0),
                geoname_id          : row.get_checked(7).unwrap_or(0),
                timezone_offset     : row.get_checked(8).unwrap_or(0.0),
                timezone_name       : row.get_checked(9).unwrap_or(String::new()),
                isp_name            : row.get_checked(10).unwrap_or(String::new()),
                connection_type     : row.get_checked(11).unwrap_or(String::new()),
                organization_name   : row.get_checked(12).unwrap_or(String::new()),
            })
        } else {
            Err(Error::NotFound)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::DBIP;
    use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};

    #[test]
    fn ipv4() {
        let _ = DBIP::new("database.db")
            .map_err(|error| panic!(error.to_string()))
            .map(|dbip| dbip
                .location_by_ip(&IpAddr::V4(Ipv4Addr::new(5, 58, 93, 247)))
                .map_err(|error| panic!(error.to_string())));
    }

    #[test]
    fn ipv6() {
        let _ = DBIP::new("database.db")
            .map_err(|error| panic!(error.to_string()))
            .map(|dbip| dbip
                .location_by_ip(&IpAddr::V6(Ipv6Addr::new(0x1eab,0xc342,0,0,0,0,0,0)))
                .map_err(|error| panic!(error.to_string())));
    }
}
