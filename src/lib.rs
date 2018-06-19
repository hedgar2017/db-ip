mod error;

pub use self::error::Error;

extern crate sqlite;
#[macro_use] extern crate failure;

use std::net::Ipv4Addr;

pub struct DBIP {
    connection          : sqlite::Connection,
}

#[derive(Debug)]
pub struct Location {
    pub country         : String,
    pub district        : String,
    pub city            : String,
}

impl DBIP {

    ///
    /// The database must contain tables
    /// described above the methods below.
    ///
    pub fn new(path: &str) -> Result<DBIP, Error> {
        let connection = sqlite::open(path)?;

        Ok(DBIP{
            connection,
        })
    }

    ///
    /// The IPv4-location mapping table
    /// was created by the following query:
    ///
    /// CREATE TABLE ipv4_location (
    ///     start           INTEGER NOT NULL UNIQUE,
    ///     end             INTEGER NOT NULL,
    ///     country         TEXT NOT NULL,
    ///     district        TEXT NOT NULL,
    ///     city            TEXT NOT NULL
    /// );
    ///
    /// The start and the end of an IPv4 range
    /// is represented by an SQLite integer.
    ///
    pub fn location_by_ipv4(&self, ip: &Ipv4Addr) -> Result<Location, Error> {
        let mut statement = self.connection.prepare("\
            SELECT country, district, city \
            FROM ipv4_location \
            WHERE start < ? \
            LIMIT 1 \
        ;")?;
        statement.bind(1, Self::ipv4_to_integer(ip))?;
        if let sqlite::State::Row = statement.next()? {
            Ok(Location{
                country     : statement.read::<String>(0)?,
                district    : statement.read::<String>(1)?,
                city        : statement.read::<String>(2)?,
            })
        } else {
            Err(Error::NotFound)
        }
    }

    fn ipv4_to_integer(ip: &Ipv4Addr) -> i64 {
        ((ip.octets()[0] as i64) << 24) +
        ((ip.octets()[1] as i64) << 16) +
        ((ip.octets()[2] as i64) << 8) +
        ((ip.octets()[3] as i64) << 0)
    }
}

#[cfg(test)]
mod tests {
    use super::DBIP;
    use std::net::Ipv4Addr;

    #[test]
    fn integration() {
        let dbip = DBIP::new("dbip.db")
            .expect("Connection error");

        let _location = dbip
            .location_by_ipv4(&Ipv4Addr::new(5, 58, 93, 247))
            .expect("Not found");
    }
}
