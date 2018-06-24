use rusqlite;
use csv;

#[derive(Fail, Debug)]
pub enum Error {
    #[fail(display = "SQLite: {}", _0)]
    Sqlite(rusqlite::Error),
    #[fail(display = "Not found")]
    NotFound,
    #[fail(display = "Unknown")]
    Unknown,
}

impl From<rusqlite::Error> for Error {
    fn from(error: rusqlite::Error) -> Error {
        Error::Sqlite(error)
    }
}

#[derive(Fail, Debug)]
pub enum ConverterError {
    #[fail(display = "I/O: {}", _0)]
    IO(::std::io::Error),
    #[fail(display = "Sqlite piping")]
    SqlitePiping,
    #[fail(display = "Sqlite process")]
    SqliteProcess(::std::process::ExitStatus),
    #[fail(display = "CSV record getting")]
    CsvRecordGetting(csv::Error),
    #[fail(display = "CSV value getting")]
    CsvValueGetting(&'static str),
    #[fail(display = "CSV IP address parsing")]
    CsvIpAddressParsing(::std::net::AddrParseError),
}

impl From<::std::io::Error> for ConverterError {
    fn from(error: ::std::io::Error) -> ConverterError {
        ConverterError::IO(error)
    }
}

impl From<csv::Error> for ConverterError {
    fn from(error: csv::Error) -> ConverterError {
        ConverterError::CsvRecordGetting(error)
    }
}

impl From<::std::net::AddrParseError> for ConverterError {
    fn from(error: ::std::net::AddrParseError) -> ConverterError {
        ConverterError::CsvIpAddressParsing(error)
    }
}