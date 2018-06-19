use sqlite;

#[derive(Fail, Debug)]
pub enum Error {
    #[fail(display = "SQLite: {}", _0)]
    SQLite(sqlite::Error),
    #[fail(display = "Not found")]
    NotFound,
}

impl From<sqlite::Error> for Error {
    fn from(error: sqlite::Error) -> Error {
        Error::SQLite(error)
    }
}