use rusqlite;

#[derive(Fail, Debug)]
pub enum Error {
    #[fail(display = "SQLite: {}", _0)]
    SQLite(rusqlite::Error),
    #[fail(display = "Not found")]
    NotFound,
}

impl From<rusqlite::Error> for Error {
    fn from(error: rusqlite::Error) -> Error {
        Error::SQLite(error)
    }
}