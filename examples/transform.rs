extern crate csv;
extern crate sqlite;

use std::net::Ipv4Addr;
use std::str::FromStr;
use std::io::Write;

fn ipv4_to_numeric(ip: &Ipv4Addr) -> i64 {
    ((ip.octets()[0] as i64) << 24) +
    ((ip.octets()[1] as i64) << 16) +
    ((ip.octets()[2] as i64) << 8) +
    ((ip.octets()[3] as i64) << 0)
}

fn main() {
    let csv_in_file = std::fs::File::open("in.csv").unwrap();
    let mut csv_out_file = std::fs::File::create("out.csv").unwrap();

    let mut reader = csv::Reader::from_reader(std::io::BufReader::new(csv_in_file));
    for result in reader.records() {
        let record = result.unwrap();

        let start = ipv4_to_numeric(&Ipv4Addr::from_str(record.get(0).unwrap()).unwrap());
        let end = ipv4_to_numeric(&Ipv4Addr::from_str(record.get(1).unwrap()).unwrap());
        let country = record.get(2).unwrap();
        let district = record.get(3).unwrap();
        let city = record.get(4).unwrap();

        csv_out_file
            .write(format!("{},{},\"{}\",\"{}\",\"{}\"\n", start, end, country, district, city).as_bytes())
            .unwrap();
    }
}