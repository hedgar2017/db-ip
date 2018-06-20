extern crate csv;

use std::net::IpAddr;
use std::fs::File;
use std::str::FromStr;
use std::io::BufReader;
use csv::{Reader, Writer};

fn main() {
    let mut ipv4_writer = Writer::from_path("ipv4.csv").unwrap();
    let mut ipv6_writer = Writer::from_path("ipv6.csv").unwrap();

    let mut reader = Reader::from_reader(BufReader::new(File::open("in.csv").unwrap()));
    let mut index = 0usize;
    for result in reader.records() {
        let mut record = result.unwrap();
        let mut csv_out_file = &mut ipv4_writer;

        let ip_start = match IpAddr::from_str(record.get(0).unwrap()).unwrap() {
            IpAddr::V4(start) => u32::from(start).to_string(),
            IpAddr::V6(start) => {
                csv_out_file = &mut ipv6_writer;
                format!("{:x}", u128::from(start))
            },
        };
        let ip_end = match IpAddr::from_str(record.get(1).unwrap()).unwrap() {
            IpAddr::V4(end) => u32::from(end).to_string(),
            IpAddr::V6(end) => format!("{:x}", u128::from(end)),
        };

        csv_out_file.write_record(&[
            ip_start.as_str(),
            ip_end.as_str(),
            record.get(2).unwrap(),
            record.get(3).unwrap(),
            record.get(4).unwrap(),
            record.get(5).unwrap(),
            record.get(6).unwrap(),
            record.get(7).unwrap(),
            record.get(8).unwrap(),
            record.get(9).unwrap(),
            record.get(10).unwrap(),
            record.get(11).unwrap(),
            record.get(12).unwrap(),
            record.get(13).unwrap(),
            record.get(14).unwrap(),
        ]).unwrap();
        index += 1;
        if index % 1000 == 0 {
            println!("{} records processed...", index);
        }
    }
    println!("{} records processed...", index);
}