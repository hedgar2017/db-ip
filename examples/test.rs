extern crate dbip;

use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};

fn main() {
    let _ = dbip::DBIP::new("database.db")
        .map_err(|error| println!("{}", error.to_string()))
        .map(|dbip| {
            let _ = dbip
                .location_by_ip(&IpAddr::V4(Ipv4Addr::new(5, 58, 93, 247)))
                .map_err(|error| println!("{}", error.to_string()))
                .map(|location| println!("{:?}", location));
            let _ = dbip
                .location_by_ip(&IpAddr::V6(Ipv6Addr::new(0x1eab,0xc342,0,0,0,0,0,0)))
                .map_err(|error| println!("{}", error.to_string()))
                .map(|location| println!("{:?}", location));
        });
}