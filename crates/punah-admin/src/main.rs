use std::path::PathBuf;
use std::{fmt, io};

use punah_admin::{import_unmanaged, inspect, quarantine_orphans, repair_corrupt_root};
use punah_store::StoreConfig;
use punah_types::{Durability, Namespace};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut arguments = std::env::args().skip(1);
    let command = arguments.next().ok_or(usage())?;
    let directory = PathBuf::from(arguments.next().ok_or(usage())?);
    match command.as_str() {
        "inspect" => println!("{:#?}", inspect(directory)?),
        "quarantine-orphans" => println!("{:#?}", quarantine_orphans(directory)?),
        "import" | "repair" => {
            let segment_capacity = arguments.next().ok_or(usage())?.parse::<u64>()?;
            let namespace = arguments.next().unwrap_or_else(|| "default".to_owned());
            if arguments.next().is_some() {
                return Err(usage().into());
            }
            let config = StoreConfig {
                directory,
                namespace: Namespace::new(namespace).map_err(invalid_namespace)?,
                segment_capacity,
                durability: Durability::LocalDurable,
            };
            let report = if command == "import" {
                import_unmanaged(config)?
            } else {
                repair_corrupt_root(&config)?
            };
            println!("{report:#?}");
        }
        _ => return Err(usage().into()),
    }
    Ok(())
}

fn usage() -> &'static str {
    "usage: punah-admin inspect DIRECTORY\n       punah-admin quarantine-orphans DIRECTORY\n       punah-admin import DIRECTORY SEGMENT_BYTES [NAMESPACE]\n       punah-admin repair DIRECTORY SEGMENT_BYTES [NAMESPACE]"
}

fn invalid_namespace(error: impl fmt::Debug) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, format!("{error:?}"))
}
