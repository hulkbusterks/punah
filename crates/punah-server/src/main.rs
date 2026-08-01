use std::path::PathBuf;

use punah_config::{Config, StorageDurability};
use punah_server::Node;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut config = Config::default();
    if let Some(address) = std::env::args().nth(1) {
        config.tcp.listen_address = address;
    }
    if let Some(directory) = std::env::args().nth(2) {
        config.storage.directory = PathBuf::from(directory);
    }
    if std::env::args().nth(3).as_deref() == Some("buffered") {
        config.storage.durability = StorageDurability::Buffered;
    }
    let node = Node::bind(config)?;
    eprintln!("Punah listening on {}", node.local_addr()?);
    node.serve()?;
    Ok(())
}
