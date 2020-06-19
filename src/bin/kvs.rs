use kvs::{KvStore, Result};
use structopt::StructOpt;

#[derive(StructOpt)]
#[structopt(name = env!("CARGO_PKG_NAME"), about = env!("CARGO_PKG_DESCRIPTION"), author = env!("CARGO_PKG_AUTHORS"), version = env!("CARGO_PKG_VERSION"))]
enum Opt {
    Set { key: String, value: String },
    Get { key: String },
    Rm { key: String },
}

fn main() -> Result<()> {
    let opt = Opt::from_args();
    let mut kvs = KvStore::open("")?;
    match opt {
        Opt::Set { key, value } => {
            kvs.set(key, value)?;
        }
        Opt::Get { key } => {
            kvs.get(key)?;
        }
        Opt::Rm { key } => {
            kvs.remove(key)?;
        }
    }
    Ok(())
}
