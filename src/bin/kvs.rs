use clap::{App, Arg, SubCommand};
use kvs::KvStore;

fn main() {
    let matches = App::new(env!("CARGO_PKG_NAME"))
        .version(env!("CARGO_PKG_VERSION"))
        .author(env!("CARGO_PKG_AUTHORS"))
        .about(env!("CARGO_PKG_DESCRIPTION"))
        .subcommand(
            SubCommand::with_name("set")
                .about("set a value for key")
                .arg(Arg::with_name("key").help("the key").required(true))
                .arg(Arg::with_name("value").help("the value").required(true)),
        )
        .subcommand(
            SubCommand::with_name("get")
                .about("get a value by key")
                .arg(Arg::with_name("key").help("the key").required(true)),
        )
        .subcommand(
            SubCommand::with_name("rm")
                .about("rm a value by key")
                .arg(Arg::with_name("key").help("the key").required(true)),
        )
        .get_matches();
    let mut kvs = KvStore::new();
    match matches.subcommand() {
        ("set", Some(sub_m)) => {
            let key = sub_m.value_of("key").unwrap();
            let value = sub_m.value_of("value").unwrap();
            kvs.set(key.to_owned(), value.to_owned());
        }
        ("get", Some(sub_m)) => {
            let key = sub_m.value_of("key").unwrap();
            kvs.get(key.to_owned());
        }
        ("rm", Some(sub_m)) => {
            let key = sub_m.value_of("key").unwrap();
            kvs.remove(key.to_owned());
        }
        _ => {
            eprintln!("{}", matches.usage());
            std::process::exit(1)
        }
    }
}
