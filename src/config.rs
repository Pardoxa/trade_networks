use std::{fs::File, io::{BufWriter, Write, BufReader}};
use clap::Parser;

use crate::parser::Network;

pub const VERSION: &str = env!("CARGO_PKG_VERSION");

pub fn write_commands_and_version<W: Write>(mut w: W) -> std::io::Result<()>
{
    writeln!(w, "# {VERSION}")?;
    write!(w, "#")?;
    for arg in std::env::args()
    {
        write!(w, " {arg}")?;
    }
    writeln!(w)
}

#[derive(Parser, Debug)]
pub struct ToBinaryOpt{
    #[arg(short, long)]
    /// Path to csv to read in
    pub in_file: String,

    #[arg(short, long)]
    /// Name of output
    pub out: String,
    
    #[arg(short, long)]
    /// Item code, e.g. 27 for Rice
    pub item_code: String
}

/// Created by Yannick Feld
/// Program to read in Trade networks and do some data processing
#[derive(Parser)]
#[command(author, version, about)]
pub enum CmdChooser{
    ToBinary(ToBinaryOpt),
    DegreeDist(DegreeDist),
    MaxWeight(DegreeDist)
}

pub fn max_weight(opt: DegreeDist)
{
    let file = File::open(opt.input).unwrap();
    let reader = BufReader::new(file);
    let mut networks: Vec<Network> = bincode::deserialize_from(reader).expect("unable to deserialize");

    if opt.invert{
        networks.iter_mut().for_each(
            |n|
            {
                *n = n.invert();
            }
        );
    }

    crate::parser::weight_dist(&mut networks, &opt.out);
}

pub fn to_binary(opt: ToBinaryOpt)
{
    let networks = crate::parser::network_parser(&opt.in_file, &opt.item_code);

    let file = File::create(&opt.out).unwrap();
    let buf = BufWriter::new(file);
    bincode::serialize_into(buf, &networks)
        .expect("serialization issue");
}

#[derive(Parser, Debug)]
pub struct DegreeDist{
    #[arg(short, long)]
    /// In file, in binary format
    pub input: String,

    #[arg(short, long)]
    /// Name of output file
    pub out: String,

    #[arg(short, long)]
    /// Degree distribution of out-degree instead of in-degree
    pub invert: bool
}

pub fn degree_dist(opt: DegreeDist)
{
    let file = File::open(opt.input).unwrap();
    let reader = BufReader::new(file);
    let mut networks: Vec<Network> = bincode::deserialize_from(reader).expect("unable to deserialize");

    if opt.invert{
        networks.iter_mut().for_each(
            |n|
            {
                *n = n.invert();
            }
        );
    }

    crate::parser::degree_dist(&networks, &opt.out);
}