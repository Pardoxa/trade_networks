use std::{fs::File, io::{BufWriter, Write, BufReader}};
use clap::Parser;
use crate::network::*;

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
pub struct ToCountryBinOpt{
    #[arg(short, long)]
    /// Path to binary file to read in
    pub bin_file: String,

    #[arg(short, long)]
    /// Path to country code file
    pub country_file: String,

    #[arg(short, long)]
    /// Name of output
    pub out: String,
    
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
    ToCountryNetwork(ToCountryBinOpt),
    DegreeDist(DegreeDist),
    MaxWeight(DegreeDist),
    Misc(MiscOpt)
}

pub fn max_weight(opt: DegreeDist)
{
    let mut networks: Vec<Network> = read_networks(&opt.input);

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

pub fn to_country_file(opt: ToCountryBinOpt)
{
    let networks = read_networks(&opt.bin_file);
    let country_networks = crate::parser::country_networks(&networks, opt.country_file);
    let file = File::create(&opt.out).unwrap();
    let buf = BufWriter::new(file);
    bincode::serialize_into(buf, &country_networks)
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

pub fn read_networks(file: &str) -> Vec<Network>
{
    let file = File::open(file).unwrap();
    let reader = BufReader::new(file);
    bincode::deserialize_from(reader).expect("unable to deserialize")
}

pub fn degree_dist(opt: DegreeDist)
{
    let mut networks: Vec<Network> = read_networks(&opt.input);

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

#[derive(Parser, Debug)]
pub struct MiscOpt{
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

pub fn misc(opt: MiscOpt)
{
    let networks = read_networks(&opt.input);

    let file = File::create(opt.out).expect("unable to create file");
    let mut buf = BufWriter::new(file);

    write_commands_and_version(&mut buf).unwrap();

    writeln!(buf, "#year_id node_count nodes_with_neighbors edge_count density max_my_centrality").unwrap();

    for (id, n) in networks.iter().enumerate()
    {
        let node_count = n.node_count();
        let nodes_with_neighbors = n.nodes_with_neighbors();
        let edge_count = n.edge_count();

        let max = (nodes_with_neighbors-1) * nodes_with_neighbors;
        let density = edge_count as f64 / max as f64;

        let mut normalized = n.clone();
        normalized.normalize();
        let centrality = normalized.my_centrality_normalized();
        let max_c = centrality.iter().max().unwrap();

        writeln!(buf, "{id} {node_count} {nodes_with_neighbors} {edge_count} {density} {max_c}").unwrap();
    }
}