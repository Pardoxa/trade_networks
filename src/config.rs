use std::num::NonZeroUsize;
use clap::{Parser, Subcommand, ValueEnum};
use crate::network::{Direction, NetworkType};
use serde::{Serialize, Deserialize};

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
pub struct ParseNetworkOpt{
    #[arg(long)]
    /// Path to csv to read in
    pub in_file: String,

    #[arg(short, long)]
    /// Name of output
    pub out: String,
    
    #[arg(long)]
    /// Item code, e.g. 27 for Rice
    pub item_code: String,

    /// Which Info to parse for building the network
    #[arg(long, value_enum, default_value_t = ReadType::ImportQuantity)]
    pub read_type: ReadType,

    #[arg(long, short)]
    /// store it as json instead
    pub json: bool
}

#[derive(Parser, Debug)]
pub struct ParseAllNetworksOpt{
    #[arg(long)]
    /// Path to csv to read in
    pub in_file: String,

    #[arg(long)]
    /// Name of the file with all item codes etc
    pub item_file: String,

    #[arg(short, long)]
    /// Instead of one file containing all networks do one file per item code
    pub seperate_output: bool,

    /// Which Info to parse for building the network
    #[arg(long, value_enum, default_value_t = ReadType::ImportQuantity)]
    pub read_type: ReadType,
}

#[derive(Parser, Debug)]
pub struct EnrichOpt{
    #[arg(long)]
    /// Path to binary network file
    pub bin_file: String,

    #[arg(short, long)]
    /// Name of output
    pub out: String,
    
    #[arg(short, long)]
    /// Path to csv containing enrichment data
    pub enrich_file: String,

    #[arg(long)]
    /// Item code, e.g. 27 for Rice
    pub item_code: String,

    #[arg(short, long)]
    /// Use json output format instead of bincode
    pub json: bool
}

/// Created by Yannick Feld
/// Program to read in Trade networks and do some data processing
#[derive(Parser)]
#[command(author, version, about)]
pub enum CmdChooser{
    DegreeDist(DegreeDist),
    Enrichment(EnrichOpt),
    MaxWeight(DegreeDist),
    Misc(MiscOpt),
    Out10(MiscOpt),
    ParseNetworks(ParseNetworkOpt),
    ParseAllNetworks(ParseAllNetworksOpt),
    Tests(Tests)
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
    pub direction: Direction
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
    /// Verbose output
    pub verbose: bool,

    #[arg(short, long)]
    /// only use effective trade, i.e.,
    /// if a -> b and b -> a only the effective result is taken
    pub effective_trade: bool
}

#[derive(Parser, Debug)]
pub struct Tests{
    /// input
    #[arg(short, long)]
    pub in_file: String,

    #[command(subcommand)]
    pub command: SubCommand
}

#[derive(Subcommand, Debug)]
pub enum SubCommand{
    /// Calculate out component overlap
    OutComp(OutOpt),
    /// Calculate overlap of first layer
    FirstLayerOverlap(FirstLayerOpt),
    FirstLayerAll(FirstLayerOpt),
    Flow(FlowOpt)
}

#[derive(Parser, Debug)]
pub struct FlowOpt{
    /// name of output file
    #[arg(short, long)]
    pub out: String,

    /// id of exporter
    #[arg(short, long)]
    pub top_id: String,

    /// Which year to check
    #[arg(short, long)]
    pub year: i32,

    /// Iterations
    #[arg(short, long)]
    pub iterations: usize,

    #[arg(short, long)]
    pub item_code: String,

    #[arg(short, long)]
    pub enrich_file: String,
}

#[derive(Parser, Debug)]
pub struct OutOpt{
    /// name of output file
    #[arg(short, long)]
    pub out: String,

    /// How many countries to consider
    #[arg(short, long)]
    pub top: NonZeroUsize,

    /// Will force this direction for all networks
    #[arg(short, long)]
    pub direction: Direction,

    /// Which year to check
    #[arg(short, long)]
    pub year: i32
}

#[derive(Parser, Debug)]
pub struct FirstLayerOpt{
    /// name of output file
    #[arg(short, long)]
    pub out: String,

    /// How many countries to consider
    #[arg(short, long)]
    pub top: NonZeroUsize,

    /// Will force this direction for all networks
    #[arg(short, long)]
    pub direction: Direction,

    /// Which year to check
    #[arg(short, long)]
    pub year: i32,

    #[arg(short, long)]
    /// Input file of country id mappings
    pub print_graph: Option<String>
}

#[derive(Debug, ValueEnum, Clone, Copy, Serialize, Deserialize)]
pub enum ReadType{
    /// Use reported Import value
    ImportValue,
    /// Use reported Export Value
    ExportValue,
    /// Use reported Import Quantity
    ImportQuantity,
    /// Use reported Export Quantity
    ExportQuantity
}

impl ReadType{
    pub fn get_str(&self) -> &'static str
    {
        match self{
            ReadType::ImportQuantity => "Import Quantity",
            ReadType::ExportQuantity => "Export Quantity",
            ReadType::ExportValue => "Export Value",
            ReadType::ImportValue => "Import Value"
        }
    }

    pub fn get_direction(&self) -> Direction
    {
        match self{
            ReadType::ExportQuantity | ReadType::ExportValue => Direction::ExportTo,
            ReadType::ImportQuantity | ReadType::ImportValue => Direction::ImportFrom
        }
    }

    #[allow(dead_code)]
    pub fn get_network_type(&self) -> NetworkType
    {
        match self{
            ReadType::ExportQuantity | ReadType::ImportQuantity => NetworkType::Quantity,
            ReadType::ExportValue | ReadType::ImportValue => NetworkType::Value
        }
    }
}