use std::num::NonZeroUsize;

use clap::{Parser, Subcommand};



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
    pub seperate_output: bool   
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
    ToCountryNetwork(ToCountryBinOpt),
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
    pub invert: bool
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
    pub invert: bool,

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
    /// c
    OutComp(OutOpt),
    /// d
    C2{
        /// o
        other_stuff: String,
        /// f
        flag: bool
    }
}

#[derive(Parser, Debug)]
pub struct OutOpt{
    /// name of output file
    #[arg(short, long)]
    pub out: String,

    /// How many countries to consider
    #[arg(short, long)]
    pub top: NonZeroUsize,

    #[arg(short, long)]
    pub invert: bool
}