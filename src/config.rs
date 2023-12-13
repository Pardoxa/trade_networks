use std::{
    num::NonZeroUsize, 
    path::{PathBuf, Path}, 
    io::BufWriter,
    fs::File
};
use clap::{Parser, Subcommand, ValueEnum};
use crate::{
    network::{Direction, NetworkType, main_execs::Relative, Network}, 
    misc::{create_buf, create_buf_with_command_and_version}
};
use serde::{Serialize, Deserialize};

#[derive(Parser, Debug)]
pub struct ParseEnrichOpts{
    #[arg(short, long)]
    /// Name of output
    pub out: String,
    
    #[arg(short, long, required(true))]
    /// Path to csv containing enrichment data
    pub enrich_files: Vec<String>,

    #[arg(long)]
    /// Item code, e.g. 27 for Rice
    pub item_code: String,

    #[arg(short, long)]
    /// Use json output format instead of bincode
    pub json: bool
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

    #[arg(short, long)]
    /// Instead of one file containing all networks do one file per item code
    pub seperate_output: bool,

    /// Which Info to parse for building the network
    #[arg(long, value_enum, default_value_t = ReadType::ImportQuantity)]
    pub read_type: ReadType,
}

#[derive(Parser, Debug)]
pub struct ParseAllEnrichmentsOpt{
    #[arg(long, short, required=true)]
    /// Path to csvs to read in
    pub in_files: Vec<String>,

    /// Only consider specified unit, disregard all other entries.
    /// Can be used to get infos of items that are in the database with 
    /// different units
    #[arg(long, short)]
    pub only_unit: Option<String>

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
    pub item_code: Option<String>,

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
    Tests(Tests),
    ParseEnrichment(ParseEnrichOpts),
    ParseAllEnrichments(ParseAllEnrichmentsOpt),
    Three(ThreeS),
    PrintNetworkInfos(OnlyNetworks),
    Correlations(CorrelationOpts),
    Filter(FilterOpts)
}

#[derive(Debug, Parser)]
pub struct PartitionOpts{
    #[arg(required=true)]
    pub partition: Vec<f64>,

    /// Without this option the program will assume that the data is already sorted
    #[arg(long, short)]
    pub sort: bool,

    /// Stub of output file
    #[arg(long, short)]
    pub output_stub: String,

    /// Index of column used for partitioning
    #[arg(long, short)]
    pub col_index: usize
}

#[derive(ValueEnum, Debug, Clone, Copy)]
pub enum HowToFilter{
    Retain,
    Remove
}

impl HowToFilter {
    pub fn is_remove(self) -> bool
    {
        matches!(self, Self::Remove)
    }
}

#[derive(ValueEnum, Debug, Clone, Copy)]
pub enum Comments{
    /// Keep old comments and create new ones
    Keep,
    /// Remove old comments but create new ones
    Remove,
    /// No comments allowed!
    None
}

impl Comments{
    pub fn get_create_buf_fun<P>(self) -> fn (P) -> BufWriter<File>
    where P: AsRef<Path>
    {
        match self {
            Self::None => {
                create_buf
            },
            _ =>  create_buf_with_command_and_version
        }
    }

    pub fn is_keep(self) -> bool
    {
        matches!(self, Self::Keep)
    }

    pub fn is_none(self) -> bool 
    {
        matches!(self, Self::None)
    }
}

#[derive(Parser, Debug)]
pub struct FilterOpts
{
    /// Path to the file that is used to filter the other file
    pub filter_by: PathBuf,

    /// Which column of the filter_by file contains the information on which to filter?
    pub filter_by_col: usize,

    /// Path to the file that you want to filter
    pub other_file: PathBuf,

    /// Which col of the other_file contains the information on which to filter?
    pub other_col: usize,
    
    /// Do you want to keep or remove the specified entries?
    #[arg(value_enum, short, long, default_value_t=HowToFilter::Retain)]
    pub filter_opt: HowToFilter,

    /// Do you want to keep or remove comments?
    #[arg(value_enum, short, long, default_value_t=Comments::Keep)]
    pub comments: Comments,

    /// Output file to create. If not given, the output will be written 
    /// to the terminal instead
    #[arg(long, short)]
    pub out: Option<PathBuf>
}

#[derive(Parser, Debug)]
pub struct OnlyNetworks{
    /// Networks file
    pub in_file: String
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

#[derive(Parser, Debug)]
pub struct WorstIntegralCombineOpts{
    /// First name will be used for sorting
    #[arg(short, long)]
    pub filenames: Vec<String>
}

#[derive(Subcommand, Debug)]
pub enum SubCommand{
    /// Calculate out component overlap
    OutComp(OutOpt),
    /// Calculate overlap of first layer
    FirstLayerOverlap(FirstLayerOpt),
    FirstLayerAll(FirstLayerOpt),
    Flow(FlowOpt),
    /// Also calculates the distribution of import and export fractions
    Shock(ShockOpts),
    ShockAvail(ShockAvailOpts),
    /// Calculate distribution for total available food fractional changes
    ShockDist(ShockDistOpts),
    CountryCount(CountryCountOpt),
    ReduceX(XOpts),
    CombineWorstIntegrals(WorstIntegralCombineOpts),
    /// Order by trade volume
    VolumeOrder(OrderedTradeVolue),
    /// Partition a file according to "partition"
    Partition(PartitionOpts)
}

#[derive(Parser, Debug)]
pub struct OrderedTradeVolue
{
    /// Year to print
    pub year: i32,
    
    /// For creating output names
    pub output_stub: String,

    /// If you do not want the ID numbers but the names instead you can 
    /// provide the mapping file
    #[arg(long, short)]
    pub country_name_file: Option<PathBuf>,

    /// Limit output to top amount
    #[arg(long, short)]
    pub top: Option<NonZeroUsize>
}


#[derive(Parser, Debug)]
pub struct CountryCountOpt{

    #[arg(short, long)]
    /// Name of output
    pub out: String,
    
}

#[derive(Parser, Debug)]
pub struct ShockOpts{

    #[arg(short, long)]
    /// Name of output file
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

    /// fraction of old exports that are still exported
    #[arg(short, long)]
    pub export: f64,  
}

#[derive(Parser, Debug)]
pub struct ShockAvailOpts{

    #[arg(short, long)]
    /// Name of output file
    pub out: String,

    /// id of exporter
    #[arg(short, long)]
    pub top_id: String,

    #[arg(short, long)]
    pub enrich_file: String,

    /// Which year to check
    #[arg(short, long)]
    pub year: i32,

    /// Iterations
    #[arg(short, long)]
    pub iterations: usize,    

    /// fraction of old exports that are still exported
    #[arg(long)]
    pub export: f64,  

    #[arg(long)]
    /// Item code, e.g. 27 for Rice
    pub item_code: Option<String>,
}

#[derive(Parser, Debug)]
pub struct ShockDistOpts{
    /// further instruction
    #[command(subcommand)]
    pub top: CountryChooser,

    #[arg(long)]
    pub enrich_file: String,

    /// Which year to check
    #[arg(short, long)]
    pub year: i32,

    /// Iterations
    #[arg(short, long)]
    pub iterations: usize,    

    /// fraction of old exports that are still exported
    #[arg(short, long, required(true))]
    pub export: Vec<f64>,  

    #[arg(long)]
    /// Item code, e.g. 27 for Rice
    pub item_code: Option<String>,

    #[arg(long, default_value_t=31)]
    /// number of bins
    pub bins: usize,

    /// Do not include the country that reduces its exports in the histogram
    #[arg(long, short)]
    pub without: bool,
}

#[derive(Parser, Debug)]
pub struct XOpts{
    /// further instruction
    #[command(subcommand)]
    pub top: CountryChooser,

    #[arg(long)]
    pub enrich_file: String,

    /// Which year to check
    #[arg(short, long)]
    pub year: i32,

    /// Iterations
    #[arg(short, long)]
    pub iterations: usize,    

    /// fraction of old exports that are still exported
    #[arg(long, default_value_t=0.0)]
    pub export_start: f64,  

    /// fraction of old exports that are still exported
    #[arg(long, default_value_t=1.0)]
    pub export_end: f64,  

    /// fraction of old exports that are still exported
    #[arg(long, value_parser = clap::value_parser!(u32).range(2..))]
    pub export_samples: u32,  

    #[arg(long)]
    /// Item code, e.g. 27 for Rice
    pub item_code: Option<String>,

    #[arg(long, default_value_t=31)]
    /// number of bins
    pub bins: usize,

    /// Do not include the country that reduces its exports in the histogram
    #[arg(long, short)]
    pub without: bool,

    /// Also create distributions. WARNING: Can result in a lot of files!
    #[arg(long, short)]
    pub distributions: bool,

    /// A negative total for a country will result in NaN or Inf
    /// for this specific country. All other countries will still be 
    /// calculated as usual
    #[arg(long)]
    pub forbid_negative_total: bool,

    /// Do not write the Acc files
    #[arg(long)]
    pub no_acc: bool,

    #[arg(long)]
    /// If I want to focus on a specific country
    pub investigate: Vec<usize>,

    #[arg(long, value_enum, default_value_t = InvestigationIndexType::GnuplotIndex)]
    /// how to interpret investigate
    pub invest_type: InvestigationIndexType,

    #[arg(long)]
    /// file that maps ids to countries
    pub country_map: Option<String>
}

#[derive(Debug, ValueEnum, Clone, Copy, Serialize, Deserialize)]
pub enum InvestigationIndexType{
    /// Use the Gnuplot index
    GnuplotIndex,
    /// Use the internal index
    SliceIndex,
    /// Use the country code
    CountryId
}

impl InvestigationIndexType{
    pub fn get_interal_index(&self, reference: &Network, idx: usize) -> usize
    {
        match self{
            Self::SliceIndex => idx,
            Self::GnuplotIndex => idx - 2,
            Self::CountryId => {
                let s = idx.to_string();
                reference.get_index(&s)
                    .expect("Requested CountryId not found")
            }
        }
    }
}

#[derive(Parser, Debug)]
pub struct ThreeS{
    /// Files to read
    #[arg(long, short, required=true)]
    pub files: Vec<String>,

    #[arg(long, short)]
    /// Out file
    pub out: String,

    #[arg(long, short)]
    pub id_map_file: Option<String>,

    #[arg(long)]
    pub border_low: f64,

    #[arg(long)]
    pub border_high: f64
}

#[derive(Subcommand, Debug)]
pub enum CountryChooser{
    /// Just use the country with the corresponding ID
    TopId(TopId),
    /// Use top X exporters. Reduction of exports will be related to each individual exporter
    Top(Top),
    /// Use top X exporters. Reduction of exports will be related to the smallest exporter
    TopRef(Top)
}

impl CountryChooser{
    pub fn get_relative(&self) -> Relative
    {
        let relative = matches!(self, CountryChooser::TopRef(_));
        if relative{
            Relative::Yes
        } else {
            Relative::No
        }
    }

    pub fn get_string(&self) -> String
    {
        match self{
            Self::Top(t) => format!("Top{}", t.top),
            Self::TopId(id) => format!("Id{}", id.id),
            Self::TopRef(tr) => format!("TopR{}", tr.top)
        }
    }

    pub fn get_specifiers(&self) -> Vec<TopSpecifier>
    {
        match self{
            Self::TopId(id) => vec![TopSpecifier::Id(id.id.clone())],
            Self::Top(t) => {
                (0..t.top.get())
                    .map(TopSpecifier::Rank)
                    .collect()
            },
            Self::TopRef(t) =>
            {
                (0..t.top.get())
                    .map(|i| 
                        TopSpecifier::RankRef(
                            TopSpecifierHelper { focus: i, reference: t.top.get() - 1 }
                        )
                    )
                    .collect()
            }
        }
    }
}

#[derive(Clone)]
pub enum TopSpecifier{
    Id(String),
    Rank(usize),
    RankRef(TopSpecifierHelper)
}

impl TopSpecifier{
    pub fn get_string(&self) -> String
    {
        match self{
            Self::Id(id) => format!("ID{}", id),
            Self::Rank(r) => format!("Rank{r}"),
            Self::RankRef(r) => format!("Rank{}Ref{}", r.focus, r.reference)
        }
    }

    pub fn get_short_str(&self) -> String {
        match self{
            Self::Id(_) => "ID".to_owned(),
            Self::Rank(_) => "Rank".to_owned(),
            Self::RankRef(r) => format!("RankRef{}", r.reference)
        }
    }
}

#[derive(Clone, Copy)]
pub struct TopSpecifierHelper{
    pub focus: usize,
    pub reference: usize
}

#[derive(Parser, Debug)]
pub struct TopId{
    /// The ID of the country that restricts exports
    pub id: String
}

#[derive(Parser, Debug)]
pub struct Top{
    /// How many top exporter to consider?
    pub top: NonZeroUsize
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
    #[arg(long)]
    pub iterations: usize,

    #[arg(long)]
    pub item_code: Option<String>,

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

#[derive(Parser, Debug)]
pub struct CorrelationOpts
{
    /// The files to correlate
    pub measurement: PathBuf,

    /// If the label file should contain the country names instead of the Ids
    pub country_name_file: Option<PathBuf>
}