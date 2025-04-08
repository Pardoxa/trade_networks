use std::{
    cmp::Ordering, io::BufWriter, num::*, path::{Path, PathBuf}
};
use derivative::Derivative;
use fs_err::File;
use clap::{Parser, Subcommand, ValueEnum};
use crate::{
    match_maker::{MatchCalcAverage, MatchMakerOpts}, misc::{create_buf, create_buf_with_command_and_version}, network::{self, enriched_digraph::*, main_execs::{self, ExportRestrictionType, Relative}, Direction, Network}, sort_year_cmps, CorrelationInput, CorrelationMeasurement, WeightFun
};
use serde::{Serialize, Deserialize};
use camino::Utf8PathBuf;
use crate::group_cmp::GroupCompMultiOpts;
use main_execs::SimulationMode;

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

#[derive(Debug, Parser)]
pub struct BeefParser{
    pub input: PathBuf,
    #[arg(long, short)]
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
pub struct ImportExportDiffOpts{
    #[arg(long)]
    /// Path to csv to read in
    pub in_file: String,

    #[arg(long)]
    /// Item code, e.g. 27 for Rice
    pub item_code: String,

    /// Which Info to parse for building the network
    #[arg(long, value_enum, default_value_t = ReadType::ImportQuantity)]
    pub read_type: ReadType,

    #[arg(long, short)]
    /// store it as json instead
    pub json: bool,

    #[arg(long, short)]
    /// File for mapping country ids to countries
    pub country_file: Option<String>
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

#[derive(Parser)]
/// Shock multiple countries at once and count how many countries
/// still have enough product, i.e., above a certain threshold
pub struct MultiShockOpt{
    #[arg(long, short)]
    /// Path to json file, if not given default config will be printed
    pub json: Option<PathBuf>,

    /// Use top or percent?
    #[arg(long, short, value_enum)]
    pub which: ExportRestrictionType,

    #[arg(long, short, default_value_t)]
    /// Part of filename of output
    pub out_stub: String,

    #[arg(long, short)]
    /// Surpress warnings
    pub quiet: bool,

    #[arg(long, short)]
    /// Also output the group files
    pub group_files: bool,

    /// Compare successive years and output file
    #[arg(long, short, requires("group_files"))]
    pub compare_successive_years: bool,

    #[arg(long, short)]
    /// Decide if we want classic mode or
    /// if we want to include stock variation data
    pub mode: SimulationMode
}

/// Created by Yannick Feld
/// Program to read in Trade networks and do some data processing
/// It reads in the data from the FAO - note: You need to change the encoding to utf8 first
#[derive(Parser)]
#[command(author, version, about)]
pub enum CmdChooser{
    /// PARSING: Read in the networks and create bincode files for all contained items.
    ParseAllNetworks(ParseAllNetworksOpt),
    /// PARSING: Read in the networks and create bincode file for specified item
    ParseNetworks(ParseNetworkOpt),
    /// PARSING: Read in Production data and create bincode files for all contained items
    ParseAllEnrichments(ParseAllEnrichmentsOpt),
    /// PARSING: Read in Production data and create bincode file for specified items
    ParseEnrichment(ParseEnrichOpts),
    /// Create json from bincode enrichment
    EnrichmentToJson(EnrichmentToJson),
    /// Do random disruptions for all items specified by globbing
    ShockCloudAll(ShockCloudAllCmdOpt),
    /// Compare averages of different years to one another (see README.md)
    ShockCloudCmpYears(MatchMakerOpts),
    /// Calculate averages. See Readme.md
    ShockCloudCalcAverages(MatchCalcAverage),
    /// Compare multiple years in sequential order. You need to be in a folder with subfolders where each subfolder represents an item
    SortCompMultiYears(sort_year_cmps::SortCompareMultipleYears),
    /// Used for proportional and sequential disruptions. See Readme.md as this does a lot of stuff
    MultiShocks(MultiShockOpt),
    /// Helpful to get a feeling for specific networks
    PrintNetworkInfos(OnlyNetworks),
    /// To understand the difference between certain years
    CompareNetworkInfos(CompareNetworkInfos),
    DegreeDist(DegreeDist),
    Enrichment(EnrichOpt),
    MaxWeight(DegreeDist),
    Misc(MiscOpt),
    Out10(MiscOpt),
    ParseBeef(BeefParser),
    Tests(Tests),
    Three(ThreeS),
    Correlations(CorrelationOpts),
    Filter(FilterOpts),
    CompareEntries(CompareEntriesOpt),
    CompareGroups(GroupCompOpts),
    CompareThGroups(GroupCompMultiOpts),
    /// Create commands to use with compare groups, used to automate stuff
    CompareGroupsCommandCreator(CompGroupComCreOpt),
    ShockCloud(ShockCloudCmdOpt),
    ShockCloudDispersion(network::main_execs::av_analyzer::AnalyzerOpts),
    /// Create shadow plot
    ShockCloudShadow(ShockCloudShadoOpt),
    /// Compare years with one another. You need to be in a folder with subfolders where each subfolder represents an item
    SortYearComp(sort_year_cmps::Comparison),
    /// Get a list of how many countries trade in the respective years
    TradeCount(main_execs::trade_count::TradeCountOptions),
    /// Sort the averages and print out order
    SortAverages(sort_year_cmps::AverageSortOpt),
    /// Print maximal difference between reported import and corresponding reported export
    ImportExportDiff(ImportExportDiffOpts),
    /// Analyze stocks of FAO file
    StockAnalysis(StockOpt),
    /// Filter G values by trade volume - add trade volume to files
    FilterAddTradeG(FilterAddTradeGOpts)
}

#[derive(Debug, Clone, Parser)]
pub struct FilterAddTradeGOpts
{
    /// File containing the G data
    #[arg(long, short)]
    pub g_file: Utf8PathBuf,
    /// The years used for filtering - using minimum
    #[arg(long, short)]
    pub years: Vec<i32>,
    /// Trade matrix file used for the filtering
    #[arg(long)]
    pub trade_matrix_folder: Utf8PathBuf,
    /// threshold value
    #[arg(long)]
    pub threshold: f64
}

#[derive(Debug, Clone, Parser)]
pub struct StockOpt{
    /// FAO file
    #[arg(short, long)]
    pub file: String
}

#[derive(Debug, Clone, Parser)]
pub struct EnrichmentToJson{
    ///enrichment bincode file
    #[arg(short, long)]
    pub file: String,

    /// Item code of the enrichment you want to parse
    #[arg(short, long)]
    pub item_code: String
}

#[derive(Debug, Clone, Parser)]
pub struct ShockCloudShadoOpt{
    /// Glob of average files
    pub glob: String,
}

#[derive(Debug, Clone, Parser)]
pub struct ShockCloudCmdOpt{
    /// JSON FILE
    #[arg(short, long)]
    pub json: Option<Utf8PathBuf>,

    #[arg(short, long)]
    pub quiet: bool,

    #[arg(long, short, default_value_t)]
    pub out_stub: String
}

#[derive(Derivative, Clone, Parser)]
#[derivative(Default)]
pub struct ShockCloudAllCmdOpt{
    /// JSON FILE
    #[arg(short, long)]
    pub json: Option<Utf8PathBuf>,

    #[arg(short, long)]
    pub quiet: bool,

    #[arg(long, short, default_value_t)]
    pub out_stub: String,

    #[arg(long, short)]
    #[derivative(Default(value="NonZeroUsize::new(5).unwrap()"))]
    pub threads: NonZeroUsize,

    #[arg(long, short)]
    /// Decide if we want classic mode or
    /// if we want to include stock variation data
    pub mode: SimulationMode
}

#[derive(Debug, Parser)]
pub struct CompGroupComCreOpt{
    /// Glob to the files. They must be in year/file (year is a number)
    pub glob: String,

    #[arg(long, short)]
    /// also execute the commands
    pub execute: bool,

    /// Also restrict the groups to groups that have at least X entries
    #[arg(long, short)]
    pub restrict: Option<NonZeroUsize>,

    /// Write files to specfic directory
    #[arg(long, short)]
    pub dir: Option<Utf8PathBuf>
}

#[derive(Debug, Parser)]
pub struct CompareEntriesOpt
{
    /// Ignore lines that start with this String
    #[arg(long, short)]
    pub comment: Option<String>,

    /// File 1
    pub file1: String,

    /// File 2
    pub file2: String

}

#[derive(Clone, Copy, ValueEnum, Default, Debug)]
pub enum OrderHelper {
    StartWithSmallest,
    #[default]
    StartWithLargest
}

impl OrderHelper{
    pub fn get_order_fun(&self) -> fn(f64, f64) -> Ordering
    {
        pub fn s(a: f64, b: f64) -> Ordering{
            a.total_cmp(&b)
        }

        pub fn l(a: f64, b: f64) -> Ordering{
            b.total_cmp(&a)
        }

        match self{
            Self::StartWithSmallest => s,
            _ =>  l
        }
    }

    pub fn get_cmp_fun(&self) -> fn(f64, f64) -> bool
    {
        pub fn smaller(a: f64, b: f64) -> bool
        {
            a <= b
        }

        pub fn larger(a: f64, b: f64) -> bool
        {
            a >= b
        }

        match self {
            Self::StartWithSmallest => smaller,
            _ => larger
        }
    }
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
    pub col_index: usize,

    #[arg(long, value_enum, default_value_t)]
    pub order_direction: OrderHelper,

    /// Remove comments from old file
    #[arg(long, short)]
    pub remove_comments: bool
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
    pub other_file: String,

    /// Which col of the other_file contains the information on which to filter?
    pub other_col: usize,

    /// Do you want to keep or remove the specified entries?
    #[arg(value_enum, short, long, default_value_t=HowToFilter::Retain)]
    pub filter_opt: HowToFilter,

    /// Do you want to keep or remove comments?
    #[arg(value_enum, short, long, default_value_t=Comments::Keep)]
    pub comments: Comments,

    /// Output file to create. If not given, the output will be written
    /// to the terminal instead. This is overwritten by the 'glob' option
    #[arg(long, short, conflicts_with = "glob")]
    pub out: Option<PathBuf>,

    /// Use globbing to filter multiple files. "other_file" will be treaded
    /// as globbing and output files will be created by appending .filter to
    /// old filenames.
    #[arg(long, short, conflicts_with = "out")]
    pub glob: bool
}

#[derive(Parser, Debug)]
pub struct OnlyNetworks{
    /// Networks file
    pub in_file: Utf8PathBuf,

    /// Year of which infos should be printed
    #[arg(short, long)]
    pub year: Option<i32>,

    #[arg(short, long)]
    /// Print trade amount for top X
    pub top: Option<NonZeroU32>,

    /// Print infos of specific node. This is the identifier string
    #[arg(short, long)]
    pub ids: Vec<String>,

    /// If you also want to know the production of the product
    #[arg(short, long)]
    pub enrichment: Option<String>,

    /// If you also want to add the country names instead of ids
    #[arg(short, long)]
    pub country_name_file: Option<Utf8PathBuf>,

    /// Also output sorted list of exports and imports
    #[arg(short, long)]
    pub out: Option<String>
}


#[derive(Parser, Debug)]
pub struct CompareNetworkInfos{
    /// Networks file
    pub in_file: Utf8PathBuf,

    /// Year of which infos should be printed
    #[arg(long)]
    pub year1: i32,

    /// Year of which infos should be printed
    #[arg(long)]
    pub year2: i32,

    #[arg(short, long)]
    /// Print trade amount for top X
    pub top: Option<NonZeroU32>,

    /// Print infos of specific node. This is the identifier string
    #[arg(short, long)]
    pub ids: Vec<String>,

    /// If you also want to know the production of the product
    #[arg(short, long)]
    pub enrichment: Option<String>,

    /// If you also want to add the country names instead of ids
    #[arg(short, long)]
    pub country_name_file: Option<Utf8PathBuf>,

    /// Also print infos from adjacency list
    #[arg(long)]
    pub adj: bool
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
    pub in_file: Utf8PathBuf,

    #[command(subcommand)]
    pub command: SubCommand
}

#[derive(Parser, Debug)]
pub struct WorstIntegralCombineOpts{
    /// First name will be used for sorting
    #[arg(short, long)]
    pub filenames: Vec<String>
}

#[derive(Parser, Debug)]
pub struct CalcWeights{
    #[arg(short, long)]
    /// If I want to include the production values
    pub enrichment: Option<String>,

    #[arg(long, short)]
    /// The year of interest
    pub year: i32,

    /// If the weights should be "per person"
    #[arg(long, short)]
    pub population_file: Option<String>,

    #[arg(long, short, requires("population_file"))]
    /// If not all countries could be assined a population, then this can be used to print the countries in question
    pub country_map_file: Option<String>
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
    /// For checking if everything works as intended
    ReduceXTest(XOpts),
    CombineWorstIntegrals(WorstIntegralCombineOpts),
    /// Order by trade volume
    VolumeOrder(OrderedTradeVolue),
    /// Partition a file according to "partition"
    Partition(PartitionOpts),
    /// Beef ids
    BeefIds(BeefMap),
    /// Calculate weights for correlation
    Weights(CalcWeights)
}

#[derive(Parser, Debug)]
pub struct BeefMap {
    pub country_file: PathBuf,
    pub out_file: PathBuf
}

#[derive(Parser, Debug)]
pub struct OrderedTradeVolue
{
    /// For creating output names
    pub output_stub: String,

    /// Year to print. If not specified it will print all available years
    #[arg(long, short)]
    pub year: Option<i32>,

    /// If you do not want the ID numbers but the names instead you can
    /// provide the mapping file
    #[arg(long, short)]
    pub country_name_file: Option<PathBuf>,

    /// Limit output to top amount
    #[arg(long, short)]
    pub top: Option<NonZeroUsize>,

    /// how to order:
    #[arg(long, short, value_enum, default_value_t)]
    pub ordering: OrderHelper
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
    ExportQuantity,
    /// Beef Database
    Beef
}

impl ReadType{
    pub fn get_str(&self) -> &'static str
    {
        match self{
            ReadType::ImportQuantity => IMPORT_QUANTITY,
            ReadType::ExportQuantity => EXPORT_QUANTITY,
            ReadType::ExportValue => "Export Value",
            ReadType::ImportValue => "Import Value",
            ReadType::Beef => "Beef"
        }
    }

    pub fn get_direction(&self) -> Direction
    {
        match self{
            ReadType::ExportQuantity | ReadType::ExportValue => Direction::ExportTo,
            ReadType::ImportQuantity | ReadType::ImportValue => Direction::ImportFrom,
            ReadType::Beef => unimplemented!()
        }
    }
}

#[derive(Debug, clap::Args)]
#[group(required = true)]
pub struct JsonOrGlob{
    /// Json file (of job)
    #[arg(short, long, conflicts_with="glob")]
    json: Option<Utf8PathBuf>,

    /// Globbing
    #[arg(long, requires="out")]
    glob: Option<String>,

    #[arg(long, short, conflicts_with="json", requires="glob")]
    out: Option<String>
}

impl JsonOrGlob{
    pub fn into_either_json_or_glob(self) -> EitherJsonOrGlob
    {
        match (self.json, self.glob, self.out)
        {
            (Some(json), None, None) => {
                EitherJsonOrGlob::Json(json)
            },
            (None, Some(glob), Some(out)) => {
                EitherJsonOrGlob::Glob(
                    GlobStruct{glob, out}
                )
            },
            _ => unreachable!()
        }
    }

    pub fn into_correlation_measurement(self) -> CorrelationMeasurement
    {
        match self.into_either_json_or_glob()
        {
            EitherJsonOrGlob::Json(json) => {
                crate::misc::read_or_create(json)
            },
            EitherJsonOrGlob::Glob(glob) => {
                let mut inputs: Vec<_> = glob::glob(&glob.glob)
                    .unwrap()
                    .map(|p| Utf8PathBuf::from_path_buf(p.unwrap()).unwrap())
                    .map(
                        |path|
                        {
                            let r = r"\d*.dat";
                            let re = regex::Regex::new(r).unwrap();
                            let last_number = match re.find(path.as_str())
                            {
                                Some(m) => {
                                    let str = &path.as_str()[m.start()..m.end()];
                                    str.trim_end_matches(".dat")
                                },
                                None => panic!("No number at end of input file!")
                            };
                            CorrelationInput{
                                weight_path: None,
                                plot_name: last_number.to_owned(),
                                path: path.into_string(),
                            }
                        }
                    )
                    .collect();

                inputs.sort_by_cached_key(|input| input.plot_name.clone());

                CorrelationMeasurement{
                    inputs,
                    output_stub: glob.out
                }
            }
        }
    }
}

pub struct GlobStruct{
    pub glob: String,
    pub out: String
}

pub enum EitherJsonOrGlob{
    Json(Utf8PathBuf),
    Glob(GlobStruct)
}

#[derive(Parser, Debug)]
pub struct CorrelationOpts
{
    #[clap(flatten)]
    pub group: JsonOrGlob,

    /// If the label file should contain the country names instead of the Ids
    #[arg(long, short)]
    pub country_name_file: Option<PathBuf>,

    /// Weight function. Only applicable for weighted calculations
    #[arg(long, short, value_enum, default_value_t=WeightFun::NoWeight)]
    pub weight_fun: WeightFun,

    /// execute the python commands to create the dendrograms
    #[arg(short, long)]
    pub execute_python: bool,

    /// To print the python output
    #[arg(long, short, requires("execute_python"))]
    pub verbose_python: bool,

    #[arg(long, short)]
    /// execute the gnuplot heatmap scripts
    pub gnuplot_exec: bool,

    /// threshold for dendrogram
    #[arg(long, short)]
    pub threshold_color: Option<f64>,

    /// threshold for dendrogram of spearman
    #[arg(long, conflicts_with = "threshold_color")]
    #[clap(visible_alias="st")]
    pub spearman_threshold_color: Option<f64>,

    /// threshold for dendrogram of pearson
    #[arg(long, conflicts_with = "threshold_color")]
    #[clap(visible_alias="pt")]
    pub pearspn_threshold_color: Option<f64>,
}


/// Compare two groups of names. Files should contain only the names, groups seperated by
/// empty lines and/or lines starting with #
#[derive(Parser, Debug)]
pub struct GroupCompOpts{
    /// Path to group 1
    pub groups_a: String,

    /// Path to group 2
    pub groups_b: String,

    /// stub of output
    pub output_stub: String,

    /// Name of group a
    #[arg(long, requires("name_b"))]
    pub name_a: Option<String>,

    /// Name of group b
    #[arg(long, requires("name_a"))]
    pub name_b: Option<String>,

    /// should gnuplot be executed?
    #[arg(long, short)]
    pub exec_gnuplot: bool,

    /// remove groups that are smaller than specified
    #[arg(long, short)]
    pub remove_smaller: Option<NonZeroUsize>,

    #[arg(long, short)]
    /// also output file that shows the group size
    pub output_group_size: bool,

    #[arg(long, short)]
    /// remove all countries that appear only in a or only in b
    pub common_only: bool,

    /// scaling factor for gnuplot heatmaps
    #[arg(short, long, default_value_t=1.0)]
    pub scaling: f64
}
