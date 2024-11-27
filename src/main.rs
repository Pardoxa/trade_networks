use camino::Utf8PathBuf;
use clap::Parser;

mod parser;
mod config;
use config::CmdChooser;
mod network;
mod misc;
use network::main_execs::{self, *};
mod units;
use parser::parse_all_extras;
pub use units::*;
mod other_exec;
pub use other_exec::*;
mod correlation_coef;
pub use correlation_coef::*;
mod sync_queue;
mod sort_year_cmps;
mod group_cmp;

fn main() {
    let option = CmdChooser::parse();

    match option{
        CmdChooser::ParseNetworks(opt) => parse_networks(opt),
        CmdChooser::ParseAllNetworks(opt) => to_binary_all(opt),
        CmdChooser::DegreeDist(opt) => degree_dists(opt),
        CmdChooser::MaxWeight(opt) => max_weight(opt),
        CmdChooser::Misc(misc_opt) => misc(misc_opt),
        CmdChooser::Out10(opt) => export_out_comp(opt),
        CmdChooser::Enrichment(opt) => enrich(opt),
        CmdChooser::Tests(t) => test_chooser(t.in_file, t.command),
        CmdChooser::ParseEnrichment(o) => enrich_to_bin(o),
        CmdChooser::Three(t) => three_set_exec(t),
        CmdChooser::ParseAllEnrichments(opt) => parse_all_extras(opt.in_files, opt.only_unit),
        CmdChooser::PrintNetworkInfos(opt) => print_network_info(opt),
        CmdChooser::Correlations(opt) => correlations(opt),
        CmdChooser::Filter(filter_opts) => filter_files(filter_opts),
        CmdChooser::ParseBeef(beef_opt) => crate::network::main_execs::parse_beef_network(beef_opt),
        CmdChooser::CompareEntries(opt) => compare_entries(opt),
        CmdChooser::CompareGroups(opt) => {
            group_cmp::compare_groups(opt);
        },
        CmdChooser::CompareGroupsCommandCreator(opt) => group_cmp::command_creator(opt),
        CmdChooser::MultiShocks(opt) => {
            measure_multi_shock(
                opt.json,
                opt.which, 
                &opt.out_stub,
                opt.quiet,
                opt.group_files,
                opt.compare_successive_years
            )
        },
        CmdChooser::CompareThGroups(opt) => group_cmp::compare_th_exec(opt),
        CmdChooser::ShockCloud(opt) => {
            main_execs::random_cloud_shock(
                opt.json,
                &opt.out_stub,
                opt.quiet
            )
        },
        CmdChooser::ShockCloudAll(opt) => {
            main_execs::all_random_cloud_shocks(
                opt.json,
                &opt.out_stub,
                opt.quiet,
                opt.threads
            )
        },
        CmdChooser::ShockCloudCmpYears(opt) => {
            main_execs::match_maker::make_matches(&opt)
        },
        CmdChooser::ShockCloudCalcAverages(opt) => {
            main_execs::match_maker::calc_averages(opt)
        },
        CmdChooser::ShockCloudDispersion(opt) => {
            main_execs::av_analyzer::analyze(opt)
        },
        CmdChooser::ShockCloudShadow(opt) => {
            main_execs::av_analyzer::create_shadow_plots(opt)
        },
        CmdChooser::SortYearComp(comp) => {
            sort_year_cmps::sorting_stuff(comp);
        },
        CmdChooser::SortCompMultiYears(opt) => {
            sort_year_cmps::sort_compare_multiple_years(opt);
        },
        CmdChooser::TradeCount(opt) => trade_count::trade_count(opt),
        CmdChooser::Tmp => {
            let path: Utf8PathBuf = "/home/yannick/Documents/France/Docs/TradeData/23.10.24/cmp_all/2018-2022/987/_Y2020_Th0.7_R0.1.dat".into();
            let mut network = network::LazyNetworks::Filename("/home/yannick/Documents/France/Docs/TradeData/23.10.24/country/all/987.bincode".into());
            renormalize(&path, &mut network);
        }
    }
}


