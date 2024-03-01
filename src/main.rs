use clap::Parser;

mod parser;
mod config;
use config::CmdChooser;
mod network;
mod misc;
use network::main_execs::*;
mod units;
use parser::parse_all_extras;
pub use units::*;
mod other_exec;
pub use other_exec::*;
mod correlation_coef;
pub use correlation_coef::*;

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
                opt.group_files
            )
        },
        CmdChooser::CompareThGroups(opt) => group_cmp::compare_th_exec(opt)
    }
}


