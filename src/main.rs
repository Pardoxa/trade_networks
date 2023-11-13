use clap::Parser;

mod parser;
mod config;
use config::CmdChooser;
mod network;
mod misc;
use network::main_execs::*;

fn main() {
    let option = CmdChooser::parse();

    match option{
        CmdChooser::ParseNetworks(opt) => parse_networks(opt),
        CmdChooser::ParseAllNetworks(opt) => to_binary_all(opt),
        CmdChooser::DegreeDist(opt) => degree_dists(opt),
        CmdChooser::MaxWeight(opt) => max_weight(opt),
        CmdChooser::ToCountryNetwork(opt) => to_country_file(opt),
        CmdChooser::Misc(misc_opt) => misc(misc_opt),
        CmdChooser::Out10(opt) => export_out_comp(opt),
        CmdChooser::Enrichment(opt) => enrich(opt),
        CmdChooser::Tests(t) => test_chooser(&t.in_file, t.command)
    }
}


