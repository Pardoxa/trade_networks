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
        CmdChooser::ToBinary(opt) => to_binary(opt),
        CmdChooser::DegreeDist(opt) => degree_dists(opt),
        CmdChooser::MaxWeight(opt) => max_weight(opt),
        CmdChooser::ToCountryNetwork(opt) => to_country_file(opt),
        CmdChooser::Misc(misc_opt) => misc(misc_opt)
    }
}


