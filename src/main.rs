use clap::Parser;

mod parser;
mod config;
use config::CmdChooser;
mod network;

fn main() {
    let option = CmdChooser::parse();

    match option{
        CmdChooser::ToBinary(opt) => {
            config::to_binary(opt)
        },
        CmdChooser::DegreeDist(opt) => config::degree_dist(opt),
        CmdChooser::MaxWeight(opt) => config::max_weight(opt),
        CmdChooser::ToCountryNetwork(opt) => config::to_country_file(opt),
        CmdChooser::Misc(misc_opt) => config::misc(misc_opt)
    }
}


