use clap::Parser;

mod parser;
mod config;
use config::CmdChooser;

fn main() {
    let option = CmdChooser::parse();

    match option{
        CmdChooser::ToBinary(opt) => {
            config::to_binary(opt)
        },
        CmdChooser::DegreeDist(opt) => config::degree_dist(opt),
        CmdChooser::MaxWeight(opt) => config::max_weight(opt)
    }
}


