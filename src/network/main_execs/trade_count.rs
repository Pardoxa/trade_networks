use camino::Utf8PathBuf;
use clap::Parser;
use crate::network::LazyNetworks;

#[derive(Debug, Clone, Parser)]
pub struct TradeCountOptions{
    /// Path to network
    network: Utf8PathBuf
}

pub fn trade_count(opt: TradeCountOptions){
    let mut lazy_networks = LazyNetworks::Filename(opt.network);
    lazy_networks.assure_availability();
    let networks = lazy_networks.import_networks_unchecked();

    println!("#year trading_nodes total_edges");
    for network in networks{
        let year = network.year;
        let trading = network.list_of_trading_nodes().len();
        let edges = network.edge_count();
        println!("{year} {trading} {edges}");
    }
}