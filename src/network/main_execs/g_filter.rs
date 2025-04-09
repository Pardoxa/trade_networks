use ordered_float::OrderedFloat;

use crate::{config::FilterAddTradeGOpts, misc::{create_buf_with_command_and_version_and_header, open_as_unwrapped_lines}};
use super::super::LazyNetworks;
use std::io::Write;
use super::super::Node;

pub fn g_filter(opt: FilterAddTradeGOpts)
{
    
    let stem = opt.g_file
        .file_stem()
        .unwrap();

    let new_filename = format!("{stem}_{}.new", opt.threshold);

    let line_iter = open_as_unwrapped_lines(opt.g_file);

    let header = [
        "G",
        "country_id",
        "trade_volume(tonnes)",
        "item_name"
    ];

    let mut out = create_buf_with_command_and_version_and_header(
        new_filename,
        header
    );

    let mut counter = 0_u16;
    let mut min_top_10 = f64::MAX;
    let mut min_item_name = "".to_owned();

    for line in line_iter{
        if line.starts_with('#'){
            writeln!(out, "{line}").unwrap();
            continue;
        }

        let mut cols = line.split_ascii_whitespace();
        // lines look like this:
        // 0.18032198723135567 265 Castor oil seeds
        let g = cols.next().unwrap();
        let id = cols.next().unwrap();


        let mut network_path = opt.trade_matrix_folder.clone();
        network_path.push(format!("{id}.bincode"));
        
        let mut network = LazyNetworks::Filename(network_path);
        network.assure_availability();

        let minimum = opt.years
            .iter()
            .map(
                |&year|
                {
                    let exports = network.get_import_network_unchecked(year);
                    let trade: f64 = exports.nodes
                        .iter()
                        .map(Node::trade_amount)
                        .sum();
                    OrderedFloat::from(trade)
                }
            ).min()
            .expect("Cannot calculate trade?");

        if minimum.into_inner() < opt.threshold{
            continue;
        }
        let mut rest = String::new();
        for other in cols {
            rest.push(' ');
            rest.push_str(other);
        }

        writeln!(out, "{g} {id} {minimum}{rest}").unwrap();

        counter = counter.saturating_add(1);
        if counter <= 10 && min_top_10 > minimum.into_inner() {
            min_top_10 = minimum.into_inner();
            min_item_name = rest;
        }

    }

    writeln!(out, "#minimum trade of top 10: {min_top_10} of item {min_item_name}").unwrap();
    println!("minimum trade of top 10: {min_top_10} of item {min_item_name}");
    
}