
use camino::Utf8PathBuf;
use clap::{Parser, ValueEnum};
use itertools::Itertools;
use ordered_float::NotNan;
use std::io::Write;

use crate::misc::{create_buf_with_command_and_version, open_as_unwrapped_lines_filter_comments};

#[derive(Debug, Clone, Parser)]
pub struct Comparison
{
    /// First year of comparison
    year1: i32,
    /// Second year of comparison
    year2: i32,
    /// File used to map Ids to Items
    #[arg(long, short)]
    itemid_to_item_file: Option<Utf8PathBuf>,

    #[arg(long, short)]
    how: How,

    /// Reverse the order
    #[arg(long, short)]
    reverse: bool
}

#[derive(Debug, Default, ValueEnum, Clone, Copy)]
pub enum How
{
    #[default]
    Max,
    AbsSum,
    AbsSumIgnoreNan
}

pub fn sorting_stuff(opt: Comparison)
{
    let glob = format!("*/Item*_{}_vs_{}.dat", opt.year1, opt.year2);
    dbg!(&glob);
    let mut list = crate::misc::utf8_path_iter(&glob)
        .map(
            |path|
            {
                let item_id = path.parent().unwrap().as_str().to_owned();
                let mut maximum: f64 = 0.0;
                let mut abs_sum = 0.0;
                let mut total: u32 = 0;
                open_as_unwrapped_lines_filter_comments(path)
                    .for_each(
                        |line|
                        {
                            let mut iter = line.split_ascii_whitespace();
                            let val = iter.nth(1)
                                .unwrap()
                                .parse::<f64>()
                                .unwrap()
                                .abs();
                            match opt.how {
                                How::Max => {
                                    maximum = maximum.max(val);
                                },
                                How::AbsSum => {
                                    abs_sum += val;
                                    total += 1;
                                },
                                How::AbsSumIgnoreNan => {
                                    if !val.is_nan() {
                                        abs_sum += val;
                                        total += 1;
                                    }
                                }
                            }
                            
                        }
                    );
                let result = match opt.how {
                    How::Max =>  maximum,
                    _ => abs_sum / total as f64
                };
                (item_id, NotNan::new(result).unwrap())
            }
        ).collect_vec();

    list.sort_unstable_by_key(|tuple| tuple.1);
    if opt.reverse{
        list.reverse();
    }
    let map = opt.itemid_to_item_file.map(crate::parser::id_map);
    let name = format!("Cmp_res_{}_{}_{:?}.dat", opt.year1, opt.year2, opt.how);
    let mut buf = create_buf_with_command_and_version(name);
    for (item_id, val) in list.iter() {
        let mut item_name = "None";
        if let Some(map) = &map {
            let item = map.get(item_id);
            if let Some(item) = item {
                print!("{item} ");
                item_name = item;
            }
        }
        print!("ID: {}", item_id);
        
        println!(" {val}");
        writeln!(buf, "{val} {item_id} {item_name}").unwrap();
    }
    println!("We have a total of {} items", list.len());
}