
use camino::Utf8PathBuf;
use clap::{Parser, ValueEnum};
use itertools::Itertools;
use ordered_float::NotNan;
use std::{collections::{BTreeMap, BTreeSet}, io::Write};

use crate::misc::{create_buf_with_command_and_version, create_buf_with_command_and_version_and_header, open_as_unwrapped_lines_filter_comments};

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

pub fn sorting_stuff(opt: Comparison) -> Vec<(String, NotNan<f64>)>
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

    list.retain(|item| item.1 != 0.0);

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
    list
}

#[derive(Debug, Clone, Parser)]
pub struct SortCompareMultipleYears
{
    /// First year of comparison
    start_year: i32,
    /// Last year of comparison
    end_year: i32,
    /// File used to map Ids to Items
    #[arg(long, short)]
    itemid_to_item_file: Option<Utf8PathBuf>,

    #[arg(long, short)]
    how: How,

    /// Reverse the order
    #[arg(long, short)]
    reverse: bool
}

pub fn sort_compare_multiple_years(opt: SortCompareMultipleYears)
{
    let mut all_years = Vec::new();
    let mut all_ids = BTreeSet::new();
    for year in opt.start_year..opt.end_year
    {
        let comp_opt = Comparison{
            year1: year,
            year2: year + 1,
            itemid_to_item_file: opt.itemid_to_item_file.clone(),
            how: opt.how,
            reverse: opt.reverse
        };
        let list = sorting_stuff(comp_opt);
        all_ids.extend(
            list.iter().map(|tuple| tuple.0.clone())
        );
        all_years.push(list);
    }
    let first_year = all_years.remove(0);
    let first_year_set: BTreeSet<_> = first_year.iter()
        .map(|tuple| tuple.0.clone())
        .collect();

    let id_map = opt.itemid_to_item_file
        .as_deref()
        .map(crate::parser::id_map);

    let other_year_maps: Vec<BTreeMap<_, _>> = all_years
        .into_iter()
        .map(
            |list|
            {
                list.into_iter()
                    .enumerate()
                    .map(
                        |(rank, (id, val))|
                        {
                            (id, (rank, val))
                        }
                    ).collect()

            }
        ).collect();

    let name = format!("From_{}_to_{}_{:?}_cmp.dat", opt.start_year, opt.end_year, opt.how);
    let name2 = format!("From_{}_to_{}_{:?}_cmp_val.dat", opt.start_year, opt.end_year, opt.how);

    let mut header = vec!["ID".to_string()];

    header.extend(
        (opt.start_year..=opt.end_year)
            .map(|year| year.to_string())
    );
    if opt.itemid_to_item_file.is_some(){
        header.push("humanreadable_ID".into());
    }        

    let mut buf = create_buf_with_command_and_version_and_header(
        name, 
        header.as_slice()
    );
    let mut buf_value = create_buf_with_command_and_version_and_header(
        name2, 
        header
    );

    for (rank, (id, amount)) in first_year.iter().enumerate()
    {
        write!(
            buf,
            "{id} {rank}"
        ).unwrap();
        write!(
            buf_value,
            "{id} {amount}"
        ).unwrap();
        for other in other_year_maps.iter()
        {
            match other.get(id){
                None => {
                    write!(buf, " NaN").unwrap();
                    write!(buf_value, " NaN").unwrap();
                },
                Some((rank, value)) => {
                    write!(buf, " {rank}").unwrap();
                    write!(buf_value, " {value}").unwrap();
                }
            }
        }
        if let Some(map) = id_map.as_ref(){
            match map.get(id) {
                Some(name) => {
                    write!(buf, " {name}").unwrap();
                    write!(buf_value, " {name}")
                },
                None => {
                    write!(buf, " Unknown").unwrap();
                    write!(buf_value, " Unkown")
                }
            }.unwrap();
        }
        writeln!(buf).unwrap();
        writeln!(buf_value).unwrap();
    }

    for id in all_ids.difference(&first_year_set)
    {
        write!(
            buf,
            "{id} NaN"
        ).unwrap();
        write!(
            buf_value,
            "{id} NaN"
        ).unwrap();
        for other in other_year_maps.iter()
        {
            match other.get(id){
                None => {
                    write!(buf, " NaN").unwrap();
                    write!(buf_value, " NaN").unwrap()
                },
                Some((rank, value)) => {
                    write!(buf, " {rank}").unwrap();
                    write!(buf_value, " {value}").unwrap();
                }
            }
        }
        if let Some(map) = id_map.as_ref(){
            match map.get(id) {
                Some(name) => {
                    write!(buf, " {name}").unwrap();
                    write!(buf_value, " {name}")
                },
                None => {
                    write!(buf, " Unknown").unwrap();
                    write!(buf_value, " Unknown")
                }
            }.unwrap();
        }
        writeln!(buf).unwrap();
        writeln!(buf_value).unwrap();
    }

}