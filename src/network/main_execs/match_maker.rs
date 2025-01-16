use std::collections::{BTreeMap, BTreeSet};
use std::io::Write;
use camino::Utf8PathBuf;
use itertools::Itertools;
use ordered_float::OrderedFloat;
use crate::misc::*;
use regex::Regex;
use clap::{Parser, ValueEnum};
use rayon::prelude::*;

pub fn make_matches(opt: &MatchMakerOpts)
{
    create_matches(opt)
        .into_par_iter()
        .for_each(
            |m|
            {
                m.work(opt.how, opt.norm_by_trading_countries)
            }
        );
}


#[derive(Parser)]
pub struct MatchMakerOpts{
    /// Glob to files representing the older year
    old_glob: String,
    /// Glob to files representing the newer year
    new_glob: String,
    /// What to do if matching files are in different dirs?
    #[arg(long, value_enum, default_value_t = MatchHelper::Skip)]
    how: MatchHelper,
    /// Which norming to use? Default: Norm by max.
    #[arg(long, short)]
    pub norm_by_trading_countries: bool
}

fn create_matches_helper(glob: &str) -> BTreeMap<u16, MatchItem>
{
    let expr = r"\d+";
    let re = Regex::new(expr).unwrap();

    utf8_path_iter(glob)
        .map(
            |p|
            {
                let file = p.file_name().unwrap();
                let year: u16 = regex_first_match_parsed(&re, file);
                let canon = p.canonicalize_utf8().unwrap();
                let parent = canon.parent().unwrap();
                let dir = parent.components()
                    .last()
                    .unwrap()
                    .as_str();
                let item_code: u16 = regex_first_match_parsed(&re, dir);
                let matched = MatchItem{
                    year,
                    path: p
                };
                (item_code, matched)
            }
        ).collect()
}

pub fn create_matches(opt: &MatchMakerOpts) -> Vec<Matched>
{
    let mut old_matches = create_matches_helper(&opt.old_glob);
    let mut new_matches = create_matches_helper(&opt.new_glob);
    let items_old: BTreeSet<_> = old_matches.keys().copied().collect();
    let new_items: BTreeSet<u16> = new_matches.keys().copied().collect();

    items_old.intersection(&new_items)
        .map(
            |key|
            {
                let old = old_matches.remove(key).unwrap();
                let new = new_matches.remove(key).unwrap();
                Matched{
                    old,
                    new,
                    item: *key
                }
            }
        ).collect_vec()
}

#[derive(Debug)]
pub struct MatchItem{
    year: u16,
    path: Utf8PathBuf
}

#[derive(Debug)]
pub struct Matched{
    old: MatchItem,
    new: MatchItem,
    item: u16
}

#[derive(Debug, Clone, Copy, ValueEnum)]
/// What to do when dirs don't match?
pub enum MatchHelper{
    /// Put output in dir of old file
    Old,
    /// Put output in dir of new file
    New,
    /// Skip if dir of old != dir of new
    Skip
}



impl Matched{
    fn work(&self, how: MatchHelper, norm_by_trading_countries: bool)
    {

        let fun = if norm_by_trading_countries{
            get_vals_trading
        } else {
            get_vals_max_normed
        };
        let mut result_path = match how{
            MatchHelper::New => {
                get_owned_parent_path(&self.new.path)
            },
            MatchHelper::Old => {
                get_owned_parent_path(&self.old.path)
            },
            MatchHelper::Skip => {
                let old_parent = get_owned_parent_path(&self.old.path);
                let new_parent = get_owned_parent_path(&self.new.path);
                if old_parent != new_parent {
                    println!("SKIPPING: {self:?}");
                    return;
                }
                old_parent
            }
        };

        let prefix = if norm_by_trading_countries {
            "Country_normed_"
        } else {
            ""
        };

        result_path.push(
            format!("{prefix}Item{}_{}_vs_{}.dat", self.item, self.old.year, self.new.year)
        );
        let header = [
            "total_export_fraction".to_owned(),
            format!("Y{}-Y{}", self.new.year, self.old.year)
        ];
        let mut buf = create_buf_with_command_and_version_and_header(
            result_path, 
            header
        );
        let old_iter = open_as_unwrapped_lines_filter_comments(&self.old.path);
        let new_iter = open_as_unwrapped_lines_filter_comments(&self.new.path);

        for (old, new) in old_iter.zip(new_iter)
        {
            let (o_mid, o_normed) = fun(&old);
            let (n_mid, n_normed) = fun(&new);
            assert_eq!(
                o_mid, 
                n_mid,
                "Histograms don't match?"
            );
            writeln!(
                buf,
                "{o_mid} {}",
                n_normed - o_normed
            ).unwrap();
        }
    } 
}

#[derive(Parser)]
pub struct MatchCalcAverage{
    /// Glob to files to compare
    glob: String,

    /// Output path
    out: Utf8PathBuf,

    /// which mode?
    #[arg(short, long)]
    mode: Mode,

    #[arg(long, value_enum, default_value_t)]
    /// Sum normal or just the absolute?
    how: AverageCalcOpt,

    /// Use the trading_countries normalized values? Default: Max normalized
    #[arg(long, short)]
    trading_countries_norm: bool,

    #[arg(long, short)]
    /// Ignore nan entries for the averaging
    ignore_nans: bool
}

#[derive(ValueEnum, Debug, Clone, Copy, Default)]
pub enum AverageCalcOpt{
    Abs,
    #[default]
    Normal
}

fn get_vals_trading(line: &str) -> (f64, f64)
{
    let mut iter = line.split_ascii_whitespace()
        .map(|s| s.parse::<f64>().unwrap());
    let left = iter.next().unwrap();
    let right = iter.next().unwrap();
    let normed_average = iter.last().unwrap();
    ((left + right) * 0.5, normed_average)
}

fn get_vals_max_normed(line: &str) -> (f64, f64)
{
    let mut iter = line.split_ascii_whitespace()
        .map(|s| s.parse::<f64>().unwrap());
    let left = iter.next().unwrap();
    let right = iter.next().unwrap();
    let normed_average = iter.nth(3).unwrap();
    ((left + right) * 0.5, normed_average)
}

pub fn processed_get_vals(line: &str) -> (f64, f64)
{
    line.split_ascii_whitespace()
        .map(|s| s.parse::<f64>().unwrap())
        .collect_tuple()
        .unwrap()
}

#[derive(ValueEnum, Debug, Clone, Copy)]
pub enum Mode{
    /// output of shock-cloud-all
    Raw,
    /// output of shock-cloud-cmp-years
    Processed
}

pub fn calc_averages(opt: MatchCalcAverage)
{
    let fun = match opt.mode{
        Mode::Processed => {
            processed_get_vals
        },
        Mode::Raw => {
            if opt.trading_countries_norm{
                get_vals_trading
            } else {
                get_vals_max_normed
            }
        }
    };
    let mut iter = utf8_path_iter(&opt.glob);
    let first = iter.next().unwrap();
    println!("reading {first}");
    let (mid_vec, mut sum): (Vec<_>, Vec<Vec<_>>) = open_as_unwrapped_lines_filter_comments(first)
        .map(
            |line|
            {
                let (mid, val) = fun(&line);
                let val = match opt.how{
                    AverageCalcOpt::Abs => val.abs(),
                    AverageCalcOpt::Normal => val
                };
                (mid, vec![val])
            }
        ).unzip();

    let mut nan_paths = BTreeSet::new();

    for path in iter{
        println!("Reading {path}");
        open_as_unwrapped_lines_filter_comments(&path)
        .map(
            |line|
            {
                fun(&line)
            }
        ).enumerate()
        .for_each(
            |(idx, (mid, val))|
            {
                assert_eq!(
                    mid,
                    mid_vec[idx],
                    "Mids need to match!"
                );
                let nan = val.is_nan();
                if nan{
                    println!("NAN!");
                    nan_paths.insert(path.as_str().to_owned());
                }
                if !opt.ignore_nans || !nan{
                    sum[idx].push(
                        match opt.how{
                            AverageCalcOpt::Normal => val,
                            AverageCalcOpt::Abs => val.abs()
                        }
                    );
                }
                
            }
        )
    }

    if !nan_paths.is_empty(){
        dbg!(nan_paths);
    }

    let header = [
        "mid",
        "average",
        "median"
    ];
    let mut buf = create_buf_with_command_and_version_and_header(opt.out, header);
    for (mid, mut sum) in mid_vec.iter().zip(sum)
    {
        sum.sort_unstable_by_key(|v| OrderedFloat(*v));
        let median = sum[sum.len() / 2];
        let average = sum.iter().sum::<f64>() / sum.len() as f64;
        writeln!(
            buf,
            "{mid} {average} {median}"
        ).unwrap();
    }
}