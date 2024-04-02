use camino::Utf8Path;
use clap::Parser;
use ordered_float::OrderedFloat;
use regex::Regex;
use std::io::Write;

use crate::misc::*;

#[derive(Parser)]
pub struct AnalyzerOpts{
    #[arg(long, short)]
    pub glob: String,
    #[arg(long, short)]
    pub year: i32
}


pub fn analyze(opt: AnalyzerOpts)
{
    let maximum_header= [
        "export_frac",
        "position",
        "item"
    ];

    let name = format!("maximum_{}.info", opt.year);
    let mut maximum_buf = create_buf_with_command_and_version_and_header(
        name, 
        maximum_header
    );

    let header = [
        "export_frac",
        "relative_dispersion",
        "normed_relative_dispersion",
        "lower_shadow",
        "average_normed",
        "higher_shadow"
    ];
    let regex = Regex::new(r"\d+").unwrap();
    for path in utf8_path_iter(&opt.glob)
    {
        let parent_str = path.components().nth_back(1).unwrap().as_str();
        let item_code: i32 = regex_first_match_parsed(&regex, parent_str);
        let data = Data::from_file(&path);
        let path = format!("{}.proc", path.as_str());
        let mut buf = create_buf_with_command_and_version_and_header(path, header);
        let max_av = *data.average.iter()
            .max_by_key(|&f| OrderedFloat(*f))
            .unwrap();
        let len = data.average.len();

        let mut optimum_pos = f64::NAN;
        let mut optimum_val = f64::NEG_INFINITY;
        for i in 0..len{
            let normed = data.average_normed[i];
            let dispersion = data.variance[i].sqrt()/(max_av * normed);
            let frac = data.export_fraction[i];
            if optimum_val < dispersion && frac < 0.93{
                optimum_val = dispersion;
                optimum_pos = frac;
            }
        }
        for i in 0..len
        {
            
            let normed = data.average_normed[i];
            let dispersion = data.variance[i].sqrt()/(max_av * normed);
            let lower = normed - normed * dispersion;
            let higher = normed + normed * dispersion;
            let frac = data.export_fraction[i];
            let normed_dispersion = dispersion / optimum_val;
            writeln!(
                buf, 
                "{} {} {} {} {} {}",
                frac,
                dispersion,
                normed_dispersion,
                lower,
                normed,
                higher
            ).unwrap();


        }

        writeln!(
            maximum_buf,
            "{} {} {}",
            optimum_pos,
            optimum_val,
            item_code
        ).unwrap();
    }
}



struct Data{
    export_fraction: Vec<f64>,
    average: Vec<f64>,
    variance: Vec<f64>,
    average_normed: Vec<f64>
}

impl Data{
    fn from_file(path: &Utf8Path) -> Self
    {
        let mut export_frac = Vec::new();
        let mut average = Vec::new();
        let mut variance = Vec::new();
        let mut average_normed = Vec::new();
        for line in open_as_unwrapped_lines_filter_comments(path)
        {
            let mut iter = line.split_ascii_whitespace();
            let mut nth = |n: usize|
            {
                iter.nth(n).unwrap().parse::<f64>().unwrap()
            };
            let left = nth(0);
            let right = nth(0);
            let mid = (left + right) * 0.5;
            export_frac.push(mid);
            average.push(nth(1));
            variance.push(nth(0));
            average_normed.push(nth(0));
        }
        Self{
            average,
            average_normed,
            variance,
            export_fraction: export_frac
        }
    }
}