use camino::Utf8Path;
use clap::Parser;
use ordered_float::OrderedFloat;
use regex::Regex;
use std::io::Write;

use crate::{config::ShockCloudShadoOpt, misc::*};

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

pub fn create_shadow_plots(opt: ShockCloudShadoOpt)
{
    let mut all_file = create_gnuplot_buf("all_disp_test.gp");
    writeln!(
        all_file,
        "set t pdfcairo"
    ).unwrap();
    writeln!(
        all_file,
        "set output 'test_all_disp.pdf'"
    ).unwrap();
    write!(
        all_file,
        "p "
    ).unwrap();
    utf8_path_iter(&opt.glob)
        .for_each(|file|
            {
                let average_at_0 = create_shadow_plots_helper(&file);
                write!(
                    all_file,
                    "'{file}' u 1:((sqrt($5)/({average_at_0}*$6))) w lp, "
                ).unwrap();
            }
        );
    drop(all_file);
}

pub fn create_shadow_plots_helper(file: &Utf8Path) -> f64
{
    let file_name = file.file_name().unwrap();
    let first_line = open_as_unwrapped_lines_filter_comments(&file).next().unwrap();
    let mut iter = first_line.split_ascii_whitespace();
    let first: f64 = iter.next().unwrap().parse().unwrap();
    assert_eq!(
        first,
        0.0,
        "first is not 0… Cannot create plot"
    );
    let name = file.file_stem().unwrap();
    let gp_name = format!("{name}_shadow_plot.gp");
    let pdf_name = format!("{name}_shadow_plot.pdf");
    let pdf_name2 = format!("{name}_disp_plot.pdf");

    let average_at_0: f64 = iter.nth(2).unwrap().parse().unwrap();
    let gp_file_path = file.with_file_name(gp_name);

    let mut gp_buf = create_gnuplot_buf(&gp_file_path);
    writeln!(
        gp_buf,
        "set t pdfcairo"
    ).unwrap();
    writeln!(
        gp_buf, "set output '{pdf_name}'"
    ).unwrap();

    writeln!(
        gp_buf,
        "set xlabel 'export fraction'"
    ).unwrap();
    writeln!(
        gp_buf,
        "set ylabel 'normed fragile country count'"
    ).unwrap();

    writeln!(
        gp_buf,
        "p '{file_name}' u 1:($6+$6*(sqrt($5)/({average_at_0}*$6))) w lp, \"\" u 1:($6-$6*(sqrt($5)/({average_at_0}*$6))) w lp, \"\" u 1:6"
    ).unwrap();
    writeln!(
        gp_buf,
        "set output"
    ).unwrap();

    writeln!(
        gp_buf,
        "set output '{pdf_name2}'"
    ).unwrap();
    writeln!(
        gp_buf,
        "set ylabel 'dispersion'"
    ).unwrap();
    writeln!(
        gp_buf,
        "p '{file_name}' u 1:((sqrt($5)/({average_at_0}*$6))) w lp"
    ).unwrap();
    writeln!(
        gp_buf,
        "set output"
    ).unwrap();

    drop(gp_buf);
    let current_dir = std::env::current_dir().unwrap();
    let gp_dir = gp_file_path.canonicalize_utf8().unwrap().parent().unwrap().to_owned();
    let gp_file_name = gp_file_path.file_name().unwrap();
    std::env::set_current_dir(gp_dir).unwrap();
    exec_gnuplot(gp_file_name);
    std::env::set_current_dir(current_dir).unwrap();
    average_at_0

}