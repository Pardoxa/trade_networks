use {
    std::{
        collections::*,
        fs::File,
        borrow::*,
        io::{BufWriter, Write},
        path::{Path, PathBuf}
    },
    clap::ValueEnum,
    serde::{Serialize, Deserialize},
    sampling::{GnuplotTerminal, GnuplotSettings, GnuplotAxis},
    itertools::*,
    crate::{
        misc::*,
        config::*
    }
};

const SPEARMAN_TITLE: &str = "Spearman correlation coefficient";
const PEARSON_TITLE: &str = "Pearson correlation coefficient";
const WEIGHTED_PEARSON_TITLE: &str = "Weighted Pearson corr. coef.";
const WEIGHTED_SAMPLE_TITLE: &str = "Weighted sample corr. coef.";

#[derive(Clone, Copy, Debug)]
pub struct Stats{
    pub average: f64,
    pub variance: f64,
    pub min: f64,
    pub max: f64,
    pub median: f64
}

impl Stats{
    pub fn get_std_dev(&self) -> f64
    {
        self.variance.sqrt()
    }

    pub fn get_cv(&self) -> f64
    {
        self.get_std_dev() / self.average
    }
}

impl FromIterator<f64> for Stats{
    fn from_iter<T: IntoIterator<Item = f64>>(iter: T) -> Self {
        let mut sum = 0.0;
        let mut sum_sq = 0.0;
        let mut counter = 0_u64;
        let mut min = f64::INFINITY;
        let mut max = f64::NEG_INFINITY;

        let mut vals: Vec<_> = iter.into_iter().collect();
        vals.iter()
            .for_each(
                |v| 
                {
                    sum += v;
                    sum_sq = v.mul_add(*v, sum_sq);
                    counter += 1;
                    min = min.min(*v);
                    max = max.max(*v);
                }
            );
        vals.sort_unstable_by(|a, b| a.total_cmp(b));
        let median = match vals.len(){
            0 => f64::NAN,
            1 => vals[0],
            len if len % 2 == 0 => {
                let mid = len / 2;
                (vals[mid] + vals[mid - 1]) / 2.0
            },
            len => vals[len / 2]
        };


        let factor = (counter as f64).recip();
        let average = sum * factor;
        let variance = sum_sq * factor - average * average;
        Self { average, variance, min, max, median }
    }
}




#[derive(Debug, Clone, Copy)]
pub struct CorrelationItem<'a>
{
    val: f64,
    weight: &'a ImportAndProduction
}

fn weighted_goods_cor_iter<'a>(
    a: &'a HashMap<u16, f64>, 
    b: &'a HashMap<u16, f64>,
    weights_a: &'a ProductionImportMap,
    weights_b: &'a ProductionImportMap
)-> impl Iterator<Item = (CorrelationItem<'a>, CorrelationItem<'a>)>
{
    let (small, large) = if a.len() <= b.len() {
        ((a, weights_a), (b, weights_b))
    } else {
        ((b, weights_b), (a, weights_a))
    };

    small.0.iter()
        .filter_map(
            |(key, value)|
            {
                if !value.is_finite(){
                    None
                } else {
                    large.0
                        .get(key)
                        .filter(|val| val.is_finite())
                        .map(
                            |other_val| 
                            {
                                let small_p = small.1.map.get(key).unwrap();
                                let large_p = large.1.map.get(key).unwrap();
                                (
                                    CorrelationItem{val: *value, weight: small_p},
                                    CorrelationItem{val: *other_val, weight: large_p}
                                )
                            }
                        )
                }
                
            }
        )
}


fn weighted_paper_correlation_coef<'a, I, F>(
    iterator: I
) -> f64
where I: IntoIterator<Item = (F, F)>,
    F: Borrow<CorrelationItem<'a>>
{
    let all_samples = iterator.into_iter().collect_vec();
    let mut weight_sum_a = 0.0;
    let mut weight_sum_b = 0.0;
    let mut sum_a = 0.0;
    let mut sum_b = 0.0;

    for (a, b) in all_samples.iter()
    {
        let a: &CorrelationItem<'a> = a.borrow();
        let b = b.borrow();
        let weight_a = a.weight.sum();
        let weight_b = b.weight.sum();
        weight_sum_a += weight_a;
        weight_sum_b += weight_b;
        sum_a = weight_a.mul_add(a.val, sum_a);
        sum_b = weight_b.mul_add(b.val, sum_b);
    }

    let average_a = sum_a / weight_sum_a;
    let average_b = sum_b / weight_sum_b;

    let mut sum_above = 0.0;
    let mut sum_below_a = 0.0;
    let mut sum_below_b = 0.0;

    for (a, b) in all_samples
    {
        let a = a.borrow();
        let b = b.borrow();
        let weight_a = a.weight.sum();
        let weight_b = b.weight.sum();
        let a_diff = a.val - average_a;
        let b_diff = b.val - average_b;
        sum_above += a_diff * b_diff * (weight_a * weight_b).sqrt();
        sum_below_a += a_diff * a_diff * weight_a;
        sum_below_b += b_diff * b_diff * weight_b;
    }

    let below = (sum_below_a * sum_below_b).sqrt();

    sum_above / below
}

fn weighted_pearson_correlation_coefficient<'a, I, F, WF>(
    iterator: I,
    weight_fun: WF
) -> f64
where I: IntoIterator<Item = (F, F)>,
    F: Borrow<CorrelationItem<'a>>,
    WF: Fn(ImportAndProduction, ImportAndProduction) -> f64
{   
    let mut weight_sum = 0_f64;
    let mut a_w_sum = 0_f64;
    let mut b_w_sum = 0_f64;
    let mut a_w_sq_sum = 0_f64;
    let mut b_w_sq_sum = 0_f64;
    let mut ab_w_sum = 0_f64;

    for (a, b) in iterator{
        let a = a.borrow();
        let b = b.borrow();
        let a_val = a.val;
        let b_val = b.val;
        let w = weight_fun(*a.weight, *b.weight);
        
        weight_sum += w;
        a_w_sum = a_val.mul_add(w, a_w_sum);
        b_w_sum = b_val.mul_add(w, b_w_sum);
        a_w_sq_sum = (a_val * a_val).mul_add(w, a_w_sq_sum);
        b_w_sq_sum = (b_val * b_val).mul_add(w, b_w_sq_sum);
        ab_w_sum = (a_val * b_val).mul_add(w, ab_w_sum);
    }
    let w_recip = weight_sum.recip();
    let a_av = a_w_sum * w_recip;
    let b_av = b_w_sum * w_recip;
    let ab_av = ab_w_sum * w_recip;
    let a2_av = a_w_sq_sum * w_recip;
    let b2_av = b_w_sq_sum * w_recip;

    let variance_a = a2_av - a_av * a_av;
    let variance_b = b2_av - b_av * b_av;
    let std_a = variance_a.sqrt();
    let std_b = variance_b.sqrt();

    let cov = ab_av - a_av * b_av;

    cov / (std_a * std_b)

}

fn spearman_correlation_coefficent<I, F>(iterator: I) -> f64
where I: IntoIterator<Item = (F, F)>,
    F: Borrow<f64>
{
    let (a, b) = iterator
        .into_iter()
        .enumerate()
        .map(
            |(idx, item)| 
            {
                let a = *item.0.borrow();
                let b = *item.1.borrow();
                ((a, idx), (b, idx))
            }    
        
        ).unzip();

    let mut always_unique = true;
    let mut create_map = |mut vec: Vec<(f64, usize)>|
    {
        vec.sort_unstable_by(|a, b| a.0.partial_cmp(&b.0).unwrap());
        
        let mut map = vec![0_i64; vec.len()];
        let mut counter = -1;
        let mut last_val = f64::NEG_INFINITY;
        for (val, old_idx) in vec 
        {
            if last_val != val {
                counter += 1;
                last_val = val;
            } else {
                always_unique = false;
            }
            map[old_idx] = counter;
        }
        map
    };
    
    let a_map = create_map(a);
    let b_map = create_map(b);

    if always_unique{
        let n = a_map.len() as u64;
        let d_sq_6 = a_map.iter()
            .zip(b_map.iter())
            .map(
                |(&a, &b)| {
                    let dif = a.abs_diff(b);
                    dif * dif
                }
            ).sum::<u64>() * 6;
        
        1.0 - d_sq_6 as f64 / (n * (n * n - 1)) as f64
    } else {
        let iter = a_map
            .iter()
            .zip(b_map.iter())
            .map(|(&a, &b)| (a as f64, b as f64));

        pearson_correlation_coefficient(iter)
    }
}

fn pearson_correlation_coefficient<I, F>(iterator: I) -> f64
where I: IntoIterator<Item = (F, F)>,
    F: Borrow<f64>
{
    let mut product_sum = 0.0;
    let mut x_sum = 0.0;
    let mut x_sq_sum = 0.0;
    let mut y_sum = 0.0;
    let mut y_sq_sum = 0.0;
    let mut counter = 0_u64;

    for (x, y) in iterator
    {
        let x = *x.borrow();
        let y = *y.borrow();
        product_sum = x.mul_add(y, product_sum);
        x_sq_sum = x.mul_add(x, x_sq_sum);
        y_sq_sum = y.mul_add(y, y_sq_sum);
        x_sum += x;
        y_sum += y;
        counter += 1;
    }

    let factor = (counter as f64).recip();
    let average_x = x_sum * factor;
    let average_y = y_sum * factor;
    let average_product = product_sum * factor;

    let covariance = average_product - average_x * average_y;
    let variance_x = x_sq_sum * factor - average_x * average_x;
    let variance_y = y_sq_sum * factor - average_y * average_y;
    let std_product = (variance_x * variance_y).sqrt();

    covariance / std_product
}

fn goods_cor_iter<'a>(a: &'a HashMap<u16, f64>, b: &'a HashMap<u16, f64>) -> impl Iterator<Item = (f64, f64)> + 'a
{
    let (small, large) = if a.len() <= b.len() {
        (a, b)
    } else {
        (b, a)
    };
    small.iter()
        .filter_map(
            |(key, value)|
            {
                if !value.is_finite(){
                    None
                } else {
                    large.get(key)
                        .filter(|v| v.is_finite())
                        .map(|other_val| (*value, *other_val))
                }   
            }
        )
}


#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CorrelationInput{
    pub path: String,
    pub plot_name: String,
    pub weight_path: Option<String>
}

impl CorrelationInput{
    pub fn has_weights(&self) -> bool{
        self.weight_path.is_some()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CorrelationMeasurement{
    pub inputs: Vec<CorrelationInput>,
    pub output_stub: String
}

impl Default for CorrelationMeasurement{
    fn default() -> Self {
        let example = CorrelationInput{
            path: "InputPath".to_owned(), 
            plot_name: "Corresponding Name".to_string(),
            weight_path: None
        };
        Self { 
            inputs: vec![example],
            output_stub: "example".to_owned()
        }
    }
}

#[derive(Clone, Copy, Debug, Default)]
pub struct ImportAndProduction
{
    import: f64,
    production: f64
}

impl ImportAndProduction{
    fn sum(self) -> f64
    {
        self.import + self.production
    }
}

pub struct ProductionImportMap{
    map: HashMap<u16, ImportAndProduction>,
    has_production_data: bool
}

fn read_weights<P>(weight_file: P) -> ProductionImportMap
where P: AsRef<Path>
{
    let mut any_production_data = false;
    let map = open_as_unwrapped_lines_filter_comments(weight_file)
        .map(
            |line|
            {
                let mut iter = line.split_whitespace();
                let country_id = iter.next().unwrap().parse().unwrap();
                let import = iter.next().unwrap().parse().unwrap();
                let mut production = 0.0;
                if let Some(p) = iter.next(){
                    any_production_data = true;
                    production = p.parse().unwrap();
                }
                let val: ImportAndProduction = ImportAndProduction{import, production};
                (country_id, val)
            }
        ).collect();
    ProductionImportMap{
        map,
        has_production_data: any_production_data
    }
}

#[derive(ValueEnum, Clone, Copy, Debug)]
pub enum WeightFun{
    NoWeight,
    Product,
    Min,
    Max,
    OnlyImportProduct,
    OnlyImportMin,
    OnlyImportMax
}

impl WeightFun{
    pub fn stub(self) -> &'static str
    {
        match self{
            Self::NoWeight => "NoWeight",
            Self::Product => "Product",
            Self::Max => "Max",
            Self::Min => "Min",
            Self::OnlyImportProduct => "OnlyImportProduct",
            Self::OnlyImportMax => "OnlyImportMax",
            Self::OnlyImportMin => "OnlyImportMin"
        }
    }

    pub fn get_fun(self) -> fn (ImportAndProduction, ImportAndProduction) -> f64
    {
        fn no_weight(_: ImportAndProduction, _: ImportAndProduction) -> f64
        {
            2.0
        }

        fn product(a: ImportAndProduction, b: ImportAndProduction) -> f64
        {
            a.sum() * b.sum()
        }

        fn only_import_product(a: ImportAndProduction, b: ImportAndProduction) -> f64
        {
            a.import * b.import
        }

        fn min(a: ImportAndProduction, b: ImportAndProduction) -> f64
        {
            a.sum().min(b.sum())
        }

        fn only_import_min(a: ImportAndProduction, b: ImportAndProduction) -> f64
        {
            a.import.min(b.import)
        }

        fn max(a: ImportAndProduction, b: ImportAndProduction) -> f64
        {
            a.sum().max(b.sum())
        }

        fn only_import_max(a: ImportAndProduction, b: ImportAndProduction) -> f64
        {
            a.import.max(b.import)
        }

        match self{
            Self::NoWeight => no_weight,
            Self::Product => product,
            Self::Max => max,
            Self::Min => min,
            Self::OnlyImportProduct => only_import_product,
            Self::OnlyImportMax => only_import_max,
            Self::OnlyImportMin => only_import_min
        }
    }
}

pub fn correlations(opt: CorrelationOpts)
{
    let country_name_map: Option<BTreeMap<String, String>> = opt.country_name_file
        .map(crate::parser::country_map);
    let inputs: CorrelationMeasurement = read_or_create(opt.measurement);
    assert!(!inputs.inputs.is_empty());

    let has_weights = inputs.inputs
        .iter()
        .any(CorrelationInput::has_weights);
    if has_weights
    {
        assert!(
            inputs.inputs.iter().all(CorrelationInput::has_weights),
            "Some, but not all inputs have associated weights. Abbort!"
        );
    }
    let weights = has_weights.then(
        ||
        inputs.inputs
            .iter()
            .map(|i| read_weights(i.weight_path.as_deref().unwrap()))
            .collect_vec()
    );

    if let Some(w) = weights.as_deref()
    {
        let mut iter = w.iter();
        let production = unsafe{iter.next().unwrap_unchecked()}.has_production_data;
        let different = iter.any(|e| e.has_production_data != production);
        if different{
            eprintln!("Warning: Only some of the data has production info");
        }
    }

    let mut all_countries = BTreeSet::new();

    let all_infos: Vec<HashMap<_, _>> = inputs.inputs
        .iter()
        .map(
            |i|
            {
                let path = i.path.as_str();
                open_as_unwrapped_lines(path)
                    .filter(|l| !l.starts_with('#'))
                    .map(
                        |line|
                        {
                            let mut split_iter = line.split_whitespace();
                            let country: u16 = split_iter.next().unwrap().parse().unwrap();
                            let worst_integral_value: f64 = split_iter.next().unwrap().parse().unwrap();
                            all_countries.insert(country);
                            (country, worst_integral_value)
                        }
                    ).collect()
            }
        ).collect();

    let matrix_name = format!("{}.matrix", &inputs.output_stub);
    let mut buf_pearson = create_buf_with_command_and_version(matrix_name.as_str());

    let matrix_name_spear = format!("{}_spear.matrix", &inputs.output_stub);
    let mut buf_spear = create_buf_with_command_and_version(matrix_name_spear.as_str());

    let mut weight_cor_name = None;
    let mut buf_weighted_cor = has_weights.then(
        ||
        {
            let name = format!("{}_{}Weighted.matrix", &inputs.output_stub, opt.weight_fun.stub());
            let buf = create_buf_with_command_and_version::<&Path>(name.as_ref());
            weight_cor_name = Some(name);
            buf
        }
    );

    let mut paper_weighted_cor_name = None;
    let mut buf_paper_weighted_cor = has_weights.then(
        ||
        {
            let name = format!("{}_PaperWeighted.matrix", &inputs.output_stub);
            let buf = create_buf_with_command_and_version::<&Path>(name.as_ref());
            paper_weighted_cor_name = Some(name);
            buf
        }
    );

    let average_variance_c_name = format!("{}.av_var", &inputs.output_stub);
    let mut buf_av_var_c = create_buf_with_command_and_version(average_variance_c_name);

    let mut stats_header = vec![
        "Average",
        "Variance",
        "STD_DEV",
        "CV",
        "Median",
        "Min",
        "Max",
        "Country_ID"
    ];
    if country_name_map.is_some(){
        stats_header.push("Country_name");
    }
    write_slice_head(&mut buf_av_var_c, &stats_header).unwrap();

    all_countries
        .iter()
        .for_each(
            |country|
            {
                let iter = all_infos
                    .iter()
                    .filter_map(|v| v.get(country))
                    .filter(|v| v.is_finite())
                    .copied();
                let stats: Stats = iter.collect();
                write!(
                    buf_av_var_c, 
                    "{:e} {:e} {:e} {:e} {:e} {:e} {:e} {}",
                    stats.average,
                    stats.variance,
                    stats.get_std_dev(),
                    stats.get_cv(),
                    stats.median,
                    stats.min,
                    stats.max,
                    country
                ).unwrap();
                if let Some(country_map) = country_name_map.as_ref() {
                    writeln!(
                        buf_av_var_c,
                        " \"{}\"",
                        country_map.get(&country.to_string()).unwrap()
                    )
                } else {
                    writeln!(buf_av_var_c)
                }.unwrap();
            }
        );
    let average_variance_name = format!("{}_goods.av_var", &inputs.output_stub);
    let mut buf_av_var = create_buf_with_command_and_version(average_variance_name);
    let head = [
        "Average",
        "Variance",
        "STD_DEV",
        "CV",
        "Median",
        "Min",
        "Max",
        "Info"
    ];
    write_slice_head(&mut buf_av_var, head).unwrap();
    all_infos
        .iter()
        .zip(inputs.inputs.iter())
        .for_each(
            |(table, input)|
            {
                let iter = table
                    .values()
                    .filter(|v| v.is_finite())
                    .copied();
                let stats: Stats = iter.collect();
                writeln!(
                    buf_av_var, 
                    "{:e} {:e} {:e} {:e} {:e} {:e} {:e} {}",
                    stats.average,
                    stats.variance,
                    stats.get_std_dev(),
                    stats.get_cv(),
                    stats.median,
                    stats.min,
                    stats.max,
                    input.plot_name
                ).unwrap();
            }
        );


    let w_fun = opt.weight_fun.get_fun();

    let try_writeln = |o: &mut Option<BufWriter<File>>|
    {
        if let Some(buf) = o{
            writeln!(buf).unwrap();
        }
    };

    // writing matrix that contains the pearson correlation coefficients
    all_infos
        .iter()
        .enumerate()
        .for_each(
            |(index_a, a)|
            {
                // correlations
                all_infos.iter()
                    .enumerate()
                    .for_each(
                        |(index_b, b)|
                        {
                            let pearson = pearson_correlation_coefficient(
                                goods_cor_iter(a, b)
                            );
                            let spear = spearman_correlation_coefficent(
                                goods_cor_iter(a, b)
                            );
                            if let Some(w) = weights.as_deref(){
                                let w_a = &w[index_a];
                                let w_b = &w[index_b];
                                let iter = weighted_goods_cor_iter(a, b, w_a, w_b);
                                let w_pearson = weighted_pearson_correlation_coefficient(iter, w_fun);
                                let buf: &mut std::io::BufWriter<std::fs::File> = buf_weighted_cor.as_mut().unwrap();
                                write!(buf, "{:e} ", w_pearson).unwrap();

                                let iter = weighted_goods_cor_iter(a, b, w_a, w_b);
                                let paper_coef = weighted_paper_correlation_coef(iter);
                                let paper_buf = buf_paper_weighted_cor.as_mut().unwrap();
                                write!(paper_buf, "{:e} ", paper_coef).unwrap();
                            }
                            write!(
                                buf_pearson, 
                                "{} ",
                                pearson
                            ).unwrap();
                            write!(
                                buf_spear,
                                "{} ",
                                spear
                            ).unwrap();
                        }
                    );
                writeln!(buf_pearson).unwrap();
                writeln!(buf_spear).unwrap();
                try_writeln(&mut buf_weighted_cor);
                try_writeln(&mut buf_paper_weighted_cor);
            }
        );

    let labels: Vec<_> = inputs.inputs
        .iter()
        .map(|c| c.plot_name.clone())
        .collect();

    let label_name = format!("{}.labels", inputs.output_stub);
    let mut label_buf = create_buf_with_command_and_version(label_name);
    labels.iter().for_each(|l| writeln!(label_buf, "{l}").unwrap());
    drop(label_buf);

    let mut axis = GnuplotAxis::from_labels(labels);
    let y_axis = axis.clone();
    axis.set_rotation(45.0);
    let terminal = GnuplotTerminal::PDF(inputs.output_stub.clone());
    
    let mut settings = GnuplotSettings::default();
    settings.x_axis(axis)
        .y_axis(y_axis)
        .terminal(terminal)
        .title(PEARSON_TITLE)
        .size("5in,5in");


    let gp_name = format!("{}.gp", inputs.output_stub);
    let writer = create_buf_with_command_and_version(gp_name);
    let good_len = inputs.inputs.len();
    settings.write_heatmap_external_matrix(
        writer, 
        good_len, 
        good_len, 
        matrix_name
    ).unwrap();
    // now spear
    let spear_gp_stub = format!("{}_spear", inputs.output_stub);
    let mut spear_gp = PathBuf::from(&spear_gp_stub);
    spear_gp.set_extension("gp");
    let spear_gp_writer = create_buf_with_command_and_version(spear_gp);
    let spear_terminal = GnuplotTerminal::PDF(spear_gp_stub);
    settings.terminal(spear_terminal)
        .title(SPEARMAN_TITLE)
        .write_heatmap_external_matrix(
            spear_gp_writer, 
            good_len, 
            good_len, 
            matrix_name_spear
        ).unwrap();

    // now write the weighted heatmap if applicable
    if let Some(weighted_matrix_name) = weight_cor_name{
        let gp_stub = format!("{}_{}Weighted", inputs.output_stub, opt.weight_fun.stub());
        let gp_name = format!("{gp_stub}.gp");
        let writer = create_buf_with_command_and_version(gp_name);
        let terminal = GnuplotTerminal::PDF(gp_stub);
        settings.terminal(terminal)
            .title(WEIGHTED_PEARSON_TITLE)
            .write_heatmap_external_matrix(
                writer, 
                good_len, 
                good_len, 
                weighted_matrix_name
            ).unwrap();
    }
    // and the other weighted heatmap if applicable
    if let Some(paper_weighted_matrix_name) = paper_weighted_cor_name
    {
        let gp_stub = format!("{}_PaperWeighted", inputs.output_stub);
        let gp_name = format!("{gp_stub}.gp");
        let writer = create_buf_with_command_and_version(gp_name);
        let terminal = GnuplotTerminal::PDF(gp_stub);
        settings.terminal(terminal)
            .title(WEIGHTED_SAMPLE_TITLE)
            .write_heatmap_external_matrix(
                writer, 
                good_len, 
                good_len, 
                paper_weighted_matrix_name
            ).unwrap();
    }

    // Now I need to calculate the other correlations.
    let country_matrix_name = format!("{}_country.matrix", inputs.output_stub);
    let mut buf_pearson = create_buf_with_command_and_version(&country_matrix_name);
    let country_spear_matrix_name = format!("{}_country_spear.matrix", inputs.output_stub);
    let mut buf_spear = create_buf_with_command_and_version(&country_spear_matrix_name);

    let mut country_weighted_pearson_matrix_name = None;
    let mut country_weighted_paper_matrix_name = None;

    let mut country_weighted_pearson_buf = has_weights.then(
        ||
        {
            let name = format!("{}_{}Weighted_country.matrix", &inputs.output_stub, opt.weight_fun.stub());
            let buf = create_buf_with_command_and_version::<&Path>(name.as_ref());
            country_weighted_pearson_matrix_name = Some(name);
            buf
        }
    );

    let mut country_weighted_paper_matrix_buf = has_weights.then(
        ||
        {
            let name = format!("{}_PaperWeighted_country.matrix", &inputs.output_stub);
            let buf = create_buf_with_command_and_version::<&Path>(name.as_ref());
            country_weighted_paper_matrix_name = Some(name);
            buf
        }
    );


    let old_country_len = all_countries.len();
    all_countries
        .retain(
            |country|
            {
                let mut count = 0;
                let any = all_infos
                    .iter()
                    .filter_map(|set| set.get(country))
                    .filter(|val| val.is_finite())
                    .inspect(|_| count += 1)
                    .tuple_windows()
                    .any(|(a,b)| a.ne(b));
                if country_name_map.is_some() && !any {
                    let country = country_name_map.as_ref().unwrap().get(&country.to_string()).unwrap();
                    println!("Country: {country} {count}");
                }
                any
            }
        );

    println!("REMOVED {} countries", old_country_len - all_countries.len());

    let mut nan_counter = 0;
    // NOTE: So far I do not ignore countries that trade not enough goods
    let mut counter = 0;
    let mut in_common = 0;
    all_countries
        .iter()
        .for_each(
            |this|
            {
                all_countries
                    .iter()
                    .for_each(
                        |other|
                        {
                            let all = all_infos
                                .iter()
                                .filter_map(
                                    |map|
                                    {
                                        map.get(this)
                                            .zip(map.get(other))
                                            .filter(|(t,o)| t.is_finite() && o.is_finite())
                                    }
                                )
                                .collect_vec();
                            in_common += all.len();
                            counter += 1;
                            let pearson = pearson_correlation_coefficient(all.iter().copied());
                            let spear = spearman_correlation_coefficent(all);
                            if let Some(weight_slice) = weights.as_deref(){
                                let samples = all_infos
                                    .iter()
                                    .zip(weight_slice)
                                    .filter_map(
                                        |(all_info_map, weight_map)|
                                        {
                                            // currently "zip_with" is nightly only, otherwise I could use that here
                                            match all_info_map.get(this){
                                                Some(t) if t.is_finite() => {
                                                    match all_info_map.get(other)
                                                    {
                                                        Some(o) if o.is_finite() => {
                                                            let weight_this = weight_map.map.get(this).unwrap();
                                                            let weight_other = weight_map.map.get(other).unwrap();
                                                            let c_this = CorrelationItem{val: *t, weight: weight_this};
                                                            let c_other = CorrelationItem{val: *o, weight: weight_other};
                                                            Some((c_this, c_other))
                                                        },
                                                        _ => None
                                                    }
                                                },
                                                _ => None,
                                            }
                                        }
                                    ).collect_vec();
                                let pearson_weighted = weighted_pearson_correlation_coefficient(
                                    samples.iter().copied(), 
                                    opt.weight_fun.get_fun()
                                );
                                let w_pearson_buf = country_weighted_pearson_buf.as_mut().unwrap();
                                write!(w_pearson_buf, "{pearson_weighted:e} ").unwrap();
                                let paper_weighted = weighted_paper_correlation_coef(
                                    samples
                                );
                                let w_paper_buf = country_weighted_paper_matrix_buf.as_mut().unwrap();
                                write!(w_paper_buf, "{paper_weighted:e} ").unwrap();
                            }
                            if pearson.is_nan(){
                                println!("NaN for: THIS {this} other {other}:");
                                nan_counter += 1;
                            }
                            write!(
                                buf_pearson, 
                                "{} ",
                                pearson
                            ).unwrap();
                            write!(
                                buf_spear,
                                "{} ",
                                spear
                            ).unwrap();
                        }
                    );
                    writeln!(buf_pearson).unwrap();
                    writeln!(buf_spear).unwrap();
                    try_writeln(&mut country_weighted_pearson_buf);
                    try_writeln(&mut country_weighted_paper_matrix_buf);
            }
        );
    let percentage = in_common as f64 / counter as f64;
    println!("In common: {percentage}");
    println!("NaN counter {nan_counter}");

    let names = all_countries
        .iter()
        .map(|v| v.to_string())
        .collect();
    let mut x_axis = GnuplotAxis::from_labels(names);
    let y_axis = x_axis.clone();
    x_axis.set_rotation(65.1);
    let output_stub = format!("{}_country", inputs.output_stub);
    let gp_name = format!("{output_stub}.gp");
    let label_name = format!("{output_stub}.labels");
    let terminal = GnuplotTerminal::PDF(output_stub);

    settings
        .title(PEARSON_TITLE)
        .x_axis(x_axis)
        .y_axis(y_axis)
        .terminal(terminal)
        .size("35in,30in")
        .palette(sampling::GnuplotPalette::PresetRGB)
        .x_label("Country ID")
        .y_label("Country ID");

    let buf = create_buf_with_command_and_version(gp_name);
    let c_len = all_countries.len();
    settings.write_heatmap_external_matrix(
        buf, 
        c_len, 
        c_len, 
        country_matrix_name
    ).unwrap();

    let output_stub = format!("{}_country_spear", inputs.output_stub);
    let gp_name = format!("{output_stub}_spear.gp");
    let terminal = GnuplotTerminal::PDF(output_stub);
    settings.terminal(terminal)
        .title(SPEARMAN_TITLE);
    let buf = create_buf_with_command_and_version(gp_name);
    settings.write_heatmap_external_matrix(
        buf, 
        c_len, 
        c_len, 
        country_spear_matrix_name
    ).unwrap();
    
    let mut writer = create_buf_with_command_and_version(label_name);

    all_countries.iter()
        .for_each(
            |c|
            {
                match &country_name_map{
                    None => writeln!(writer, "{c}"),
                    Some(m) => {
                        let name = m.get(&c.to_string())
                            .unwrap();
                        writeln!(writer, "{name}")
                    }
                }.unwrap();
            }
        );

    // now the weighted stuff
    if let Some(name) = country_weighted_pearson_matrix_name{
        let output_stub = format!("{}_{}_WeightedPearson_country", inputs.output_stub, opt.weight_fun.stub());
        let mut gp_path = PathBuf::from(&output_stub);
        gp_path.set_extension("gp");
        let buf = create_buf_with_command_and_version(gp_path);
        let terminal = GnuplotTerminal::PDF(output_stub);
        settings.terminal(terminal)
            .title(WEIGHTED_PEARSON_TITLE)
            .write_heatmap_external_matrix(
                buf, 
                c_len, 
                c_len, 
                name
            ).unwrap();
    }
    if let Some(name) = country_weighted_paper_matrix_name{
        let output_stub = format!("{}_PaperWeighted_country", inputs.output_stub);
        let mut gp_path = PathBuf::from(&output_stub);
        gp_path.set_extension("gp");
        let buf = create_buf_with_command_and_version(gp_path);
        let terminal = GnuplotTerminal::PDF(output_stub);
        settings.terminal(terminal)
            .title(WEIGHTED_SAMPLE_TITLE)
            .write_heatmap_external_matrix(
                buf, 
                c_len, 
                c_len, 
                name
            ).unwrap();
    }
}