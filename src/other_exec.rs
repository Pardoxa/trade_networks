use {
    crate::parser::{country_map, line_to_vec, LineIter},
    super::{
        config::*,
        misc::*
    },
    std::{
        collections::*,
        io::{
            BufRead,
            Write,
            stdout
        },
        borrow::Borrow,
        path::Path
    },
    serde::{Serialize, Deserialize},
    sampling::{GnuplotTerminal, GnuplotSettings, GnuplotAxis},
    itertools::Itertools
};

pub fn worst_integral_sorting(opt: WorstIntegralCombineOpts)
{
    println!("Sorting");
    assert!(opt.filenames.len() >= 2, "Nothing to sort, specify more files!");
    let mut sorting = HashMap::new();
    let buf = open_bufreader(&opt.filenames[0]);
    let lines = buf.lines()
        .map(|l| l.unwrap())
        .filter(|l| !l.starts_with('#'));

    let mut order_counter = 0_u32;
    for line in lines {
        let this_id = line.split_whitespace().next().unwrap();
        sorting.insert(this_id.to_owned(), order_counter);
        order_counter += 1;
    }

    for other_name in opt.filenames[1..].iter(){
        let filename = other_name.split('/').last().unwrap();
        let new_name = format!("{filename}.sorted");
        println!("CREATING {new_name}");
        let mut buf = create_buf_with_command_and_version(new_name);
        let lines = open_as_unwrapped_lines(other_name);

        let mut for_sorting = Vec::new();
        for line in lines{
            if line.starts_with('#'){
                writeln!(buf, "{line}").unwrap();
            } else{
                let country = line.split_whitespace().next().unwrap();
                if let Some(order) = sorting.get(country){
                    for_sorting.push((*order, line));
                } else {
                    sorting.insert(country.to_owned(), order_counter);
                    for_sorting.push((order_counter, line));
                    order_counter += 1;
                }
            }
        }

        for_sorting.sort_unstable_by_key(|a| a.0);

        let mut written_counter = 0;
        for (order, line) in for_sorting{
            if written_counter == order{
                writeln!(buf, "{line}").unwrap();
                written_counter += 1;
            }
            else{
                let missing = order.checked_sub(written_counter).unwrap();
                for _ in 0..missing{
                    writeln!(buf, "NaN NaN NaN NaN NaN").unwrap();
                }
                written_counter += missing;
                writeln!(buf, "{line}").unwrap();
                written_counter += 1;
            }
            
        }

    }

}

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
    let std_x = variance_x.sqrt();
    let std_y = variance_y.sqrt();

    covariance / (std_x * std_y)
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
                large.get(key)
                    .map(|other_val| (*value, *other_val))
            }
        ).filter(|(a, b)| a.is_finite() && b.is_finite())
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CorrelationInput{
    pub path: String,
    pub plot_name: String
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
            plot_name: "Corresponding Name".to_string()
        };
        Self { 
            inputs: vec![example],
            output_stub: "example".to_owned()
        }
    }
}


pub fn correlations(opt: CorrelationOpts)
{
    let country_name_map = opt.country_name_file
        .map(crate::parser::country_map);
    let inputs: CorrelationMeasurement = read_or_create(opt.measurement);

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
                        " {}",
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



    // writing matrix that contains the pearson correlation coeffizients
    all_infos
        .iter()
        .for_each(
            |a|
            {
                // correlations
                all_infos.iter()
                    .for_each(
                        |b|
                        {
                            let pearson = pearson_correlation_coefficient(
                                goods_cor_iter(a, b)
                            );
                            write!(
                                buf_pearson, 
                                "{} ",
                                pearson
                            ).unwrap();
                        }
                    );
                writeln!(buf_pearson).unwrap();
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
        .title("Pearson Correlation Coefficients")
        .size("5in,5in");


    let gp_name = format!("{}.gp", inputs.output_stub);
    let writer = create_buf_with_command_and_version(gp_name);
    settings.write_heatmap_external_matrix(
        writer, 
        inputs.inputs.len(), 
        inputs.inputs.len(), 
        matrix_name
    ).unwrap();

    // Now I need to calculate the other correlations.
    let country_matrix_name = format!("{}_country.matrix", inputs.output_stub);
    let mut buf_pearson = create_buf_with_command_and_version(&country_matrix_name);
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
                            let iter = all_infos
                                .iter()
                                .filter_map(
                                    |map|
                                    {
                                        match map.get(this)
                                        {
                                            Some(t) if t.is_finite() => 
                                            {
                                                match map.get(other){
                                                    Some(o) if o.is_finite() => {
                                                        Some((t, o))
                                                    },
                                                    _ => None
                                                }
                                            },
                                            _ => None
                                        }
                                    }
                                ).inspect(|_| in_common += 1);
                            counter += 1;

                            let pearson = pearson_correlation_coefficient(iter);
                            if pearson.is_nan(){
                                println!("NaN for: THIS {this} other {other}:");
                                nan_counter += 1;
                            }
                            write!(
                                buf_pearson, 
                                "{} ",
                                pearson
                            ).unwrap();
                        }
                    );
                    writeln!(buf_pearson).unwrap();
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
        .x_axis(x_axis)
        .y_axis(y_axis)
        .terminal(terminal)
        .size("35in,30in")
        .palette(sampling::GnuplotPalette::PresetRGB)
        .x_label("Country ID")
        .y_label("Country ID");

    let buf = create_buf_with_command_and_version(gp_name);
    settings.write_heatmap_external_matrix(
        buf, 
        all_countries.len(), 
        all_countries.len(), 
        country_matrix_name
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
        )

}

pub fn filter_files(opt: FilterOpts)
{
    match opt.out.as_deref(){
        Some(out_path) => {
            let buf = opt.comments.get_create_buf_fun()(out_path);
            filter_files_helper(opt, buf)
        },
        None => {
            let mut writer = stdout();
            if !opt.comments.is_none(){
                write_commands_and_version(&mut writer).unwrap();
            }
            filter_files_helper(opt, writer)
        }
    }
}

fn filter_files_helper<W>(opt: FilterOpts, mut writer: W)
where W: Write
{
    let csv = opt.filter_by
        .extension()
        .is_some_and(|ext| ext == "csv");
    let filter_set: HashSet<String> = open_as_unwrapped_lines(opt.filter_by)
        .filter(|line| !line.starts_with('#'))
        .map(
            |line|
            {
                if csv {
                    let mut v = line_to_vec(&line);
                    v.swap_remove(opt.filter_by_col)
                } else {
                    line.split_whitespace()
                        .nth(opt.filter_by_col)
                        .expect("filter_by column not found")
                        .to_owned()
                }
            }
        ).collect();

    let iter = open_as_unwrapped_lines(opt.other_file);
    for line in iter {
        if line.starts_with('#')
        {
            if opt.comments.is_keep(){
                writeln!(writer, "{line}").unwrap();
            }
            continue;
        }
        let col_of_interest = line
            .split_whitespace()
            .nth(opt.other_col)
            .expect("Other column not found");
        
        let mut proceed = filter_set.contains(col_of_interest);
        if opt.filter_opt.is_remove() {
            proceed = !proceed;
        }
        if proceed {
            writeln!(writer, "{line}").unwrap();
        }
    }
}

pub fn partition<P>(mut opt: PartitionOpts, in_file: P)
where P: AsRef<Path>
{
    let path = in_file.as_ref();
    let comments = (!opt.remove_comments)
        .then(
            ||
            {
                open_as_unwrapped_lines(path)
                    .filter(|s| s.starts_with('#'))
                    .collect_vec()
            }
        );

    let iter = open_as_unwrapped_lines(in_file)
            .filter(|s| !s.starts_with('#'))
            .map(
                |s|
                {
                    let val_str = s.split_whitespace()
                        .nth(opt.col_index)
                        .expect("Column not long enough");
                    let val: f64 = val_str
                        .parse()
                        .expect("Parsing error");
                    (val, s)
                }
            );
    let order_fun = opt.order_direction.get_order_fun();
    opt.partition.sort_unstable_by(|&a, &b| order_fun(a, b));
    
    if opt.sort{
        let mut all = iter
            .collect_vec();
        
        all.sort_by(|a, b| order_fun(a.0, b.0));
        partition_helper(&opt.output_stub, all, opt.partition, opt.order_direction, comments);
    } else {
        partition_helper(&opt.output_stub, iter, opt.partition, opt.order_direction, comments);
    }
}

fn partition_helper<I, I2>(stub: &str, iter: I, partition: I2, order_helper: OrderHelper, comments: Option<Vec<String>>)
where I: IntoIterator<Item=(f64, String)>,
    I2: IntoIterator<Item=f64>
{
    
    let mut par_iter = partition.into_iter();
    let mut next = par_iter.next();
    let mut counter = 0_u32;
    let new_buf = |counter: u32, partition_border| {
        let name = format!("{counter}_{stub}");
        let mut buf = create_buf_with_command_and_version(name);
        writeln!(buf, "# HOW? {order_helper:?} Next Partition at: {partition_border:?}").unwrap();
        if let Some(comments) = comments.as_deref()
        {
            comments.iter()
                .for_each(|comment| writeln!(buf, "{comment}").unwrap())
        }
        buf
    };
    let mut buf = new_buf(counter, next);
    
    let cmp_fun = order_helper.get_cmp_fun();
    for (val, line) in iter {
        match next {
            Some(v) if !cmp_fun(val, v) => {
                loop{
                    next = par_iter.next();
                    counter += 1;
                    match next {
                         Some(v) if !cmp_fun(val, v) =>  {
                            continue;
                        },
                        _ => {
                            break;
                        }
                    }
                }
                buf = new_buf(counter, next);
            },
            _ => {
                
            }
        }
        writeln!(buf, "{line}").unwrap();
    }
}




pub fn beef_map_to_id<P>(in_file: P, opt: BeefMap)
where P: AsRef<Path>
{
    let country_map = country_map(opt.country_file);
    // need to reverse the map
    let country_map: HashMap<String, String> = country_map.into_iter()
        .map(|(a, b)| (b, a))
        .collect();

    let mut buf = create_buf_with_command_and_version(opt.out_file);
    let mut line_iter = open_as_unwrapped_lines(in_file);

    let header_line = line_iter.next().unwrap();
    writeln!(buf, "#{header_line}").unwrap();
    let mut unknown_exporter = HashSet::new();
    let mut unknown_importer = HashSet::new();
    for line in line_iter{
        let mut iter = LineIter::new(&line);
        let exporter = iter.next().unwrap();
        let exporter_id = match country_map.get(exporter){
            Some(c) => c,
            None => {
                unknown_exporter.insert(exporter.to_owned());
                continue;
            }
        };
        let importer = iter.next().unwrap();
        let importer_id = match country_map.get(importer){
            Some(c) => c,
            None => {
                unknown_importer.insert(importer.to_owned());
                continue;
            }
        };

        write!(buf, " {exporter_id},{importer_id},").unwrap();
        let rest: &str = iter.into();
        writeln!(buf, "{rest}").unwrap();
    }
    if !unknown_exporter.is_empty(){
        println!("Unknown exporter! {}", unknown_exporter.len());
        dbg!(unknown_exporter);
    }
    if !unknown_importer.is_empty(){
        println!("Unknown exporter! {}", unknown_importer.len());
        dbg!(unknown_importer);
    }
}