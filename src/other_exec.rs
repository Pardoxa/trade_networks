use {
    super::{
        config::*,
        misc::*
    },
    std::{
        collections::*,
        io::{
            BufRead,
            Write
        }
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

fn pearson_correlation_coefficient<I>(iterator: I) -> f64
where I: IntoIterator<Item = (f64, f64)>
{
    let mut product_sum = 0.0;
    let mut x_sum = 0.0;
    let mut x_sq_sum = 0.0;
    let mut y_sum = 0.0;
    let mut y_sq_sum = 0.0;
    let mut counter = 0_u64;

    for (x, y) in iterator
    {
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
    let mut buf = create_buf_with_command_and_version(matrix_name.as_str());

    // writing matrix that contains the pearson correlation coeffizients
    all_infos
        .iter()
        .for_each(
            |a|
            {
                all_infos.iter()
                    .for_each(
                        |b|
                        {
                            let pearson = pearson_correlation_coefficient(
                                goods_cor_iter(a, b)
                            );
                            write!(buf, "{pearson} ").unwrap();
                        }
                    );
                writeln!(buf).unwrap();
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
    let mut buf = create_buf_with_command_and_version(&country_matrix_name);
    let old_country_len = all_countries.len();
    all_countries
        .retain(
            |country|
            {
                all_infos
                    .iter()
                    .filter_map(|set| set.get(country))
                    .filter(|val| val.is_finite())
                    .tuple_windows()
                    .any(|(a,b)| a.ne(b))
            }
        );
    println!("REMOVED {} countries", old_country_len - all_countries.len());

    let mut nan_counter = 0;
    // NOTE: So far I do not ignore countries that trade not enough goods
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
                                        map.get(this)
                                            .zip(map.get(other))
                                    }
                                )
                                .filter(|(a,b)| a.is_finite() && b.is_finite())
                                .map(|(a,b)| (*a, *b));

                            let pearson = pearson_correlation_coefficient(iter);
                            if pearson.is_nan(){
                                println!("NaN for: THIS {this} other {other}:");
                                nan_counter += 1;
                            }
                            write!(buf, "{pearson} ").unwrap();
                        }
                    );
                    writeln!(buf).unwrap();
            }
        );
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

    let country_name_map = opt.country_name_file
        .map(crate::parser::country_map);

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