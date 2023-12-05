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
    }
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
        let lines = open_as_lines_unchecked(other_name);

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

pub fn correlations(opt: CorrelationOpts)
{
    let mut all_countries = HashSet::new();

    let all_infos: Vec<HashMap<_, _>> = opt.files
        .iter()
        .map(
            |path|
            {
                open_as_lines_unchecked(path)
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

    let name = "test.cor";
    let mut buf = create_buf_with_command_and_version(name);

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

}