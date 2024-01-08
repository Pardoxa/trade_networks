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
        path::{Path, PathBuf},
    },
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


pub fn filter_files(opt: FilterOpts)
{

    if opt.glob{
        let iter = glob::glob(&opt.other_file)
            .expect("globbing_error")
            .map(Result::unwrap);
        let buf_creation = opt.comments.get_create_buf_fun();
        for path in iter {
            let file_name = path.file_name().expect("no file name");
            let mut new_name = PathBuf::from(file_name);
            new_name.set_extension("filtered");
            println!("Reading: {path:?}, creating: {:?}", new_name);
            let buf = buf_creation(new_name);
            filter_files_helper(&opt, buf, path);
        }
    } else {
        let path = opt.other_file.as_str();
        match opt.out.as_deref(){
            Some(out_path) => {
                let buf = opt.comments.get_create_buf_fun()(out_path);
                filter_files_helper(&opt, buf, path)
            },
            None => {
                let mut writer = stdout();
                if !opt.comments.is_none(){
                    write_commands_and_version(&mut writer).unwrap();
                }
                filter_files_helper(&opt, writer, path)
            }
        }
    }
}

fn filter_files_helper<W, P>(opt: &FilterOpts, mut writer: W, work_file: P)
where W: Write,
    P: AsRef<Path>
{
    let csv = opt.filter_by
        .extension()
        .is_some_and(|ext| ext == "csv");
    let filter_set: HashSet<String> = open_as_unwrapped_lines(opt.filter_by.as_path())
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

    let iter = open_as_unwrapped_lines(work_file);
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

        write!(buf, "{exporter_id},{importer_id},").unwrap();
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

pub fn compare_entries(opt: CompareEntriesOpt)
{
    let file1 = open_as_unwrapped_lines(&opt.file1);
    let file2 = open_as_unwrapped_lines(&opt.file2);

    match opt.comment{
        None => compare(file1, file2, &opt.file1, &opt.file2),
        Some(comment) => {
            let file1 = file1.filter(|line| !line.starts_with(&comment));
            let file2 = file2.filter(|line| !line.starts_with(&comment));
            compare(file1, file2, &opt.file1, &opt.file2)
        }
    }
}

fn compare<I1, I2>(file1: I1, file2:I2, filename1: &str, filename2: &str)
where I1: Iterator<Item=String>,
    I2: Iterator<Item=String>
{
    let set1: BTreeSet<_> = file1.collect();
    let set2: BTreeSet<_> = file2.collect();

    let x_or_minus = |item: &str, set: &BTreeSet<String>|
    {
        if set.contains(item)
        {
            "x"
        } else {
            "-"
        }
    };

    println!("#{filename1} {filename2}");
    for item in set1.union(&set2)
    {
        println!("{} {}", x_or_minus(item, &set1), x_or_minus(item, &set2));
    }
}