use {
    super::{
        config::*,
        misc::*
    }, crate::parser::{country_map, line_to_vec, LineIter}, camino::*, itertools::Itertools, rayon::prelude::*, sampling::{GnuplotAxis, GnuplotSettings, GnuplotTerminal}, std::{
        collections::*,
        io::{
            stdout, BufRead, Write
        },
        path::{Path, PathBuf},
        process::Command
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
        None => printing_compare(file1, file2, &opt.file1, &opt.file2),
        Some(comment) => {
            let file1 = file1.filter(|line| !line.starts_with(&comment));
            let file2 = file2.filter(|line| !line.starts_with(&comment));
            printing_compare(file1, file2, &opt.file1, &opt.file2)
        }
    }
}

fn printing_compare<I1, I2>(file1: I1, file2:I2, filename1: &str, filename2: &str)
where I1: Iterator<Item=String>,
    I2: Iterator<Item=String>
{
    let set1: BTreeSet<_> = file1.collect();
    let set2: BTreeSet<_> = file2.collect();

    fn x_or_minus(contained: bool) -> char
    {
        match contained{
            true => 'x',
            false => '-'
        }
    }

    println!("#{filename1} {filename2}");
    let mut counter_both = 0_u64;
    let mut counter_total = 0_u64;
    for item in set1.union(&set2)
    {
        counter_total += 1;
        let first_b = set1.contains(item);
        let second_b = set2.contains(item);
        let first_c = x_or_minus(first_b);
        let second_c = x_or_minus(second_b);
        if first_b && second_b{
            counter_both += 1;
        }
        println!("{first_c} {second_c} {item}");
    }
    let fraction = counter_both as f64 / counter_total as f64;
    println!("#{counter_both} of {counter_total} are equal");
    println!("#Fraction: {fraction}");
}

fn read_sets(file: &Path) -> Vec<BTreeSet<String>>
{
    let mut set_vec = vec![BTreeSet::new()];

    open_as_unwrapped_lines(file)
        .for_each(
            |line|
            {
                if line.is_empty() || line.starts_with('#'){
                    if !set_vec.last().unwrap().is_empty(){
                        set_vec.push(BTreeSet::new());
                    }
                } else {
                    set_vec.last_mut().unwrap().insert(line);
                }

            }
        );
    if set_vec.last().unwrap().is_empty(){
        set_vec.pop();
    }
    set_vec
}

pub struct SetComp
{
    min: usize,
    total_elements: usize,
    in_both: usize
}

impl SetComp{
    fn compare_sets<A>(a: &BTreeSet<A>, b: &BTreeSet<A>) -> Self
    where A: Ord
    {
        let min = a.len().min(b.len());
        let total_elements = a.union(b).count();
        let in_both = a.intersection(b).count();
        Self { 
            min, 
            total_elements, 
            in_both 
        }
    }
}

pub struct Overlap{
    pub max_in_both: usize,
    pub size_self: usize
}

pub struct GnuplotGroupSizeTuple{
    pub file1: String,
    pub file2: String,
    pub file1_overlap: Vec<Overlap>
}

pub fn compare_groups(mut opt: GroupCompOpts) -> Option<GnuplotGroupSizeTuple>
{
    let mut gp_gs_tuple = None;
    let mut a_sets = read_sets(opt.groups_a.as_ref());
    let mut b_sets = read_sets(opt.groups_b.as_ref());

    let mut gp_names = Vec::new();
    if let Some(threshold) = opt.remove_smaller{
        let additon = format!("_th{threshold}");
        opt.output_stub.push_str(&additon);
    }
    if opt.common_only{
        opt.output_stub.push_str("_common_only");
        let all_a: BTreeSet<_> = a_sets.iter().flat_map(|s| s.iter()).collect();
        let all_b: BTreeSet<_> = b_sets.iter().flat_map(|s| s.iter()).collect();
        let in_both: BTreeSet<_> = all_a.intersection(&all_b).map(|&e| e.to_owned()).collect();
        println!("Original in both: {}", in_both.len());
        let ret = |sets: &mut Vec<BTreeSet<String>>|
        {
            sets.iter_mut()
                .for_each(
                    |set|
                    {
                        set.retain(|e| in_both.contains(e));
                    }
                );
            sets.retain(|s| !s.is_empty());
        };
        ret(&mut a_sets);
        ret(&mut b_sets);
    }

    if let Some(threshold) = opt.remove_smaller{
        a_sets.retain(|list| list.len() >= threshold.get());
        b_sets.retain(|list| list.len() >= threshold.get());
    }

    let total_a = a_sets.iter().map(|entry| entry.len()).sum::<usize>();
    let total_b = b_sets.iter().map(|entry| entry.len()).sum::<usize>();
    println!("total a: {total_a}");
    println!("total b: {total_b}");
    let all_a: BTreeSet<_> = a_sets.iter().flat_map(|s| s.iter()).collect();
    let all_b: BTreeSet<_> = b_sets.iter().flat_map(|s| s.iter()).collect();
    let comp = SetComp::compare_sets(&all_a, &all_b);
    println!("in both: {}", comp.in_both);
    println!("overall total: {}", comp.total_elements);

    a_sets.sort_by_key(|entry| entry.len());
    b_sets.sort_by_key(|entry| entry.len());



    if opt.output_group_size{
        let overlap_a = a_sets.iter()
            .map(
                |a|
                {
                    let mut max = 0;
                    for b in b_sets.iter()
                    {
                        let count = a.intersection(b).count();
                        if count > max {
                            max = count;
                        }
                    }
                    Overlap{
                        max_in_both: max,
                        size_self: a.len()
                    }
                }
            ).collect_vec();
        let mut data_names = Vec::new();
        let mut out = |slice: &[BTreeSet<String>], name_addition: char|
        {
            let name = format!("{}_{}.group_size", opt.output_stub, name_addition);
            let mut buf = create_buf_with_command_and_version(&name);
            data_names.push(name);
            for e in slice{
                writeln!(buf, "{}", e.len()).unwrap();
            }
        };
        out(&a_sets, 'a');
        out(&b_sets, 'b');

        let gp_name = format!("{}_group_size.gp", opt.output_stub);
        let pdf_name = format!("{}_group_size.pdf", opt.output_stub);
        let mut buf = create_gnuplot_buf(&gp_name);
        gp_names.push(gp_name);
        writeln!(buf, "set t pdf\nset output '{pdf_name}'").unwrap();
        write!(buf, "p ").unwrap();
        for name in data_names.iter(){
            write!(buf, "'{}' u 0:1 w lp, ", name).unwrap();
        }
        let file2 = data_names.pop().unwrap();
        let file1 = data_names.pop().unwrap();
        gp_gs_tuple = Some(
            GnuplotGroupSizeTuple{
                file1,
                file2,
                file1_overlap: overlap_a
            }
        );
        writeln!(buf).unwrap();
        writeln!(buf, "set output").unwrap();
    }

    let relative_name_stub = format!("{}_relative", opt.output_stub);
    let relative_name = format!("{}.matrix", relative_name_stub);
    let relative_gp_name = format!("{}.gp", relative_name_stub);

    let min_name_stub = format!("{}_min", opt.output_stub);
    let min_name = format!("{}.matrix", min_name_stub);
    let min_gp_name = format!("{}.gp", min_name_stub);

    let total_name_stub = format!("{}_total", opt.output_stub);
    let total_name = format!("{}.matrix", total_name_stub);
    let total_gp_name = format!("{}.gp", total_name_stub);
    println!("Creating {relative_name}");
    println!("Creating {total_name}");
    println!("Creating {min_name}");

    let mut writer_relative = create_buf_with_command_and_version::<&Path>(relative_name.as_ref());
    let mut writer_total = create_buf_with_command_and_version::<&Path>(total_name.as_ref());
    let mut writer_min = create_buf_with_command_and_version(&min_name);

    

    for a in a_sets.iter(){
        for b in b_sets.iter(){
            let c = SetComp::compare_sets(a, b);
            write!(writer_total, "{} ", c.in_both).unwrap();
            let relative = c.in_both as f64 / c.total_elements as f64;
            write!(writer_relative, "{:e} ", relative).unwrap();

            let min_normed = c.in_both as f64 / c.min as f64;
            write!(writer_min, "{} ", min_normed).unwrap();
        }
        writeln!(writer_total).unwrap();
        writeln!(writer_relative).unwrap();
        writeln!(writer_min).unwrap();
    }
    let size_x = 7.4 * opt.scaling;
    let size_y = 5.0 * opt.scaling;
    let heatmap_size = format!("{size_x}cm, {size_y}cm");
    let mut settings = GnuplotSettings::new();

    let terminal = GnuplotTerminal::PDF(relative_name_stub);

    let b_labels = (0..b_sets.len())
        .map(|num| num.to_string())
        .collect_vec();
    let a_labels = (0..a_sets.len())
        .map(|num| num.to_string())
        .collect_vec();
    
    let x_label = opt.name_b.unwrap_or(opt.groups_b);
    let y_label = opt.name_a.unwrap_or(opt.groups_a);
    settings.terminal(terminal)
        .x_label(x_label)
        .y_label(y_label)
        .x_axis(GnuplotAxis::from_labels(b_labels))
        .y_axis(GnuplotAxis::from_labels(a_labels))
        .cb_range(0.0, 1.0)
        .title("relative")
        .size(heatmap_size);

    let relative_gp_writer = create_gnuplot_buf::<&Path>(relative_gp_name.as_ref());

    let matrix_width = b_sets.len();
    let matrix_height = a_sets.len();

    settings.write_heatmap_external_matrix(
        relative_gp_writer, 
        matrix_width, 
        matrix_height, 
        relative_name
    ).unwrap();

    let terminal = GnuplotTerminal::PDF(min_name_stub);
    settings.terminal(terminal)
        .title("min relative");
    let min_gp_writer = create_gnuplot_buf(&min_gp_name);
    gp_names.push(min_gp_name);
    settings.write_heatmap_external_matrix(
        min_gp_writer, 
        matrix_width, 
        matrix_height, 
        min_name
    ).unwrap();


    let terminal = GnuplotTerminal::PDF(total_name_stub);
    settings.terminal(terminal)
        .title("absolut")
        .remove_cb_range();
    let total_writer = create_gnuplot_buf::<&Path>(total_gp_name.as_ref());

    settings.write_heatmap_external_matrix(
        total_writer, 
        matrix_width, 
        matrix_height, 
        total_name
    ).unwrap();

    drop(writer_relative);
    drop(writer_total);
    drop(writer_min);

    if opt.exec_gnuplot{
        gp_names.push(relative_gp_name);
        gp_names.push(total_gp_name);
        gp_names.into_par_iter()
            .for_each(
                |gp_name|
                {
                    let output = Command::new("gnuplot")
                        .arg(gp_name)
                        .output()
                        .expect("command_failed");
                    if !output.status.success(){
                        dbg!(output);
                    }
                }
            );
    }
    gp_gs_tuple
}


#[derive(Debug)]
struct CommandHelper{
    path: Utf8PathBuf,
    year: u16
}

pub fn command_creator(opt: CompGroupComCreOpt)
{
    let mut all_files: Vec<_> = glob::glob(&opt.glob)
        .unwrap()
        .map(Result::unwrap)
        .map(
            |path| 
            {
                let path = Utf8PathBuf::from_path_buf(path).unwrap();
                let parent = path.parent().unwrap();
                let year = parent.as_str().parse().unwrap();
                CommandHelper{
                    year,
                    path
                }
            }
        )
        .collect();
    
    all_files.sort_unstable_by_key(|item| item.year);

    let r = match opt.restrict{
        Some(r) => {
            format!("-r {r}")
        },
        None =>  String::new()
    };

    all_files.windows(2)
        .for_each(
            |slice|
            {
                let old = &slice[0];
                let new = &slice[1];
                println!(
                    "trade_networks compare-groups {} {} -c -e --name-a {} --name-b {} -o {}_vs_{} {r}",
                    old.path.as_str(),
                    new.path.as_str(),
                    old.year,
                    new.year,
                    old.year,
                    new.year
                )
            }
        );
    if opt.execute{

        if let Some(dir) = &opt.dir{
            std::fs::create_dir_all(dir).expect("unable to create dir");
            std::env::set_current_dir(dir).expect("unable to set dir");
        }

        let group_sizes: Vec<_> = all_files
            .par_windows(2)
            .map(
                |slice|
                {
                    let old = &slice[0];
                    let new = &slice[1];
                    let (new_path, old_path) = if let Some(dir) = &opt.dir{
                        let num = dir.components().count();
                        let up: Utf8PathBuf = "..".into();
                        let mut all_up = up.clone();
                        for _ in 1..num {
                            all_up.push(&up);
                        }
                        let mut old_path = all_up.clone();
                        old_path.push(&old.path);
                        all_up.push(&new.path);
                        (all_up, old_path)
                    } else {
                        (new.path.to_owned(), old.path.to_owned())
                    };
                    let out_name = format!("{}_vs_{}", old.year, new.year);
                    let g_opt = GroupCompOpts{
                        groups_a: old_path.as_str().to_owned(),
                        groups_b: new_path.as_str().to_owned(),
                        output_stub: out_name,
                        name_a: Some(old.year.to_string()),
                        name_b: Some(new.year.to_string()),
                        exec_gnuplot: true,
                        remove_smaller: opt.restrict,
                        output_group_size: true,
                        common_only: true,
                        scaling: 1.0
                    };
                    let gs_tuple = compare_groups(g_opt).unwrap();
                    GnuplotGroupSizeTupleExtra{
                        file1: gs_tuple.file1,
                        file2: gs_tuple.file2,
                        year1: old.year,
                        year2: new.year,
                        overlap: gs_tuple.file1_overlap
                    }
                }
            ).collect();
        let group_gp_name = "all_group_sizes.gp";
        let max_len_overlap = group_sizes.iter().map(|tup| tup.overlap.len()).max().unwrap();
        let mut overlap_writers = (0..max_len_overlap)
            .map(
                |i|
                { 
                    let name = format!("largest_overlap_{i}.dat");
                    let mut overlap_writer = create_buf_with_command_and_version(name);
                    let header = [
                        "year",
                        "TotalOverlap",
                        "SizeOfCorrespondingGroup",
                        "RelativeOverlap"
                    ];
                    write_slice_head(&mut overlap_writer, header).unwrap();
                    overlap_writer
                }
            ).collect_vec();
        
        
        let mut writer = create_gnuplot_buf(group_gp_name);
        let mut lines = vec![
            "set t pdfcairo",
            r#"set output "all_group_sizes.pdf""#,
            "set key left",
        ];
        let max = format!("set key maxrows {}", group_sizes.len());
        lines.push(&max);
        for line in lines {
            writeln!(
                writer,
                "{line}"
            ).unwrap();
        }
        write!(
            writer,
            "p "
        ).unwrap();
        for (index, tuple) in group_sizes.iter().enumerate()
        {
            let pt = index + 1;
            writeln!(
                writer,
                "'{}' w lp lc {index} pt {pt} lw 2 t '{} (vs {})',\\",
                tuple.file1,
                tuple.year1,
                tuple.year2
            ).unwrap();

            let mut write_iter = overlap_writers.iter_mut();
            for (overlap, overlap_writer) in tuple.overlap.iter().rev().zip(&mut write_iter){
                
                let frac = overlap.max_in_both as f64 / overlap.size_self as f64;
                writeln!(
                    overlap_writer,
                    "{} {} {} {frac}",
                    tuple.year1,
                    overlap.max_in_both,
                    overlap.size_self
                ).unwrap();
            }
            for writer in write_iter{
                writeln!(writer, "{} NaN NaN NaN", tuple.year1).unwrap();
            }
            
        }
        for (index, tuple) in group_sizes.iter().enumerate()
        {
            let pt = index + 1;
            writeln!(
                writer,
                "'{}' w lp lc {index} pt {pt} dt (5,5) lw 2 t '{} (vs {})',\\",
                tuple.file2,
                tuple.year2,
                tuple.year1
            ).unwrap();
        }
       
        writeln!(writer).unwrap();
        writeln!(writer, "set output").unwrap();
        drop(writer);
        exec_gnuplot(group_gp_name);
    }
}

struct GnuplotGroupSizeTupleExtra{
    file1: String,
    year1: u16,
    file2: String,
    year2: u16,
    overlap: Vec<Overlap>
}