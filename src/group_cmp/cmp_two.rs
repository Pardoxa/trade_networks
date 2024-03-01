use {
    crate::{
        config::*,
        misc::*
    }, 
    camino::*, 
    itertools::Itertools, 
    rayon::prelude::*, 
    sampling::{GnuplotAxis, GnuplotSettings, GnuplotTerminal}, 
    std::{
        collections::*,
        io::Write,
        path::Path,
        process::Command
    }
};

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

        let group_size_matrix_name = "overlap_groupsize_matrix.dat";
        let mut group_size_matrix_writer = create_buf_with_command_and_version(group_size_matrix_name);

        let relative_overlap_matrix_name = "relative_overlap_matrix.dat";
        let mut relative_overlap_matrix_writer = create_buf_with_command_and_version(relative_overlap_matrix_name); 
        
        let overlap_matrix_name = "overlap_matrix.dat";
        let mut overlap_matrix_writer = create_buf_with_command_and_version(overlap_matrix_name); 
        
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
                write!(
                    overlap_matrix_writer,
                    "{} ",
                    overlap.max_in_both
                ).unwrap();
                write!(
                    relative_overlap_matrix_writer,
                    "{} ",
                    frac
                ).unwrap();
                write!(
                    group_size_matrix_writer,
                    "{} ",
                    overlap.size_self
                ).unwrap();
            }
            for writer in write_iter{
                writeln!(writer, "{} NaN NaN NaN", tuple.year1).unwrap();
                write!(overlap_matrix_writer, "-1 ").unwrap();
                write!(relative_overlap_matrix_writer, "-1 ").unwrap();
                write!(group_size_matrix_writer, "0 ").unwrap();
            }
            writeln!(overlap_matrix_writer).unwrap();
            writeln!(relative_overlap_matrix_writer).unwrap();
            writeln!(group_size_matrix_writer).unwrap();
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
        drop(overlap_matrix_writer);
        drop(relative_overlap_matrix_writer);
        drop(group_size_matrix_writer);

        let mut settings = GnuplotSettings::new();

        let terminal = GnuplotTerminal::PDF("overlap".to_owned());

        let y_labels = group_sizes.iter()
            .map(|tup| tup.year1.to_string())
            .collect_vec();
        let x_labels = (0..overlap_writers.len())
            .map(|i| i.to_string())
            .collect_vec();

        let matrix_width = x_labels.len();
        let matrix_height = y_labels.len();

        let max = group_sizes.iter()
            .flat_map(|tup| tup.overlap.iter().map(|o| o.max_in_both))
            .max()
            .unwrap();
        
        settings.x_label("group rank")
            .y_label("year")
            .x_axis(GnuplotAxis::from_labels(x_labels))
            .y_axis(GnuplotAxis::from_labels(y_labels))
            .terminal(terminal)
            .cb_range(-1.0, max as f64)
            .title("Absolute Overlap");

        let overlap_gp_writer = create_gnuplot_buf("overlap.gp");

        settings.write_heatmap_external_matrix(
            overlap_gp_writer, 
            matrix_width, 
            matrix_height, 
            overlap_matrix_name
        ).unwrap();
        exec_gnuplot("overlap.gp");
        let terminal = GnuplotTerminal::PDF("relative_overlap".to_owned());

        settings.terminal(terminal)
            .cb_range(0.0, 1.0)
            .title("Relative Overlap");
        let overlap_gp_writer = create_gnuplot_buf("relative_overlap.gp");
        settings.write_heatmap_external_matrix(
            overlap_gp_writer, 
            matrix_width, 
            matrix_height, 
            relative_overlap_matrix_name
        ).unwrap();
        exec_gnuplot("relative_overlap.gp");

        let group_size_gp_name = "group_size_heatmap.gp";
        let gp_writer = create_gnuplot_buf(group_size_gp_name);
        let terminal = GnuplotTerminal::PDF("group_size_heatmap".to_owned());
        let max = group_sizes.iter()
            .flat_map(|tup| tup.overlap.iter().map(|item| item.size_self))
            .max()
            .unwrap();
        settings.terminal(terminal)
            .cb_range(-1.0, max as f64)
            .title("Group Size");

        settings.write_heatmap_external_matrix(
            gp_writer, 
            matrix_width, 
            matrix_height, 
            group_size_matrix_name
        ).unwrap();
        exec_gnuplot(group_size_gp_name);
    }
}

struct GnuplotGroupSizeTupleExtra{
    file1: String,
    year1: u16,
    file2: String,
    year2: u16,
    overlap: Vec<Overlap>
}