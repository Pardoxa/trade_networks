use {
    crate::misc::*, clap::{Parser, ValueEnum}, itertools::Itertools, std::{
        collections::*,
        io::Write, path::Path
    }
};

#[derive(ValueEnum, Debug, Clone, Copy, PartialEq, Eq)]
pub enum X{
    Percent,
    Count
}

impl X{
    pub fn str(self) -> &'static str
    {
        match self{
            Self::Percent => "percent",
            Self::Count => "country_count"
        }
    }
}

/// Compare two groups obtained with trade_networks multi-shocks --group-files
#[derive(Parser, Debug)]
pub struct GroupCompMultiOpts{
    /// Path to group 1
    pub groups_a: String,

    /// Path to group 2
    pub groups_b: String,

    /// output
    pub output: String,

    /// What to put on x axis?
    #[arg(value_enum)]
    pub x: X

}

#[derive(Debug)]
pub struct SetInfo
{
    pub set: BTreeSet<i16>,
    pub c_count: i32,
    pub percent: f64
}

fn read_set_infos(path: &Path) -> Vec<SetInfo>
{
    let reader = open_as_unwrapped_lines_filter_comments(path);
    let mut next_set = BTreeSet::new();
    let mut percent = f64::NAN;
    let mut c_count = i32::MIN;
    let mut res = Vec::new();
    let mut valid = false;
    for line in reader {
        if let Some(line) = line.strip_prefix('§')
        {
            if valid{
                let mut set = BTreeSet::new();
                std::mem::swap(&mut set, &mut next_set);
                res.push(
                    SetInfo{
                        c_count,
                        percent,
                        set
                    }
                );
            }
            let mut iter = line.split_ascii_whitespace();
            c_count = iter.next().unwrap().parse().unwrap();
            percent = iter.next().unwrap().parse().unwrap();
            valid = true;
        } else {
            let parsed = match line.parse(){
                Ok(v) => v,
                Err(e) => {
                    dbg!(line);
                    dbg!(e);
                    dbg!(path);
                    panic!()
                }
            };
            next_set.insert(
                parsed
            );
        }
    }
    if !next_set.is_empty(){
        res.push(
            SetInfo{
                c_count,
                percent,
                set: next_set
            }
        );
    }
    res
}

pub fn compare_th_exec(opt: GroupCompMultiOpts)
{
    let set_info_a = read_set_infos(opt.groups_a.as_ref());
    let set_info_b = read_set_infos(opt.groups_b.as_ref());

    assert_eq!(
        set_info_a.len(),
        set_info_b.len(),
        "Size error! Amount of sets in the files does not match!"
    );

    let header = match opt.x{
        X::Percent => {
            [
                "Percent_a",
                "Percent_b",
                "Overlap",
                "Total"
            ]
        },
        X::Count => {
            [
                "Count_a",
                "Count_b",
                "Overlap",
                "Total"
            ]
        }
    };

    let mut buf = create_buf_with_command_and_version_and_header(
        opt.output, 
        header
    );

    for (a, b) in set_info_a.iter().zip(set_info_b.iter())
    {
        match opt.x {
            X::Percent => {
                write!(buf, "{} {}", a.percent, b.percent)
            },
            X::Count => {
                write!(buf, "{} {}", a.c_count, b.c_count)
            }
        }.unwrap();

        let overlap = a.set.intersection(&b.set).count();
        let total = a.set.union(&b.set).count();
        writeln!(buf, " {overlap} {total}").unwrap();
    }
}

pub fn compare_multiple(
    paths: &[&Path],
    x: X,
    name: &Path
)
{
    // Outer vector is vector over years
    // Inner vector is vector over different disruption levels
    let set_infos = paths
        .iter()
        .copied()
        .map(read_set_infos)
        .collect_vec();

    // check that all years have the same number of disruption levels
    let equal_len = set_infos.iter()
        .tuple_windows()
        .all(|(a,b)| a.len() == b.len());

    assert!(
        equal_len,
        "You are trying to compare vectors of sets - but the vectors are of different lengths"
    );

    // Since we just asserted that all Vectors have the same len, we might as well read the first one
    let len = set_infos[0].len();

    let mut header = vec![x.str().to_owned(), "1hit".to_owned()];
    header.extend(
        (2..=set_infos.len())
            .map(|i| format!("{i}hits"))
    );

    let mut writer = create_buf_with_command_and_version_and_header(name, header);


    // iterate over number of disruption levels
    for i in 0..len
    {
        let mut map = BTreeMap::new();
        // iterate over all years
        for set_info in set_infos.iter()
        { 
            // get set of current disruption level
            let set = &set_info[i].set;
            // iterate over the set and increment counter for the country ids of severely affected countries
            for &val in set.iter(){
                map.entry(val)
                    .and_modify(|count| *count += 1)
                    .or_insert(1_u16);
            }
        }
        match x {
            X::Percent => {
                let percent = set_infos[0][i].percent;
                write!(writer, "{percent}").unwrap();
            },
            X::Count => {
                let percent = set_infos[0][i].c_count;
                write!(writer, "{percent}").unwrap();
            }
        }
        
        let mut counts = vec![0_u16; set_infos.len()];
        for count in map.into_values()
        {
            counts[count as usize - 1] += 1;
        }
        for c in counts{
            write!(writer, " {c}").unwrap();
        }
        writeln!(writer).unwrap();
    }
}