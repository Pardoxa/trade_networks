use {
    crate::misc::{create_buf_with_command_and_version_and_header, open_as_unwrapped_lines_filter_comments}, 
    camino::Utf8PathBuf, 
    clap::{Parser, ValueEnum}, 
    std::{
        collections::*,
        io::Write
    }
};

#[derive(ValueEnum, Debug, Clone, Copy, PartialEq, Eq)]
pub enum X{
    Percent,
    Count
}

/// Compare two groups obtained with trade_networks multi-shocks --group-files
#[derive(Parser, Debug)]
pub struct GroupCompMultiOpts{
    /// Path to group 1
    pub groups_a: Utf8PathBuf,

    /// Path to group 2
    pub groups_b: Utf8PathBuf,

    /// output
    pub output: Utf8PathBuf,

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

fn read_set_infos(path: &Utf8PathBuf) -> Vec<SetInfo>
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
            next_set.insert(
                line.parse().unwrap()
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
    let set_info_a = read_set_infos(&opt.groups_a);
    let set_info_b = read_set_infos(&opt.groups_b);

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