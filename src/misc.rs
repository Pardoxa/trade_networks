use std::fmt::{Debug, Display};
use std::io::{Write, BufWriter, BufReader, BufRead, stdin};
use std::str::FromStr;
use camino::Utf8PathBuf;
use fs_err::File;
use regex::Regex;
use std::path::Path;
use indicatif::{ProgressBar, ProgressStyle};
use serde::{Serialize, de::DeserializeOwned};
use std::process::{Command, exit};
use serde_json::Value;
use std::sync::RwLock;

pub const VERSION: &str = env!("CARGO_PKG_VERSION");
pub const AVAILABILITY_ERR: &str = "You forgot to call self.assure_availabiliy()";
pub const YEAR_ERR: &str = "Year not found";
pub static GLOBAL_ADDITIONS: RwLock<Option<String>> = RwLock::new(None);

pub fn write_commands_and_version<W: Write>(mut w: W) -> std::io::Result<()>
{
    writeln!(w, "# {VERSION}")?;
    writeln!(w, "# Git Hash: {} Compile-time: {}", env!("GIT_HASH"), env!("BUILD_TIME_CHRONO"))?;
    let l = GLOBAL_ADDITIONS.read().unwrap();
    if let Some(add) = l.as_deref(){
        writeln!(w, "# {add}")?;
    }
    drop(l);
    write!(w, "#")?;
    for arg in std::env::args()
    {
        write!(w, " {arg}")?;
    }
    writeln!(w)
}

#[allow(dead_code)]
pub fn indication_bar(len: u64) -> ProgressBar
{
        // for indication on when it is finished
        let bar = ProgressBar::new(len);
        bar.set_style(ProgressStyle::default_bar()
            .template("{msg} [{elapsed_precise} - {eta_precise}] {wide_bar}")
            .unwrap()
        );
        bar
}

pub fn create_buf<P>(path: P) -> BufWriter<File>
where P: AsRef<Path>
{
    let file = File::create(path.as_ref())
        .expect("Unable to create file");
    BufWriter::new(file)
}

pub fn open_bufreader<P>(path: P) -> BufReader<File>
where P: AsRef<Path>
{
    let p = path.as_ref();
    let file = match File::open(p){
        Err(e) => panic!("Unable to open {p:?} - encountered {e:?}"),
        Ok(file) =>  file
    };
    BufReader::new(file)
}

pub fn open_as_unwrapped_lines<P>(path: P) -> impl Iterator<Item = String>
where P: AsRef<Path>
{
    open_bufreader(path)
        .lines()
        .map(Result::unwrap)
}

pub fn open_as_unwrapped_lines_filter_comments<P>(path: P) -> impl Iterator<Item = String>
where P: AsRef<Path>
{
    open_bufreader(path)
        .lines()
        .map(Result::unwrap)
        .filter(|line| !line.starts_with('#'))
}

pub fn create_buf_with_command_and_version<P>(path: P) -> BufWriter<File>
where P: AsRef<Path>
{
    let mut buf = create_buf(path);
    write_commands_and_version(&mut buf)
        .expect("Unable to write Version and Command in newly created file");
    buf
}

pub fn create_buf_with_command_and_version_and_header<P, S, D>(path: P, header: S) -> BufWriter<File>
where P: AsRef<Path>,
    S: IntoIterator<Item=D>,
    D: Display
{
    let mut buf = create_buf_with_command_and_version(path);
    write_slice_head(&mut buf, header)
        .expect("unable to write header");
    buf
}

pub fn create_gnuplot_buf<P>(path: P) -> BufWriter<File>
where P: AsRef<Path>
{
    let mut buf = create_buf_with_command_and_version(path);
    writeln!(buf, "reset session").unwrap();
    buf
}

pub fn write_slice_head<W, S, D>(mut w: W, slice: S) -> std::io::Result<()>
where W: std::io::Write,
    S: IntoIterator<Item=D>,
    D: Display
{
    write!(w, "#")?;
    for (s, i) in slice.into_iter().zip(1_u16..){
        write!(w, " {s}:col{i}")?;
    }
    writeln!(w)
}

pub fn read_or_create<T, P>(path: P) -> T
where P: AsRef<Path>,
    T: DeserializeOwned + Default + Serialize
{
    let p = path.as_ref();
    match File::open(p)
    {
        Err(e) => {
            eprintln!("While opening master file {p:?} encountered {e:?}");
            let handle = stdin();
            let mut line = String::new();
            let create_file = loop{
                line.clear();
                println!("Do you wish to create the master file? y/n");
                match handle.read_line(&mut line){
                    Err(e) => {
                        panic!("STDIN problem {e:?} - abbort")
                    },
                    Ok(_) => {
                        let l = line.trim_end_matches('\n');
                        match l
                        {
                            "y" | "Y" | "yes" | "Yes" => {
                                break true;
                            },
                            "n" | "N" | "no" | "No" => {
                                break false;
                            },
                            otherwise => {
                                println!("Unrecognized: {otherwise}");
                            }
                        }
                    }
                }
            };
            if create_file{
                let measurement = T::default();
                let buf = create_buf(p);
                serde_json::to_writer_pretty(buf, &measurement).unwrap();
            }
            std::process::exit(0);
        }, 
        Ok(file) => {
            let buf = BufReader::new(file);
            serde_json::from_reader(buf)
                .expect("unable to deserialize")
        }
    }
}

pub fn exec_gnuplot<S>(gp_name: S)
where S: AsRef<std::ffi::OsStr>{
    let output = Command::new("gnuplot")
        .arg(gp_name)
        .output()
        .expect("failed gnuplot");
    if !output.status.success(){
        dbg!(output);
    }
}

pub fn parse_and_add_to_global<P, T>(file: Option<P>) -> T
where P: AsRef<Path>,
    T: Default + Serialize + DeserializeOwned
{
    match file
    {
        None => {
            let example = T::default();
            serde_json::to_writer_pretty(
                std::io::stdout(),
                &example
            ).expect("Unable to reach stdout");
            exit(0)
        }, 
        Some(file) => {
            let f = File::open(file.as_ref())
                .expect("Unable to open file");
            let buf = BufReader::new(f);

            let json_val: Value = match serde_json::from_reader(buf)
            {
                Ok(v) => v,
                Err(e) => {
                    eprintln!("json parsing error!");
                    dbg!(e);
                    exit(1);
                }
            };

            let opt: T = match serde_json::from_value(json_val.clone())
            {
                Ok(o) => o,
                Err(e) => {
                    eprintln!("json parsing error!");
                    dbg!(e);
                    exit(1);
                }
            };
            let s = serde_json::to_string(&opt).unwrap();
            let mut w = GLOBAL_ADDITIONS.write().unwrap();
            *w = Some(s);
            drop(w);

            opt  
        }
    }
}

pub fn utf8_path_iter(globbing: &str) -> impl Iterator<Item = Utf8PathBuf>
{
    glob::glob(globbing)
        .unwrap()
        .map(Result::unwrap)
        .map(|p| Utf8PathBuf::from_path_buf(p).unwrap())
}

pub fn regex_first_match<'a>(re: &Regex, s: &'a str) -> &'a str
{
    match re.find(s){
        None => {
            panic!("Cannot find label in globbed file {s:?}")
        },
        Some(m) =>
        {
            &s[m.start()..m.end()]
        }
    }
}

pub fn regex_first_match_parsed<T>(re: &Regex, s: &str) -> T
where T: FromStr,
 <T as std::str::FromStr>::Err: Debug
{
    regex_first_match(re, s).parse().unwrap()
}