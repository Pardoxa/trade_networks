use std::fmt::Display;
use std::io::{Write, BufWriter, BufReader, BufRead, stdin};
use std::fs::File;
use std::path::Path;
use indicatif::{ProgressBar, ProgressStyle};
use serde::{Serialize, de::DeserializeOwned};

pub const VERSION: &str = env!("CARGO_PKG_VERSION");
pub const AVAILABILITY_ERR: &str = "You forgot to call self.assure_availabiliy()";
pub const YEAR_ERR: &str = "Year not found";

pub fn write_commands_and_version<W: Write>(mut w: W) -> std::io::Result<()>
{
    writeln!(w, "# {VERSION}")?;
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
    let file = File::create(path)
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

pub fn create_buf_with_command_and_version<P>(path: P) -> BufWriter<File>
where P: AsRef<Path>
{
    let mut buf = create_buf(path);
    write_commands_and_version(&mut buf)
        .expect("Unable to write Version and Command in newly created file");
    buf
}

pub fn write_slice_head<W, S, D>(mut w: W, slice: S) -> std::io::Result<()>
where W: std::io::Write,
    S: IntoIterator<Item=D>,
    D: Display
{
    write!(w, "#")?;
    for (s, i) in slice.into_iter().zip(1_u16..){
        write!(w, " {s}_{i}")?;
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