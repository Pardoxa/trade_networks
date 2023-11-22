use std::io::{Write, BufWriter, BufReader};
use std::fs::File;
use std::path::Path;
use indicatif::{ProgressBar, ProgressStyle};

pub const VERSION: &str = env!("CARGO_PKG_VERSION");

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
    let file = File::open(path)
        .expect("Unable to create file");
    BufReader::new(file)
}

pub fn create_buf_with_command_and_version<P>(path: P) -> BufWriter<File>
where P: AsRef<Path>
{
    let mut buf = create_buf(path);
    write_commands_and_version(&mut buf)
        .expect("Unable to write Version and Command in newly created file");
    buf
}