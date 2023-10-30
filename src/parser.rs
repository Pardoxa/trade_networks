use std::io::BufWriter;

use crate::config::write_commands_and_version;

use{
    std::{
        io::{
            BufReader, 
            BufRead,
            Write
        },
        fs::File,
        collections::{
            BTreeMap,
            BTreeSet
        }
    },
    serde::{Serialize, Deserialize},
    sampling::{HistUsizeFast, histogram::Histogram}
};

fn line_to_vec(line: &str) -> Vec<String>
{
    let mut w = String::new();
    let mut all = Vec::new();
    let mut inside = false;
    for c in line.chars(){
        if c == '"' {
            inside = !inside;
        }else if !inside && c == ','{
            let mut n = String::new();
            std::mem::swap(&mut n, &mut w);
            all.push(n);
        } else {
            w.push(c);
        }
    }
    all.push(w);
    all
}



pub fn network_parser(file_name: &str, item_code: &str) -> Vec<Network>
{

    let wanted_transaction_type = "Import Quantity";
    let file = File::open(file_name)
        .unwrap();
    let reader = BufReader::with_capacity(64 * 1024, file);

    let mut lines = reader.lines();
    let header = lines.next().unwrap().unwrap(); 
    let entry_iter = header.split(',');

    let entry_names: Vec<_> = entry_iter.collect();

    // Reporter country, partner country, item code

    for (idx, &entry) in entry_names.iter().enumerate() {
        println!("Entry {idx} is {entry}");
    }

    let mut map = BTreeMap::new();
    for (idx, &entry) in entry_names.iter().enumerate()
    {
        map.insert(entry, idx);
    }
    let item_id = *map.get("Item Code").unwrap();
    let reporter_country = "Reporter Country Code";
    let reporter_country_id = *map.get(reporter_country).unwrap();
    let partner_country = "Partner Country Code";
    let partner_country_id = *map.get(partner_country).unwrap();
    let unit = "Unit";
    let unit_id = *map.get(unit).unwrap();

    let transaction_type = *map.get("Element").unwrap();

    let mut glob_unit: Option<String> = None;

    let mut countries: BTreeSet<String> = BTreeSet::new();

    let y1986 = *map.get("Y1986").unwrap();


    lines
        .map(|line| {
            let line = line.unwrap();
            line_to_vec(&line)
        })
        .filter(
            |item| 
            {
                item[item_id] == item_code 
                && item[transaction_type] == wanted_transaction_type
            })
        .for_each(
            |line_vec|
            {
                let unit = line_vec.get(unit_id).unwrap();
                if let Some(u) = &glob_unit{
                    if !u.eq(unit){
                        panic!("Unit error! old {} new {}", u, unit);
                    }
                } else {
                    glob_unit = Some(unit.to_owned());
                }
                let rep_c = line_vec.get(reporter_country_id).unwrap();
                countries.insert(rep_c.clone());
                let part_c = line_vec.get(partner_country_id).unwrap();
                countries.insert(part_c.clone());
            }
        );

    let all: Vec<_> = countries.iter()
        .map(
            |item| 
            Node::new(item.clone())
        ).collect();

    let mut id_map = BTreeMap::new();

    all.iter().enumerate()
        .for_each(
            |(id, item)|
            {
                let code = &item.country_code;
                id_map.insert(code.to_owned(), id);
            }
        );

    let network = Network{nodes: all};

    let mut years: Vec<_> = (y1986..map.len())
        .map(|_| network.clone())
        .collect();

    let file = File::open(file_name)
        .unwrap();
    let reader = BufReader::with_capacity(64 * 1024, file);

    let iter = reader.lines()
        .skip(1)
        .map(|line| {
            let line = line.unwrap();
            line_to_vec(&line)
        })
        .filter(
            |item| 
            {
                item[item_id] == item_code 
                && item[transaction_type] == wanted_transaction_type
            });

    iter.for_each(
        |line|
        {
            let rep_c = line.get(reporter_country_id).unwrap();
            let part_c = line.get(partner_country_id).unwrap();

            let rep_id = *id_map.get(rep_c).unwrap();
            let part_id = *id_map.get(part_c).unwrap();

            (y1986..).zip(years.iter_mut())
                .for_each(
                    |(idx, network)|
                    {
                        let amount_entry = &line[idx];
                        if !amount_entry.is_empty(){
                            let amount: f64 = amount_entry.parse().unwrap();
                            let node = network.nodes.get_mut(rep_id).unwrap();
                            let edge = Edge{
                                amount,
                                index: part_id
                            };
                            node.adj.push(edge);
                        }
                        
                    }
                )
        }
    );

    years
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Network{
    nodes: Vec<Node>
}

impl Network{
    pub fn invert(&self) -> Self
    {
        let mut all: Vec<_> = self.nodes.iter()
            .map(
                |item|
                {
                    Node{
                        country_code: item.country_code.clone(),
                        adj: Vec::new()
                    }
                }
            ).collect();
        
        for (idx, node) in self.nodes.iter().enumerate()
        {
            for other_node in node.adj.iter()
            {
                let edge = Edge{
                    amount: other_node.amount,
                    index: idx
                };
                all[other_node.index].adj.push(edge);
            }
        }
        Network { nodes: all }
    }
}

pub fn degree_dist(networks: &[Network], out: &str)
{
    let max_degree = networks.iter()
        .flat_map(
            |network|
            {
                network.nodes.iter().map(|node| node.adj.len())
            }
        ).max().unwrap();
    
    let hist = HistUsizeFast::new_inclusive(0, max_degree).unwrap();

    let mut hists: Vec<_> = (0..networks.len()).map(|_| hist.clone()).collect();

    networks.iter()
        .zip(hists.iter_mut())
        .for_each(
            |(network, hist)|
            {
                for node in network.nodes.iter()
                {
                    hist.increment_quiet(node.adj.len());
                }
            }
        );

    let file = File::create(out).expect("unable to create file");
    let mut buf = BufWriter::new(file);

    write_commands_and_version(&mut buf).unwrap();

    let first = hists.first().unwrap();

    for (index, (bin, hits)) in first.bin_hits_iter().enumerate()
    {
        write!(buf, "{bin} {hits}").unwrap();
        for hist in hists[1..].iter()
        {
            let hit = hist.hist()[index];
            write!(buf, " {hit}").unwrap();
        }
        writeln!(buf).unwrap();
    }
    println!("years: {}", networks.len());

}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Node{
    country_code: String,
    adj: Vec<Edge>
}

impl Node {
    pub fn new(code: String) -> Self
    {
        Self{
            country_code: code,
            adj: Vec::new()
        }
    }
}

#[allow(dead_code)]
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Edge{
    index: usize,
    amount: f64
}