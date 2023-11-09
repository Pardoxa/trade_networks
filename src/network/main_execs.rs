use std::{collections::{BTreeSet, BTreeMap}, fmt::Display};

use indicatif::ProgressIterator;

use crate::parser;

use {
    std::{
        fs::File,
        io::{BufWriter, Write, BufRead, BufReader}
    },
    super::{*, helper_structs::*},
    crate::{config::*, misc::*},
    rayon::prelude::*,
    net_ensembles::sampling::*
};

pub fn to_binary(opt: ToBinaryOpt)
{
    let networks = crate::parser::network_parser(
        &opt.in_file, 
        &opt.item_code, 
        false
    ).expect("unable to parse");

    let file = File::create(&opt.out).unwrap();
    let buf = BufWriter::new(file);
    bincode::serialize_into(buf, &networks)
        .expect("serialization issue");
}

pub fn to_binary_all(opt: AllToBinaryOpt)
{

    let item_file = File::open(&opt.item_file)
        .unwrap();
    let buf_reader = BufReader::new(item_file);

    let lines = buf_reader.lines().skip(1);

    let mut item_codes = BTreeSet::new();

    for item_line in lines{
        let l = item_line.unwrap();
        let split = parser::line_to_vec(&l);
        assert_eq!(split.len(), 3);
        item_codes.insert(split[0].clone());
    }

    if opt.seperate_output {
        println!("Found {} item codes", item_codes.len());
        let bar = crate::misc::indication_bar(item_codes.len() as u64);
        for item_code in item_codes.iter().progress_with(bar){
            let networks = crate::parser::network_parser(&opt.in_file, item_code, true);
            let networks = match networks{
                Err(e) => {
                    println!("Error in {e} - Item code {item_code}");
                    continue;
                }, 
                Ok(n) => n
            };
            let output_name = format!("{item_code}.bincode");
            let file = File::create(&output_name).unwrap();
            let buf = BufWriter::new(file);
            bincode::serialize_into(buf, &networks)
                .expect("serialization issue");
        }
    } else {
        unimplemented!()
    }
    

    
}

pub fn to_country_file(opt: ToCountryBinOpt)
{
    let networks = read_networks(&opt.bin_file);
    let country_networks = crate::parser::country_networks(&networks, opt.country_file);
    let file = File::create(&opt.out).unwrap();
    let buf = BufWriter::new(file);
    bincode::serialize_into(buf, &country_networks)
        .expect("serialization issue");
}

pub fn max_weight(opt: DegreeDist)
{
    let mut networks: Vec<Network> = read_networks(&opt.input);

    if opt.invert{
        networks.iter_mut().for_each(
            |n|
            {
                *n = n.invert();
            }
        );
    }

    max_weight_dist(&mut networks, &opt.out);
}

fn max_weight_dist(networks: &mut [Network], out: &str){
    let hist = HistF64::new(0.0, 1.1, 50).unwrap();

    let mut hists: Vec<_> = (0..networks.len()).map(|_| hist.clone()).collect();

    networks.par_iter_mut()
        .zip(hists.par_iter_mut())
        .for_each(
            |(network, hist)|
            {
                network.normalize();
                network.nodes.iter()
                    .for_each(
                        |node|
                        {
                            let max = node.adj.iter()
                                .map(|v| v.amount)
                                .max_by(f64::total_cmp);
                            if let Some(max) = max{
                                hist.increment_quiet(max);
                            }
                        }
                    )
            }
        );

    let file = File::create(out).expect("unable to create file");
    let mut buf = BufWriter::new(file);

    write_commands_and_version(&mut buf).unwrap();

    let first = hists.first().unwrap();

    for (index, (bins, hits)) in first.bin_hits_iter().enumerate()
    {
        let bin = bins[0] + (bins[1]-bins[0]) / 2.0;
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

pub fn degree_dists(opt: DegreeDist)
{
    let mut networks: Vec<Network> = read_networks(&opt.input);

    if opt.invert{
        networks.iter_mut().for_each(
            |n|
            {
                *n = n.invert();
            }
        );
    }

    degree_dists_helper(&networks, &opt.out);
}

fn degree_dists_helper(networks: &[Network], out: &str)
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


pub fn export_out_comp(opt: MiscOpt)
{
    let networks = read_networks(&opt.input);

    let file = File::create(opt.out).expect("unable to create file");
    let mut buf = BufWriter::new(file);

    write_commands_and_version(&mut buf).unwrap();

    write!(buf, "#year").unwrap();
    for i in 0..10
    {
        write!(buf, " {i}").unwrap();
    }
    writeln!(buf).unwrap();

    for (id, n) in networks.iter().enumerate()
    {
        
        let no_unconnected = n.without_unconnected_nodes();
        if no_unconnected.node_count() < 10 {
            continue;
        } 
        let mut digraph = if !opt.invert{
            no_unconnected.invert()
        } else{
            no_unconnected
        };

        if opt.effective_trade{
            digraph = digraph.effective_trade_only();
        }

        let mut for_sorting: Vec<(_,f64)> = digraph.nodes
            .iter()
            .enumerate()
            .map(
                |(i, n)|
                {
                    (i, n.adj.iter().map(|e| e.amount).sum())
                }
            ).collect();
        for_sorting
            .sort_unstable_by(|a, b| b.1.total_cmp(&a.1));

        assert!(for_sorting[0].1 >= for_sorting[1].1);

        write!(buf, "{id} ").unwrap();
        for &(i, a) in for_sorting[0..10].iter(){
            if a > 0.0 {
                let out = digraph.out_component(i, ComponentChoice::IncludingSelf).len();
                write!(buf, " {out}").unwrap();
            } else {
                write!(buf, " NaN").unwrap();
            }
        }
        writeln!(buf).unwrap();
    }

}

pub fn misc(opt: MiscOpt)
{
    let mut networks = read_networks(&opt.input);

    let file = File::create(opt.out).expect("unable to create file");
    let mut buf = BufWriter::new(file);

    write_commands_and_version(&mut buf).unwrap();

    let entries = [
        "year_id",
        "exporting_nodes",
        "importing_nodes",
        "trading_nodes",
        "edge_count",
        "max_my_centrality",
        "component_count",
        "largest_component",
        "largest_component_percent",
        "largest_component_edges",
        "largest_out_size",
        "largest_in_size",
        "num_scc",
        "largest_scc",
        "largest_scc_diameter"
    ];

    write!(buf, "#{}_1", entries[0]).unwrap();
    for (e, j) in entries[1..].iter().zip(2..) {
        write!(buf, " {e}_{j}").unwrap();
    }
    writeln!(
        buf
    ).unwrap();

    if opt.effective_trade{
        networks.iter_mut()
            .for_each(|n| *n = n.effective_trade_only());
    }
    

    for (id, n) in networks.iter().enumerate()
    {
        let no_unconnected = n.without_unconnected_nodes();

        if no_unconnected.node_count() == 0{
            if opt.verbose{
                println!("Empty year {id}");
            }
            continue;
        }
        let mut res_map: BTreeMap<&str, Box<dyn Display>> = BTreeMap::new();
        res_map.insert("year_id", Box::new(id));

        let trading_nodes = no_unconnected.node_count();
        res_map.insert("trading_nodes", Box::new(trading_nodes));
        let inverted = n.invert();
        let node_count = inverted.nodes_with_non_empty_adj();
        res_map.insert("exporting_nodes", Box::new(node_count));
        let importing_nodes = n.nodes_with_non_empty_adj();
        res_map.insert("importing_nodes", Box::new(importing_nodes));
        let edge_count = n.edge_count();
        res_map.insert("edge_count", Box::new(edge_count));

        let mut normalized = n.clone();
        normalized.normalize();
        let centrality = normalized.my_centrality_normalized();
        let max_c = *centrality.iter().max().unwrap();
        res_map.insert("max_my_centrality", Box::new(max_c));

        let component = largest_component(&no_unconnected);

        let largest_component_percent = component.size_of_largest_component as f64 / no_unconnected.node_count() as f64;
        res_map.insert("largest_component_percent", Box::new(largest_component_percent));
        res_map.insert("component_count", Box::new(component.num_components));

        let reduced = no_unconnected.filtered_network(&component.members_of_largest_component);
        res_map.insert("largest_component", Box::new(component.size_of_largest_component));
        
        let giant_comp_edge_count = reduced.edge_count();
        res_map.insert("largest_component_edges", Box::new(giant_comp_edge_count));

        let out_size = n.largest_out_component(ComponentChoice::ExcludingSelf);
        res_map.insert("largest_out_size", Box::new(out_size));

        
        let in_size = inverted.largest_out_component(ComponentChoice::ExcludingSelf);
        res_map.insert("largest_in_size", Box::new(in_size));

        let scc_components = no_unconnected.scc_recursive();
        res_map.insert("num_scc", Box::new(scc_components.len()));
        
        let mut check = vec![false; no_unconnected.node_count()];
        for &i in scc_components.iter().flat_map(|e| e.iter())
        {
            check[i] = true;
        }
        assert!(check.iter().all(|x| *x));
        let total: usize = scc_components.iter().map(|e| e.len()).sum();
        assert_eq!(total, no_unconnected.node_count());

        let mut index_largest_scc = 0;
        let mut size_largest_scc = 0;

        scc_components.iter()
            .enumerate()
            .for_each(
                |(index, comp)|
                {
                    if comp.len() > size_largest_scc {
                        size_largest_scc = comp.len();
                        index_largest_scc = index;
                    }
                }
            );

        let scc_network = no_unconnected.filtered_network(&scc_components[index_largest_scc]);
        let largest_scc_diameter = scc_network.diameter();
        res_map.insert("largest_scc", Box::new(scc_network.node_count()));
        let diam = if let Some(dia) = largest_scc_diameter {
            format!("{}", dia)
        } else {
            "NaN".to_owned()
        };
        res_map.insert("largest_scc_diameter", Box::new(diam));

        write!(buf, "{}", res_map.get(entries[0]).unwrap()).unwrap();
        for e in entries[1..].iter()
        {
            write!(buf, " {}", res_map.get(e).expect(e)).unwrap();
        }
        writeln!(buf).unwrap();

    }
}