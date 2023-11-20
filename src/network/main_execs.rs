use std::f64::consts::TAU;
use std::ops::Deref;
use super::enriched_digraph::ExtraInfo;

use {
    std::{
        fs::File,
        io::{BufWriter, Write, BufRead, BufReader},
        collections::{BTreeSet, BTreeMap}, 
        fmt::Display
    },
    super::{*, helper_structs::*},
    crate::{config::*, misc::*, parser},
    rayon::prelude::*,
    net_ensembles::sampling::*,
    indicatif::ProgressIterator,
};

fn assert_same_direction_write_direction<W>(networks: &[Network], mut writer: W)
where W: Write
{
    assert!(
        networks.windows(2)
            .all(|slice| slice[0].direction == slice[1].direction)
    );
    writeln!(writer, "#Direction {:?}", networks[0].direction)
        .unwrap();
}

fn force_direction(networks: &mut [Network], direction: Direction)
{
    networks
        .iter_mut()
        .for_each(|n| n.force_direction(direction));
}

pub fn parse_networks(opt: ParseNetworkOpt)
{
    let networks = crate::parser::network_parser(
        &opt.in_file, 
        &opt.item_code, 
        false,
        opt.read_type
    ).expect("unable to parse");

    let file = File::create(&opt.out).unwrap();
    let buf = BufWriter::new(file);
    if opt.json{
        serde_json::to_writer_pretty(buf, &networks)
            .expect("unable to create json");
    } else {
        bincode::serialize_into(buf, &networks)
            .expect("serialization issue");
    }
    
}

pub fn to_binary_all(opt: ParseAllNetworksOpt)
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
            let networks = crate::parser::network_parser(
                &opt.in_file, 
                item_code, 
                true,
                opt.read_type
            );
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

pub fn max_weight(opt: DegreeDist)
{
    let mut networks: Vec<Network> = read_networks(&opt.input);
    force_direction(&mut networks, opt.direction);

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
    assert_same_direction_write_direction(networks, &mut buf);

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
    force_direction(&mut networks, opt.direction);

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
    
    let mul = (max_degree+1).next_multiple_of(2) - 1;
    let hist = HistUsize::new_inclusive(0, mul, (mul+1)/2)
        .unwrap();
    //let hist = HistUsizeFast::new_inclusive(0, max_degree).unwrap();
        
    let mut hists: Vec<_> = (0..networks.len()).map(|_| hist.clone()).collect();

    networks.iter()
        .zip(hists.iter_mut())
        .for_each(
            |(network, hist)|
            {
                let network = network.without_unconnected_nodes();
                for node in network.nodes.iter()
                {
                    hist.increment_quiet(node.adj.len());
                }
            }
        );

    let file = File::create(out).expect("unable to create file");
    let mut buf = BufWriter::new(file);

    write_commands_and_version(&mut buf).unwrap();
    assert_same_direction_write_direction(networks, &mut buf);

    let first = hists.first().unwrap();

    for (index, (bin, hits)) in first.bin_hits_iter().enumerate()
    {
        write!(buf, "{} {hits}", bin[0]).unwrap();
        for hist in hists[1..].iter()
        {
            let hit = hist.hist()[index];
            write!(buf, " {hit}").unwrap();
        }
        writeln!(buf).unwrap();
    }
    println!("years: {}", networks.len());

}


/// Direction needs to be export to
pub fn export_out_comp(opt: MiscOpt)
{
    let mut networks = read_networks(&opt.input);

    force_direction(&mut networks, Direction::ExportTo);

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
        
        let mut digraph = n.without_unconnected_nodes();
        if digraph.node_count() < 10 {
            continue;
        }

        if opt.effective_trade{
            digraph = digraph.effective_trade_only();
        }

        let sorted = digraph.sorted_by_largest_in();

        assert!(sorted[0].1 >= sorted[1].1);

        write!(buf, "{id} ").unwrap();
        for &(i, a) in sorted[0..10].iter(){
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
        "connected_component_count",
        "largest_connectd component",
        "largest_connectd component_percent",
        "largest_connectd component_edges",
        "largest_exporting_out_comp",
        "largest_importing_out_comp",
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
    

    for (id, current_n) in networks.iter().enumerate()
    {
        let inv = current_n.invert();
        let (importing, exporting) = match current_n.direction{
            Direction::ExportTo => (&inv, current_n),
            Direction::ImportFrom => (current_n, &inv)
        };
        let no_unconnected_exporting = exporting.without_unconnected_nodes();

        if no_unconnected_exporting.node_count() == 0{
            if opt.verbose{
                println!("Empty year {id}");
            }
            continue;
        }
        let mut res_map: BTreeMap<&str, Box<dyn Display>> = BTreeMap::new();
        res_map.insert("year_id", Box::new(id));

        let trading_nodes = no_unconnected_exporting.node_count();
        res_map.insert("trading_nodes", Box::new(trading_nodes));
        
        
        let node_count = exporting.nodes_with_non_empty_adj();
        res_map.insert("exporting_nodes", Box::new(node_count));
        let importing_nodes = importing.nodes_with_non_empty_adj();
        res_map.insert("importing_nodes", Box::new(importing_nodes));
        let edge_count = no_unconnected_exporting.edge_count();
        res_map.insert("edge_count", Box::new(edge_count));

        let component = largest_connected_component(&no_unconnected_exporting);

        let largest_component_percent = 
            component.size_of_largest_component as f64 / no_unconnected_exporting.node_count() as f64;
        res_map.insert("largest_connected_component_percent", Box::new(largest_component_percent));
        res_map.insert("connected_component_count", Box::new(component.num_components));

        let largest_connected_component = no_unconnected_exporting
            .filtered_network(component.members_of_largest_component.iter());
        res_map.insert("largest_component", Box::new(component.size_of_largest_component));
        
        let giant_comp_edge_count = largest_connected_component.edge_count();
        res_map.insert("largest_connected_component_edges", Box::new(giant_comp_edge_count));

        let out_size = exporting.largest_out_component(ComponentChoice::IncludingSelf);
        res_map.insert("largest_exporting_out_comp", Box::new(out_size));

        
        let in_size = importing.largest_out_component(ComponentChoice::IncludingSelf);
        res_map.insert("largest_importing_out_comp", Box::new(in_size));

        let scc_components = no_unconnected_exporting.scc_recursive();
        res_map.insert("num_scc", Box::new(scc_components.len()));
        
        let mut check = vec![false; no_unconnected_exporting.node_count()];
        for &i in scc_components.iter().flat_map(|e| e.iter())
        {
            check[i] = true;
        }
        assert!(check.iter().all(|x| *x));
        let total: usize = scc_components.iter().map(|e| e.len()).sum();
        assert_eq!(total, no_unconnected_exporting.node_count());

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

        let scc_network = no_unconnected_exporting
            .filtered_network(scc_components[index_largest_scc].iter());
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

pub fn enrich(opt: EnrichOpt){
    let enrichments = crate::parser::parse_extra(
        &opt.enrich_file, 
        &opt.item_code
    );
    let networks = read_networks(&opt.bin_file);
    let enriched = crate::network::enriched_digraph::enrich_networks(
        &networks, 
        enrichments
    );
    let out_file = File::create(opt.out)
        .unwrap();
    let buf_writer = BufWriter::new(out_file);
    if opt.json{
        serde_json::to_writer_pretty(buf_writer, &enriched)
            .expect("unable to create json");
    } else {
        bincode::serialize_into(buf_writer, &enriched)
            .expect("unable to serialize");
    }
}

pub fn test_chooser(in_file: &str, cmd: SubCommand){
    match cmd
    {
        SubCommand::OutComp(o) => {
            out_comparison(in_file, o)
        },
        SubCommand::FirstLayerOverlap(o) => {
            first_layer_overlap(in_file, o)
        },
        SubCommand::FirstLayerAll(a) => flow_of_top_first_layer(in_file, a),
        SubCommand::Flow(f) => flow(f, in_file),
        SubCommand::Shock(s) => shock_exec(s, in_file),
        SubCommand::CountryCount(c) => country_count(in_file, c)
    }
}

pub fn country_count(in_file: &str, opt: CountryCountOpt){
    let networks = read_networks(in_file);

    let file = File::create(opt.out)
        .unwrap();
    let mut buf = BufWriter::new(file);
    write_commands_and_version(&mut buf).unwrap();
    writeln!(buf, "#Year Trading Exporter").unwrap();

    for n in networks{
        let mut without = n.without_unconnected_nodes();
        without.force_direction(Direction::ExportTo);
        let exporter = without.nodes.iter()
            .filter(|n| !n.adj.is_empty())
            .count();
        writeln!(buf, "{} {} {exporter}", n.year, without.node_count())
            .unwrap();
    }
}

pub fn out_comparison(in_file: &str, cmd: OutOpt){
    let networks = read_networks(in_file);

    let mut network = None;
    for n in networks{
        if n.year == cmd.year {
            network = Some(n);
            break;
        }
    }
    let mut network = network
        .expect("could not find specified year");
    network.force_direction(cmd.direction);

    let ordering = network.sorted_by_largest_in();
    let sets: Vec<_> = ordering[0..cmd.top.get()]
        .iter()
        .map(
            |&(index, _)|
            {
                let comps = network.out_component(
                    index, 
                    ComponentChoice::IncludingSelf
                );
                BTreeSet::from_iter(comps)
            }
        ).collect();
    
    let out = File::create(&cmd.out)
        .expect("unable to create file");
    let mut buf = BufWriter::new(out);
    write_commands_and_version(&mut buf).unwrap();

    let matrix: Vec<Vec<_>> = sets.iter()
        .map(
            |set|
            {
                sets.iter()
                    .map(
                        |other|
                        {
                            let inter = set.intersection(other);
                            inter.count()
                        }
                    ).collect()
            }
        ).collect();

    for row in matrix.iter(){
        for val in row.iter(){
            write!(buf, "{val} ").unwrap();
        }
        writeln!(buf).unwrap()
    }

}

pub fn first_layer_overlap(in_file: &str, cmd: FirstLayerOpt){
    let networks = read_networks(in_file);

    let mut network = None;
    for n in networks{
        if n.year == cmd.year {
            network = Some(n);
            break;
        }
    }
    let mut network = network
        .expect("could not find specified year");
    network.force_direction(cmd.direction);

    let ordering = network.sorted_by_largest_in();
    let mut layers: Vec<BTreeSet<usize>> = ordering.iter()
        .take(cmd.top.get())
        .map(
            |(idx, _)|
            {
                network.nodes[*idx]
                    .adj
                    .iter()
                    .map(|e| e.index)
                    .collect()
            }
        ).collect();
    
    
    

    let create_file = |filename: &str| {
        let out = File::create(filename)
            .expect("unable to create file");
        let mut buf_overlap = BufWriter::new(out);
        write_commands_and_version(&mut buf_overlap).unwrap();
        buf_overlap
    };


    let overlap_name = format!("layer_overlap_{}", cmd.out);
    let mut buf_overlap = create_file(&overlap_name);
    layers.iter()
        .for_each(
            |set|
            {
                layers.iter()
                    .for_each(
                        |other|
                        {
                            let inter = set.intersection(other);
                            write!(buf_overlap, "{} ", inter.count()).unwrap();
                        }
                    );
                writeln!(buf_overlap).unwrap();
            }
        );

    let size_name = format!("layer_size_{}", cmd.out);
    let mut buf_size = create_file(&size_name);

    let d = match cmd.direction{
        Direction::ExportTo => "Export",
        Direction::ImportFrom => "Import"
    };
    writeln!(buf_size, "#layer1 index_of_parent {d}_amount_parent").unwrap();
    for (l, (index, amount)) in layers.iter().zip(ordering.iter()){
        writeln!(buf_size, "{} {index} {amount}", l.len()).unwrap();
    }

    if let Some(country_file) = cmd.print_graph{
        let map = Some(parser::country_map(&country_file));
        let iter = layers
            .iter_mut()
            .zip(ordering.iter())
            .enumerate();
        for (i, (layer, (idx, _))) in iter{
            let filter_iter = std::iter::once(idx)
                .chain(layer.iter());

            let n = network.filtered_network(filter_iter);
            let graph_name = format!("{i}_{}.dot", cmd.out);
            let file = File::create(graph_name)
                .expect("unable to create graph file");
            let buf = BufWriter::new(file);
            let extra = GraphVizExtra{
                highlight: network.nodes[*idx].identifier.clone(),
                map: map.clone()
            };
            n.graphviz(buf, &extra).unwrap();

            let graph_name2 = format!("L_{i}_{}.dot", cmd.out);
            let file = File::create(graph_name2)
                .expect("unable to create graph file");
            let buf2 = BufWriter::new(file);
            graphviz_one_layer(
                &network, 
                buf2, 
                *idx, 
                &extra.map
            ).unwrap();
        }
        
    }

}

pub fn graphviz_one_layer<'a, W>(
    net: &'a Network, 
    mut w: W, 
    parent: usize,
    map: &'a Option<BTreeMap<String, String>>
) -> std::io::Result<()>
where W: Write
{
    writeln!(w, "digraph {{")?;
    writeln!(w, "overlap=false")?;
    writeln!(w, "splines=true")?;

    let map: Box<dyn Fn(&'a str) -> &'a str> = match map{
        None => Box::new(|s| s),
        Some(m) => {
            Box::new(
                |s| {
                    m.get(s).unwrap()
                }
            )
        }
    };

    let parent_node = &net.nodes[parent];
    writeln!(w, "\"{}\" [fillcolor=red, style=filled]", map(&parent_node.identifier))?;

    for e in parent_node.adj.iter()
    {
        let other_node = &net.nodes[e.index];
        writeln!(w, "\"{}\"", map(&other_node.identifier))?;
    }

    
    let parend_id = map(&parent_node.identifier);
    let mut edge_max: f64 = 0.0;
    for e in parent_node.adj.iter(){
        if e.amount > edge_max{
            edge_max = e.amount;
        }
    }
    for e in parent_node.adj.iter()
    {
        let we = e.amount / edge_max;
        let red = u8::MAX - (255.0 * we) as u8;
        let green = u8::MAX - (255.0 * 10.0 * we).min(255.0) as u8;
        let blue = u8::MAX - (255.0 * 100.0 * we).min(255.0) as u8;
        writeln!(
            w, 
            "\"{}\" -> \"{}\" [color=\"#{:02X}{:02X}{:02X}\"]", 
            parend_id, 
            map(&net.nodes[e.index].identifier),
            red,
            green,
            blue
        )?;
    }
    
    writeln!(w, "}}")
}

pub fn flow_of_top_first_layer(in_file: &str, opt: FirstLayerOpt)
{
    let networks = read_networks(in_file);

    let mut network = None;
    for n in networks{
        if n.year == opt.year {
            network = Some(n);
            break;
        }
    }
    let mut network = network
        .expect("could not find specified year");
    network.force_direction(opt.direction);

    let ordering = network.sorted_by_largest_in();

    let graph_name = format!("A_{}.dot", opt.out);
    let file = File::create(graph_name)
        .expect("unable to create graph file");
    let buf = BufWriter::new(file);

    let map = opt.print_graph
        .map(|s| parser::country_map(&s));

    flow_of_top_first_layer_helper(
        &network, 
        &ordering[0..opt.top.get()], 
        buf,
        &map
    ).unwrap();
}

fn flow_of_top_first_layer_helper<'a, W>(
    net: &'a Network, 
    parents: &[(usize, f64)],
    mut w: W,
    map: &'a Option<BTreeMap<String, String>>
) -> std::io::Result<()>
where W: Write
{
    let layer: Vec<BTreeSet<_>> = parents.iter()
        .map(
            |p|
            {
                net.nodes[p.0]
                    .adj
                    .iter()
                    .map(|e| e.index)
                    .collect()
            }
        ).collect();
    // color rest by number of links?
    let all_relevant_nodes: BTreeSet<usize> = layer.iter()
        .flat_map(
            |l|
            {
               l.iter().copied()
            }
        ).collect();

    let parent_set: BTreeSet<usize> = parents.iter()
        .map(|i| i.0)
        .collect();

    writeln!(w, "digraph {{")?;
    writeln!(w, "overlap=false")?;
    writeln!(w, "splines=true")?;

    let map: Box<dyn Fn(&'a str) -> &'a str> = match map{
        None => Box::new(|s| s),
        Some(m) => {
            Box::new(
                |s| {
                    m.get(s).unwrap()
                }
            )
        }
    };

    for (i, parent) in parent_set.iter().enumerate(){
        let hue = 1.0 / layer.len() as f64 * i as f64;
        let parent_node = &net.nodes[*parent];
        writeln!(
            w, 
            "\"{}\" [fillcolor=\"{},1.0,0.7\", style=filled]", 
            map(&parent_node.identifier),
            hue
        )?;
    }
    
    let colors: Vec<_> = (0..layer.len())
        .map(
            |i|
            {
                (255.0 - (i * 255) as f64 /(layer.len() as f64)) as u8
            }
        ).collect();

    let mut other_nodes = Vec::new();
    let mut other_map = BTreeMap::new();
    let len = all_relevant_nodes.difference(&parent_set).count();

    for (idx, e) in all_relevant_nodes.difference(&parent_set).enumerate()
    {
        other_nodes.push((e, 0.0, TAU * (idx as f64 / len as f64)));
        other_map.insert(e, idx);
        let count = layer.iter()
            .filter(|l| l.contains(e))
            .count();
        let c = colors[count-1];
        let other_node = &net.nodes[*e];
        writeln!(
            w, 
            "\"{}\" [fillcolor=\"#{:02x}{:02x}{:02x}\", style=filled]", 
            map(&other_node.identifier),
            c,
            c,
            c
        )?;
    }


    for (i, parent) in parent_set.iter().enumerate(){
        let hue = 1.0 / layer.len() as f64 * i as f64;
        let parent_node = &net.nodes[*parent];
        let parend_id = map(&parent_node.identifier);
        for e in parent_node.adj.iter()
        {
            if let Some(other_idx) = other_map.get(&e.index){
                other_nodes[*other_idx].1 += e.amount;
            }

            writeln!(
                w, 
                "\"{}\" -> \"{}\" [color=\"{},1.0,0.7\"]", 
                parend_id, 
                map(&net.nodes[e.index].identifier),
                hue
            )?;
        }
    }
    
    let mut max_amount = 0.0;
    for amount in other_nodes.iter().map(|e| e.1){
        max_amount = amount.max(max_amount);
    }

    let file = File::create("test.dat").unwrap();
    let mut buf = BufWriter::new(file);

    for item in other_nodes.iter(){
        let r = (1.1 - item.1 / max_amount) / 1.1;
        
        writeln!(buf, "{r} {}", item.2).unwrap();
    }
    
    
    writeln!(w, "}}")
}

pub fn flow(opt: FlowOpt, in_file: &str)
{
    let networks = read_networks(in_file);

    let mut network = None;
    for n in networks{
        if n.year == opt.year {
            network = Some(n);
            break;
        }
    }
    let network = network
        .expect("could not find specified year");

    let enrichments = crate::parser::parse_extra(
        &opt.enrich_file, 
        &opt.item_code
    );
    let idx = (network.year - enrichments.starting_year) as usize;
    let extra = &enrichments.enrichments[idx];

    let flow = flow_calc(&network, &opt.top_id, opt.iterations, extra);

    let file = File::create(opt.out)
        .expect("unable to create file");
    let mut buf = BufWriter::new(file);

    for (index, (total, import)) in flow.total.iter().zip(flow.imports.iter()).enumerate() {
        writeln!(buf, "{index} {total} {import}").unwrap();
    }
}


pub fn flow_calc(
    net: &Network, 
    focus: &str, 
    iterations: usize, 
    extra: &BTreeMap<String, ExtraInfo>
) -> Flow
{
    let info_map = super::enriched_digraph::GLOBAL_NODE_INFO_MAP.deref();
    let unit_tester = crate::UNIT_TESTER.deref();
    let info_idx = info_map.get("Production");
    let mut percent = vec![0.0; net.node_count()];
    let mut new_percent = percent.clone();

    let mut production = Vec::new();
    let mut map = BTreeMap::new();
    for (i, n) in net.nodes.iter().enumerate()
    {
        map.insert(n.identifier.as_str(), i);
        let pr = match extra.get(n.identifier.as_str()){
            None => 0.0,
            Some(e) => {
                match e.map.get(&info_idx){
                    None => 0.0,
                    Some(pr) => {
                        assert!(
                            unit_tester.is_equiv(&pr.unit, &net.unit), 
                            "incompatible units"
                        );
                        pr.amount
                    }
                }
            }
        };
        production.push(pr);
    }
    let focus_idx = map.get(focus).unwrap();
    percent[*focus_idx] = 1.0;

    let import_from = net.get_network_with_direction(Direction::ImportFrom);

    for _ in 0..iterations{
        #[allow(clippy::needless_range_loop)]
        for i in 0..production.len(){
            let new_p = new_percent.get_mut(i).unwrap();
            let n = &import_from.nodes[i];
            let mut total = production[i];
            *new_p = 0.0;
            for e in n.adj.iter(){
                *new_p += e.amount * percent[e.index];
                total += e.amount; 
            }

            if total > 0.0{
                *new_p /= total;
            }
        }
        new_percent[*focus_idx] = 1.0;
        std::mem::swap(&mut new_percent, &mut percent);
    }

    let mut imports = new_percent;

    #[allow(clippy::needless_range_loop)]
    for i in 0..production.len(){
        let import = &mut imports[i];
        let mut total_import = 0.0;
        *import = 0.0;

        for e in import_from.nodes[i].adj.iter(){
            *import += e.amount * percent[e.index];
            total_import += e.amount;
        }
        
        *import /= total_import;
    }

    Flow{
        total: percent,
        imports
    }
}

pub struct Flow{
    total: Vec<f64>,
    imports: Vec<f64>
}

pub fn shock_exec(opt: ShockOpts, in_file: &str)
{
    let networks = read_networks(in_file);

    let mut network = None;
    for n in networks{
        if n.year == opt.year {
            network = Some(n);
            break;
        }
    }
    let network = network
        .expect("could not find specified year")
        .without_unconnected_nodes();


    let focus = network.nodes.iter()
        .position(|item| item.identifier == opt.top_id)
        .unwrap();

    let fracts = shock_distribution(
        &network, 
        focus, 
        opt.export, 
        opt.iterations
    );

    let name = format!("{}.dat", opt.out);
    let file = File::create(name)
        .expect("unable to create file");
    let mut buf = BufWriter::new(file);

    write_commands_and_version(&mut buf).unwrap();
    writeln!(buf, "#index import_frac export_frac").unwrap();

    for (index, (import, export)) in fracts.import_fracs.iter().zip(fracts.export_fracs.iter()).enumerate()
    {
        writeln!(buf, "{index} {import} {export}").unwrap()
    }

    let write_dist = |slice: &[f64], name: &str| {
        let mut hist = HistF64::new(0.0, 1.0 + f64::EPSILON, 20)
            .unwrap();
        for v in slice {
            hist.increment(*v).unwrap();
        }
        let total: usize = hist.hist().iter().sum();
        let total_f = total as f64;
        
        let file = File::create(name)
            .unwrap();
        let mut buf = BufWriter::new(file);
        write_commands_and_version(&mut buf).unwrap();
        writeln!(buf, "#Bin_left Bin_right Bin_center hits normalized").unwrap();
        for (bins, hits) in hist.bin_hits_iter()
        {
            let normed = hits as f64 / total_f;
            let center = (bins[0] + bins[1]) * 0.5;
            writeln!(buf, "{} {} {center} {hits} {normed}", bins[0], bins[1])
                .unwrap();
        }
    };
    let import_name = format!("{}.import.dist", opt.out);
    write_dist(&fracts.import_fracs, &import_name);
    let export_name = format!("{}.export.dist", opt.out);
    write_dist(&fracts.export_fracs, &export_name);

}


pub fn shock_distribution(
    network: &Network, 
    focus: usize, 
    export_frac: f64,
    iterations: usize
) -> ShockRes
{
    assert!(
        (0.0..=1.0).contains(&export_frac),
        "Invalid export fraction, has to be in range 0.0..=1.0"
    );
    let inverted = network.invert();
    let (import, export) = match network.direction{
        Direction::ExportTo => (&inverted, network),
        Direction::ImportFrom => (network, &inverted)
    };
    debug_assert!(import.direction.is_import());
    debug_assert!(export.direction.is_export());

    let calc_total = |net: &Network| -> Vec<f64> {
        net
            .nodes
            .iter()
            .map(
                |n| 
                {
                    n.adj
                        .iter()
                        .map(|e| e.amount)
                        .sum()
                }
            ).collect()
    };

    let original_exports = calc_total(export);
    let original_imports = calc_total(import);

    let mut current_export_frac = vec![1.0; original_exports.len()];
    current_export_frac[focus] = export_frac;
    let mut reduced_import_frac = vec![1.0; current_export_frac.len()];

    for _ in 0..iterations{
        for (index, n) in import.nodes.iter().enumerate(){
            reduced_import_frac[index] = 0.0;
            for e in n.adj.iter(){
                reduced_import_frac[index] += e.amount * current_export_frac[e.index];
            }
            reduced_import_frac[index] /= original_imports[index];
        }

        for index in 0..current_export_frac.len()
        {
            if index == focus{
                continue;
            }
            let missing_imports = (1.0 - reduced_import_frac[index]) * original_imports[index];
            let available_for_export = original_exports[index] - missing_imports;
            current_export_frac[index] = if available_for_export <= 0.0 {
                0.0
            } else {
                available_for_export / original_exports[index]
            };
        }
    }

    ShockRes { import_fracs: reduced_import_frac, export_fracs: current_export_frac }
}


pub struct ShockRes{
    pub import_fracs: Vec<f64>,
    pub export_fracs: Vec<f64>
}
