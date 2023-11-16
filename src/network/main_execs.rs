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
    assert_same_direction_write_direction(networks, &mut buf);

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
        1986, 
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
        }
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
        let green = u8::MAX - (255.0 * 2.0 * we).min(255.0) as u8;
        let blue = u8::MAX - (255.0 * 4.0 * we).min(255.0) as u8;
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