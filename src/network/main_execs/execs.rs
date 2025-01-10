use camino::{Utf8Path, Utf8PathBuf};
use itertools::Itertools;
use ordered_float::OrderedFloat;
use crate::{network::enriched_digraph::{LazyEnrichmentInfos, PRODUCTION, TOTAL_POPULATION}, parser::{country_map, id_map, parse_all_networks}, partition};
use fs_err::File;
use {
    std::{
        io::{BufWriter, Write, BufRead},
        collections::{BTreeSet, BTreeMap}, 
        fmt::Display,
        f64::consts::TAU,
        path::Path,
        num::*,
        cmp::Reverse
    },
    crate::network::{*, helper_structs::*},
    crate::{config::*, misc::*, parser},
    super::*,
    rayon::prelude::*,
    net_ensembles::sampling::*
};

const JSON_CREATION_ERROR: &str = "unable to create json";
const BINCODE_CREATION_ERROR: &str = "bincode serialization issue";

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



pub fn parse_beef_network(opt: BeefParser)
{
    let networks = crate::parser::parse_beef_network(opt.input);

    if opt.json{
        let buf = create_buf("beef.json");
        serde_json::to_writer_pretty(buf, &networks)
            .expect(JSON_CREATION_ERROR)
    } else {
        let buf = create_buf("beef.bincode");
        bincode::serialize_into(buf, &networks)
            .expect(BINCODE_CREATION_ERROR)
    }

}

pub fn parse_networks(opt: ParseNetworkOpt)
{
    let networks = crate::parser::network_parser(
        &opt.in_file, 
        &opt.item_code, 
        false,
        opt.read_type
    ).expect("unable to parse");

    let buf = create_buf(&opt.out);
    if opt.json{
        serde_json::to_writer_pretty(buf, &networks)
            .expect(JSON_CREATION_ERROR);
    } else {
        bincode::serialize_into(buf, &networks)
            .expect(BINCODE_CREATION_ERROR);
    }
    
}

pub fn max_diff_reported_import_vs_reported_export(opt: ImportExportDiffOpts)
{
    println!("Parsing imports");
    let import_networks = crate::parser::network_parser(
        &opt.in_file, 
        &opt.item_code, 
        true,
        ReadType::ImportQuantity
    ).expect("unable to parse");

    println!("Parsing exports");
    let export_networks = crate::parser::network_parser(
        &opt.in_file, 
        &opt.item_code, 
        true,
        ReadType::ExportQuantity
    ).expect("unable to parse");

    let id_map = opt.country_file
        .map(id_map);


    'outer: for (import, export) in import_networks.into_iter().zip(export_networks)
    {
        assert!(import.direction.is_import());
        assert!(export.direction.is_export());

        let export_map: BTreeMap<_, _> = export.nodes
            .iter()
            .enumerate()
            .map(
                |(idx, node)|
                {
                    (node.identifier.as_str(), idx)
                }
            ).collect();

        let mut diff_max = 0.0;
        let mut import_amount = 0.0;
        let mut respective_import_country_id = "";
        let mut respective_export_country_id = "";
        let mut export_amount = None;
        for import_node in import.nodes.iter()
        {
            let import_country_id = import_node.identifier.as_str();
            for import_edge in import_node.adj.iter(){
                let export_country_id = import.nodes[import_edge.index].identifier.as_str();
                
                match export_map.get(export_country_id){
                    None => {
                        // Not even the country is found?
                        let year = import.year;
                        eprintln!("Y {year} Country missing! Maybe it did not report it's exports?");
                        continue 'outer;
                        
                    },
                    Some(export_index) => {
                        let export_node = &export.nodes[*export_index];
                        let test_id = export_node.identifier.as_str();
                        assert_eq!(
                            export_country_id,
                            test_id
                        );
                        let mut success = false;
                        // now I need to find the edge that corresponds to this id
                        for export_edge in export_node.adj.iter(){
                            let import_id = export.nodes[export_edge.index].identifier.as_str();
                            let found =  import_id == import_country_id;
                            if found {
                                // check if this is really the correct edge
                                let import_id = export.nodes[export_edge.index].identifier.as_str();
                                assert_eq!(
                                    import_country_id,
                                    import_id
                                );
        
                                let difference = (import_edge.amount - export_edge.amount).abs();
                                if diff_max < difference{
                                    diff_max = difference;
                                    import_amount = import_edge.amount;
                                    export_amount = Some(export_edge.amount);
                                    respective_import_country_id = import_country_id;
                                    respective_export_country_id = export_country_id;
                                }
                                success = true;
                                break;
                            }
                        }

                        if !success{
                            // If the exporting country does not report the export, it is the same as if the reported export was 0
                            let diff = import_edge.amount.abs();
                            if diff > diff_max{
                                diff_max = diff;
                                export_amount = None;
                                import_amount = import_edge.amount;
                                respective_export_country_id = export_country_id;
                                respective_import_country_id = import_country_id;
                            }
                        }
                    }
                }
            }
        }
        let unit = import.unit.as_str();
        let year = import.year;
        assert_eq!(year, export.year);
        let (import_country, export_country) = match id_map.as_ref(){
            None => ("".to_owned(), "".to_owned()),
            Some(map) => {
                let import = map.get(respective_import_country_id)
                    .unwrap()
                    .to_owned();
                let export = map.get(respective_export_country_id)
                    .unwrap()
                    .to_owned();
                (import, export)
            }
        };
        println!("Y {year} Max Difference {diff_max} {unit}. Import: {import_amount} {import_country} Export: {export_amount:?} {export_country}");
    }
    
}

pub fn to_binary_all(opt: ParseAllNetworksOpt)
{
    let all = parse_all_networks(&opt.in_file, opt.read_type)
        .unwrap();
    println!("Found {} item codes", all.len());

    if opt.seperate_output {
        
        for (item_code, networks) in all.into_iter(){
            
            let output_name = format!("{item_code}.bincode");
            let buf = create_buf(output_name);
            bincode::serialize_into(buf, &networks)
                .expect(BINCODE_CREATION_ERROR);
        }
    } else {
        let name = "everything.bincode";
        println!("creating {name}");
        let buf = create_buf(name);
        bincode::serialize_into(buf, &all)
                .expect(BINCODE_CREATION_ERROR);
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

    let mut buf = create_buf_with_command_and_version(out);

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

    let mut buf = create_buf_with_command_and_version(out);
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

    let mut buf = create_buf_with_command_and_version(opt.out);

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

    let mut buf = create_buf_with_command_and_version(opt.out);
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
        let diam: Box<dyn Display> = match largest_scc_diameter{
            Some(dia) => Box::new(dia),
            None => Box::new("NaN")
        };
        res_map.insert("largest_scc_diameter", diam);

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
    let buf_writer = create_buf(opt.out);
    if opt.json{
        serde_json::to_writer_pretty(buf_writer, &enriched)
            .expect("unable to create json");
    } else {
        bincode::serialize_into(buf_writer, &enriched)
            .expect("unable to serialize");
    }
}

pub fn enrich_to_bin(opt: ParseEnrichOpts){
    let item_code = Some(opt.item_code);
    let mut enrichments: Vec<_> = opt.enrich_files
        .iter()
        .map(
            |s| 
            {
                println!("parsing {s}");
                crate::parser::parse_extra(s, &item_code)
            }   
        )
        .collect();
    let mut fused = enrichments.pop().unwrap();
    for e in enrichments{
        fused.fuse(&e);
    }

    let buf = create_buf(opt.out);
    if opt.json{
        serde_json::to_writer_pretty(buf, &fused)
            .unwrap();
    } else {
        bincode::serialize_into(buf, &fused)
            .unwrap();
    }

}

pub fn enrichment_to_json(opt: EnrichmentToJson)
{
    let mut enrichment = LazyEnrichmentInfos::Filename(opt.file, None);
    enrichment.assure_availability();
    let enrichment = enrichment.enrichment_infos_unchecked();
    let out = format!("{}.json", opt.out);
    let out = create_buf(out);
    serde_json::to_writer_pretty(out, enrichment).unwrap();
}

pub fn test_chooser(in_file: Utf8PathBuf, cmd: SubCommand){
    match cmd
    {
        SubCommand::OutComp(o) => out_comparison(in_file, o),
        SubCommand::FirstLayerOverlap(o) => first_layer_overlap(in_file, o),
        SubCommand::FirstLayerAll(a) => flow_of_top_first_layer(in_file, a),
        SubCommand::Flow(f) => super::flow(f, in_file),
        SubCommand::Shock(s) => super::shock_exec(s, in_file),
        SubCommand::CountryCount(c) => country_count(in_file, c),
        SubCommand::ShockAvail(s) => shock_avail(s, in_file),
        SubCommand::ShockDist(d) => shock_dist(d, in_file),
        SubCommand::ReduceX(o) => reduce_x(o, in_file),
        SubCommand::ReduceXTest(o) => reduce_x_test(o, in_file),
        SubCommand::CombineWorstIntegrals(opts) => crate::other_exec::worst_integral_sorting(opts),
        SubCommand::VolumeOrder(order_opt) => order_trade_volume(order_opt, in_file),
        SubCommand::Partition(opt) => partition(opt, in_file),
        SubCommand::BeefIds(opt) => crate::other_exec::beef_map_to_id(in_file, opt),
        SubCommand::Weights(opt) => calc_cor_weights(in_file, opt)
    }
}

fn calc_cor_weights(in_file: Utf8PathBuf, opt: CalcWeights)
{
    let population = LazyEnrichmentInfos::lazy_option(opt.population_file);
    let population_year = population.as_ref()
        .map(|population| population.get_year_unchecked(opt.year));
    let population_id = match population.as_ref()
    {
        None => u8::MAX,
        Some(population) => population.extra_info_idmap_unchecked().get(TOTAL_POPULATION)
    };

    let mut networks = LazyNetworks::Filename(in_file);
    networks.assure_availability();
    let raw_import = networks.get_import_network_unchecked(opt.year);
    let trading_import = raw_import.without_unconnected_nodes();

    let enrichment = LazyEnrichmentInfos::lazy_option(opt.enrichment);

    let production_id = enrichment
        .as_ref()
        .map(|e| e.extra_info_idmap_unchecked().get(PRODUCTION))
        .unwrap_or_default();
    
    if let Some(enrichment) = enrichment.as_ref()
    {
        let items_e = enrichment.get_item_codes_unchecked();
        let items_n = trading_import.sorted_item_codes.as_slice();
        assert_eq!(items_e, items_n);
    }

    let enriched_year = enrichment
        .as_ref()
        .map(|e| e.get_year_unchecked(opt.year));

    let item = trading_import.item_codes_as_string();
    let mut addition = match enriched_year{
        None => "None",
        Some(_) => "Enrich"
    }.to_owned();
    if population_year.is_some(){
        addition.push_str("_Population");
    }
    let out_name = format!("Item{item}_{addition}_Y{}_Weights.dat", opt.year);
    println!("Creating: {out_name}");
    let mut buf = create_buf_with_command_and_version(out_name);

    let mut header = vec![
        "CountryID",
        "Import"
    ];
    if enriched_year.is_some(){
        header.push(PRODUCTION);
    }
    write_slice_head(&mut buf, &header).unwrap();

    let mut unknown_population: Vec<&str> = Vec::new();

    for node in trading_import.nodes.iter(){
        let mut total_import = node.trade_amount();

        let mut p = enriched_year
            .and_then(|e| 
                e.get(&node.identifier)
                    .and_then(|extra| extra.map.get(&production_id).cloned())
            );

        if let Some(p_map) = population_year{
            match p_map.get(&node.identifier)
            {
                None => unknown_population.push(&node.identifier),
                Some(extra) => {
                    let p_amount = extra.map
                        .get(&population_id)
                        .unwrap()
                        .amount;
                    total_import /= p_amount;
                    if let Some(extra) = p.as_mut()
                    {
                        extra.amount /= p_amount;
                    }
                }
            }
        }
        
        write!(buf, "{} {}", node.identifier, total_import).unwrap();
        match p {
            None => writeln!(buf),
            Some(extra) => writeln!(buf, " {}", extra.amount)
        }.unwrap();
    }

    if !unknown_population.is_empty(){
        println!("Unknown Population for {} countries", unknown_population.len());
        dbg!(&unknown_population);
        if let Some(c_file) = opt.country_map_file{
            let country_map = parser::country_map(c_file);
            for c in unknown_population{
                println!("{}", country_map.get(c).unwrap())
            }
        }
    } else if opt.country_map_file.is_some(){
        println!("Country map is ignored since all relevant populations are known");
    }

}

fn order_trade_volume<P>(opt: OrderedTradeVolue, in_file: P)
where P: AsRef<Utf8Path>
{
    let in_file = in_file.as_ref();
    let map = opt
        .country_name_file
        .map(country_map);
    let mut head = vec![
            "order_index",
            "ID",
            "total_trade",
            "relative_trade",
            "running_sum",
            "running_relative"
        ];

    let mut total_head = vec![
        "order_index",
        "ID",
        "TotalTrade",
        "RelativeTrade",
        "RunningTotalTrade",
        "RunningRelativeTrade",
        "TotalImport",
        "TotalExport",
        "TotalImport/(TotalImport+TotalExport)"
    ];
    if map.is_some(){
        let c_n = "Country_name";
        head.push(c_n);
        total_head.push(c_n);
    }

    let write_id_or_newline = |id: &str, buf: &mut BufWriter<File>| 
    {
        match &map {
            Some(country_map) => {
                let name = country_map
                    .get(id)
                    .unwrap();
                writeln!(buf, " {name}")
            },
            None => writeln!(buf)
        }.unwrap();
    };

    let sort_fun = opt.ordering.get_order_fun();
    let total_trade_volume = |import: &Network, export: &Network|
    {
        assert_eq!(import.year, export.year);
        let import_without = import.without_unconnected_nodes();
        let export_without = export.without_unconnected_nodes();

        let mut trade = import_without.nodes.iter()
            .zip(export_without.nodes.iter())
            .map(
                |(import_node, export_node)|
                {
                    assert_eq!(import_node.identifier, export_node.identifier);
                    let total_import = import_node.trade_amount();
                    let total_export = export_node.trade_amount();
                    let total = total_export + total_import;
                    (total, total_import, total_export, import_node.identifier.as_str())
                }
            ).collect_vec();


        trade.sort_unstable_by(|a,b| sort_fun(a.0, b.0));
        let name = format!("{}_TotalTradeVolume_Y{}.dat", opt.output_stub, import.year);
        let mut buf = create_buf_with_command_and_version(name);
        write_slice_head(&mut buf, &total_head).unwrap();
        let total: f64 = trade.iter().map(|a| a.0).sum();
        let mut running_sum = 0.0;

        for (index, e) in trade.into_iter().enumerate()
        {
            let relative_export = e.0 / total;
            running_sum += e.0;
            let running_relative = running_sum / total;
            let import_frac = e.1/e.0;
            write!(
                buf, 
                "{index} {} {:e} {relative_export:e} {running_sum:e} {running_relative:e} {} {} {import_frac}",
                e.3,
                e.0,
                e.1,
                e.2
            ).unwrap();
            write_id_or_newline(e.3, &mut buf);
        }
    };
    

    let write_output = |name_addition, network: &Network|
    {
        let network_without_unconnected = network.without_unconnected_nodes();
        let mut sorted = network_without_unconnected.ordered_by_trade_volume(opt.ordering);
        let name = format!("{}_{name_addition}_Y{}.dat", opt.output_stub, network_without_unconnected.year);
        let mut buf = create_buf_with_command_and_version(name);
        write_slice_head(&mut buf, &head).unwrap();
        let total: f64 = sorted.iter()
            .map(|entry| entry.0)
            .sum();
        let mut running_sum = 0.0;

        if let Some(top) = opt.top {
            sorted.truncate(top.get());
        }

        let iter = sorted.into_iter().enumerate();

        for (order_index, (amount, node)) in iter {
            running_sum += amount;
            let running_relative = running_sum / total;
            let relative = amount / total;
            let id = node.identifier.as_str();
            write!(
                buf, 
                "{order_index} {id} {amount} {relative} {running_sum} {running_relative}"
            ).unwrap();
            write_id_or_newline(id, &mut buf);
        }
        
    };

    let mut lazy = LazyNetworks::Filename(in_file.to_owned());
    lazy.assure_availability();
    let import_str = "import";
    let export_str = "export";
    if let Some(year) = opt.year {
        let import = lazy.get_import_network_unchecked(year);
        write_output(import_str, import);
        let export = lazy.get_export_network_unchecked(year);
        write_output(export_str, export);
        total_trade_volume(import, export);
    } else {
        let all_writer = |name_addition: &'static str, network_slice: &[Network]|
        {
            network_slice
                .iter()
                .for_each(
                    |network| write_output(name_addition, network)
                )
        };
        let import = lazy.import_networks_unchecked();
        let export = lazy.export_networks_unchecked();
        all_writer(import_str, import);
        all_writer(export_str, export);
        import.iter().zip(export)
            .for_each(|(import, export)| total_trade_volume(import, export));

    }
    
}


pub fn country_count<P>(in_file: P, opt: CountryCountOpt)
where P: AsRef<Path>
{
    let networks = read_networks(in_file);

    let mut buf = create_buf_with_command_and_version(opt.out);
    writeln!(buf, "#Year Trading Exporter EdgeCount").unwrap();

    for n in networks{
        let mut without = n.without_unconnected_nodes();
        without.force_direction(Direction::ExportTo);
        let exporter = without.nodes.iter()
            .filter(|n| !n.adj.is_empty())
            .count();
        let edge_count: usize = without.nodes
            .iter()
            .map(|n| n.adj.len())
            .sum();
        writeln!(buf, "{} {} {exporter} {edge_count}", n.year, without.node_count())
            .unwrap();
    }
}

pub fn out_comparison(in_file: Utf8PathBuf, cmd: OutOpt){
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
    
    let mut buf = create_buf_with_command_and_version(&cmd.out);

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

pub fn first_layer_overlap<P>(in_file: P, cmd: FirstLayerOpt)
    where P: AsRef<Path>
{
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
        


    let overlap_name = format!("layer_overlap_{}", cmd.out);
    let mut buf_overlap = create_buf_with_command_and_version(overlap_name);
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
    let mut buf_size = create_buf_with_command_and_version(size_name);

    let d = match cmd.direction{
        Direction::ExportTo => "Export",
        Direction::ImportFrom => "Import"
    };
    writeln!(buf_size, "#layer1 index_of_parent {d}_amount_parent").unwrap();
    for (l, (index, amount)) in layers.iter().zip(ordering.iter()){
        writeln!(buf_size, "{} {index} {amount}", l.len()).unwrap();
    }

    if let Some(country_file) = cmd.print_graph{
        let map = Some(parser::country_map(country_file));
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

pub fn flow_of_top_first_layer<P: AsRef<Path>>(in_file: P, opt: FirstLayerOpt)
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
    let buf = create_buf(graph_name);

    let map = opt.print_graph
        .map(parser::country_map);

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


pub fn three_set_exec(opt: ThreeS)
{
    let map = opt.id_map_file
        .as_ref()
        .map(country_map);
    let sets: Vec<_> = opt.files.iter()
        .map(|f| to_three_sets(f, opt.border_low, opt.border_high))
        .collect();

    let all: BTreeSet<_> = sets.iter()
        .flat_map(|t| t.low.iter().chain(t.high.iter()).chain(t.middle.iter()))
        .map(|v| *v.0)
        .collect();

    let name_var = format!("{}.var", opt.out);
    let name_av = format!("{}.av", opt.out);
    let mut buf = create_buf_with_command_and_version(opt.out);
    let mut var_buf = create_buf_with_command_and_version(name_var);
    let mut av_buf = create_buf_with_command_and_version(name_av);
    write!(buf, "#").unwrap();
    
    for f in opt.files.iter()
    {
        write!(buf, " s:{f}").unwrap();
    }
    
    let h = if map.is_some(){
        " country_name"
    } else {
        ""
    };
    writeln!(var_buf, "#var country_id{h}").unwrap();
    writeln!(av_buf, "#av country_id{h}").unwrap();
    writeln!(buf, " low middle high total country_id{h}").unwrap();



    struct Tmp{
        low: u32,
        middle: u32,
        high: u32,
        c_idx: u32,
        deltas: Vec<f64>
    }

    let mut var_vec = Vec::new();
    let mut av_vec = Vec::new();

    let mut to_write = Vec::new();
    for i in all.iter(){
        let mut low = 0;
        let mut middle = 0;
        let mut high = 0;
        let mut deltas = Vec::new();
        
        for s in sets.iter() {
            let mut delta = f64::NAN;
            let mut t = 0;
            if let Some(v) = s.high.get(i){
                high += 1;
                t += 1;
                delta = *v;
            }
            if let Some(v) = s.middle.get(i){
                middle += 1;
                t += 1;
                delta = *v;
            }
            if let Some(v) = s.low.get(i){
                low += 1;
                t += 1;
                delta = *v;
            }
            deltas.push(delta);
            assert!(t <= 1);

        }
        let count = deltas
            .iter()
            .filter(|a| a.is_finite())
            .count();
        let sum: f64 = deltas.iter()
            .filter(|f| f.is_finite())
            .sum();
        let sum_sq: f64 = deltas.iter()
            .filter(|a| a.is_finite())
            .map(|a| a*a)
            .sum();
        let average = sum / count as f64;
        let var = sum_sq / count as f64 - average * average;

        av_vec.push((average, *i));
        var_vec.push((var, *i));

        let tmp = Tmp{
            low,
            middle,
            high,
            c_idx: *i,
            deltas
        };
        to_write.push(tmp);

    }

    let country_name_new_line = |b: &mut BufWriter<File>, country_id: u32|
    {
        if let Some(m) = &map
        {
            let s = country_id.to_string();
            let c = m.get(&s).unwrap();
            writeln!(b, " '{c}'").unwrap();
        } else {
            writeln!(b).unwrap();
        }
    };

    to_write
        .sort_by_cached_key(|e| e.middle*2+ e.low + e.high*3);

    let write_stat = |b: &mut BufWriter<File>, mut v: Vec<(f64, u32)>|
    {
        v.sort_unstable_by(|a,b| a.0.total_cmp(&b.0));
        for (val, country_id) in v
        {
            write!(b, "{val:e} {country_id}").unwrap();
            country_name_new_line(b, country_id);
        }
    };
    write_stat(&mut var_buf, var_vec);
    write_stat(&mut av_buf, av_vec);

    for l in to_write{

        for d in l.deltas{
            write!(buf, "{d} ").unwrap();
        }

        let total = l.low + l.middle + l.high;
        write!(
            buf, 
            "{} {} {} {total} {}",
            l.low,
            l.middle,
            l.high,
            l.c_idx
        ).unwrap();
        country_name_new_line(&mut buf, l.c_idx);
    }

    
}

#[derive(Default)]
pub struct ThreeSets{
    pub low: BTreeMap<u32, f64>,
    pub middle: BTreeMap<u32, f64>,
    pub high: BTreeMap<u32, f64>
}


pub fn to_three_sets(file: &str, border_low: f64, border_high: f64) -> ThreeSets
{
    let buf = open_bufreader(file);

    let lines = buf.lines()
        .map(|l| l.unwrap())
        .filter(|l| !l.starts_with('#'));

    let mut sets = ThreeSets::default();

    for (plot_idx, line) in lines.enumerate() {
        let (first, rest) = line.split_once(' ').unwrap();
        let (delta, country_id) = rest.split_once(' ').unwrap();
        let p_id: usize = first.parse().unwrap();
        assert_eq!(p_id, plot_idx);
        let delta: f64 = delta.parse().unwrap();
        let c_id: u32 = country_id.parse().unwrap();

        if delta.is_finite(){
            let set = if delta >= border_high {
                &mut sets.high
            } else if delta > border_low {
                &mut sets.middle
            } else {
                &mut sets.low
            };
            set.insert(c_id, delta);
        }
    }
    sets
}

pub fn print_network_info(opt: OnlyNetworks)
{
    fn print_info(n: &Network, top: Option<NonZeroU32>, identifier: &[String])
    {
        let without_unconnected = n.without_unconnected_nodes();
        println!(
            "Unit: {} DataOrigin {:?} Year {} Direction {:?} #TradingNodes: {}", 
            n.unit, 
            n.data_origin, 
            n.year,
            n.direction,
            without_unconnected.node_count()
        );
        if let Some(t) = top{
            println!("TOP:");
            let mut list = without_unconnected
                .nodes
                .iter()
                .map(
                    |n|
                    {
                        (
                            OrderedFloat(n.trade_amount()),
                            &n.identifier
                        )

                    }
                ).collect_vec();
            list.sort_unstable_by_key(|item| Reverse(item.0));
            for (trade, id) in list.iter().take(t.get() as usize){
                let idx = without_unconnected.get_index(id).unwrap();
                let node = &without_unconnected.nodes[idx];
                print!(
                    "ID: {id}, trade_amount: {trade} "
                );
                node.print_infos(&without_unconnected);
                println!();
            }
        }

        if !identifier.is_empty()
        {
            println!("Identifier:");
        }

        'outer: for id in identifier{
            for node in n.nodes.iter(){
                if node.identifier.as_str() == id {
                    node.print_infos(n);
                    println!();
                    continue 'outer;
                } 
            }
            eprintln!("Could not find ID {id}");
        }
        
    }

    let mut networks = LazyNetworks::Filename(opt.in_file);
    networks.assure_availability();
    println!("Export:");
    if let Some(y) = opt.year{
        let network = networks.get_export_network_unchecked(y);
        print_info(network, opt.top, &opt.ids);
    } else {
        let export = networks.export_networks_unchecked();
        export.iter().for_each(|e| print_info(e, opt.top, &opt.ids));
    }

    println!("Import:");
    if let Some(y) = opt.year{
        let network = networks.get_import_network_unchecked(y);
        print_info(network, opt.top, &opt.ids);
    } else {
        let import = networks.import_networks_unchecked();
        import.iter().for_each(|i| print_info(i, opt.top, &opt.ids));
    }
}