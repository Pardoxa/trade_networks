use std::collections::*;
use crate::{network::*, UNIT_TESTER};
use crate::network::enriched_digraph::*;
use std::ops::Deref;
use crate::config::*;
use std::fs::File;
use std::io::{BufWriter, Write};
use crate::misc::*;
use net_ensembles::sampling::{HistF64, Histogram};


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
    let info_map = crate::network::enriched_digraph::GLOBAL_NODE_INFO_MAP.deref();
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
    pub total: Vec<f64>,
    pub imports: Vec<f64>
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
    writeln!(buf, "#index import_frac export_frac country").unwrap();

    for (index, (import, export)) in fracts.import_fracs.iter().zip(fracts.export_fracs.iter()).enumerate()
    {
        let c = network.nodes[index].identifier.as_str();
        writeln!(buf, "{index} {import} {export} {c}").unwrap()
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

    let original_exports = calc_acc_trade(export);
    let original_imports = calc_acc_trade(import);

    let mut current_export_frac = vec![1.0; original_exports.len()];
    current_export_frac[focus] = export_frac;
    let mut reduced_import_frac = vec![1.0; current_export_frac.len()];

    for _ in 0..iterations{
        for (index, n) in import.nodes.iter().enumerate(){
            reduced_import_frac[index] = 0.0;
            if original_imports[index] == 0.0{
                assert_eq!(n.adj.len(), 0);
                continue;
            }
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

fn calc_acc_trade(network: &Network) -> Vec<f64>
{
    network
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
}

#[derive(Debug)]
pub struct ShockRes{
    pub import_fracs: Vec<f64>,
    pub export_fracs: Vec<f64>
}

pub fn shock_avail(opt: ShockAvailOpts, in_file: &str){
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

    let enrich_infos = crate::parser::parse_extra(
        &opt.enrich_file, 
        &opt.item_code
    );

    let enrich = enrich_infos.get_year(opt.year);

    let node_info_map = GLOBAL_NODE_INFO_MAP.deref();

    let avail_after_shock = calc_available(
        &network, 
        enrich, 
        &fracts, 
        node_info_map
    );

    let no_shock = ShockRes{
        import_fracs: vec![1.0; fracts.import_fracs.len()],
        export_fracs: vec![1.0; fracts.import_fracs.len()]
    };

    let available_before_shock = calc_available(
        &network, 
        enrich, 
        &no_shock, 
        node_info_map
    );

    let file = File::create(opt.out)
        .unwrap();
    let mut buf = BufWriter::new(file);
    write_commands_and_version(&mut buf).unwrap();
    writeln!(buf, "#idx before_shock after_shock country").unwrap();

    for (idx, n) in network.nodes.iter().enumerate()
    {
        writeln!(buf, 
            "{idx} {} {} {} {}",
            available_before_shock[idx],
            avail_after_shock[idx],
            available_before_shock[idx] - avail_after_shock[idx],
            n.identifier
        ).unwrap();
    }

}


fn calc_available(
    network: &Network,
    enrich: &BTreeMap<String, ExtraInfo>,
    shock: &ShockRes,
    node_map: &NodeInfoMap
) -> Vec<f64>
{
    let inverted = network.invert();
    let (import, export) = match network.direction{
        Direction::ExportTo => (&inverted, network),
        Direction::ImportFrom => (network, &inverted)
    };

    let original_import = calc_acc_trade(import);
    let original_export = calc_acc_trade(export);


    let production_id = node_map.get("Production");
    let mut at_least_some_production = false;
    let unit = &import.unit;
    let unit_tester = UNIT_TESTER.deref();

    let stock_id = node_map.get("Stocks");
    let mut at_least_some_stock = false;

    

    let res =(0..original_export.len())
        .map(
            |i|
            {
                let import_node = &import.nodes[i];
                let imported = original_import[i] * shock.import_fracs[i];
                let exported = original_export[i] * shock.export_fracs[i];

                let mut total = imported - exported;


                if let Some(extra) = enrich.get(import_node.identifier.as_str()){
                    if let Some(production) = extra.map.get(&production_id){
                        assert!(unit_tester.is_equiv(unit, &production.unit));
                        total += production.amount;
                        at_least_some_production = true;
                    }
                    if let Some(stock) = extra.map.get(&stock_id){
                        assert!(unit_tester.is_equiv(unit, &stock.unit));
                        total += stock.amount;
                        at_least_some_stock = true;
                    }

                    // TODO Stock variation
                }
                // think about what to do if total is negative
                if total < 0.0 {
                    eprintln!("small total! {total}");
                }

                total
            }
        ).collect();

    assert!(at_least_some_production, "No production data!");
    assert!(at_least_some_stock, "No Stock data!");
    
    res

}