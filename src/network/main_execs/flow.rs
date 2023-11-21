use std::collections::*;
use crate::network::*;
use crate::network::enriched_digraph::ExtraInfo;
use std::ops::Deref;
use crate::config::*;
use std::fs::File;
use std::io::{BufWriter, Write};


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