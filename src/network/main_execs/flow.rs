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
    let mut buf = create_buf(name);

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

    ShockRes { 
        import_fracs: reduced_import_frac, 
        export_fracs: current_export_frac
    }
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

pub struct CalculatedShocks{
    pub available_before_shock: Vec<f64>,
    pub available_after_shock: Vec<f64>,
    pub focus_index: usize,
    pub network: Network,
    pub item_code: String
}


#[derive(Clone)]
pub enum TopSpecifier{
    Id(String),
    Rank(usize),
    RankRef(TopSpecifierHelper)
}

impl TopSpecifier{
    pub fn get_string(&self) -> String
    {
        match self{
            Self::Id(id) => format!("ID{}", id),
            Self::Rank(r) => format!("Rank{r}"),
            Self::RankRef(r) => format!("Rank{}Ref{}", r.focus, r.reference)
        }
    }

    pub fn get_short_str(&self) -> String {
        match self{
            Self::Id(_) => "ID".to_owned(),
            Self::Rank(_) => "Rank".to_owned(),
            Self::RankRef(r) => format!("RankRef{}", r.reference)
        }
    }
}

#[derive(Clone, Copy)]
pub struct TopSpecifierHelper{
    pub focus: usize,
    pub reference: usize
}

pub fn calc_shock(
    in_file: &str, 
    year: i32, 
    top_id: TopSpecifier, 
    export_frac: f64,
    iterations: usize,
    enrich_file: &str,
    target_item_code: &Option<String>
) -> CalculatedShocks
{
    let networks = read_networks(in_file);

    let mut network = None;
    for n in networks{
        if n.year == year {
            network = Some(n);
            break;
        }
    }
    let network = network
        .expect("could not find specified year")
        .without_unconnected_nodes();

    let get_sorting = ||
    {
        let export = network.get_network_with_direction(Direction::ExportTo);
        let all = calc_acc_trade(&export);
        let mut for_sorting: Vec<_> = all.into_iter()
            .enumerate()
            .collect();
        for_sorting
            .sort_unstable_by(|a,b| b.1.total_cmp(&a.1));
        assert!(for_sorting.windows(2).all(|s| s[0].1 >= s[1].1));
        for_sorting
    };

    let (focus, export_frac) = match top_id{
        TopSpecifier::Id(id) => {
            let focus = network.nodes
                .iter()
                .position(|item| item.identifier == id)
                .unwrap();
            (focus, export_frac)
        },
        TopSpecifier::Rank(r) => {
            let sorted = get_sorting();

            let focus = sorted[r].0;
            (focus, export_frac)
            
        },
        TopSpecifier::RankRef(r) => {
            let sorted = get_sorting();

            let wanted_ref_export = export_frac * sorted[r.reference].1;
            let wanted_export_reduction = sorted[r.reference].1 - wanted_ref_export;
            let possible_export = sorted[r.focus].1;
            let reduced_export = possible_export - wanted_export_reduction;
            let frac = reduced_export / possible_export;
            dbg!(frac);

            (sorted[r.focus].0, frac)
        }
    };

    let fracts = shock_distribution(
        &network, 
        focus, 
        export_frac, 
        iterations
    );

    let enrich_infos = crate::parser::parse_extra(
        enrich_file, 
        target_item_code
    );

    let enrich = enrich_infos.get_year(year);

    let node_info_map = NodeInfoMap::from_slice(enrich_infos.possible_node_info.as_slice());

    let avail_after_shock = calc_available(
        &network, 
        enrich, 
        &fracts, 
        &node_info_map
    );

    let no_shock = ShockRes{
        import_fracs: vec![1.0; fracts.import_fracs.len()],
        export_fracs: vec![1.0; fracts.import_fracs.len()]
    };

    let available_before_shock = calc_available(
        &network, 
        enrich, 
        &no_shock, 
        &node_info_map
    );

    let shock_amount = avail_after_shock[focus] - available_before_shock[focus];
    println!("SHOCK AMOUNT: {shock_amount}");
    let actual_export: f64 = network.get_network_with_direction(Direction::ExportTo)
        .nodes[focus]
        .adj
        .iter()
        .map(|a| a.amount * fracts.export_fracs[focus])
        .sum();
    println!("Export: {actual_export} fraction {}", fracts.export_fracs[focus]);

    CalculatedShocks { 
        available_after_shock: avail_after_shock,
        available_before_shock,
        focus_index: focus,
        network,
        item_code: enrich_infos.item_code
    }
}

pub fn shock_avail(opt: ShockAvailOpts, in_file: &str){
    let res = calc_shock(
        in_file, 
        opt.year, 
        TopSpecifier::Id(opt.top_id), 
        opt.export, 
        opt.iterations, 
        &opt.enrich_file, 
        &opt.item_code
    );
    
    let available_before_shock = res.available_before_shock;
    let avail_after_shock = res.available_after_shock;
    let focus = res.focus_index;

    let total_before: f64 = available_before_shock.iter().sum();
    let total_after: f64 = avail_after_shock.iter().sum();
    let total_missing = avail_after_shock[focus] - available_before_shock[focus];

    println!("Missing amount: {total_missing:e}");
    println!("Missing fract: {}", total_missing / total_before);
    println!("before: {total_before:e}");
    println!("after: {total_after:e}");
    println!("difference: {:e}", total_before - total_after);

    let file = File::create(opt.out)
        .unwrap();
    let mut buf = BufWriter::new(file);
    write_commands_and_version(&mut buf).unwrap();
    writeln!(buf, "#idx before_shock after_shock country").unwrap();

    for (idx, n) in res.network.nodes.iter().enumerate()
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


pub fn shock_dist(opt: ShockDistOpts, in_file: &str)
{
    let hist_header = "#left right center hits normalized";
    let specifiers: Vec<_> = match &opt.top{
        CountryChooser::TopId(id) => vec![TopSpecifier::Id(id.id.clone())],
        CountryChooser::Top(t) => {
            (0..t.top.get())
                .map(TopSpecifier::Rank)
                .collect()
        },
        CountryChooser::TopRef(t) =>
        {
            (0..t.top.get())
                .map(|i| 
                    TopSpecifier::RankRef(
                        TopSpecifierHelper { focus: i, reference: t.top.get()-1 }
                    )
                )
                .collect()
        }
    };

    let mut names = Vec::new();
    let mut gp_names = Vec::new();
    let mut is_first = true;

    let mut bins = None;
    let mut hists: Vec<_> = opt.export.iter()
        .map(|_| 
            vec![0; opt.bins]
        ).collect();

    let mut item_code = opt.item_code.clone();

    for s in specifiers.iter(){
        let mut v = Vec::new();
        
        for (e_index, &e) in opt.export.iter().enumerate(){
            println!("E: {e}");
            let res = calc_shock(
                in_file, 
                opt.year, 
                s.clone(), 
                e, 
                opt.iterations, 
                &opt.enrich_file, 
                &opt.item_code
            );

            let name_stub = format!(
                "{}_item{}_y{}_e{e}.dat", 
                s.get_string(), 
                res.item_code, 
                opt.year
            );

            v.push(name_stub);
            let name = v.last().unwrap();
            if is_first{
                gp_names.push(
                    format!(
                        "{}_item{}_y{}_e{e}", 
                        s.get_short_str(), 
                        res.item_code, 
                        opt.year
                    )
                );
            }

            if item_code.is_none(){
                item_code = Some(res.item_code);
            }
            
            let mut hist = HistF64::new(-1.0, 1.0 + f64::EPSILON, opt.bins)
                .unwrap();
            if bins.is_none(){
                let b: Vec<_> = hist.bin_iter()
                    .copied()
                    .collect();
                bins = Some(b);
            }
        
            for i in 0..res.available_after_shock.len(){
                // check if focus county is to be counted in hist
                if opt.without && i == res.focus_index{
                    // skip focus country
                    continue;
                }
                // fraction of missing product after shock, negative to show that it is removed
                let delta = (res.available_after_shock[i] - res.available_before_shock[i])
                    / res.available_before_shock[i];
                if delta > 1.0 {
                    println!("{delta}");
                }
                hist.increment(delta).unwrap();
            }
        
            let mut buf = create_buf_with_command_and_version(name);
            writeln!(buf, "{hist_header}").unwrap();
            let total: usize = hist.hist().iter().sum();
        
            for (bin, hits) in hist.bin_hits_iter(){
                let center = (bin[0] + bin[1]) / 2.0;
                let normalized = hits as f64 / total as f64;
                writeln!(
                    buf,
                    "{} {} {center} {hits} {normalized}",
                    bin[0],
                    bin[1]
                ).unwrap()
            }

            hists[e_index]
                .iter_mut()
                .zip(
                    hist.hist()
                        .iter()
                ).for_each(
                    |(this, other)|
                    {
                        *this += *other
                    }
                );
        }
        names.push(v);
        is_first = false;
    }

    let bins = bins.unwrap();
    let relative = matches!(opt.top, CountryChooser::TopRef(_));
    let mut combined_names = Vec::new();

    for (i, e) in opt.export.iter().enumerate(){
        let r = if relative{
            Relative::YesWith(*e)
        } else {
            Relative::No
        };
        let name_iter = names
            .iter()
            .enumerate()
            .map(
                |(index, name_vec)|
                {
                    let e_name = &name_vec[i];
                    let title = format!("top {index}");
                    GnuplotHelper { file_name: e_name, title}
                }
            );
        write_gnuplot(
            &gp_names[i], 
            r, 
            name_iter
        ).unwrap();

        if names.len() > 1 {
            // write fused hist
            let hist = hists[i].as_slice();
            let total: usize = hist.iter().sum();

            let name = format!("{}.combined", &gp_names[i]);
            let mut buf = create_buf_with_command_and_version(&name);
            writeln!(buf, "{hist_header}").unwrap();

            for (bin, &hits) in bins.iter().zip(hist)
            {
                let center = (bin[0] + bin[1]) / 2.0;
                let normalized = hits as f64 / total as f64;
                writeln!(
                    buf,
                    "{} {} {center} {hits} {normalized}",
                    bin[0],
                    bin[1]
                ).unwrap()
            }

            let iter = std::iter::once(
                GnuplotHelper { file_name: &name, title: format!("combined {}", names.len()) }
            );
            write_gnuplot(
                &name, 
                r, 
               iter
            ).unwrap();

            combined_names.push(name);
        }
    }

    if !combined_names.is_empty(){
        let item_code = item_code.unwrap();
        let name = format!("{}_combined_Item{}", opt.top.get_string(), item_code);
        let iter = combined_names
            .iter()
            .zip(opt.export.iter())
            .map(
                |(n, e)|
                {
                    GnuplotHelper{file_name: n, title: format!("Export {e}")}
                }
            );
        let r = if relative{
            Relative::Yes
        } else {
            Relative::No
        };
        write_gnuplot(
            &name, 
            r, 
            iter
        ).unwrap();
    }
    
}

#[derive(Debug, Clone, Copy)]
pub enum Relative{
    YesWith(f64),
    Yes,
    No
}

pub struct GnuplotHelper<'a>{
    pub file_name: &'a str,
    pub title: String
}

fn write_gnuplot<'a, I>(gnuplot_name_stub: &'a str, relative: Relative, name_iter: I) -> std::io::Result<()>
where I: IntoIterator<Item = GnuplotHelper::<'a>>
{
    let gnuplot_name = format!("{gnuplot_name_stub}.gp");
    let mut buf = create_buf_with_command_and_version(gnuplot_name);
    writeln!(buf, "reset session")?;
    writeln!(buf, "set t pdfcairo")?;
    writeln!(buf, "set xrange [-1:0.1]")?;
    writeln!(buf, "set xlabel \"Δ\"")?;
    writeln!(buf, "set ylabel \"normalized hits\"")?;
    writeln!(buf, "set key center")?;

    match relative{
        Relative::YesWith(e) => {
            writeln!(buf, "set title \"relative {}\"", e)?;
        },
        Relative::Yes => {
            writeln!(buf, "set title \"relative\"")?;
        },
        _ => ()
    };
    
    writeln!(buf, "set output \"{gnuplot_name_stub}.pdf\"")?;
    write!(buf, "p ")?;
    for helper in name_iter{
        writeln!(
            buf, 
            "\"{}\" u 3:5 w boxes t \"{}\",\\", 
            helper.file_name, 
            helper.title
        )?
    }
    writeln!(buf, "\nset output")
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
    if !at_least_some_stock{
        eprintln!("WARNING: NO STOCK DATA")
    }
    eprintln!("Stock Variation data is unimplemented!");
    
    res

}