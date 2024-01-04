use{
    std::{
        collections::*,
        path::Path,
        fmt::Display,
        ops::{
            Deref, 
            RangeInclusive
        },
        fs::File,
        io::{
            BufWriter, 
            Write
        }
    },
    crate::{
        parser::country_map,
        network::*, 
        UNIT_TESTER,
        network::enriched_digraph::*,
        config::*,
        misc::*
    },
    net_ensembles::sampling::{
        HistF64, 
        Histogram
    }
};

const HIST_HEADER: [&str; 5] = ["left", "right", "center", "hits", "normalized"];

fn derivative(data: &[f64]) -> Vec<f64>
{
    let mut d = vec![f64::NAN; data.len()];
    if data.len() >= 3 {
        for i in 1..data.len()-1 {
            d[i] = (data[i+1] - data[i-1]) / 2.0;
        }
    }
    if data.len() >= 2 {
        d[0] = data[1] - data[0];

        d[data.len() - 1] = data[data.len() - 1] - data[data.len() - 2];
    }
    d
}

fn integrate(slice: &[f64], delta: f64) -> f64
{
    let factor = delta * 0.5;
    slice.windows(2)
        .map(|w| (w[0] + w[1]) * factor)
        .sum()
}

pub fn flow<P>(opt: FlowOpt, in_file: P)
    where P: AsRef<Path>
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
    let info_idx = info_map.get(PRODUCTION);
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

pub fn shock_exec<P>(opt: ShockOpts, in_file: P)
    where P: AsRef<Path>
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


    let focus = network.get_index(&opt.top_id).unwrap();

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
        write_slice_head(&mut buf, HIST_HEADER).unwrap();
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
    after_export_fract: Vec<f64>
}

impl CalculatedShocks{
    /// fraction of missing product after shock, negative to show that it is removed
    pub fn delta_iter(&'_ self) -> impl Iterator<Item = f64> + '_
    {
        self.available_after_shock
            .iter()
            .zip(self.available_before_shock.iter())
            .map(
                |(after, before)|
                    (after - before) / before
            )
    }

    /// fraction of missing product after shock, negative to show that it is removed
    pub fn delta_or_nan_iter(&'_ self) -> impl Iterator<Item = f64> + '_
    {
        self.available_after_shock
            .iter()
            .zip(self.available_before_shock.iter())
            .map(
                |(after, before)|{
                    if *before < 0.0 {
                        f64::NAN
                    } else {
                        (after - before) / before
                    }
                }
            )
    }

    pub fn choose_delta_iter(&'_ self, nan_for_neg_before: bool) -> Box<dyn Iterator<Item = f64> + '_> 
    {
        if nan_for_neg_before{
            Box::new(self.delta_or_nan_iter())
        } else {
            Box::new(self.delta_iter())
        }
    }
}



pub fn calc_shock(
    lazy_network: &mut LazyNetworks, 
    year: i32, 
    top_id: TopSpecifier, 
    export_frac: f64,
    iterations: usize,
    lazy_enrichment: &mut LazyEnrichmentInfos,
) -> CalculatedShocks
{
    lazy_network.assure_availability();
    let export = lazy_network
        .get_export_network_unchecked(year)
        .without_unconnected_nodes();

    let get_sorting = ||
    {
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
            let focus = export.get_index(&id).unwrap();
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
        &export, 
        focus, 
        export_frac, 
        iterations
    );

    lazy_enrichment.assure_availability();
    let enrichment_infos = lazy_enrichment.enrichment_infos_unchecked();
    let enrich = enrichment_infos.get_year(year);

    let node_info_map = lazy_enrichment.extra_info_idmap_unchecked();

    let avail_after_shock = calc_available(
        &export, 
        enrich, 
        &fracts, 
        &node_info_map
    );

    let no_shock = ShockRes{
        import_fracs: vec![1.0; fracts.import_fracs.len()],
        export_fracs: vec![1.0; fracts.import_fracs.len()]
    };

    let available_before_shock = calc_available(
        &export, 
        enrich, 
        &no_shock, 
        &node_info_map
    );

    let shock_amount = avail_after_shock[focus] - available_before_shock[focus];
    println!("SHOCK AMOUNT: {shock_amount}");
    let actual_export: f64 = export
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
        network: export,
        after_export_fract: fracts.export_fracs
    }
}

pub fn shock_avail<P>(opt: ShockAvailOpts, in_file: P)
where P: AsRef<Path>
{
    let mut lazy_network = LazyNetworks::Filename(in_file.as_ref().to_owned());
    let mut lazy_enrichment = LazyEnrichmentInfos::Filename(opt.enrich_file.clone(), opt.item_code.clone());
    let res = calc_shock(
        &mut lazy_network, 
        opt.year, 
        TopSpecifier::Id(opt.top_id), 
        opt.export, 
        opt.iterations, 
        &mut lazy_enrichment
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

fn write_res<I, A>(buf: &mut BufWriter<File>, e: f64, iter: I)
where I: IntoIterator<Item = A>,
    A: Display
{
    write!(buf, "{e}").unwrap();
    for v in iter{
        write!(buf, " {v}").unwrap();
    }
    writeln!(buf).unwrap();
}


pub fn reduce_x<P>(opt: XOpts, in_file: P)
where P: AsRef<Path>
{
    let specifiers: Vec<_> = opt.top.get_specifiers();

    let mut lazy_networks = LazyNetworks::Filename(in_file.as_ref().to_owned());
    lazy_networks.assure_availability();
    let export_without_unconnected = lazy_networks
        .get_export_network_unchecked(opt.year)
        .without_unconnected_nodes();
    let import_without_unconnected = export_without_unconnected.invert();

    let mut lazy_enrichments = LazyEnrichmentInfos::Filename(
        opt.enrich_file, 
        opt.item_code
    );
    lazy_enrichments.assure_availability();

    let stub = opt.top.get_string();
    let stub = format!(
        "{stub}_{}", 
        lazy_enrichments.item_codes_as_string_unchecked()
    );

    let country_map = opt.country_map
        .as_deref()
        .map(country_map);

    fn c_map<'a>(id: &str, country_map: &'a Option<BTreeMap<String, String>>) -> &'a str
    {
        if let Some(map) = country_map{
            map.get(id).unwrap()
        } else {
            ""
        }
    }

    opt.investigate
        .iter()
        .for_each(
            |&i|
            {
                let internal_idx = opt.invest_type
                    .get_interal_index(&export_without_unconnected, i);
                let node = &export_without_unconnected.nodes[internal_idx];
                let id = &node.identifier;
                let name = format!("{stub}_investigate{id}.info");
                let mut b = create_buf_with_command_and_version(name);
                
                writeln!(
                    b, 
                    "Investigating Country {} {}\nExports:", 
                    id,
                    c_map(id, &country_map)
                ).unwrap();
                serde_json::to_writer_pretty(&mut b, node)
                    .unwrap();
                writeln!(b).unwrap();
                let write_country_names = |node: &Node, buf: &mut BufWriter<File>|
                {
                    if country_map.is_some(){
                        for e in node.adj.iter(){
                            let e_id = &export_without_unconnected.nodes[e.index].identifier;
                            writeln!(
                                buf, 
                                "index {} -> id {} -> name {}",
                                e.index,
                                e_id,
                                c_map(e_id, &country_map)
                            ).unwrap();
                        }
                    }
                };
                    
                write_country_names(node, &mut b);

                writeln!(b, "Imports:").unwrap();
                let node = &import_without_unconnected.nodes[internal_idx];
                serde_json::to_writer_pretty(&mut b, node).unwrap();
                let extra_map = lazy_enrichments
                    .get_year_unchecked(opt.year);
                
                let extra = extra_map.get(id);
                writeln!(b, "\nExtra:").unwrap();
                serde_json::to_writer_pretty(&mut b, &extra).unwrap();
                writeln!(b).unwrap();
                write_country_names(node, &mut b);
            }
        );

    let var_name = format!("{stub}var.dat");
    let mut buf_var = create_buf_with_command_and_version(&var_name);

    let av_name = format!("{stub}av.dat");
    let mut buf_av = create_buf_with_command_and_version(&av_name);

    let av_d_name = format!("{stub}av_derivative.dat");
    let mut buf_av_d = create_buf_with_command_and_version(&av_d_name);

    let max_name = format!("{stub}max.dat");
    let mut buf_max = create_buf_with_command_and_version(&max_name);

    let min_name = format!("{stub}min.dat");
    let mut buf_min = create_buf_with_command_and_version(&min_name);

    let abs_name = format!("{stub}min_max_abs.dat");
    let mut buf_abs = create_buf_with_command_and_version(&abs_name);

    let import_name = format!("{stub}import.dat");
    let mut buf_import = create_buf_with_command_and_version(&import_name);

    let import_totals_name = format!("{stub}import_totals.dat");
    let mut buf_import_totals = create_buf_with_command_and_version(&import_totals_name);

    let distance_name = format!("{stub}distance_to_top0.dat");
    let mut buf_top0_dist = create_buf_with_command_and_version(&distance_name);

    let write_header = |buf: &mut BufWriter<File>|
    {
        write!(buf, "#IDX_1_Export_frac").unwrap();
        export_without_unconnected.nodes
            .iter()
            .zip(2..)
            .for_each(
                |(n, i)|
                {
                    write!(buf, " GP{i}_ID{}", n.identifier).unwrap();
                }
            );
        writeln!(buf).unwrap();
    };
    write_header(&mut buf_av);
    write_header(&mut buf_var);
    write_header(&mut buf_max);
    write_header(&mut buf_min);
    write_header(&mut buf_abs);
    write_header(&mut buf_av_d);
    write_header(&mut buf_import);
    write_header(&mut buf_import_totals);
    write_header(&mut buf_top0_dist);

    let export_diff = opt.export_end - opt.export_start;
    let export_delta = export_diff / (opt.export_samples - 1) as f64;


    let export_vals: Vec<_> = (0..opt.export_samples-1)
        .map(|i| opt.export_start + export_delta * i as f64)
        .chain(std::iter::once(opt.export_end))
        .collect();

    let len_recip = (specifiers.len() as f64).recip();
    let mut is_first = true;
    let mut foci = Vec::new();
    let mut dist_names = Vec::new();
    let mut min_names = Vec::new();
    let mut max_names = Vec::new();

    let mut av_matrix: Vec<Vec<f64>> = (0..export_without_unconnected.node_count())
        .map(|_| Vec::new())
        .collect();

    let mut all_deltas: Vec<Vec<Vec<f64>>> = (0..export_vals.len())
        .map(
            |_| 
            {
                (0..specifiers.len())
                    .map(|_| Vec::new())
                    .collect()
            }
        ).collect();

    for e in export_vals.iter().copied()
    {
        let mut sum = vec![0.0; export_without_unconnected.node_count()];
        let mut sum_sq = vec![0.0; sum.len()];
        let mut max = vec![f64::NEG_INFINITY; sum.len()];
        let mut min = vec![f64::INFINITY; sum.len()];
        let mut after_shock_avail_total = vec![0.0; sum.len()];
        let mut is_top = true;
        for (s_index, s) in specifiers.iter().enumerate(){
            let res = calc_shock(
                &mut lazy_networks, 
                opt.year, 
                s.clone(), 
                e, 
                opt.iterations, 
                &mut lazy_enrichments
            );

            let iter = res
                .choose_delta_iter(opt.forbid_negative_total);

            iter.zip(all_deltas.iter_mut())
                .for_each(
                    |(delta, list_of_lists)|
                    {
                        let correct_list = &mut list_of_lists[s_index];
                        correct_list.push(delta);
                    }
                );

            if is_top{
                is_top = false;
                let mut export = export_without_unconnected.clone();
                // remove links that are no linger present
                export
                    .nodes
                    .iter_mut()
                    .zip(res.after_export_fract.iter())
                    .for_each(
                        |(node, &export)|
                        {
                            if export <= 0.0 {
                                node.adj.clear();
                            }
                        }
                    );
                let distances = export.distance_from_index(res.focus_index);
                let dist_iter = distances
                    .into_iter()
                    .map(
                        |v|
                        v.map_or_else(
                            || -> Box<dyn Display> {Box::new(-0.5_f32)},
                            |v| -> Box<dyn Display> {Box::new(v)} 
                        )
                        // The below was my first approach and is 
                        // basically equivalent to the one above.
                        // Only reason for the one above: I wanted to see if I can do it with closures
                        /*
                        let boxed: Box<dyn Display> = match v{
                            Some(val) => Box::new(val),
                            None => Box::new(-0.5_f64)
                        };*/
                    );
                write_res(&mut buf_top0_dist, e, dist_iter);
            }

            res.available_after_shock.iter()
                .zip(after_shock_avail_total.iter_mut())
                .for_each(|(v, acc)| *acc += v);

            if is_first{
                foci.push(res.focus_index);
            }

            let iter = res
                .choose_delta_iter(opt.forbid_negative_total);

            for (i, delta) in iter.enumerate()
            {
                sum[i] += delta;
                sum_sq[i] += delta * delta;
                max[i] = delta.max(max[i]);
                min[i] = delta.min(min[i]);
            }

            let import_iter = import_without_unconnected.nodes
                .iter()
                .map(
                    |node|
                    {
                        node.adj
                            .iter()
                            .filter(|edge| res.after_export_fract[edge.index] > 0.0)
                            .count()
                    }
                );
            write_res(&mut buf_import, e, import_iter);

        }
        is_first = false;
        sum.iter_mut()
            .for_each(|v| *v *= len_recip);
        let average = sum;
        let variance = sum_sq
            .into_iter()
            .zip(average.iter())
            .map(
                |(sq, a)|
                {
                    sq * len_recip - a*a
                }
            );

        let abs: Vec<_> = min
            .iter()
            .zip(max.iter())
            .map(|(min, max)| (max - min).abs())
            .collect();

        if opt.distributions{
            let mut hist = HistF64::new(0.0, 2.0, opt.bins)
                .unwrap();
            for (i, &a) in abs.iter().enumerate(){
                if opt.without && foci.contains(&i){
                    continue;
                }
                hist.increment_quiet(a);
            }
            let name = format!("{stub}abs{e}.dist");
            let mut buf = create_buf_with_command_and_version(&name);
            dist_names.push(GnuplotHelper{file_name: name, title: format!("Export {e}")});
            write_slice_head(&mut buf, HIST_HEADER).unwrap();
            let total: usize = hist.hist().iter().sum();
            for (bin, hits) in hist.bin_hits_iter(){
                let center = (bin[0] + bin[1]) / 2.0;
                let normed = hits as f64 / total as f64;
                writeln!(buf, "{} {} {center} {hits} {normed}", bin[0], bin[1]).unwrap();
            }
        }
        // write acc
        if !opt.no_acc{
            let write_sorted = |unsorted: &[f64], name: &str|
            {
                let mut sorted: Vec<_> = unsorted.iter()
                    .zip(export_without_unconnected.nodes.iter())
                    .map(
                        |(v, n)|
                        {
                            let id = n.identifier.as_str();
                            (*v, id)
                        }
                    ).collect();
                sorted.sort_unstable_by(|a,b| a.0.total_cmp(&b.0));
                
                let mut buf = create_buf_with_command_and_version(name);
    
                writeln!(buf, "#Sort_idx delta CountryID").unwrap();
                
                for (i, (val, id)) in sorted.iter().enumerate(){
                    writeln!(buf, "{i} {val} {id}").unwrap();
                }
            };
            let name = format!("{stub}min{e}.txt");
            write_sorted(&min, &name);
            min_names.push((e, name));
    
            let name = format!("{stub}max{e}.txt");
            write_sorted(&max, &name);
            max_names.push((e, name));
        }
        

        

        write_res(&mut buf_abs, e, abs);
        write_res(&mut buf_var, e, variance);
        av_matrix.iter_mut()
            .zip(average.iter())
            .for_each(|(vec, val)| vec.push(*val)); 
        write_res(&mut buf_av, e, average);
        
        write_res(&mut buf_max, e, max);
        write_res(&mut buf_min, e, min);
        write_res(&mut buf_import_totals, e, after_shock_avail_total);
    }

    let write_focus = |buf: &mut BufWriter<File>|
    {
        for focus in foci.iter(){
            let id = export_without_unconnected.nodes[*focus].identifier.as_str();
            let gnuplot_index = focus + 2;
            writeln!(buf, "#Focus GP: {gnuplot_index} ID: {id}").unwrap();
        }
    };
    write_focus(&mut buf_av);
    write_focus(&mut buf_var);
    write_focus(&mut buf_max);
    write_focus(&mut buf_min);
    write_focus(&mut buf_abs);
    write_focus(&mut buf_av_d);
    write_focus(&mut buf_import);
    write_focus(&mut buf_import_totals);

    let derivatives: Vec<_> = av_matrix
        .iter()
        .map(|av| derivative(av))
        .collect();

    let integrals = av_matrix
        .iter()
        .map(|slice| integrate(slice, export_delta));

    let misc_name = format!("{stub}misc.dat");
    let mut misc_buf = create_buf_with_command_and_version(misc_name);

    let original_dists = export_without_unconnected.distance_from_index(foci[0]);
    let extra = lazy_enrichments.get_year_unchecked(opt.year);
    let focus_id = &export_without_unconnected.nodes[foci[0]].identifier;
    let flow = flow_calc(&export_without_unconnected, focus_id, opt.iterations, extra);

    let production_u8 = GLOBAL_NODE_INFO_MAP.deref().get(PRODUCTION);

    let original_exports: Vec<f64> = export_without_unconnected
        .nodes.iter()
        .map(|n| n.adj.iter().map(|e| e.amount).sum())
        .collect();
    let original_imports: Vec<f64> = import_without_unconnected
        .nodes.iter()
        .map(|n| n.adj.iter().map(|e| e.amount).sum())
        .collect();
    let original_production: Vec<f64> = export_without_unconnected
        .nodes
        .iter()
        .map(
            |n| 
            {
                extra.get(&n.identifier)
                    .and_then(|e| 
                        e.map.get(&production_u8)
                    ).map_or(0.0, |e| e.amount)
            }
        ).collect();
    

    let head = [
        "integral", 
        "depth(top0)", 
        "num_importing_from", 
        "num_exporting_to",
        "flow_imports",
        "flow_total",
        "original_exports",
        "original_imports",
        "original_production",
        "estimation"
    ];

    write_slice_head(&mut misc_buf, head).unwrap();
    let estimation = import_without_unconnected.estimation(foci[0]);
    integrals.zip(original_dists)
        .enumerate()
        .filter(|(_, (_, o))| o.is_some())
        .for_each(
            |(index, (i, o))|
            {
                let import_node = &import_without_unconnected.nodes[index];
                let importing_from = import_node.adj.len(); 
                let export_node = &export_without_unconnected.nodes[index];
                let exporting_to = export_node.adj.len();

                writeln!(
                    misc_buf, 
                    "{} {} {importing_from} {exporting_to} {} {} {} {} {} {}",
                    i,
                    o.unwrap(),
                    flow.imports[index],
                    flow.total[index],
                    original_exports[index],
                    original_imports[index],
                    original_production[index],
                    estimation[index]
                ).unwrap();
            }
            
        );

    let all_integrals: Vec<Vec<f64>> = all_deltas
        .iter()
        .map(
            |list_of_deltalists|
            {
                list_of_deltalists
                    .iter()
                    .map(|deltas| integrate(deltas, export_delta))
                    .collect()
            }
        ).collect();

    let worst_integral_name = format!("{stub}worst_integrals.dat");
    let mut buf_worst_integral = create_buf_with_command_and_version(worst_integral_name);


    let mut header_of_worst_integral = vec![
        "This_country_ID", 
        "WorstIntegral", 
        "ResponsibleExporterID"
    ];
    if country_map.is_some(){
        header_of_worst_integral.push("ResponsibleCountryName");
        header_of_worst_integral.push("ThisCountryName");
    }
    write_slice_head(&mut buf_worst_integral, &header_of_worst_integral)
        .unwrap();

    for (i, responsible) in foci.iter().enumerate(){
        let integral_name = format!("{stub}_integral_{i}.dat");
        let mut integral_buf = create_buf_with_command_and_version(integral_name);

        let iter = export_without_unconnected.nodes
            .iter()
            .zip(all_integrals.iter());

        let responsible_exporter_id = export_without_unconnected
            .nodes[*responsible]
            .identifier
            .as_str();
        

        for (node, integral) in iter {
            let id = node.identifier.as_str();
            if id == responsible_exporter_id {
                continue;
            }
            write!(
                integral_buf, 
                "{id} {} {responsible_exporter_id}",
                integral[i]
            ).unwrap();
            if let Some(map) = &country_map{
                let responsible_name = map.get(responsible_exporter_id).unwrap();
                let this_name = map.get(id).unwrap();
                write!(integral_buf, " {responsible_name} {this_name}").unwrap();
            }
            writeln!(integral_buf).unwrap(); 
        }
    }

    let mut for_sorting_worst_integral: Vec<_> = export_without_unconnected
        .nodes
        .iter()
        .zip(all_integrals.iter())
        .map(
            |(node, integral_res)|
            {
                let mut min = integral_res[0];
                let mut min_index = 0;
                for (&v, index) in integral_res[1..].iter().zip(1..)
                {
                    if v < min {    
                        min_index = index; 
                        min = v;
                    } 
                }
                let responsible_exporter_network_idx = foci[min_index];
                
                let responsible_exporter_id = export_without_unconnected
                    .nodes[responsible_exporter_network_idx]
                    .identifier
                    .as_str();
                (node.identifier.as_str(), min, responsible_exporter_id)
            }
        ).collect();

    for_sorting_worst_integral
        .sort_unstable_by(|a,b| a.1.total_cmp(&b.1));

    for (id, max, responsible_exporter) in for_sorting_worst_integral{
        write!(
            buf_worst_integral, 
            "{} {max} {responsible_exporter}",
            id
        ).unwrap();
        if let Some(map) = &country_map{
            let responsible_name = map.get(responsible_exporter).unwrap();
            let this_name = map.get(id).unwrap();
            write!(buf_worst_integral, " {responsible_name} {this_name}").unwrap();
        }
        writeln!(buf_worst_integral).unwrap();
    }
        


    for (index, e) in export_vals.iter().enumerate(){
        let d_iter = derivatives
            .iter()
            .map(|v| v[index] / export_delta);
        write_res(&mut buf_av_d, *e, d_iter);
    }

    let max_gp_index = export_without_unconnected.node_count() + 1;
    let create_gp = |data_name: &str, ylabel: &str, y_min: f64, y_max: f64|
    {
        let stub = data_name.strip_suffix(".dat").unwrap();
        let name = format!("{stub}.gp");
        let mut buf = create_buf_with_command_and_version(name);
        writeln!(buf, "reset session").unwrap();
        writeln!(buf, "set t pdfcairo").unwrap();
        writeln!(buf, "set xlabel \"export fraction\"").unwrap();
        writeln!(buf, "set ylabel \"{}\"", ylabel).unwrap();
        writeln!(buf, "set output \"{stub}.pdf\"").unwrap();
        writeln!(buf, "set yrange [{}:{}]", y_min, y_max).unwrap();

        write!(buf, "p ").unwrap();

        for gp_index in 2..=max_gp_index
        {
            let this_idx = gp_index - 2;
            if foci.contains(&this_idx){
                continue;
            } else {
                writeln!(
                    buf, 
                    "\"{data_name}\" u 1:{gp_index} w l t \"\",\\"
                ).unwrap();
            }
        }
        for (pos, &this_idx) in foci.iter().enumerate(){
            let gp_index = this_idx + 2;
            writeln!(
                buf, 
                "\"{data_name}\" u 1:{gp_index} w l dt (5,5) t \"top {pos}\",\\"
            ).unwrap();
        }
    };

    create_gp(&var_name, "Var(delta)", 0.0, 1.0);
    create_gp(&av_name, "Average(delta)", -1.1, 0.1);
    create_gp(&max_name, "Max(delta)", -1.0, 1.0);
    create_gp(&min_name, "Min(delta)", -1.0, 0.0);
    create_gp(&abs_name, "abs(Max - Min)", 0.0, 1.0);
    create_gp(&av_d_name, "derivative of average", 0.0, 2.0);
    create_gp(&import_name, "Number of imports", 0.0, 20.0);
    create_gp(&import_totals_name, "Import total", 0.0, 1e6);
    create_gp(&distance_name, "Distance from top 0", -1.0, 8.0);

    if opt.distributions{
        let name = format!("{stub}abs.dist");
        let relative = opt.top.get_relative();
        write_gnuplot(
            &name, 
            relative, 
            dist_names,
            0.0..=2.0
        ).unwrap();
    }

    let acc_gnuplot = |name: &str, names: &[(f64, String)]|
    {
        let gp_name = format!("ACC_{stub}{name}.gp");
        let out_name = format!("ACC_{stub}{name}.pdf");
        let mut buf = create_buf_with_command_and_version(gp_name);
        writeln!(buf, "reset session").unwrap();
        writeln!(buf, "set t pdfcairo").unwrap();
        writeln!(buf, "set output \"{out_name}\"").unwrap();
        write!(buf, "p ").unwrap();
        for (e, n) in names {
            writeln!(buf, "\"{n}\" u 1:2 w lp t \"{e}\",\\").unwrap();
        }
        writeln!(buf, "\nset output").unwrap();
    };

    if !opt.no_acc{
        acc_gnuplot("min", &min_names);
        acc_gnuplot("max", &max_names);
    }

}

pub fn shock_dist<P>(opt: ShockDistOpts, in_file: P)
where P: AsRef<Path>
{
    let mut lazy_networks = LazyNetworks::Filename(in_file.as_ref().to_path_buf());
    let mut lazy_enrichment = LazyEnrichmentInfos::Filename(opt.enrich_file, opt.item_code);
    let specifiers = opt.top.get_specifiers();

    let mut names = Vec::new();
    let mut gp_names = Vec::new();
    let mut is_first = true;

    let mut bins = None;
    let mut hists: Vec<_> = opt.export.iter()
        .map(|_| 
            vec![0; opt.bins]
        ).collect();

    let enrich_item_name_string = lazy_enrichment.item_codes_as_string_unchecked();
    for s in specifiers.iter(){
        let mut v = Vec::new();
        
        for (e_index, &e) in opt.export.iter().enumerate(){
            println!("E: {e}");
            let res = calc_shock(
                &mut lazy_networks, 
                opt.year, 
                s.clone(), 
                e, 
                opt.iterations, 
                &mut lazy_enrichment
            );

            
            let name_stub = format!(
                "{}_{}_y{}_e{e}.dat", 
                s.get_string(), 
                enrich_item_name_string, 
                opt.year
            );

            v.push(name_stub);
            let name = v.last().unwrap();
            if is_first{
                gp_names.push(
                    format!(
                        "{}_item{}_y{}_e{e}", 
                        s.get_short_str(), 
                        enrich_item_name_string, 
                        opt.year
                    )
                );
            }
            
            let mut hist = HistF64::new(-1.0, 1.0 + f64::EPSILON, opt.bins)
                .unwrap();
            if bins.is_none(){
                let b: Vec<_> = hist.bin_iter()
                    .copied()
                    .collect();
                bins = Some(b);
            }
        
            for (i, delta) in res.delta_iter().enumerate(){
                // check if focus county is to be counted in hist
                if opt.without && i == res.focus_index{
                    // skip focus country
                    continue;
                }
                
                if delta > 1.0 {
                    println!("{delta}");
                }
                hist.increment(delta).unwrap();
            }
        
            let mut buf = create_buf_with_command_and_version(name);
            write_slice_head(&mut buf, HIST_HEADER).unwrap();
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
    let relative = opt.top.get_relative();
    let mut combined_names = Vec::new();

    for (i, e) in opt.export.iter().enumerate(){
        let r = relative.if_yes_with(*e);
        let name_iter = names
            .iter()
            .enumerate()
            .map(
                |(index, name_vec)|
                {
                    let e_name = &name_vec[i];
                    let title = format!("top {index}");
                    GnuplotHelper { file_name: e_name.clone(), title}
                }
            );
        write_gnuplot(
            &gp_names[i], 
            r, 
            name_iter,
            -1.0..=1.0
        ).unwrap();

        if names.len() > 1 {
            // write fused hist
            let hist = hists[i].as_slice();
            let total: usize = hist.iter().sum();

            let name = format!("{}.combined", &gp_names[i]);
            let mut buf = create_buf_with_command_and_version(&name);
            write_slice_head(&mut buf, HIST_HEADER).unwrap();

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
                GnuplotHelper { file_name: name.clone(), title: format!("combined {}", names.len()) }
            );
            write_gnuplot(
                &name, 
                r, 
               iter,
               -1.0..=1.0
            ).unwrap();

            combined_names.push(name);
        }
    }

    if !combined_names.is_empty(){
        let name = format!("{}_combined_{}", opt.top.get_string(), enrich_item_name_string);
        let iter = combined_names
            .iter()
            .zip(opt.export.iter())
            .map(
                |(n, e)|
                {
                    GnuplotHelper{file_name: n.clone(), title: format!("Export {e}")}
                }
            );
        write_gnuplot(
            &name, 
            relative, 
            iter,
            -1.0..=1.0
        ).unwrap();
    }
    
}

#[derive(Debug, Clone, Copy)]
pub enum Relative{
    YesWith(f64),
    Yes,
    No
}

impl Relative{
    pub fn is_not_relative(self) -> bool
    {
        matches!(self, Self::No)
    }

    pub fn is_relative(self) -> bool
    {
        !self.is_not_relative()
    }

    pub fn if_yes_with(self, with: f64) -> Self 
    {
        if self.is_relative(){
            Self::YesWith(with)
        } else {
            Relative::No
        }
    }
}

pub struct GnuplotHelper{
    pub file_name: String,
    pub title: String
}

fn write_gnuplot<I>(
    gnuplot_name_stub: &str, 
    relative: Relative, 
    name_iter: I,
    xrange: RangeInclusive<f64>
) -> std::io::Result<()>
where I: IntoIterator<Item = GnuplotHelper>
{
    let gnuplot_name = format!("{gnuplot_name_stub}.gp");
    let mut buf = create_buf_with_command_and_version(gnuplot_name);
    writeln!(buf, "reset session")?;
    writeln!(buf, "set t pdfcairo")?;
    let range_start = xrange.start();
    let range_end = xrange.end(); 
    writeln!(buf, "set xrange [{range_start}:{range_end}]")?;
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
        Relative::No => {
            writeln!(buf, "set title \"absolut\"")?;
        }
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
    node_map: &ExtraInfoMap
) -> Vec<f64>
{
    let inverted = network.invert();
    let (import, export) = match network.direction{
        Direction::ExportTo => (&inverted, network),
        Direction::ImportFrom => (network, &inverted)
    };

    let original_import = calc_acc_trade(import);
    let original_export = calc_acc_trade(export);


    let production_id = node_map.get(PRODUCTION);
    let mut at_least_some_production = false;
    let unit = &import.unit;
    let unit_tester = UNIT_TESTER.deref();

    //let stock_id = node_map.get("Stocks");
    //let mut at_least_some_stock = false;


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
                    //if let Some(stock) = extra.map.get(&stock_id){
                    //    assert!(unit_tester.is_equiv(unit, &stock.unit));
                    //    total += stock.amount;
                    //    at_least_some_stock = true;
                    //}

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
    //if !at_least_some_stock{
    //    eprintln!("WARNING: NO STOCK DATA")
    //}
    eprintln!("Stock Variation data is unimplemented! STOCK data unimplemented!");
    
    res

}

