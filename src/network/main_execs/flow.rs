use{
    super::flow_helper::*, crate::{
        config::*, group_cmp::{GroupCompMultiOpts, X}, misc::*, network::{enriched_digraph::*, *}, parser::country_map, sync_queue, UNIT_TESTER
    }, camino::{Utf8Path, Utf8PathBuf}, clap::ValueEnum, derivative::Derivative, fs_err::File, itertools::Itertools, kahan::KahanSum, ordered_float::OrderedFloat, rand::{distributions::{Distribution, Uniform}, seq::SliceRandom, Rng, SeedableRng}, rand_pcg::Pcg64, rayon::prelude::*, sampling::{
        HistF64, 
        Histogram
    }, serde::{Deserialize, Serialize}, std::{
        cmp::Reverse, collections::*, fmt::Display, io::{
            BufWriter, 
            Write
        }, num::NonZeroUsize, ops::{
            AddAssign, 
            Deref, 
            RangeInclusive
        }, path::Path, str::FromStr, sync::{Mutex, RwLock}
    }
};

#[derive(Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Default, Debug)]
pub enum SimulationMode{
    #[default]
    Classic,
    WithStockVariation,
    OnlyStock
}


impl FromStr for SimulationMode{
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "c" | "classic" => {
                Ok(Self::Classic)
            },
            "withstockvariation" | "wsv" | "with_stock_variation" => {
                Ok(Self::WithStockVariation)
            },
            "only_stock" | "onlystock" | "os" => {
                Ok(Self::OnlyStock)
            },
            _ => {
                Err(
                    "Unknown option for Simulation Mode. Try 'classic' or 'with_stock_variation'"
                        .to_owned()
                )
            }
        }
    }
}

// Not pretty, but this was the easiest way to retrofit it in
static MODE: RwLock<SimulationMode> = RwLock::new(SimulationMode::Classic);

fn global_simulation_mode_as_str() -> &'static str
{
    let lock = MODE.read().unwrap();
    let s = match lock.deref()
    {
        SimulationMode::Classic => "CLASSIC",
        SimulationMode::WithStockVariation => "W_Stock_Variation",
        SimulationMode::OnlyStock => "OnlyStock",
    };
    drop(lock);
    s
}


pub fn set_global_simulation_mode(mode: SimulationMode){
    let mut lock = MODE.write().unwrap();
    *lock = mode;
    drop(lock);
}

const ORIGINAL_AVAIL_FILTER_MIN: f64 = 1e-9;
const HIST_HEADER: [&str; 5] = [
    "left",
    "right",
    "center",
    "hits",
    "normalized"
];

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
    let mode_lock = MODE.read()
        .unwrap();
    let mode = mode_lock.deref();


    let info_map = crate::network::enriched_digraph::GLOBAL_NODE_INFO_MAP.deref();
    let unit_tester = crate::UNIT_TESTER.deref();
    let production_index = info_map.get(PRODUCTION);
    let stock_variation_idx = info_map.get(STOCK_VARIATION);
    let stock_idx = info_map.get(STOCK);
    let mut percent = vec![0.0; net.node_count()];
    let mut new_percent = percent.clone();

    let mut production = Vec::new();
    let mut stock_variation_vec = Vec::new();
    let mut stock_vec = Vec::new();
    let mut map = BTreeMap::new();
    for (i, n) in net.nodes.iter().enumerate()
    {
        map.insert(n.identifier.as_str(), i);
        let pr = match extra.get(n.identifier.as_str()){
            None => 0.0,
            Some(e) => {
                match mode {
                    SimulationMode::Classic => {
                        // Classic needs nothing additional:
                        // Do Nothing
                    }
                    SimulationMode::WithStockVariation => {
                        let stock_variation = match e.map.get(&stock_variation_idx){
                            None => 0.0,
                            Some(variation) => {
                                variation.amount
                            }
                        };
                        stock_variation_vec.push(stock_variation);
                    },
                    SimulationMode::OnlyStock => {
                        let stock = match e.map.get(&stock_idx)
                        {
                            None =>  0.0,
                            Some(stock) => {
                                stock.amount
                            }
                        };
                        stock_vec.push(stock);
                    }
                }

                match e.map.get(&production_index){
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
    dbg!(mode);
    dbg!(&stock_vec);
    let focus_idx = map.get(focus).unwrap();
    percent[*focus_idx] = 1.0;

    let import_from = net.get_network_with_direction(Direction::ImportFrom);

    for _ in 0..iterations{
        for i in 0..production.len(){
            let new_p = new_percent.get_mut(i).unwrap();
            let n = &import_from.nodes[i];
            let mut total = production[i];
            match mode {
                SimulationMode::Classic => {
                    // Noting additional needs to be done in classic case
                },
                SimulationMode::WithStockVariation => {
                    // negative sign 
                    // -> negative stock variation means 
                    //    that the country took something out of the stock and into
                    //    the market (or whatever else)
                    total -= stock_variation_vec[i];
                },
                SimulationMode::OnlyStock => {
                    // In this mode we tread the stock similar to 
                    // production. It is completely available and
                    // we can IGNORE the STOCK VARIATION
                    total += stock_vec[i];
                }
            }
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
    after_export_fract: Vec<f64>,
    pub flow_status: FlowStatus
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

    let (focus, export_frac) = match top_id{
        TopSpecifier::Id(id) => {
            let focus = export.get_index(&id).unwrap();
            (focus, export_frac)
        },
        TopSpecifier::Rank(r) => {
            let sorted =  get_top_ordered(&export);

            let focus = sorted[r].0;
            (focus, export_frac)
            
        },
        TopSpecifier::RankRef(r) => {
            let sorted = get_top_ordered(&export);

            let r_ref_trade_amount = sorted[r.reference].1.trade_amount();
            let wanted_ref_export = export_frac * r_ref_trade_amount;
            let wanted_export_reduction = r_ref_trade_amount - wanted_ref_export;
            let possible_export = sorted[r.focus].1.trade_amount();
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

    let (avail_after_shock, _) = calc_available(
        &export, 
        enrich, 
        &fracts, 
        &node_info_map,
        false
    );

    let no_shock = ShockRes{
        import_fracs: vec![1.0; fracts.import_fracs.len()],
        export_fracs: vec![1.0; fracts.import_fracs.len()]
    };

    let (available_before_shock, flow_status) = calc_available(
        &export, 
        enrich, 
        &no_shock, 
        &node_info_map,
        false
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
        after_export_fract: fracts.export_fracs,
        flow_status
    }
}


pub fn get_top_ordered(
    export_network: &Network
) -> Vec<(usize, &Node)>
{
    export_network
        .nodes
        .iter()
        .enumerate()
        .sorted_by_cached_key(
            |(_id, node)| 
                Reverse(OrderedFloat(node.trade_amount()))
        ).collect_vec()
}


pub fn get_top_k_ids(
    export_network: &Network,
    k: usize
) -> Vec<usize>
{
    get_top_ordered(export_network)
        .into_iter()
        .map(|(id, _)| id)
        .take(k)
        .collect_vec()
}

pub fn multi_shock_distribution(
    import_network: &Network,
    job: &CalcShockMultiJob
) -> ShockRes
{
    let interval = 0.0..=1.0;
    assert!(
        job.exporter.iter().all(|e| interval.contains(&e.export_frac)),
        "At least one Invalid export fraction - they have to be in range 0.0..=1.0"
    );
    assert!(import_network.direction.is_import());

    
    let mut current_export_frac = vec![1.0; job.original_exports.len()];
    for e in job.exporter.iter(){
        current_export_frac[e.export_id] = e.export_frac;
    }
    let mut reduced_import_frac = vec![1.0; current_export_frac.len()];

    for _ in 0..job.iterations{
        for (index, n) in import_network.nodes.iter().enumerate(){
            reduced_import_frac[index] = 0.0;
            if job.original_imports[index] == 0.0{
                assert_eq!(n.adj.len(), 0);
                continue;
            }
            for e in n.adj.iter(){
                reduced_import_frac[index] += e.amount * current_export_frac[e.index];
            }
            reduced_import_frac[index] *= job.original_imports_recip[index];
        }

        for &index in job.unrestricted_node_idxs.iter()
        {
            let missing_imports = (1.0 - reduced_import_frac[index]) * job.original_imports[index];
            let available_for_export = job.original_exports[index] - missing_imports;
            current_export_frac[index] = if available_for_export <= 0.0 {
                0.0
            } else {
                available_for_export * job.original_exports_recip[index]
            };
        }
    }

    ShockRes { 
        import_fracs: reduced_import_frac, 
        export_fracs: current_export_frac
    }
}

#[derive(ValueEnum, Debug, Clone, Copy)]
pub enum ExportRestrictionType{
    Percentages,
    WholeCountries
}

#[derive(Debug, Serialize, Deserialize, Derivative)]
#[derivative(Default)]
pub struct InCommon{
    /// File with enrich infos
    pub enrich_file: String,

    /// File with the network data
    pub network_file: Utf8PathBuf,

    /// Which year to check
    #[derivative(Default(value = "2000..=2019"))]
    pub years: RangeInclusive<i32>,

    /// Iterations
    #[derivative(Default(value="10000"))]
    pub iterations: usize,    

    /// Item code, e.g. 27 for Rice
    pub item_code: Option<String>,

    /// how many countrys should restrict their exports?
    #[derivative(Default(value="5"))]
    pub top: usize,

    /// the fraction at which countries are counted as unstable
    #[derivative(Default(value="0.7"))]
    pub unstable_country_threshold: f64,

    /// Countries that have less than this amount of 
    /// Product without shock will not be counted as unstable ever
    /// The idea being that they dont depend on the product so they should not be unstable
    ///
    /// NOTE: Countries with negative total will be automatically excluded!
    /// Also, for numerical reasons, this value is not allowed to be lower than the default value
    pub original_avail_filter: f64
}


#[derive(Debug, Serialize, Deserialize, Default)]
pub struct MeasureMultiShockOpts<Extra>
{
    pub common: InCommon,

    pub extra: Extra
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Percentages{
    pub start: f64,
    pub end: f64,
    pub amount: NonZeroUsize
}

impl Default for Percentages{
    fn default() -> Self {
        Self { start: 0.0, end: 1.0, amount: NonZeroUsize::new(100).unwrap() }
    }
}

fn get_files(glob: &str) -> BTreeMap<usize, Utf8PathBuf>
{   
    let expr = r"\d+";
    let re = regex::Regex::new(expr)
        .unwrap();
    utf8_path_iter(glob)
        .map(
            |utf8path|
            {
                let s = utf8path.file_name().unwrap();
                let number: usize = regex_first_match_parsed(&re, s);
                (number, utf8path)
            }
        ).collect()
}

pub fn all_random_cloud_shocks<P>(
    json: Option<P>, 
    out_stub: &str,
    quiet: bool,
    threads: NonZeroUsize
)where P: AsRef<Path>
{
    let opt: ShockCloudAll = crate::misc::parse_and_add_to_global(json);
    
    let enrich_files = get_files(&opt.enrich_glob);
    dbg!(&enrich_files);
    let network_files = get_files(&opt.network_glob);
    let all_item_codes: BTreeSet<usize> = enrich_files.keys()
        .chain(network_files.keys())
        .copied()
        .collect();
    let mut job_opts = VecDeque::new();
    for key in all_item_codes{
        let enrich_path = match enrich_files.get(&key){
            Some(path) => path,
            None => {
                println!("No enrichment for item {key}");
                continue;
            }
        };
        let network_path = match network_files.get(&key){
            Some(path) => path,
            None => {
                println!("No enrichment for item {key}");
                continue;
            }
        };
        for y in opt.years.clone(){
            let shock_opt = ShockCloud{
                enrich_file: enrich_path.to_owned(),
                network_file: network_path.to_owned(),
                years: y..=y,
                cloud_m: opt.cloud_m,
                cloud_steps: opt.cloud_steps,
                item_code: Some(key.to_string()),
                top: opt.top,
                iterations: opt.iterations,
                unstable_country_threshold: opt.unstable_country_threshold,
                original_avail_filter: opt.original_avail_filter,
                seed: opt.seed,
                reducing_factor: opt.reducing_factor,
                hist_bins: opt.hist_bins
            };
            job_opts.push_back(shock_opt);
        }
        
    }
    
    let issues = Mutex::new(Vec::new());

    let sync_queue = sync_queue::SyncQueue::new(job_opts);
    (0..threads.get())
        .into_par_iter()
        .for_each(
            |_|
            {
                while let Some(opt) = sync_queue.pop(){
                    sync_queue.print_remaining();
                    let folder = opt.item_code.as_deref();
                    let result = random_cloud_shock_helper(
                        &opt, 
                        out_stub, 
                        quiet,
                        folder
                    );
                    if let Err(info) = result {
                        let mut lock = issues.lock()
                            .unwrap();
                        lock.push(info);
                        drop(lock);
                    }
                }
            }
        );

    let issues = issues.into_inner().unwrap();
    dbg!(&issues);
    if !issues.is_empty(){
        let id_map = opt.id_file.map(crate::parser::id_map);
        
        let error_log_name = format!("{out_stub}_shock_cloud_error.log");
        let mut buf = create_buf_with_command_and_version(error_log_name);
        for missing in issues{

            if let (Some(map), Some(id)) = (&id_map, &missing.item_id){
                let name = map.get(id);
                writeln!(
                    buf,
                    "{:?} {} {:?} {:?}",
                    missing.item_id,
                    missing.year,
                    missing.why,
                    name
                ).unwrap();
            } else {
                writeln!(
                    buf,
                    "{:?} {} {:?}",
                    missing.item_id,
                    missing.year,
                    missing.why
                ).unwrap(); 
            }


        }
    }
    
}

pub fn random_cloud_shock<P>(
    json: Option<P>, 
    out_stub: &str,
    quiet: bool
)
where P: AsRef<Path>
{
    let opt: ShockCloud = crate::misc::parse_and_add_to_global(json);
    let _ = random_cloud_shock_helper(
        &opt, 
        out_stub, 
        quiet,
        None
    );
}

#[derive(Debug)]
pub enum Reason{
    Production,
    Network
}

#[derive(Debug)]
pub struct MissingInfo{
    pub item_id: Option<String>,
    pub year: i32,
    pub why: Reason

}

pub fn random_cloud_shock_helper(
    opt: &ShockCloud, 
    out_stub: &str,
    quiet: bool,
    folder: Option<&str>,
) -> Result<(), MissingInfo>
{

    let mut lazy_networks = LazyNetworks::Filename(opt.network_file.clone());
    lazy_networks.assure_availability();

    let mut lazy_enrichments = LazyEnrichmentInfos::Filename(
        opt.enrich_file.as_str().to_owned(), 
        opt.item_code.clone()
    );
    lazy_enrichments.assure_availability();
    let map = lazy_enrichments.extra_info_idmap_unchecked();
    let production_idx = map.get(PRODUCTION);
    for year in opt.years.clone()
    {
        let enrich = lazy_enrichments.get_year_unchecked(year);
        let mut any = false;
        for e in enrich.values(){
            if let Some(extra) = e.map.get(&production_idx)
            {
                if extra.amount > 0.0 {
                    any = true;
                    break;
                }
            }
        }
        if !any{
            println!("Missing production in file {} - for year {year}: SKIPPING ITEM", opt.enrich_file);
            return Err(
                MissingInfo{
                    item_id: opt.item_code.clone(),
                    year,
                    why: Reason::Production
                }
            );
        }
    }

    for year in opt.years.clone()
    {
        let network = lazy_networks.get_export_network_unchecked(year)
            .without_unconnected_nodes();
        if network.node_count() < opt.top
        {
            println!("Empty network in file {} for year {year} - SKIPPING ITEM", opt.network_file);
            return Err(
                MissingInfo{
                    item_id: opt.item_code.clone(),
                    year,
                    why: Reason::Network
                }
            );
        }
    }

    let folder = match folder{
        Some(f) => {
            let _ = std::fs::create_dir(folder.unwrap());
            format!("{f}/")
        },
        None => String::default()
    };

    
    let enrichment_infos = lazy_enrichments.enrichment_infos_unchecked();
    let node_info_map = lazy_enrichments.extra_info_idmap_unchecked();

    let mut original_avail_filter = opt.original_avail_filter;
    if original_avail_filter < ORIGINAL_AVAIL_FILTER_MIN {
        println!(
            "FILTER for original avail will be set to {:e}, lower values are not allowed!", 
            ORIGINAL_AVAIL_FILTER_MIN
        );
        original_avail_filter = ORIGINAL_AVAIL_FILTER_MIN;
    }

    let header = [
        "disruption",
        "num_of_countries"
    ];

    let mode_str = global_simulation_mode_as_str();

    let mut rng = Pcg64::seed_from_u64(opt.seed);

    let years_and_rngs = opt.years
        .clone()
        .map(|y| (y, Pcg64::from_rng(&mut rng).unwrap()))
        .collect_vec();


    years_and_rngs
        .into_par_iter()
        .filter_map(
            |(year, rng)|
            {
                let export_without_unconnected = lazy_networks
                    .get_export_network_unchecked(year)
                    .without_unconnected_nodes();
            
                let import_without_unconnected = export_without_unconnected.invert();
        
                let enrich = enrichment_infos.get_year(year);
    
                let top = get_top_k_ids(&export_without_unconnected, opt.top);

                let is_good = check_quick_and_dirty(
                    &top, 
                    &export_without_unconnected, 
                    enrich,
                    opt.item_code.as_deref().unwrap(),
                    year
                );
                is_good.then_some(
                    (
                        year,
                        rng,
                        export_without_unconnected,
                        import_without_unconnected,
                        enrich,
                        top
                    )
                )
            }
        )
        .for_each(
            |
                (
                    year, 
                    mut rng,
                    export_without_unconnected,
                    import_without_unconnected,
                    enrich,
                    top
                )
            |
            {

                let (no_shock, flow_status) = {
                    let one = vec![1.0; import_without_unconnected.node_count()];
                    let no_shock = ShockRes{
                        import_fracs: one.clone(),
                        export_fracs: one
                    };
                    calc_available(
                        &export_without_unconnected, 
                        enrich, 
                        &no_shock, 
                        &node_info_map,
                        quiet
                    )
                };

                let flow_status_name_addition = flow_status.name_addition();

                let out_name = format!(
                    "{folder}{}{out_stub}_Y{year}_Th{}_R{}_{mode_str}.dat", 
                    flow_status_name_addition,
                    opt.unstable_country_threshold,
                    opt.reducing_factor
                );
                let av_name = format!(
                    "{folder}{}{out_stub}_Y{year}_Th{}_R{}_{mode_str}.average", 
                    flow_status_name_addition,
                    opt.unstable_country_threshold,
                    opt.reducing_factor
                );
            
                let mut buf = create_buf_with_command_and_version_and_header(out_name, header);

                let len = export_without_unconnected.node_count();
                let countries_where_country_count_is_applicable = 
                    (0..len)
                        .filter(
                            |idx|
                            !top.contains(idx)
                            && no_shock[*idx] >= original_avail_filter
                        ).collect_vec();
                let original_exports = calc_acc_trade(&export_without_unconnected);
                let original_exports_recip = calc_recip(&original_exports);
                let original_imports =  calc_acc_trade(&import_without_unconnected);
                let original_imports_recip = calc_recip(&original_imports);
                let total_export = top.iter()
                    .map(|&idx| original_exports[idx])
                    .sum::<f64>();
                let mut hist = HistF64::new(0.0, 1.0, opt.hist_bins.get())
                        .unwrap();
                let mut sum = vec![0_u64; hist.bin_count() + 1];
                let mut sum_sq = sum.clone();
                let last_sum_idx = sum.len() - 1;
                let mut last_hits = 0;

                let max = top.len();
                let delta = max as f64 / (opt.cloud_steps.get() - 1) as f64;

                let maximal_target = top.len() as f64;
                for i in 0..opt.cloud_steps.get(){
                    let target = (i as f64 * delta).min(maximal_target);
                    let matrix = rand_fixed_sum(
                        top.len(), 
                        opt.cloud_m, 
                        target, 
                        0.0, 
                        1.0, 
                        &mut rng
                    );
                    for random_export_fracs in matrix.iter(){
                        let exports = top.iter()
                            .zip(random_export_fracs.iter())
                            .map(
                                |(id, frac)|
                                {
                                    ExportShockItem{
                                        export_frac: *frac,
                                        export_id: *id
                                    }
                                }
                            ).collect_vec();
    
                        let job = CalcShockMultiJob::new_exporter(
                            exports, 
                            opt.iterations, 
                            &export_without_unconnected, 
                            &original_imports,
                            &original_imports_recip,
                            &original_exports,
                            &original_exports_recip
                        );
    
                        let shock_result = multi_shock_distribution(&import_without_unconnected, &job);
                
                        let remaining_export = top.iter()
                            .map(|&idx| job.original_exports[idx] * shock_result.export_fracs[idx])
                            .sum::<f64>();
                
                        let percent = remaining_export / total_export;
                
                        let (avail_after_shock, _) = calc_available(
                            &export_without_unconnected, 
                            enrich, 
                            &shock_result, 
                            &node_info_map,
                            quiet
                        );
                        let mut country_counter = 0;
                        
                        for &idx in countries_where_country_count_is_applicable.iter()
                        {
                            let original = no_shock[idx];
                            let shocked = avail_after_shock[idx];
                            let frac = shocked / original;
                            if frac < opt.unstable_country_threshold{
                                country_counter += 1;
                            }
                        }
                        writeln!(buf, "{percent:e} {country_counter}").unwrap();
                        let idx = match hist.increment(percent){
                            Ok(idx) => idx,
                            Err(_) => {
                                assert!(target >= top.len() as f64);
                                last_hits += 1;
                                last_sum_idx
                            }
                        };
                        sum[idx] += country_counter;
                        sum_sq[idx] += country_counter * country_counter;
                    }
                    
                }
                let mut hist_buf = create_buf_with_command_and_version(av_name);
                let header = [
                    "interval_left",
                    "interval_right",
                    "hits",
                    "average",
                    "variance",
                    "average_normed_by_max",
                    "average_normed_by_trading_countries"
                ];
                write_slice_head(&mut hist_buf, header).unwrap();
                let iter = hist.bin_hits_iter()
                    .chain(std::iter::once((&[1.0, 1.0], last_hits)))
                    .zip(sum)
                    .zip(sum_sq);

                let mut norm = None;

                let trading_norm_factor = (countries_where_country_count_is_applicable.len() as f64).recip();
                
                for (((interval, hits), sum), sum_sq) in iter {
                    let average = sum as f64 / hits as f64;
                    let av_2 = sum_sq as f64 / hits as f64;
                    let var = av_2 - average * average;

                    let normed = match norm{
                        None => {
                            norm = Some(average);
                            1.0
                        },
                        Some(n) => average / n
                    };

                    let normed_by_trading = average * trading_norm_factor;

                    writeln!(
                        hist_buf,
                        "{} {} {hits} {average:e} {var:e} {normed:e} {normed_by_trading:e}",
                        interval[0],
                        interval[1]
                    ).unwrap();
                }
            }
        );
    Ok(())
}
 
pub fn measure_multi_shock<P>(
    json: Option<P>, 
    which: ExportRestrictionType,
    out_stub: &str,
    quiet: bool,
    group_files: bool,
    compare_successive: bool
)
where P: AsRef<Path>
{
    let opt = match which{
        ExportRestrictionType::Percentages => {
            let opt: MeasureMultiShockOpts<Percentages> = crate::misc::parse_and_add_to_global(json);
            either::Either::Left(opt)
        },
        ExportRestrictionType::WholeCountries => {
            let opt: MeasureMultiShockOpts<()> = crate::misc::parse_and_add_to_global(json);
            either::Either::Right(opt)
        }
    };
    let common_opt = opt.as_ref()
        .either(
            |o| &o.common,
            |o| &o.common
        );

    let mut lazy_networks = LazyNetworks::Filename(common_opt.network_file.clone());
    lazy_networks.assure_availability();

    let mut lazy_enrichments = LazyEnrichmentInfos::Filename(
        common_opt.enrich_file.clone(), 
        common_opt.item_code.clone()
    );
    lazy_enrichments.assure_availability();
    let enrichment_infos = lazy_enrichments.enrichment_infos_unchecked();
    let node_info_map = lazy_enrichments.extra_info_idmap_unchecked();

    let mode_str = global_simulation_mode_as_str();
    let header = [
        "disrupting_countries",
        "disruption_percent",
        "num_countries"
    ];


    let mut original_avail_filter = common_opt.original_avail_filter;
    if original_avail_filter < ORIGINAL_AVAIL_FILTER_MIN {
        println!(
            "FILTER for original avail will be set to {:e}, lower values are not allowed!", 
            ORIGINAL_AVAIL_FILTER_MIN
        );
        original_avail_filter = ORIGINAL_AVAIL_FILTER_MIN;
    }

    let files: Vec<_> = common_opt.years
        .clone()
        .into_par_iter()
        .map(
            |year|
            {
                let mut out_stub = format!(
                    "{out_stub}_Y{year}_Th{}_{mode_str}_", 
                    common_opt.unstable_country_threshold
                );
                let export_without_unconnected = lazy_networks
                    .get_export_network_unchecked(year)
                    .without_unconnected_nodes();
                
                let import_without_unconnected = export_without_unconnected.invert();
            
                let enrich = enrichment_infos.get_year(year);
        
                let top = get_top_k_ids(&export_without_unconnected, common_opt.top);


                let original_exports = calc_acc_trade(&export_without_unconnected);
                let original_exports_recip = calc_recip(&original_exports);
                let original_imports =  calc_acc_trade(&import_without_unconnected);
                let original_imports_recip = calc_recip(&original_imports);
            
                #[allow(clippy::type_complexity)]
                let (mut iterate, mut job, mut x): (Box<dyn FnMut(&mut CalcShockMultiJob) -> Option<u16>>, _, u16) = match opt.as_ref()
                {
                    either::Either::Left(percent) => {
                        let per = "percent";
                        out_stub.push_str(per);
                        let p = &percent.extra;
                        let delta = (p.end - p.start) / (p.amount.get() - 1) as f64;
                        let mut iter = (0..p.amount.get() - 1)
                            .map(move |i| p.start + delta * i as f64)
                            .chain(std::iter::once(p.end));
                        let first = iter.next().unwrap();
                        let country_count = top.len() as u16;
                        let fun = move |job: &mut CalcShockMultiJob| -> Option<u16>
                        {
                            match iter.next(){
                                None =>  None,
                                Some(val) => {
                                    job.change_export_frac(val);
                                    Some(country_count)
                                }
                            }
                        };
                        let job = CalcShockMultiJob::new_const_export(
                            &top, 
                            first, 
                            common_opt.iterations, 
                            &export_without_unconnected, 
                            &original_exports,
                            &original_exports_recip,
                            &original_imports,
                            &original_imports_recip
                        );
                        (Box::new(fun), job, country_count)
                    },
                    either::Either::Right(_) => {
                        out_stub.push_str(&format!("Top{}", common_opt.top));
                        let (first, mut slice) = top.split_at(1);
                        let mut count = 1;
                        let fun = move |job: &mut CalcShockMultiJob| -> Option<u16>
                        {
                            if slice.is_empty() {
                                None
                            } else {
                                let to_add;
                                (to_add, slice) = slice.split_first().unwrap();
                                job.add_exporter(ExportShockItem{export_id: *to_add, export_frac: 0.0});
                                count += 1;
                                Some(count)
                            }
                        };
                        let job = CalcShockMultiJob::new_const_export(
                            first, 
                            0.0, 
                            common_opt.iterations, 
                            &export_without_unconnected, 
                            &original_exports,
                            &original_exports_recip,
                            &original_imports,
                            &original_imports_recip
                        );
                        (Box::new(fun), job, 1)
                    }
                };

            

                let (no_shock, flow_status) = {
                    let one = vec![1.0; import_without_unconnected.node_count()];
                    let no_shock = ShockRes{
                        import_fracs: one.clone(),
                        export_fracs: one
                    };
                    calc_available(
                        &export_without_unconnected, 
                        enrich, 
                        &no_shock, 
                        &node_info_map,
                        quiet
                    )
                };
                let flow_status_name_addition = flow_status.name_addition();
                let mut group_out_name = "".to_owned();
                let mut group_buf = group_files.then(
                    ||
                    {
                        group_out_name = format!("{flow_status_name_addition}{out_stub}.group");
                        create_buf_with_command_and_version(&group_out_name)
                    }
                );
                out_stub = format!("{flow_status_name_addition}{out_stub}.dat");
                let mut buf = create_buf_with_command_and_version_and_header(&out_stub, header);

                // filter out countries that have a very small total of the item in question
                let countries_where_country_count_is_applicable = job.unrestricted_node_idxs
                    .iter()
                    .copied()
                    .filter(|idx| no_shock[*idx] >= original_avail_filter)
                    .collect_vec();
            
                let total_export = top.iter()
                    .map(|&idx| job.original_exports[idx])
                    .sum::<f64>();
            
                loop{
                    let shock_result = multi_shock_distribution(&import_without_unconnected, &job);
            
                    let remaining_export = top.iter()
                        .map(|&idx| job.original_exports[idx] * shock_result.export_fracs[idx])
                        .sum::<f64>();
            
                    let percent = remaining_export / total_export;
            
                    let (avail_after_shock, _) = calc_available(
                        &export_without_unconnected, 
                        enrich, 
                        &shock_result, 
                        &node_info_map,
                        quiet
                    );
                    let mut country_counter = 0;
                    if let Some(b) = group_buf.as_mut()
                    {
                        writeln!(b, "§{x} {percent}").unwrap();
                    }
                    for &idx in countries_where_country_count_is_applicable.iter()
                    {
                        let original = no_shock[idx];
                        let shocked = avail_after_shock[idx];
                        let frac = shocked / original;
                        if frac < common_opt.unstable_country_threshold{
                            country_counter += 1;
                            if let Some(b) = group_buf.as_mut()
                            {
                                writeln!(
                                    b,
                                    "{}",
                                    export_without_unconnected.nodes[idx].identifier
                                ).unwrap();
                            }
                        }
                    }
                    writeln!(buf, "{} {percent} {country_counter}", x).unwrap();
                    match iterate(&mut job) {
                        None => break,
                        Some(d) => {
                            x = d;
                        }
                    }
                }
                (group_out_name, year)
            }
        ).collect();
    
    if compare_successive{
        let x = match which{
            ExportRestrictionType::Percentages => {
                X::Percent
            },
            ExportRestrictionType::WholeCountries => {
                X::Count
            }
        };
        for (a, b) in files.iter().tuple_windows()
        {
            let output = format!(
                "{out_stub}_Y{}_vs_Y{}_Th{}_{mode_str}.dat",
                a.1,
                b.1,
                common_opt.unstable_country_threshold
            );

            println!("{output}");

            let opt = GroupCompMultiOpts{
                groups_a: a.0.clone(),
                groups_b: b.0.clone(),
                output,
                x
            };
            
            crate::group_cmp::compare_th_exec(opt);
        }
        
        let paths = files.iter()
            .map(|(p, _)| p.as_ref())
            .collect_vec();
        
        let name = format!(
            "{out_stub}_Y{}-Y{}_{}_{mode_str}.dat",
            common_opt.years.start(),
            common_opt.years.end(),
            x.str()
        );
        crate::group_cmp::compare_multiple(
            &paths,
            x,
            name.as_ref()
        );
        println!("{name}");
    }
}

pub fn shock_avail<P>(opt: ShockAvailOpts, in_file: P)
where P: AsRef<Utf8Path>
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

    let mut out = opt.out.clone();
    if let FlowStatus::Missing(missing) = res.flow_status{
        out = format!("MIS_{missing}_{out}");
    }

    let file = File::create(out)
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

pub fn reduce_x_test<P>(opt: XOpts, in_file: P)
where P: AsRef<Utf8Path>
{
    let mut lazy_networks = LazyNetworks::Filename(in_file.as_ref().to_owned());
    lazy_networks.assure_availability();
    let mut lazy_enrichments = LazyEnrichmentInfos::Filename(
        opt.enrich_file, 
        opt.item_code
    );
    lazy_enrichments.assure_availability();

    let file_name = format!(
        "{}_{}_test.dat", 
        opt.top.get_string(),
        lazy_enrichments.item_codes_as_string_unchecked()
    );
    // missing is None if nothing is missing
    let header = [
        "Export_fraction",
        "sum",
        "sum_without_focus",
        "missing"
    ];
    let mut buf = create_buf_with_command_and_version_and_header(&file_name, header);

    let export_diff = opt.export_end - opt.export_start;
    let export_delta = export_diff / (opt.export_samples - 1) as f64;


    let export_vals = (0..opt.export_samples-1)
        .map(|i| opt.export_start + export_delta * i as f64)
        .chain(std::iter::once(opt.export_end));

    let specifiers = opt.top.get_specifiers();

    let recip = (specifiers.len() as f64).recip();
    for e in export_vals
    {
        let mut sum = KahanSum::new();
        let mut sum_without = KahanSum::new();
        let mut flow_status = FlowStatus::AllGood;
        for s in specifiers.iter(){
            let res = calc_shock(
                &mut lazy_networks, 
                opt.year, 
                s.clone(), 
                e, 
                opt.iterations, 
                &mut lazy_enrichments
            );

            let focus = res.focus_index;
            let (slice_a, slice_b) = res.available_after_shock
                .split_at(focus);
            slice_a.iter()
                .for_each(|val| sum_without.add_assign(*val));
            slice_b.iter()
                .skip(1)
                .for_each(
                    |val| sum_without.add_assign(*val)
                );
            sum += slice_b[0];
            flow_status = res.flow_status;
        }
        let av_without = sum_without.sum() * recip;
        sum += sum_without;
        let av = sum.sum() * recip;
        let status = match flow_status{
            FlowStatus::AllGood => "None".to_owned(),
            FlowStatus::Missing(missing) => missing
        };
        writeln!(buf, "{e} {av} {av_without} {status}").unwrap();
    }
    println!("created {file_name}");
}

#[inline]
fn c_map<'a>(id: &str, country_map: &'a Option<BTreeMap<String, String>>) -> &'a str
{
    if let Some(map) = country_map{
        map.get(id).unwrap()
    } else {
        ""
    }
}


pub fn reduce_x<P>(opt: XOpts, in_file: P)
where P: AsRef<Utf8Path>
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

    let country_map = opt
        .country_map
        .as_deref()
        .map(country_map);

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
        // Keep the warning for unused variable as reminder that
        let mut flow_status = FlowStatus::AllGood;
        // CURRENTLY FLOW STATUS IS IGNORED HERE!
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
                // remove links that are no longer present
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
            flow_status = res.flow_status;
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
    let mut header_of_worst_integral = vec![
        "This_country_ID", 
        "WorstIntegral", 
        "ResponsibleExporterID"
    ];
    if country_map.is_some(){
        header_of_worst_integral.push("ResponsibleCountryName");
        header_of_worst_integral.push("ThisCountryName");
    }
    let mut buf_worst_integral = create_buf_with_command_and_version_and_header(worst_integral_name, header_of_worst_integral);

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
where P: AsRef<Utf8Path>
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

            let mut flow_status_name_addition = "";
            if matches!(res.flow_status, FlowStatus::Missing(_)){
                flow_status_name_addition = "MIS_";
            }

            
            let name_stub = format!(
                "{}{}_{}_y{}_e{e}.dat",
                flow_status_name_addition, 
                s.get_string(), 
                enrich_item_name_string, 
                opt.year
            );

            v.push(name_stub);
            let name = v.last().unwrap();
            if is_first{
                gp_names.push(
                    format!(
                        "{}{}_item{}_y{}_e{e}",
                        flow_status_name_addition, 
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
        
            let mut buf = create_buf_with_command_and_version_and_header(name, HIST_HEADER);
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
            let mut buf = create_buf_with_command_and_version_and_header(&name, HIST_HEADER);

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


#[derive(Serialize, Deserialize, Clone, Debug)]
pub enum FlowStatus{
    AllGood,
    Missing(String)
}

impl FlowStatus{
    fn name_addition(&self) -> &'static str
    {
        match self{
            Self::AllGood => {
                ""
            },
            Self::Missing(_) => {
                "MIS_"
            }
        }
    }
}

fn calc_available(
    network: &Network,
    enrich: &BTreeMap<String, ExtraInfo>,
    shock: &ShockRes,
    node_map: &ExtraInfoMap,
    quiet: bool
) -> (Vec<f64>, FlowStatus)
{
    let mode_lock = MODE.read().unwrap();
    let mode = mode_lock.deref();
    let inverted = network.invert();
    let (import, export) = match network.direction{
        Direction::ExportTo => (&inverted, network),
        Direction::ImportFrom => (network, &inverted)
    };

    let original_import = calc_acc_trade(import);
    let original_export = calc_acc_trade(export);


    let production_id = node_map.get(PRODUCTION);
    let stock_id = node_map.get(STOCK);
    let stock_variation_id = node_map.get(STOCK_VARIATION);
    let mut at_least_some_stock = false;
    let mut at_least_some_stock_variation = false;
    let mut at_least_some_production = false;
    let unit = &import.unit;
    let unit_tester = UNIT_TESTER.deref();


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

                    match mode {
                        SimulationMode::Classic => {
                            // Noting additional needs to be done in classic case
                        },
                        SimulationMode::WithStockVariation => {
                            // negative sign 
                            // -> negative stock variation means 
                            //    that the country took something out of the stock and into
                            //    the market (or whatever else)
                            if let Some(stock_variation) = extra.map.get(&stock_variation_id){
                                assert!(unit_tester.is_equiv(unit, &stock_variation.unit));
                                at_least_some_stock_variation = true;
                                total -= stock_variation.amount;
                            }
                        },
                        SimulationMode::OnlyStock => {
                            // In this mode we tread the stock similar to 
                            // production. It is completely available and
                            // we can IGNORE the STOCK VARIATION
                            if let Some(stock) = extra.map.get(&stock_id){
                                assert!(unit_tester.is_equiv(unit, &stock.unit));
                                at_least_some_stock = true;
                                total += stock.amount;
                            }
                        }
                    }
                }
                // think about what to do if total is negative
                if !quiet && total < 0.0 {
                    eprintln!("small total! {total}");
                }

                total
            }
        ).collect();

    let mut missing = String::new();

    if !at_least_some_production{
        missing.push_str("Production");
        if !quiet{
            eprintln!("No production data!")
        }
    }
    match mode{
        SimulationMode::Classic => {
            // ignore
        },
        SimulationMode::OnlyStock => {
            if !at_least_some_stock{
                missing.push_str("Stock");
                if !quiet{
                    eprintln!("No stock data!")
                }
            }
        },
        SimulationMode::WithStockVariation => {
            if !at_least_some_stock_variation{
                missing.push_str("Stockvariation");
                if !quiet{
                    eprintln!("No stock variation data!")
                }
            } 
        }
    }

    drop(mode_lock);
    let status = if missing.is_empty(){
        FlowStatus::AllGood
    } else {
        FlowStatus::Missing(missing)
    };
    (res, status)

}

// uses: https://de.mathworks.com/matlabcentral/fileexchange/9700-random-vectors-with-fixed-sum
// see also: https://www.cs.york.ac.uk/rts/static/papers/R:Emberson:2010a.pdf
fn rand_fixed_sum<R>(
    n: usize, 
    m: NonZeroUsize, 
    sum: f64, 
    a: f64, 
    b: f64,
    mut rng: R
) -> Vec<Vec<f64>> 
where R: Rng
{
    let n_isize = n as isize;
    if sum <= 0.0 {
        return vec![vec![0.0;n]; m.get()];
    } else if sum >= n as f64
    {
        return vec![vec![1.0; n]; m.get()];
    }
    let b_minus_a = b - a;
    let dist = Uniform::new(0.0, 1.0);
    let rescale = (sum-n as f64 * a)/(b_minus_a);
    let k = (rescale.floor() as isize).min((n-1) as isize).max(0);
    let s = rescale.min((k + 1) as f64).max(k as f64);
    let s1 = (k -n_isize+1..=k).rev()
        .map(|i| rescale - i as f64)
        .collect_vec();
    let s2 = (k+1..=k+n_isize)
        .rev()
        .map(|i| i as f64 - rescale)
        .collect_vec();

    let mut w = vec![vec![0.0;n+1]; n];
    w[0][1] = f64::MAX;
    let mut t = vec![vec![0.0; n]; n-1];
    let tiny = f64::MIN_POSITIVE;


    for i in 2..=n{
        let a = &w[i-2][1..=i];
        let b = &s1[..i];
        debug_assert_eq!(a.len(), b.len());
        let recip = (i as f64).recip();
        let tmp1 = a.iter()
            .zip(b)
            .map(|(w_val, s1_val)| *w_val * *s1_val * recip)
            .collect_vec();
        let a = &w[i-2][..i];
        let b = &s2[n-i..n];
        let tmp2 = a.iter()
            .zip(b)
            .map(|(val_w, val_s2)| *val_w * *val_s2 * recip)
            .collect_vec();
        let to_change = &mut w[i-1][1..=i];
        debug_assert_eq!(to_change.len(), tmp1.len());
        debug_assert_eq!(tmp1.len(), tmp2.len());
        to_change.iter_mut()
            .zip(tmp1.iter())
            .zip(tmp2.iter())
            .for_each(
                |((ch, t1), t2)|
                {
                    *ch = t1 + t2;
                }
            );
        let tmp3 = w[i-1][1..=i]
            .iter()
            .map(|val| *val + tiny)
            .collect_vec();
        let a = &s2[n-i..n];
        let b = &s1[..i];
        let tmp4 = a.iter()
            .zip(b)
            .map(|(left, right)| left > right)
            .collect_vec();
        t[i-2][..i]
            .iter_mut()
            .enumerate()
            .for_each(
                |(idx, val)|
                {
                    *val = (tmp2[idx] / tmp3[idx]) * (tmp4[idx] as u8 as f64)
                            + (1.0 - tmp1[idx] / tmp3[idx]) * ((!tmp4[idx]) as u8 as f64);   
                }
            )
    }

    let mut x = vec![vec![0.0; m.get()]; n];
    let mut gen_rand = |len: usize|
    {
        (0..len)
        .map(
            |_|
            {
                dist.sample_iter(&mut rng)
                    .take(m.get())
                    .collect_vec()
            }
        ).collect_vec()
    };
    let rt = gen_rand(n-1);
    let rs = gen_rand(n-1);
    let mut s = vec![s; m.get()];
    let mut j = vec![(k+1) as usize; m.get()];
    let mut sm = vec![0.0; m.get()];
    let mut pr = vec![1.0; m.get()];

    for i in (1..n).rev()
    {
        // use rt to choose a transition
        let e = rt[n-i-1]
            .iter()
            .zip(j.iter())
            .map(
                |(rt, j)|
                {
                    *rt <= t[i-1][*j-1]
                }
            ).collect_vec();
        let i_recip = (i as f64).recip();
        let ip1_recip = ((i+1) as f64).recip();
        // use rs to compute next simplex coord
        let sx = rs[n-i-1]
            .iter()
            .map(|val| val.powf(i_recip))
            .collect_vec();
        // update sum
        sm.iter_mut()
            .enumerate()
            .for_each(
                |(idx, val)|
                {
                    *val += (1.0 - sx[idx]) * pr[idx] * s[idx] * ip1_recip;
                }
            );
        // update product
        pr.iter_mut()
            .zip(sx.iter())
            .for_each(|(p,s)| *p *= *s);
        // calc x using simplex coord
        x[n-i-1].iter_mut()
            .enumerate()
            .for_each(
                |(idx, x)|
                {
                    *x = pr[idx].mul_add(e[idx] as u64 as f64, sm[idx])
                }
            );
        // transition adjustment
        s.iter_mut()
            .zip(e.iter())
            .for_each(
                |(s, e)| *s -= *e as u64 as f64
            );
        j.iter_mut()
            .zip(e.iter())
            .for_each(
                |(j, e)|
                *j -= *e as usize
            );
    }
    // compute last x
    x[n-1]
        .iter_mut()
        .enumerate()
        .for_each(
            |(idx, x)|
            {
                *x = pr[idx].mul_add(s[idx], sm[idx]);
                *x = *x * b_minus_a + a;
            }
        );

   let len = x[0].len();
   (0..len)
        .map(
            |i|
            {
                let mut vec = x.iter()
                    .map(|slice| slice[i])
                    .collect_vec();
                vec.shuffle(&mut rng);
                vec
            }
        ).collect_vec()
}

pub fn check_quick_and_dirty(
    top: &[usize], 
    network: &Network, 
    enrich: &BTreeMap<String, ExtraInfo>,
    product_id: &str,
    year: i32
) -> bool
{
    let mode_lock = MODE.read().unwrap();
    let mode = mode_lock.deref();
    eprint!("Y {year} - ");
    let import_dir = network.get_network_with_direction(Direction::ImportFrom);
    let production_key = GLOBAL_NODE_INFO_MAP.get(PRODUCTION);
    let stock_key = GLOBAL_NODE_INFO_MAP.get(STOCK);
    let stock_variation_key = GLOBAL_NODE_INFO_MAP.get(STOCK_VARIATION);
    let mut is_good = true;
    for idx in top{
        let id = network.nodes[*idx]
            .identifier
            .as_str();
        let extra = match enrich.get(id){
            Some(p) =>  p,
            None => {
                eprintln!("product_ID {product_id} has no Extra - idx {idx}");
                is_good = false;
                continue;
            }
        };
        let production = extra.map.get(&production_key);
        let production = match production{
            Some(e) => e.amount,
            None => {
                eprintln!("product_ID {product_id} has no Production - idx {idx}");
                is_good = false;
                continue;
            }
        };
        let vs = network.nodes[*idx].trade_amount();

        let item_available_from_self = match mode {
            SimulationMode::Classic => {
                production
            },
            SimulationMode::OnlyStock => {
                let stock = match extra.map.get(&stock_key){
                    None => 0.0,
                    Some(extra_item) => extra_item.amount
                };
                stock + production
            },
            SimulationMode::WithStockVariation => {
                let stock_variation = match extra.map.get(&stock_variation_key){
                    None => 0.0,
                    Some(extra_item) => extra_item.amount
                };
                production - stock_variation
            }
        };
        if item_available_from_self < vs {
            let import = import_dir.nodes[*idx].trade_amount();
            let frac = vs / item_available_from_self;
            eprintln!("SELF < T  SELF: {item_available_from_self} E: {vs} I: {import} F: {frac} product_ID {product_id} - idx {idx}");
            is_good = false;
        }
    }
    drop(mode_lock);
    if is_good{
        eprintln!("Product {product_id} is good");
    }
    is_good
}