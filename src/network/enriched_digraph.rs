use {
    serde::{Serialize, Deserialize},
    std::{
        collections::BTreeMap,
        cmp::Ordering,
        ops::Deref
    },
    super::*,
};
use lazy_static::lazy_static;

lazy_static! {
    pub static ref GLOBAL_NODE_INFO_MAP: ExtraInfoMap<'static> = {
        ExtraInfoMap::new()
    };
}

pub const PRODUCTION: &str = "Production";
pub const TOTAL_POPULATION: &str = "Total Population - Both sexes";
pub const EXPORT_QUANTITY: &str = "Export Quantity";
pub const IMPORT_QUANTITY: &str = "Import Quantity";
pub const STOCK_VARIATION: &str = "Stock Variation";
pub const STOCK: &str = "Stocks";

const POSSIBLE_NODE_INFO: [&str; 37] = [
    "Area harvested",
    "Domestic supply quantity",
    EXPORT_QUANTITY,
    "Fat supply quantity (g/capita/day)",
    "Fat supply quantity (t)",
    "Feed",
    "Food",
    "Food supply (kcal)",
    "Food supply (kcal/capita/day)",
    "Food supply quantity (kg/capita/yr)",
    IMPORT_QUANTITY,
    "Laying",
    "Losses",
    "Milk Animals",
    "Other uses (non-food)",
    "Processing",
    PRODUCTION,
    "Prod Popultn",
    "Producing Animals/Slaughtered",
    "Protein supply quantity (g/capita/day)",
    "Protein supply quantity (t)",
    "Residuals",
    "Seed",
    STOCK,
    STOCK_VARIATION,
    "Tourist consumption",
    TOTAL_POPULATION,
    "Yield",
    "Yield/Carcass Weight",
    "Calories/Year",
    "Fats/Year",
    "Food supply quantity (tonnes)",
    "Food supply quantity (g/capita/day)",
    "Loss",
    "Processed",
    "Proteins/Year",
    "Total Population - Both sexes"
];

const EQUIV_LIST: [(&str, &str); 3] = [
    ("Export quantity", EXPORT_QUANTITY),
    ("Import quantity", IMPORT_QUANTITY),
    ("Opening stocks", STOCK)
];

lazy_static! {
    pub static ref PARSE_EQUIV_MAP: BTreeMap<&'static str, &'static str> = {
        EQUIV_LIST.into_iter()
            .collect()
    };
}

pub struct ExtraInfoMap<'a>{
    pub map: BTreeMap<&'a str, u8>
}

impl<'a> ExtraInfoMap<'a>{
    pub fn new() -> Self
    {
        Self::from_slice(&POSSIBLE_NODE_INFO)
    }

    pub fn from_slice<S>(s: &'a [S]) -> Self
    where S: Deref<Target = str>
    {
        let mut map = BTreeMap::new();
        for (s, i) in s.iter().zip(0..){
            map.insert(s.deref(), i);
        }
        Self{map}
    }

    pub fn get(&self, key: &str) -> u8
    {
        match self.map.get(key)
        {
            Some(id) => *id,
            None => {
                match PARSE_EQUIV_MAP.get(key)
                {
                    Some(equiv_key) => {
                        self.get(equiv_key)
                    }, 
                    None => {
                        panic!("Unknown key: {key}")
                    }
                }
            }   
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Extra{
    pub unit: String,
    pub amount: f64
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ExtraInfo{
    pub map: BTreeMap<u8, Extra>
}

impl ExtraInfo {
    pub fn push(&mut self, entry_id: u8, extra: Extra)
    {
        let old = self.map.insert(entry_id, extra);
        assert!(old.is_none());
    }

    fn new() -> Self
    {
        Self{map: BTreeMap::new()}
    }

    pub fn fuse(&mut self, other: &Self){
        for (key, value) in other.map.iter(){
            let old = self.map.insert(*key, value.clone());
            assert!(
                old.is_none(),
                "Extra info already present!"
            );
        }
    }
}

pub enum LazyEnrichmentInfos{
    Filename(String, Option<String>),
    Enriched(EnrichmentInfos)
}

impl LazyEnrichmentInfos{
    pub fn lazy_option(filename: Option<String>) -> Option<Self>
    {
        filename.map(
            |f| 
            {
                let mut enrichment = LazyEnrichmentInfos::Filename(f, None);
                enrichment.assure_availability();
                enrichment
            }
        )
    }

    #[inline]
    pub fn assure_availability(&mut self){
        if let Self::Filename(f, target_item_code) = self{
            let e = crate::parser::parse_extra(
                f, 
                target_item_code
            );
            *self = Self::Enriched(
                e
            );
        }
    }

    pub fn get_year_unchecked(&self, year: i32) -> &BTreeMap<String, ExtraInfo>
    {
        self.enrichment_infos_unchecked().get_year(year)
    }

    pub fn enrichment_infos_unchecked(&self) -> &EnrichmentInfos
    {
        if let Self::Enriched(e) = self {
            return e;
        } 
        panic!("{}", crate::misc::AVAILABILITY_ERR)
    }

    pub fn extra_info_idmap_unchecked(&self) -> ExtraInfoMap
    {
        self.enrichment_infos_unchecked().get_node_map()
    }

    pub fn get_item_codes_unchecked(&self) -> &[String]{
        &self.enrichment_infos_unchecked()
            .sorted_item_codes
    }

    pub fn item_codes_as_string_unchecked(&self) -> String
    {
        let slice = self.get_item_codes_unchecked();
        let cap = 10 * slice.len();
        let mut s = String::with_capacity(cap);
        for item_code in slice{
            s.push_str("Item");
            s.push_str(item_code);
            s.push('_');
        }
        s
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct EnrichmentInfos{
    pub starting_year: i32,
    pub sorted_item_codes: Vec<String>,
    pub possible_node_info: Vec<String>,
    pub enrichments: Vec<BTreeMap<String, ExtraInfo>>
}

impl EnrichmentInfos{
    pub fn get_node_map(&self) -> ExtraInfoMap
    {
        ExtraInfoMap::from_slice(self.possible_node_info.as_slice())
    }

    pub fn fuse(&mut self, other: &Self)
    {
        self.possible_node_info
            .iter()
            .zip(other.possible_node_info.iter())
            .for_each(
                |(s,o)|
                {
                    assert_eq!(s,o);
                }
            );
        let mut other_slice = other.enrichments.as_slice();
        let s = match self.starting_year.cmp(&other.starting_year)
        {
            Ordering::Equal => self.enrichments.as_mut_slice(),
            Ordering::Less => {
                let diff = (other.starting_year - self.starting_year) as usize;
                &mut self.enrichments[diff..]
            },
            Ordering::Greater => {
                let diff = (self.starting_year - other.starting_year) as usize;
                other_slice = &other_slice[diff..];
                self.enrichments.as_mut_slice()
            }
        };

        s.iter_mut()
            .zip(other_slice)
            .for_each(
                |(this, other)|
                {
                    for (key, val) in other.iter(){
                        if let Some(e) = this.get_mut(key){
                            e.fuse(val);
                        } else {
                            this.insert(key.clone(), val.clone());
                        }
                    }
                }
            );
    }

    pub fn new(num_entries: usize, starting_year: i32, item_code: String) -> Self
    {
        let e = (0..num_entries)
            .map(|_| BTreeMap::new())
            .collect();
        let infos = POSSIBLE_NODE_INFO.iter()
            .map(|i| i.to_string())
            .collect();
        Self{
            starting_year,
            possible_node_info: infos,
            enrichments: e,
            sorted_item_codes: vec![item_code]
        }
    }

    #[inline]
    pub fn year_to_idx(&self, year: i32) -> usize
    {
        (year - self.starting_year) as usize
    }

    pub fn get_year(&self, year: i32) -> & BTreeMap<String, ExtraInfo>
    {
        let idx =  self.year_to_idx(year);
        &self.enrichments[idx]
    }

    pub fn get_mut_inserting<'a>(&'a mut self, year_idx: usize, country: &str) -> &'a mut ExtraInfo
    {
        let year = &mut self.enrichments[year_idx];
        if year.get_mut(country).is_none(){
            year.insert(country.to_string(), ExtraInfo::new());
        }
        
        year.get_mut(country).unwrap()
    }
}

#[derive(Clone, Debug)]
pub struct EnrichedDigraphHelper{
    nodes: Vec<EnrichedNodeHelper>,
    direction: Direction
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct EnrichedDigraph{
    units: Vec<String>,
    extra_header: Vec<u8>,
    direction: Direction,
    nodes: Vec<EnrichedNode>
}

impl EnrichedDigraph{
    #[allow(dead_code)]
    pub fn get_idx(&self, id: u8) -> Option<usize>
    {
        self.extra_header
            .iter()
            .position(|&e| e == id)
    }
}

impl From<EnrichedDigraphHelper> for EnrichedDigraph{
    fn from(other: EnrichedDigraphHelper) -> Self {

        let mut unit_map: BTreeMap<u8, String> = BTreeMap::new();
        
        for node in other.nodes.iter(){
            for (&id, e) in node.extra.map.iter(){
                if let Some(unit) = unit_map.get(&id){
                    if  !e.unit.eq(unit){
                        panic!("unit error - {} ({}) vs {} ({})", unit, unit.len(), e.unit, e.unit.len());
                    }
                } else {
                    unit_map.insert(id, e.unit.clone());
                }
            }
        }
        let (extra_header, units): (Vec<_>, Vec<_>) = unit_map
            .into_iter()
            .unzip();

        let nodes: Vec<_> = other.nodes
            .into_iter()
            .map(
                |n|
                {
                    let mut extra_map = BTreeMap::new();
                    for (id, unit) in extra_header.iter().zip(units.iter()){
                        let extra = n.extra.map.get(id);
                        if let Some(e) = extra{
                            assert_eq!(&e.unit, unit);
                            extra_map.insert(*id, e.amount);
                        }
                    }
                    
                    EnrichedNode { 
                        identifier: n.identifier, 
                        extra: extra_map, 
                        adj: n.adj
                    }                  
                }
            ).collect();


        Self { units, extra_header, nodes, direction: other.direction }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct EnrichedDigraphs{
    pub extra_header_map: Vec<String>,
    pub digraphs: Vec<EnrichedDigraph>,
    pub start_year: i32
}


#[derive(Clone, Debug)]
pub struct EnrichedNodeHelper{
    pub identifier: String,
    pub extra: ExtraInfo,
    pub adj: Vec<Edge>
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct EnrichedNode{
    pub identifier: String,
    pub extra: BTreeMap<u8, f64>,
    pub adj: Vec<Edge>
}

pub fn enrich_networks(
    networks: &[Network], 
    enrichments: EnrichmentInfos
) -> EnrichedDigraphs
{
    let start_year_networks = networks[0].year;
    let (starting_year, networks, enrichments): (_, _, &[_]) = match start_year_networks
        .cmp(&enrichments.starting_year)
    {
        Ordering::Equal => {
            (start_year_networks, networks, &enrichments.enrichments)
        },
        Ordering::Less => {
            let start_idx = enrichments.starting_year - start_year_networks;
            (enrichments.starting_year, &networks[start_idx as usize..], &enrichments.enrichments)
        },
        Ordering::Greater => {
            let start_idx = start_year_networks - enrichments.starting_year;
            (start_year_networks, networks, &enrichments.enrichments[start_idx as usize..])
        }
    };

    assert_eq!(networks.len(), enrichments.len());

    let digraphs: Vec<_> = networks
        .iter()
        .zip(enrichments.iter())
        .map(
            |(network, enrichment)|
            {
                //let network = network.without_unconnected_nodes();
                //if network.node_count() != enrichment.len(){
                //    eprintln!("WARNING: Enrichment in index {index} - {} vs {}", network.node_count(), enrichment.len());
                //}
                let e_nodes = network.nodes
                .iter()
                .map(
                    |node|
                    {
                        let extra = match enrichment
                            .get(node.identifier.as_str()){
                            Some(extra) => extra.clone(),
                            None => {
                                //println!("{index}, no extra for {}", &node.identifier);
                                ExtraInfo::new()
                            }        
                        };
                        EnrichedNodeHelper{
                            identifier: node.identifier.clone(),
                            extra: extra.clone(),
                            adj: node.adj.clone()
                        }
                    }
                ).collect();
                    
                EnrichedDigraphHelper{nodes: e_nodes, direction: network.direction}
                    .into()
            }
        ).collect();
    
    let extra_header_meaning = POSSIBLE_NODE_INFO.iter()
        .map(|e| e.to_string())
        .collect();

    EnrichedDigraphs { 
        digraphs, 
        start_year: starting_year,
        extra_header_map: extra_header_meaning
    }
}