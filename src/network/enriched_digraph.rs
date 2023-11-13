use {
    serde::{Serialize, Deserialize},
    std::{
        collections::BTreeMap,
        cmp::Ordering,
        ops::Deref
    },
    super::*
};


const POSSIBLE_NODE_INFO: [&str; 26] = [
    "Area harvested",
    "Domestic supply quantity",
    "Export Quantity",
    "Fat supply quantity (g/capita/day)",
    "Fat supply quantity (t)",
    "Feed",
    "Food",
    "Food supply (kcal)",
    "Food supply (kcal/capita/day)",
    "Food supply quantity (kg/capita/yr)",
    "Import Quantity",
    "Laying",
    "Losses",
    "Other uses (non-food)",
    "Processing",
    "Production",
    "Producing Animals/Slaughtered",
    "Protein supply quantity (g/capita/day)",
    "Protein supply quantity (t)",
    "Residuals",
    "Seed",
    "Stocks",
    "Stock Variation",
    "Total Population - Both sexes",
    "Yield",
    "Yield/Carcass Weight"
];

pub struct NodeInfoMap<'a>{
    pub map: BTreeMap<&'a str, u8>
}

impl<'a> NodeInfoMap<'a>{
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
        *self.map
            .get(key)
            .expect(key)
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
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct EnrichmentInfos{
    starting_year: usize,
    enrichments: Vec<BTreeMap<String, ExtraInfo>>
}

impl EnrichmentInfos{
    pub fn new(num_entries: usize, starting_year: usize) -> Self
    {
        let e = (0..num_entries)
            .map(|_| BTreeMap::new())
            .collect();
        Self{
            starting_year,
            enrichments: e
        }
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
    pub start_year: usize
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
    start_year_networks: usize, 
    networks: &[Network], 
    enrichments: EnrichmentInfos
) -> EnrichedDigraphs
{
    let (starting_year, networks, enrichments): (_, _, &[_]) = match start_year_networks
        .cmp(&enrichments.starting_year)
    {
        Ordering::Equal => {
            (start_year_networks, networks, &enrichments.enrichments)
        },
        Ordering::Less => {
            let start_idx = enrichments.starting_year - start_year_networks;
            (enrichments.starting_year, &networks[start_idx..], &enrichments.enrichments)
        },
        Ordering::Greater => {
            let start_idx = start_year_networks - enrichments.starting_year;
            (start_year_networks, networks, &enrichments.enrichments[start_idx..])
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