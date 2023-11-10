use {
    serde::{Serialize, Deserialize},
    std::collections::BTreeMap,
    super::*
};


const POSSIBLE_NODE_INFO: [&str; 20] = [
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
    "Losses",
    "Other uses (non-food)",
    "Processing",
    "Production",
    "Protein supply quantity (g/capita/day)",
    "Protein supply quantity (t)",
    "Residuals",
    "Seed",
    "Stock Variation",
    "Total Population - Both sexes",
];

pub struct NodeInfoMap{
    pub map: BTreeMap<&'static str, u8>
}

impl NodeInfoMap{
    pub fn new() -> Self
    {
        let mut map = BTreeMap::new();
        for (&s, i) in POSSIBLE_NODE_INFO.iter().zip(0..){
            map.insert(s, i);
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
    nodes: Vec<EnrichedNodeHelper>
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct EnrichedDigraph{
    units: Vec<String>,
    extra_header: Vec<u8>,
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

        let first = other.nodes.first()
            .expect("Empty network!");
        let mut units = Vec::with_capacity(first.extra.map.len());
        let mut extra_header = Vec::with_capacity(units.len());
        for (&id, e) in first.extra.map.iter(){
            extra_header.push(id);
            units.push(e.unit.clone());
        }

        let nodes: Vec<_> = other.nodes
            .into_iter()
            .map(
                |n|
                {
                    assert_eq!(units.len(), n.extra.map.len());
                    let mut extra_vec = Vec::with_capacity(extra_header.len());
                    for (id, unit) in extra_header.iter().zip(units.iter()){
                        let extra = n.extra.map.get(id)
                            .expect("missing extra");
                        assert_eq!(&extra.unit, unit);
                        extra_vec.push(extra.amount);
                    }
                    EnrichedNode { 
                        identifier: n.identifier, 
                        extra: extra_vec, 
                        adj: n.adj
                    }
                }
            ).collect();

        Self { units, extra_header, nodes }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct EnrichedDigraphs{
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
    pub extra: Vec<f64>,
    pub adj: Vec<Edge>
}

pub fn enrich_networks(
    start_year_networks: usize, 
    networks: &[Network], 
    enrichments: EnrichmentInfos
) -> EnrichedDigraphs
{
    if start_year_networks > enrichments.starting_year{
        unimplemented!()
    }
    let start_idx = enrichments.starting_year - start_year_networks;

    let digraphs: Vec<_> = networks[start_idx..]
        .iter()
        .zip(enrichments.enrichments.iter())
        .map(
            |(network, enrichment)|
            {
                let network = network.without_unconnected_nodes();
                let e_nodes = network.nodes
                    .iter()
                    .map(
                        |node|
                        {
                            let extra = enrichment
                                .get(node.identifier.as_str())
                                .expect("No extra available!");
                            EnrichedNodeHelper{
                                identifier: node.identifier.clone(),
                                extra: extra.clone(),
                                adj: node.adj.clone()
                            }
                        }
                    ).collect();
                EnrichedDigraphHelper{nodes: e_nodes}
                    .into()
            }
        ).collect();

    EnrichedDigraphs { 
        digraphs, 
        start_year: enrichments.starting_year 
    }
}