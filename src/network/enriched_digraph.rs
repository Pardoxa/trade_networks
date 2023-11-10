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

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct EnrichedDigraphHelper{
    nodes: Vec<EnrichedNodeHelper>
}

pub struct EnrichedDigraphs{
    pub digraphs: Vec<()>,
    pub start_year: usize
}


#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct EnrichedNodeHelper{
    pub identifier: String,
    pub extra: ExtraInfo,
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
            }
        ).collect();
    todo!("check the units. Also, all nodes should contain the same infos, check that");
    // If they do contain the same infos: maybe I want to transform the BTreeMap into a Vec
    // and attach a map to the digraph so that one knows what the indices mean.
    //EnrichedDigraphs { digraphs, start_year: enrichments.starting_year }
}