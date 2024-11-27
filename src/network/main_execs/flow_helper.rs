use crate::network::{Network, Node};
use camino::Utf8PathBuf;
use itertools::*;
use derivative::*;
use std::{
    ops::RangeInclusive,
    num::*
};
use serde::{Serialize, Deserialize};

pub fn calc_acc_trade(network: &Network) -> Vec<f64>
{
    network
        .nodes
        .iter()
        .map(Node::trade_amount)
        .collect()
}

pub fn calc_recip(original: &[f64]) -> Vec<f64>
{
    original.iter()
        .copied()
        .map(f64::recip)
        .collect()
}

#[derive(Debug)]
pub struct ExportShockItem{
    pub export_id: usize,
    pub export_frac: f64
}

pub struct CalcShockMultiJob<'a>{
    pub exporter: Vec<ExportShockItem>,
    pub unrestricted_node_idxs: Vec<usize>,
    pub original_imports: &'a [f64],
    pub original_imports_recip: &'a [f64],
    pub original_exports: &'a [f64],
    pub original_exports_recip: &'a [f64],
    pub iterations: usize
}


impl<'a> CalcShockMultiJob<'a>{

    pub fn new_exporter(
        mut exporter: Vec<ExportShockItem>,
        iterations: usize,
        export_network: &Network,
        original_imports: &'a[f64],
        original_imports_recip: &'a[f64],
        original_exports: &'a[f64],
        original_exports_recip: &'a[f64],
    ) -> Self
    {
        exporter.sort_unstable_by_key(|e| e.export_id);

        let (mut first, mut slice) = match exporter.split_first()
        {
            None => {
                dbg!(exporter);
                panic!("ERROR");
            },
            Some(v) => v
        };
        
        let free_ids = (0..export_network.nodes.len())
            .filter(
                |&id|
                {
                    if id == first.export_id{
                        if !slice.is_empty(){
                            (first, slice) = slice.split_first()
                                .unwrap();
                        }
                        false
                    } else {
                        true
                    }
                }
            ).collect_vec();
        Self { 
            exporter, 
            unrestricted_node_idxs: free_ids, 
            iterations, 
            original_exports, 
            original_imports,
            original_imports_recip,
            original_exports_recip
        }
    }

    /// Len is length of network
    /// ids need to be sorted
    #[allow(clippy::too_many_arguments)]
    pub fn new_const_export(
        ids: &[usize],
        export_frac: f64,
        iterations: usize,
        export_network: &Network,
        original_exports: &'a [f64],
        original_exports_recip: &'a [f64],
        original_imports: &'a [f64],
        original_imports_recip: &'a [f64]
    ) -> Self
    {
        let sorted_ids = ids.iter()
            .copied()
            .sorted_unstable()
            .collect_vec();

        let (mut first, mut slice) = sorted_ids
            .split_first()
            .map(|(f, s)| (*f, s))
            .unwrap();
        
        let free_ids = (0..export_network.nodes.len())
            .filter(
                |&id|
                {
                    if id == first{
                        if !slice.is_empty(){
                            (first, slice) = slice.split_first()
                                .map(|(f,s)| (*f, s))
                                .unwrap();
                        }
                        false
                    } else {
                        true
                    }
                }
            ).collect_vec();
        let exporter = sorted_ids.iter()
            .copied()
            .map(|id| ExportShockItem{export_id: id, export_frac})
            .collect_vec();

        Self { 
            exporter, 
            unrestricted_node_idxs: free_ids, 
            iterations, 
            original_exports, 
            original_imports,
            original_imports_recip,
            original_exports_recip
        }
    }

    pub fn change_export_frac(&mut self, export_frac: f64)
    {
        self.exporter
            .iter_mut()
            .for_each(|e| e.export_frac = export_frac);
    }

    pub fn add_exporter(&mut self, exporter: ExportShockItem)
    {
        let pos = self.unrestricted_node_idxs
            .iter()
            .position(|&id| id == exporter.export_id)
            .unwrap();
        self.unrestricted_node_idxs.remove(pos);
        self.exporter.push(exporter);
        self.exporter.sort_unstable_by_key(|e| e.export_id);
    }

    #[allow(dead_code)]
    pub fn reduce_or_add(&mut self, exporter: usize, reducing_factor: f64)
    {
        let pos = self.exporter
            .iter()
            .position(|ex| ex.export_id == exporter);
        match pos{
            None => {
                let item = ExportShockItem{
                    export_id: exporter,
                    export_frac: reducing_factor
                };
                self.add_exporter(item);
            },
            Some(idx) => {
                self.exporter[idx].export_frac *= reducing_factor;
            }
        }
    }
}


#[derive(Debug, Serialize, Deserialize, Derivative)]
#[derivative(Default)]
pub struct ShockCloud
{
    /// File with enrich infos
    pub enrich_file: Utf8PathBuf,

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
    pub original_avail_filter: f64,

    #[derivative(Default(value="NonZeroUsize::new(1000).unwrap()"))]
    pub cloud_steps: NonZeroUsize,

    #[derivative(Default(value="NonZeroUsize::new(5).unwrap()"))]
    pub cloud_m: NonZeroUsize,

    pub seed: u64,

    pub reducing_factor: f64,

    #[derivative(Default(value="NonZeroUsize::new(100).unwrap()"))]
    pub hist_bins: NonZeroUsize
}

#[derive(Debug, Serialize, Deserialize, Derivative)]
#[derivative(Default)]
pub struct ShockCloudAll
{
    /// File with enrich infos
    pub enrich_glob: String,

    /// File with the network data
    pub network_glob: String,

    /// Which year to check
    #[derivative(Default(value = "2000..=2019"))]
    pub years: RangeInclusive<i32>,

    /// Iterations
    #[derivative(Default(value="10000"))]
    pub iterations: usize,    

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
    pub original_avail_filter: f64,

    #[derivative(Default(value="NonZeroUsize::new(1000).unwrap()"))]
    pub cloud_steps: NonZeroUsize,

    #[derivative(Default(value="NonZeroUsize::new(5).unwrap()"))]
    pub cloud_m: NonZeroUsize,

    pub seed: u64,

    pub reducing_factor: f64,

    #[derivative(Default(value="NonZeroUsize::new(100).unwrap()"))]
    pub hist_bins: NonZeroUsize,

    /// File to map ids to countries
    pub id_file: Option<String>,
}