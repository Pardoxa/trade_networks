use std::collections::BTreeMap;
use net_ensembles::Graph;
use serde::{Serialize, Deserialize};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Node{
    pub identifier: String,
    pub adj: Vec<Edge>
}

impl Node {
    pub fn new(code: String) -> Self
    {
        Self{
            identifier: code,
            adj: Vec::new()
        }
    }
}

#[allow(dead_code)]
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Edge{
    pub index: usize,
    pub amount: f64
}


#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Network{
    pub nodes: Vec<Node>
}

impl Network{
    #[allow(dead_code)]
    pub fn without_unconnected_nodes(&self) -> Self
    {

        let mut list_of_connected = vec![false; self.node_count()];
        self.nodes.iter()
            .enumerate()
            .for_each(
                |(index, node)|
                {
                    if !node.adj.is_empty(){
                        list_of_connected[index] = true;
                        node.adj
                            .iter()
                            .for_each(
                                |edge|
                                {
                                    list_of_connected[edge.index] = true;
                                }
                            )
                    }
                }
            );

        if list_of_connected.iter().all(|x| *x){
            eprintln!("All connected, nothing to do");
            self.clone()
        } else {
            eprintln!("Not all connected, something to do");
            let nodes: Vec<Node> = self.nodes
                .iter()
                .zip(list_of_connected.iter())
                .filter(|(_, &in_list)| in_list)
                .map(|(node, _)| Node{identifier: node.identifier.clone(), adj: Vec::new()})
                .collect();
        
            let mut network = Network{nodes};
            let mut new_map = BTreeMap::new();

            for (index, node) in network.nodes.iter().enumerate(){
                new_map.insert(node.identifier.clone(), index);
            }

            for (old_node, connected) in self.nodes.iter().zip(list_of_connected){
                if !connected{
                    continue;
                }
                let this_id = *new_map.get(old_node.identifier.as_str()).unwrap();
                let adj = &mut network.nodes[this_id].adj;
                for edge in old_node.adj.iter()
                {
                    let new_index = *new_map.get(self.nodes[edge.index].identifier.as_str()).unwrap();
                    adj.push(Edge { index: new_index, amount: edge.amount }); 
                }
            }
            network
        }
    }

    pub fn invert(&self) -> Self
    {
        let mut all: Vec<_> = self.nodes.iter()
            .map(
                |item|
                {
                    Node{
                        identifier: item.identifier.clone(),
                        adj: Vec::new()
                    }
                }
            ).collect();
        
        for (idx, node) in self.nodes.iter().enumerate()
        {
            for other_node in node.adj.iter()
            {
                let edge = Edge{
                    amount: other_node.amount,
                    index: idx
                };
                all[other_node.index].adj.push(edge);
            }
        }
        Network { nodes: all }
    }

    pub fn normalize(&mut self)
    {
        self.nodes.iter_mut()
            .for_each(
                |node|
                {
                    if !node.adj.is_empty() {
                        let sum: f64 = node.adj.iter().map(|e| e.amount).sum();
                        node.adj.iter_mut()
                            .for_each(
                                |e| 
                                {
                                    e.amount /= sum;
                                }
                            );
                    }
                }
            )
    }

    #[inline]
    pub fn node_count(&self) -> usize
    {
        self.nodes.len()
    }

    pub fn nodes_with_neighbors(&self) -> usize
    {
        self.nodes.iter().filter(|n| !n.adj.is_empty()).count()
    }

    pub fn edge_count(&self) -> usize
    {
        self.nodes.iter().map(|n| n.adj.len()).sum()
    }

    #[allow(dead_code)]
    /// NOTE: Untested, but should work
    /// Only Intended for normalized networks
    pub fn dikstra_normalized(&self, initial_node: usize) -> (Vec<f64>, Vec<DikstraState>)
    {
        let mut distances = vec![f64::INFINITY; self.node_count()];
        distances[initial_node] = 0.0;

        let mut visit_state = vec![false; self.node_count()];
        
        let mut current_index = initial_node;
        let mut from_whom = vec![DikstraState::Unreachable; self.node_count()];
        from_whom[initial_node] = DikstraState::Initial;

        loop{
            let current_node = &self.nodes[current_index];
            for neighbors in current_node.adj.iter(){
                let id = neighbors.index;
                if !visit_state[id]{
                    let d = distances[current_index] + (1.0 - neighbors.amount);
                    if d < distances[id]{
                        distances[id] = d;
                        from_whom[id] = DikstraState::From(current_index);
                    }
                }
            }
            visit_state[current_index] = true;

            let mut min_dist = f64::INFINITY;
            let mut min_index = usize::MAX;
            distances.iter()
                .zip(visit_state.iter())
                .enumerate()
                .for_each(
                    |(index, (&distance, visited))|
                    {
                        if !visited && distance < min_dist{
                            
                            min_dist = distance;
                            min_index = index;
                            
                        }
                    }
                );
            if min_dist.is_finite(){
                current_index = min_index;
            } else {
                break;
            }
        }
        (distances, from_whom)
    }

    /// only for normalized networks
    pub fn my_centrality_normalized(&self) -> Vec<u32>
    {
        let mut centrality = vec![0; self.node_count()];
        for i in 0..self.node_count(){
            let (dists, path_helper) = self.dikstra_normalized(i);
            #[allow(clippy::needless_range_loop)]
            for j in 0..self.node_count(){
                if dists[j].is_finite(){
                    let mut current_id = j;
                    loop{
                        centrality[current_id] += 1;
                        match path_helper[current_id] {
                            DikstraState::From(from) => current_id = from,
                            DikstraState::Initial => break,
                            DikstraState::Unreachable => unreachable!()
                        }
                    }
                }
            }
        }
        centrality
    }

    pub fn to_undirected(&self) -> Graph<ReducedNode>
    {
        let mut g = Graph::from_iter(
            self.nodes.iter()
                .map(|node| ReducedNode { id: node.identifier.clone() })
        );
        self.nodes.iter()
            .enumerate()
            .for_each(
                |(index, node)|
                {
                    node.adj.iter()
                        .for_each(
                            |edge|
                            {
                                let _ = g.add_edge(index, edge.index);
                            }
                        )
                }
            );
        g
    }

    pub fn filtered_network(&self, indices: &[usize]) -> Self
    {
        let mut nodes = Vec::with_capacity(indices.len());
        let mut new_map = BTreeMap::new();
        for (new_index, i) in indices.iter().copied().enumerate() {
            let node = Node{identifier: self.nodes[i].identifier.clone(), adj: Vec::new()};
            new_map.insert(node.identifier.clone(), new_index);
            nodes.push(node);
        }

        for old_node in self.nodes.iter(){
            if let Some(this_idx) = new_map.get(old_node.identifier.as_str()) {
                let adj = &mut nodes[*this_idx].adj;
                for edge in old_node.adj.iter(){
                    if let Some(other_idx) = new_map.get(self.nodes[edge.index].identifier.as_str()){
                        let new_edge = Edge{
                            amount: edge.amount,
                            index: *other_idx
                        };
                        adj.push(new_edge);
                    }
                }  
            }
        }

        Network { nodes }
    }

    /// Note: out component of imports should be equal to in component of exports
    pub fn out_component(&self, start: usize, including_self: ComponentChoice) -> Vec<usize>
    {
        let mut processed = vec![false; self.node_count()];

        processed[start] = true;
        let mut stack = vec![start];

        let mut component = Vec::new();
        if including_self.includes_self(){
            component.push(start);
        }

        while let Some(index) = stack.pop() {
            for edge in self.nodes[index].adj.iter(){
                if !processed[edge.index]{
                    processed[edge.index] = true;
                    component.push(edge.index);
                    stack.push(edge.index);
                }
            }
        }
        component
    }

    pub fn largest_out_component(&self, including_self: ComponentChoice) -> usize 
    {
        let mut checked = vec![false; self.node_count()];
        let mut max_size = 0;
        for i in 0..self.node_count(){
            if !checked[i] {
                checked[i] = true;
                let comp = self.out_component(i, including_self);
                for &j in comp.iter(){
                    checked[j] = true;
                }
                let size = comp.len();
                if size > max_size {
                    max_size = size;
                }
            }
        }
        max_size
    }

}

#[derive(Clone, Copy, Debug)]
#[allow(dead_code)]
pub enum ComponentChoice{
    IncludingSelf,
    ExcludingSelf
}

impl ComponentChoice{
    pub fn includes_self(&self) -> bool {
        matches!(self, ComponentChoice::IncludingSelf)
    }
}

/// Returns size of largest component and indizes of members of largest component
pub fn largest_component(network: &Network) -> LargestComponents
{
    let g = network.to_undirected();
    let (num_components, ids) = g.connected_components_ids();
    let mut sizes = vec![0; num_components];
    for id in ids.iter()
    {
        sizes[*id as usize] += 1;
    }
    let mut max = 0;
    let mut max_id = 0;
    for (&size, index) in sizes.iter().zip(0..){
        if max < size {
            max = size;
            max_id = index;
        }
    }
    let entries: Vec<_> = ids.iter()
        .enumerate()
        .filter(|(_, &id)| id == max_id)
        .map(|(index, _)| index)
        .collect();

    assert_eq!(max, entries.len());

    LargestComponents { 
        ids, 
        members_of_largest_component: entries, 
        num_components, 
        size_of_largest_component: max 
    }

}

pub struct LargestComponents{
    pub ids: Vec<isize>,
    pub members_of_largest_component: Vec<usize>,
    pub num_components: usize,
    pub size_of_largest_component: usize
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub enum DikstraState{
    Unreachable,
    Initial,
    From(usize)
}


#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct ReducedNode{
    id: String
}

