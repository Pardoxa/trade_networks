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

}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub enum DikstraState{
    Unreachable,
    Initial,
    From(usize)
}