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
}