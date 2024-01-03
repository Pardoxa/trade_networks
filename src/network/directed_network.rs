use{
    std::{
        collections::{BTreeMap, VecDeque}, 
        num::NonZeroU32,
        fs::File,
        io::{BufReader, Write},
        path::{
            Path,
            PathBuf
        }
    },
    net_ensembles::Graph,
    serde::{Serialize, Deserialize},
    super::helper_structs::*,
    strum::EnumString,
    crate::{
        misc::*,
        config::*
    }
};

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq, EnumString)]
pub enum Direction{
    #[strum(ascii_case_insensitive)]
    ExportTo,
    #[strum(ascii_case_insensitive)]
    ImportFrom
}

impl Direction{
    pub fn invert(self) -> Self{
        match self{
            Self::ExportTo => Self::ImportFrom,
            Self::ImportFrom => Self::ExportTo
        }
    }

    pub fn is_import(self) -> bool
    {
        matches!(self, Direction::ImportFrom)
    }

    pub fn is_export(&self) -> bool 
    {
        matches!(self, Direction::ExportTo)
    }
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq, EnumString)]
pub enum NetworkType{
    #[strum(ascii_case_insensitive)]
    Value,
    #[strum(ascii_case_insensitive)]
    Quantity
}



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

    pub fn trade_amount(&self) -> f64
    {
        self.adj.iter().map(|edge| edge.amount).sum()
    }
}

#[allow(dead_code)]
#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
pub struct Edge{
    pub index: usize,
    pub amount: f64
}


pub fn read_networks<P>(file_name: P) -> Vec<Network>
where P: AsRef<Path>
{
    let path: &Path = file_name.as_ref();
    let file = File::open(path).unwrap();
    let reader = BufReader::new(file);
    let res = bincode::deserialize_from(reader);
    match res{
        Ok(o) => o,
        Err(e) => {
            let file = File::open(path).unwrap();
            let reader = BufReader::new(file);
            match serde_json::from_reader(reader){
                Ok(o) => o,
                Err(e2) => {
                    panic!(
                        "Bincode: {:?} JSON: {:?} Failed to deserialize",
                        e,
                        e2
                    )
                }
            }
            
        }
    }
}

pub struct GraphVizExtra{
    pub highlight: String,
    pub map: Option<BTreeMap<String, String>>
}

#[derive(Clone, Debug)]
pub enum LazyNetworks{
    Filename(PathBuf),
    Networks(Vec<Network>, Vec<Network>)
}



impl LazyNetworks{
    /// This needs to be called once before any of the unchecked members.
    /// Otherwise the unchecked members will panic!
    pub fn assure_availability(&mut self){
        if let Self::Filename(f) = self{
            let mut networks = read_networks(f);
            match &networks[0].direction
            {
                Direction::ImportFrom => {
                    networks.iter_mut().for_each(|n| n.force_direction(Direction::ImportFrom));
                    let export = networks
                        .iter()
                        .map(|n| n.get_network_with_direction(Direction::ExportTo))
                        .collect();
                    *self = Self::Networks(networks, export)
                },
                Direction::ExportTo => {
                    networks.iter_mut().for_each(|n| n.force_direction(Direction::ExportTo));
                    let import = networks
                        .iter()
                        .map(|n| n.get_network_with_direction(Direction::ImportFrom))
                        .collect();
                    *self = Self::Networks(import, networks)
                }
            }
        }
    }


    pub fn get_export_network_unchecked(&self, year: i32) -> &Network
    {
        if let Self::Networks(_, export_networks) = self {
            for n in export_networks.iter(){
                if n.year == year {
                    return n;
                }
            }
        } else {
            panic!("{AVAILABILITY_ERR}")
        }
        panic!("{YEAR_ERR}")
    }

    pub fn get_import_network_unchecked(&self, year: i32) -> &Network
    {
        if let Self::Networks(import_networks, _) = self {
            for n in import_networks.iter(){
                if n.year == year {
                    return n;
                }
            }
        } else {
            panic!("{AVAILABILITY_ERR}")
        }
        panic!("{YEAR_ERR}")
    }

    pub fn export_networks_unchecked(&self) -> &[Network]
    {
        match self{
            Self::Filename(_) => unreachable!(),
            Self::Networks(_, export) => {
                export
            }
        }
    }

    pub fn import_networks_unchecked(&self) -> &[Network]
    {
        match self{
            Self::Filename(_) => unreachable!(),
            Self::Networks(import, _) => {
                import
            }
        }
    }

}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Network{
    pub direction: Direction,
    pub data_origin: ReadType,
    pub unit: String,
    pub nodes: Vec<Node>,
    pub year: i32,
    pub sorted_item_codes: Vec<String>
}

impl Network{

    pub fn item_codes_as_string(&self) -> String
    {
        let slice = self.sorted_item_codes.as_slice();
        let (first, rest) = slice.split_first().unwrap();
        let mut result = first.clone();
        for other in rest {
            result.push('_');
            result.push_str(other);
        }
        result
    }

    pub fn ordered_by_trade_volume(&self, order_helper: OrderHelper) -> Vec<(f64, &Node)>
    {
        let mut list: Vec<_> = self.nodes
            .iter()
            .map(
                |node|
                {
                    let amount = node.trade_amount();
                    (amount, node)
                }
            ).collect();
        
        let order_fn = order_helper.get_order_fun();
        list.sort_unstable_by(|a, b| order_fn(a.0, b.0));
        list
    }

    pub fn estimation(&'_ self, focus: usize) -> Vec<f64>
    {
        /*
                New surplus = export - top*reduction = 0
                => export = top*reduction
                ergo reduction = export / top
                reduction = 1 - export fraction
                ergo export fraction = 1 - export / top
        */
        let inverted = self.invert();
        let (import, export) = match self.direction{
            Direction::ImportFrom => (self, &inverted),
            Direction::ExportTo => (&inverted, self)
        };

        let total_export: Vec<_> = export
            .nodes.iter()
            .map(Node::trade_amount)
            .collect();

        let mut result = vec![None; export.node_count()];

        export.nodes[focus]
            .adj
            .iter()
            .for_each(
                |edge|
                {
                    result[edge.index] = Some(
                        1.0 - total_export[edge.index] / edge.amount
                    );
                }
            );
        result[focus] = Some(0.0);

        import.nodes
            .iter()
            .map(
                |n|
                {
                    let total_import  = n.trade_amount();
                    n.adj.iter()
                        .map(
                            |edge| 
                            {
                                if let Some(v) = result[edge.index]{
                                    let importance = edge.amount / total_import;
                                    importance * v.clamp(0.0, 1.0)
                                } else {
                                    0.0
                                }
                            }
                        ).sum()
                }
            ).collect()
    }

    pub fn get_index(&self, s: &str) -> Option<usize>
    {
        self.nodes
            .iter()
            .position(|n| n.identifier == s)
    }

    pub fn graphviz<'a, W>(
        &'a self, 
        mut w: W, 
        extra: &'a GraphVizExtra
    ) -> std::io::Result<()>
    where W: Write
    {
        writeln!(w, "digraph {{")?;
        writeln!(w, "overlap=false")?;
        writeln!(w, "splines=true")?;

        let map: Box<dyn Fn(&'a str) -> &'a str> = match &extra.map{
            None => Box::new(|s| s),
            Some(m) => {
                Box::new(
                    |s| {
                        m.get(s).unwrap()
                    }
                )
            }
        };

        for node in self.nodes.iter()
        {
            if node.identifier != extra.highlight{
                writeln!(w, "\"{}\"", map(&node.identifier))?;
            } else {
                writeln!(w, "\"{}\" [fillcolor=red, style=filled]", map(&node.identifier))?;
            }
        }

        for node in self.nodes.iter()
        {
            let this = map(&node.identifier);
            for e in node.adj.iter()
            {
                writeln!(
                    w, 
                    "\"{}\" -> \"{}\"", 
                    this, 
                    map(&self.nodes[e.index].identifier)
                )?;
            }
        }
        writeln!(w, "}}")
    }

    #[inline]
    pub fn force_direction(&mut self, direction: Direction)
    {
        if self.direction != direction{
            *self = self.invert()
        }
    }

    #[inline]
    pub fn get_network_with_direction(&self, direction: Direction) -> Self
    {
        if self.direction == direction{
            self.clone()
        } else {
            self.invert()
        }
    }

    pub fn effective_trade_only(&self) -> Self
    {
        let mut effective_network: Vec<_> = self.nodes
            .iter()
            .map(|n| Node{identifier: n.identifier.clone(), adj: Vec::new()})
            .collect();

        // create edge map
        let mut edge_map = BTreeMap::new();
        for (i, node) in self.nodes.iter().enumerate(){
            for e in node.adj.iter(){
                edge_map.insert((i, e.index), e.amount);
            }
        }

        for (&(from, to), &amount) in edge_map.iter(){
            match edge_map.get(&(to, from)){
                Some(other_amount) => {
                    // other edge exists
                    if amount > *other_amount {
                        let new_edge = Edge{index: to, amount: amount - *other_amount};
                        effective_network[from].adj.push(new_edge);
                    }
                },
                None => {
                    // other edge does not exist
                    let edge = Edge{index: to, amount};
                    effective_network[from].adj.push(edge);
                }
            }
        }
        Network { 
            nodes: effective_network, 
            direction: self.direction, 
            year: self.year,
            data_origin: self.data_origin,
            unit: self.unit.clone(),
            sorted_item_codes: self.sorted_item_codes.clone()
        }
    }

    pub fn sorted_by_largest_in(&self) -> Vec<(usize, f64)>
    {
        let mut for_sorting: Vec<(_,f64)> = self.nodes
            .iter()
            .enumerate()
            .map(
                |(i, n)|
                {
                    (i, n.adj.iter().map(|e| e.amount).sum())
                }
            ).collect();
        for_sorting
            .sort_unstable_by(|a, b| b.1.total_cmp(&a.1));
        for_sorting
    }

    pub fn list_of_trading_nodes(&self) -> Vec<usize>
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
            (0..self.node_count()).collect()
        } else {
            list_of_connected.iter()
                .enumerate()
                .filter(|(_, &connected)| connected)
                .map(|(index, _)| index)
                .collect()
        }
    }
    
    pub fn without_unconnected_nodes(&self) -> Self
    {
        let connected_indices = self.list_of_trading_nodes();
        self.filtered_network(connected_indices.iter())
        
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
        Network { 
            nodes: all, 
            direction: self.direction.invert(),
            year: self.year,
            data_origin: self.data_origin,
            unit: self.unit.clone(),
            sorted_item_codes: self.sorted_item_codes.clone()
        }
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

    pub fn nodes_with_non_empty_adj(&self) -> usize
    {
        self.nodes.iter().filter(|n| !n.adj.is_empty()).count()
    }

    pub fn edge_count(&self) -> usize
    {
        self.nodes.iter().map(|n| n.adj.len()).sum()
    }

    /// NOTE: Untested, but should work
    /// Only Intended for normalized networks
    #[allow(dead_code)]
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
    #[allow(dead_code)]
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

    pub fn distance_from_index(&self, idx: usize) -> Vec<Option<u32>>
    {
        let mut processed = vec![false; self.node_count()];
        let mut current_queue = VecDeque::new();
        let mut next_queue = VecDeque::new();
        let mut distance = vec![None; processed.len()];

        current_queue.push_back(idx);
        processed[idx] = true;
        let mut level = 0;
        loop{
            while let Some(current_index) = current_queue.pop_front() {
                distance[current_index] = Some(level);
                let node = &self.nodes[current_index];
                for edge in node.adj.iter(){
                    if !processed[edge.index] {
                        processed[edge.index] = true;
                        next_queue.push_back(edge.index);
                    }
                }
            }
            if next_queue.is_empty(){
                break;
            }
            std::mem::swap(&mut current_queue, &mut next_queue);
            level += 1;
        }
        distance       
    }

    pub fn diameter(&self) -> Option<usize>
    {
        let mut diameter = 0;
        let mut processed = vec![false; self.node_count()];
        let mut current_queue = VecDeque::new();
        let mut next_queue = VecDeque::new();
        for i in 0..self.node_count(){
            processed.iter_mut().for_each(|e| *e = false);
            current_queue.push_back(i);
            processed[i] = true;
            let mut level = 0;
            loop{
                while let Some(current_index) = current_queue.pop_front() {
                    let node = &self.nodes[current_index];
                    for edge in node.adj.iter(){
                        if !processed[edge.index] {
                            processed[edge.index] = true;
                            next_queue.push_back(edge.index);
                        }
                    }
                }
                if next_queue.is_empty(){
                    break;
                }
                std::mem::swap(&mut current_queue, &mut next_queue);
                level += 1;
            }
            diameter = diameter.max(level);
            if processed.iter().any(|x| !x) {
                dbg!(&processed);
                dbg!(self);
                return None;
            }
        }
        Some(diameter)
    }

    pub fn filtered_network<'a, I>(&'a self, indices: I) -> Self
    where I: Iterator<Item = &'a usize>
    {
        let mut nodes = Vec::new();
        let mut new_map = BTreeMap::new();
        for (new_index, i) in indices.copied().enumerate() {
            let node = Node{identifier: self.nodes[i].identifier.clone(), adj: Vec::new()};
            new_map.insert(node.identifier.clone(), new_index);
            nodes.push(node);
        }

        for old_node in self.nodes.iter(){
            if let Some(this_idx) = new_map.get(old_node.identifier.as_str()) {
                let adj = &mut nodes[*this_idx].adj;
                for edge in old_node.adj.iter(){
                    if let Some(other_idx) = new_map.get(
                            self
                                .nodes[edge.index]
                                .identifier
                                .as_str()
                        ){
                        let new_edge = Edge{
                            amount: edge.amount,
                            index: *other_idx
                        };
                        adj.push(new_edge);
                    }
                }  
            }
        }

        Network { 
            nodes, 
            direction: self.direction,
            year: self.year,
            data_origin: self.data_origin,
            unit: self.unit.clone(),
            sorted_item_codes: self.sorted_item_codes.clone()
        }
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

    pub fn scc_recursive(&self) -> Vec<Vec<usize>>
    {
        let mut counter = NonZeroU32::new(1).unwrap();
        

        let mut stack: Vec<usize> = Vec::new();
        let mut numbers = vec![TarjanNumberHelper::NotVisited; self.node_count()];
        let mut low_link = vec![TarjanNumberHelper::NotVisited; self.node_count()];

        let mut components: Vec<Vec<usize>> = Vec::new();

        fn rec(
            n: &Network, 
            id: usize, 
            counter: &mut NonZeroU32, 
            low: &mut [TarjanNumberHelper], 
            num: &mut [TarjanNumberHelper], 
            stack: &mut Vec<usize>, 
            components: &mut Vec<Vec<usize>>
        ) {
            let this_num = *counter;
            *counter = counter.saturating_add(1);
            num[id] = TarjanNumberHelper::Visited(this_num);
            low[id] = num[id];
            stack.push(id);

            for edge in n.nodes[id].adj.iter(){
                match num[edge.index]{
                    TarjanNumberHelper::NotVisited => {
                        rec(n, edge.index, counter, low, num, stack, components);
                        low[id] = low[id].min(low[edge.index]);
                    },
                    TarjanNumberHelper::Visited(num) if num < this_num => {
                        // is frond or cross-link
                        // If this function is ever to slow:
                        // I should be able to exchange the 'contains' by just checking wheather the number 
                        // is larger or equal to a threshold, which is determined when the function is first called,
                        // i.e., before the recursion
                        if stack.contains(&edge.index) {
                            low[id] = low[id].min(TarjanNumberHelper::Visited(num));
                        }
                    },
                    _ => {
                        // edge is to be ignored
                        
                    }
                }
            }

            if low[id] == num[id] {
                // id is root!

                let mut comp = Vec::new();

                while let Some(top) = stack.pop() {
                    if num[top].get_num() < this_num {
                        stack.push(top);
                        break;
                    }
                    comp.push(top);
                }
                components.push(comp);   
            }
        }

        for i in 0..self.node_count(){
            if numbers[i].is_not_visited(){
                rec(
                    self, 
                    i, 
                    &mut counter, 
                    &mut low_link, 
                    &mut numbers, 
                    &mut stack, 
                    &mut components
                );
                stack.clear();
            }
            
        }

        let all: usize = components.iter()
            .map(|entry| entry.len())
            .sum();
        assert_eq!(self.node_count(), all);

        components
        
    }

}




/// Returns size of largest connected component and indizes of members of largest component
pub fn largest_connected_component(network: &Network) -> LargestComponents
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



#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn test_scc_recursive_tree() {
        let mut networks = read_networks("tree.bincode");
        dbg!(&networks);
        let network = networks.pop().unwrap();
        let g = network.to_undirected();

        let (comp_num, ids) = g.connected_components_ids();
        dbg!(ids);
        assert_eq!(comp_num, 1);

        let scc = network.scc_recursive();
        assert_eq!(scc.len(), network.node_count());

        
    }

    #[test]
    fn test_scc_recursive_example() {
        let mut networks = read_networks("scc_test.bincode");
        dbg!(&networks);
        let network = networks.pop().unwrap();
        let g = network.to_undirected();

        let (comp_num, ids) = g.connected_components_ids();
        dbg!(ids);
        assert_eq!(comp_num, 1);

        let mut scc = network.scc_recursive();
        

        scc.sort_by_cached_key(|el| *el.iter().min().unwrap());
        scc.iter_mut().for_each(|el| el.sort_unstable());

        dbg!(&scc);
        assert_eq!(scc.len(), 4);

        assert_eq!(&scc[0], &[0,1,2]);
        assert_eq!(&scc[1], &[3,4]);
        assert_eq!(&scc[2], &[5,6]);
        assert_eq!(&scc[3], &[7]);

        for c in scc.iter(){
            let filtered = network.filtered_network(c.iter());
            assert!(filtered.diameter().is_some());
        }
    }
}