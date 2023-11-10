use{
    std::{
        io::{
            BufReader, 
            BufRead
        },
        fs::File,
        collections::{
            BTreeMap,
            BTreeSet
        }
    },
    crate::network::*,
    crate::parser::enriched_digraph::*
};

pub fn line_to_vec(line: &str) -> Vec<String>
{
    let mut w = String::new();
    let mut all = Vec::new();
    let mut inside = false;
    for c in line.chars(){
        if c == '"' {
            inside = !inside;
        }else if !inside && c == ','{
            let mut n = String::new();
            std::mem::swap(&mut n, &mut w);
            all.push(n);
        } else {
            w.push(c);
        }
    }
    all.push(w);
    all
}


pub fn parse_extra(in_file: &str, target_item_code: &str) -> EnrichmentInfos
{
    const START_YEAR: usize = 2010;
    let year_start_str = format!("Y{START_YEAR}");
    let map = crate::network::enriched_digraph::NodeInfoMap::new();

    let file = File::open(in_file)
        .expect("unable to open file");
    let buf = BufReader::new(file);
    let mut lines = buf.lines()
        .map(|l| l.expect("read error"));
    let first_line = lines.next()
        .unwrap();
    let header = line_to_vec(&first_line);

    let mut header_map = BTreeMap::new();
    for (i, s) in header.into_iter().enumerate(){
        header_map.insert(s, i);
    }

    let item_code_id = *header_map.get("Item Code")
        .expect("no item codes available? Did you specify the correct file?");
    let unit_id = *header_map.get("Unit")
        .expect("Does not contain Unit");
    let info_id = *header_map.get("Element")
        .expect("Does not contain Element");
    let country_id = *header_map.get("Area Code")
        .expect("Does not contain Area code");

    let start_year_id = *header_map.get(&year_start_str)
        .expect(&year_start_str);
    let total = header_map.len() - start_year_id;

    let mut enrichments = EnrichmentInfos::new(total, START_YEAR);

    for l in lines{
        let v = line_to_vec(&l);
        let item_code = &v[item_code_id];
        if item_code == target_item_code{
            let unit = &v[unit_id];
            let info_type = &v[info_id];
            let entry_id = map.get(info_type);
            let country = &v[country_id];
            
            for (year_idx, amount_str) in v[start_year_id..].iter().enumerate(){
                if amount_str.is_empty(){
                    continue;
                }
                let amount: f64 = amount_str.parse()
                    .expect("Error in parsing amount as float");
                let extra = Extra{unit: unit.clone(), amount};
                let country_info = enrichments.get_mut_inserting(year_idx, country);
                country_info.push(entry_id, extra);
            }
            
        }
    }
    enrichments
}


pub fn network_parser(file_name: &str, item_code: &str, silent: bool) -> anyhow::Result<Vec<Network>>
{

    let wanted_transaction_type = "Import Quantity";
    let file = File::open(file_name)
        .unwrap();
    let reader = BufReader::with_capacity(64 * 1024, file);

    let mut lines = reader.lines();
    let header = lines.next().unwrap().unwrap(); 
    let entry_iter = header.split(',');

    let entry_names: Vec<_> = entry_iter.collect();

    // Reporter country, partner country, item code
    if !silent{
        for (idx, &entry) in entry_names.iter().enumerate() {
            println!("Entry {idx} is {entry}");
        }
    }


    let mut map = BTreeMap::new();
    for (idx, &entry) in entry_names.iter().enumerate()
    {
        map.insert(entry, idx);
    }
    let item_id = *map.get("Item Code").unwrap();
    let reporter_country = "Reporter Country Code";
    let reporter_country_id = *map.get(reporter_country).unwrap();
    let partner_country = "Partner Country Code";
    let partner_country_id = *map.get(partner_country).unwrap();
    let unit = "Unit";
    let unit_id = *map.get(unit).unwrap();

    let transaction_type = *map.get("Element").unwrap();

    let mut glob_unit: Option<String> = None;

    let mut countries: BTreeSet<String> = BTreeSet::new();

    let y1986 = *map.get("Y1986").unwrap();

    let line_len = map.len();


    let line_iter = lines
        .map(|line| {
            let line = line.unwrap();
            line_to_vec(&line)
        })
        .filter(
            |item| 
            {
                item[item_id] == item_code 
                && item[transaction_type] == wanted_transaction_type
            });

    for line_vec in line_iter{
        let unit = line_vec.get(unit_id).unwrap();
        if let Some(u) = &glob_unit{
            if !u.eq(unit){
                return Err(anyhow::anyhow!("Unit error! old {} new {}", u, unit));
            }
        } else {
            glob_unit = Some(unit.to_owned());
        }

        if line_vec.len() != line_len{
            return Err(anyhow::anyhow!("Line error! old_len {line_len} new {}", line_vec.len()));
        }

        let rep_c = line_vec.get(reporter_country_id).unwrap();
        countries.insert(rep_c.clone());
        let part_c = line_vec.get(partner_country_id).unwrap();
        countries.insert(part_c.clone());
    }

    let all: Vec<_> = countries.iter()
        .map(
            |item| 
            Node::new(item.clone())
        ).collect();

    let mut id_map = BTreeMap::new();

    all.iter().enumerate()
        .for_each(
            |(id, item)|
            {
                let code = &item.identifier;
                id_map.insert(code.to_owned(), id);
            }
        );

    let network = Network{nodes: all};

    let mut years: Vec<_> = (y1986..map.len())
        .map(|_| network.clone())
        .collect();

    let file = File::open(file_name)
        .unwrap();
    let reader = BufReader::with_capacity(64 * 1024, file);

    let iter = reader.lines()
        .skip(1)
        .map(|line| {
            let line = line.unwrap();
            line_to_vec(&line)
        })
        .filter(
            |item| 
            {
                item[item_id] == item_code 
                && item[transaction_type] == wanted_transaction_type
            });

    iter.for_each(
        |line|
        {
            let rep_c = line.get(reporter_country_id).unwrap();
            let part_c = line.get(partner_country_id).unwrap();

            let rep_id = *id_map.get(rep_c).unwrap();
            let part_id = *id_map.get(part_c).unwrap();

            (y1986..).zip(years.iter_mut())
                .for_each(
                    |(idx, network)|
                    {
                        let amount_entry = &line[idx];
                        if !amount_entry.is_empty(){
                            let amount: f64 = amount_entry.parse().unwrap();
                            let node = network.nodes.get_mut(rep_id).unwrap();
                            let edge = Edge{
                                amount,
                                index: part_id
                            };
                            node.adj.push(edge);
                        }
                        
                    }
                )
        }
    );

    Ok(years)
}


pub fn country_networks(networks: &[Network], code_file: String) -> Vec<Network>
{
    let file = File::open(code_file)
        .unwrap();
    let buf_reader = BufReader::new(file);
    let lines = buf_reader
        .lines()
        .map(|r| r.unwrap())
        .skip(1);

    let mut code_country_map: BTreeMap<_,_> = BTreeMap::new();
    let mut country_set: BTreeSet<_> = BTreeSet::new();

    for line in lines {
        let mut s_iter = line.split(',');
        let code = s_iter.next().unwrap();
        let name = s_iter.nth(1).unwrap();

        code_country_map.insert(code.to_owned(), name.to_owned());
    }

    for n in networks.iter()
    {
        for node in n.nodes.iter(){
            let country = code_country_map.get(node.identifier.as_str()).unwrap();
            country_set.insert(country.as_str());
        }
    }

    let country_network = country_set
        .into_iter()
        .map(
            |entry|
            {
                Node{
                    identifier: entry.to_string(),
                    adj: Vec::new()
                }
            }
        ).collect();
    let network = Network{
        nodes: country_network
    };

    let mut index_map = BTreeMap::new();
    for (idx, node) in network.nodes.iter().enumerate()
    {
        index_map.insert(node.identifier.as_str(), idx);
    }

    networks
        .iter()
        .map(
            |old_network|
            {
                let mut n = network.clone();

                for node in old_network.nodes.iter()
                {
                    let this_country = code_country_map.get(&node.identifier)
                        .expect("identifyer invalid");
                    let this_index = *index_map.get(this_country.as_str())
                        .expect("invalid identifier");
                    let adj = &mut n.nodes.get_mut(this_index).unwrap().adj;
                    for others in node.adj.iter(){
                        let other_code = old_network
                            .nodes[others.index]
                            .identifier.as_str();
                        let other_country = code_country_map.get(other_code)
                            .expect("country_code_unknown");
                        let index = *index_map.get(other_country.as_str())
                            .expect("other country identifier invalid");
                        adj.push(
                            Edge { index, amount: others.amount }
                        );
                    }
                }

                n.nodes.iter_mut()
                    .for_each(
                        |node|
                        {
                            if !node.adj.is_empty(){
                                node.adj.sort_unstable_by_key(|item| item.index);
                                
                                let mut iter = node.adj.iter();
                                let first = iter.next().unwrap();
                                let mut new_adj = vec![first.to_owned()];
                                for edge in iter 
                                {
                                    let last_entry = new_adj.last_mut().unwrap();
                                    if edge.index == last_entry.index{
                                        last_entry.amount += edge.amount;
                                    } else {
                                        new_adj.push(edge.to_owned());
                                    }
                                }
                                node.adj = new_adj;
                            }
                        }
                    );
                n
            }
        ).collect()

}