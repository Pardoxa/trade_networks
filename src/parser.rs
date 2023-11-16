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
    let mut start_year: Option<usize> = None;
    for (i, s) in header.into_iter().enumerate(){
        if start_year.is_none() && s.starts_with('Y') {
            let number = &s[1..];
            start_year = number.parse().ok();
        }
        header_map.insert(s, i);
    }
    let start_year = start_year.unwrap();
    let year_start_str = format!("Y{start_year}");

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

    let mut enrichments = EnrichmentInfos::new(total, start_year);
    let mut not_even_once = true;
    for l in lines{
        let v = line_to_vec(&l);
        let item_code = &v[item_code_id];
        if item_code == target_item_code{
            let unit = &v[unit_id];
            let info_type = &v[info_id];
            let entry_id = map.get(info_type);
            let country = &v[country_id];
            not_even_once = false;
            
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
    if not_even_once{
        dbg!(&enrichments);
        panic!("Item code is not contained within the specified data set!");
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
    let mut first_year: Option<usize> = None;
    for (idx, &entry) in entry_names.iter().enumerate()
    {
        if first_year.is_none() && entry.starts_with('Y'){
            let number = &entry[1..];
            first_year = number.parse().ok();
        }
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

    let first_year = first_year.unwrap();
    let year_head = format!("Y{}", first_year);
    let start_year = *map.get(year_head.as_str()).unwrap();

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

    let network = Network{
        nodes: all, 
        direction: Direction::ImportFrom, 
        year: first_year as i32
    };

    let mut years: Vec<_> = (start_year..map.len())
        .zip(0..)
        .map(|(_, i)| 
            {
                let mut n = network.clone();
                n.year += i;
                n
            }
        )
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

            (start_year..).zip(years.iter_mut())
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


pub fn country_map(code_file: &str) -> BTreeMap<String, String>
{
    let file = File::open(code_file)
        .unwrap();
    let buf_reader = BufReader::new(file);
    let lines = buf_reader
        .lines()
        .map(|r| r.unwrap())
        .skip(1);

    let mut code_country_map: BTreeMap<_,_> = BTreeMap::new();

    for line in lines {
        let mut s_iter = line.split(',');
        let code = s_iter.next().unwrap();
        let name = s_iter.nth(1).unwrap();

        code_country_map.insert(code.to_owned(), name.to_owned());
    }

    code_country_map

}