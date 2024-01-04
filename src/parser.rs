use std::{path::Path, collections::*};

use itertools::Itertools;

use crate::{config::ReadType, misc::*, UNIT_TESTER};

use{
    std::{
        io::{
            BufReader, 
            BufRead
        },
        ops::Deref,
        fs::File,
        collections::{
            BTreeMap,
            BTreeSet
        }
    },
    crate::network::*,
    crate::parser::enriched_digraph::*
};

#[derive(Clone, Copy)]
pub struct Years{
    pub start_year: i32,
    pub end_year: i32,
}

impl Years{
    pub fn min_max_bounds(&mut self, other: Years)
    {
        if self.start_year > other.start_year{
            self.start_year = other.start_year;
        }
        if self.end_year < other.end_year{
            self.end_year = other.end_year;
        }
    }
}

pub fn get_start_year(header_slice: &[String]) -> Years
{
    let mut start_year = i32::MAX;
    let mut end_year = i32::MIN;
    for s in header_slice.iter()
    {
        if let Some(s) = s.strip_prefix('Y'){
            let number: i32 = s.parse().unwrap();
            if number < start_year{
                start_year = number;
            }
            if number > end_year{
                end_year = number;
            }
        }
    }
    if start_year == i32::MAX{
        panic!("Unable to find start year");
    }
    Years { start_year, end_year }
}

pub struct LineIter<'a>
{
    line: &'a str,
}

impl<'a> LineIter<'a> {
    pub fn new(line: &'a str) -> Self
    {
        Self{line}
    }
}

impl<'a> From<LineIter<'a>> for &'a str
{
    fn from(value: LineIter<'a>) -> Self {
        value.line
    }
}

impl<'a> Iterator for LineIter<'a>
{
    type Item = &'a str;
    fn next(&mut self) -> Option<Self::Item> {
        if self.line.is_empty(){
            return None;
        }
        if let Some(rest) = self.line.strip_prefix('"')
        {
            let (next, rest) = rest.split_once('"')
                .expect("pattern wrong");
            // skip next "," if it exists
            match rest.strip_prefix(','){
                Some(remaining) => self.line = remaining,
                None => {
                    assert!(rest.is_empty());
                    self.line = rest;
                } 
            }
            Some(next)
        } else {
            match self.line.split_once(','){
                Some((next, rest)) => {
                    self.line = rest;
                    Some(next)
                },
                None => {
                    let next = self.line;
                    self.line = "";
                    Some(next)
                }
            }
        }
    }
}

pub fn line_to_str_vec(line: &'_ str) -> Vec<&'_ str>
{
    LineIter::new(line).collect()
}


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

pub fn parse_all_extras<I, P>(in_files: I, only_unit: Option<String>)
where I: IntoIterator<Item = P>,
    P: AsRef<Path>
{
    let paths: Vec<_> = in_files.into_iter().collect();
    let mut item_codes: HashMap<String, Years> = HashMap::new();
    let global_unit_tester = UNIT_TESTER.deref();
    for p in paths.iter(){
        let path = p.as_ref();
        let reader = open_bufreader(path);
        let mut lines = reader.lines()
            .map(Result::unwrap);

        let header = line_to_vec(&lines.next().unwrap());
        let item_code_idx = header.iter()
            .position(|item| item == "Item Code")
            .expect("No Item code available!");

        let unit_idx = header.iter()
            .position(|p| p == "Unit")
            .unwrap();

        let year_info = get_start_year(&header);

        for line in lines{
            let line_vec = line_to_str_vec(&line);
            if let Some(specific) = only_unit.as_deref(){
                let unit = line_vec[unit_idx];
                if !global_unit_tester.is_equiv(specific, unit){
                    continue;
                }
            }
            let item_code = line_vec[item_code_idx].to_string();
            item_codes.entry(item_code)
                .and_modify(
                    |stored_year|
                    {
                        stored_year.min_max_bounds(year_info);
                    }
                ).or_insert(year_info);
        }
    }

    let mut results: HashMap<_, _> = item_codes.iter()
        .map(
            |(item_code, year_info)|
            {
                let num_entries = year_info.end_year - year_info.start_year + 1;
                let enrichment = EnrichmentInfos::new(num_entries as usize, year_info.start_year, item_code.clone());
                (item_code.clone(), enrichment)
            } 
        ).collect();

    let global_node_info = GLOBAL_NODE_INFO_MAP.deref();
    
    let mut removed_counter = 0;

    for p in paths{
        let path = p.as_ref();
        let reader = open_bufreader(path);
        
        let mut lines = reader.lines()
            .map(Result::unwrap);
        let header = line_to_vec(&lines.next().unwrap());
        let year_info = get_start_year(&header);
        let header_map: HashMap<_,_> = header.into_iter()
            .zip(0_usize..)
            .collect();

        let item_code_idx = *header_map.get("Item Code").unwrap();

        let year_start_id = format!("Y{}", year_info.start_year);
        let year_start_idx = *header_map.get(&year_start_id).unwrap();

        let country_idx = *header_map.get("Area Code").unwrap();
        let info_idx = *header_map.get("Element")
            .expect("Does not contain Element");
        let unit_idx = *header_map.get("Unit")
            .expect("No Unit found");

        for line in lines {
            let line_vec = line_to_str_vec(&line);
            let item_code = line_vec[item_code_idx];

            let unit = line_vec[unit_idx];

            if let Some(specific_unit) = only_unit.as_deref(){
                if !global_unit_tester.is_equiv(specific_unit, unit){
                    continue;
                }
            }

            if let Some(enrichment) = results.get_mut(item_code){
                // enrichment is still valid.
                // now I need to modify it accordingly

                let country = line_vec[country_idx];

                let info_type = line_vec[info_idx];
                let info_type_u8 = global_node_info.get(info_type);
                

                for (s, year) in line_vec[year_start_idx..].iter().zip(year_info.start_year..)
                {
                    if !s.is_empty(){
                        let amount: f64 = s.parse().unwrap();
                        let year_idx = enrichment.year_to_idx(year);
                        let entry = enrichment.get_mut_inserting(year_idx, country);
                        let extra = Extra{
                            unit: unit.to_string(),
                            amount
                        };
                        if let Some(e) = entry.map.get(&info_type_u8){
                            assert!(
                                !global_unit_tester.is_equiv(&e.unit, &extra.unit),
                                "Info already present, but units equivalent?"
                            );
                            eprintln!(
                                "Removing {item_code} because of unit missmatch: {} vs {}",
                                e.unit,
                                extra.unit
                            );
                            results.remove(item_code);
                            removed_counter += 1;
                            break;
                        }
                        
                        entry.map.insert(info_type_u8, extra);
                    }
                }
            }
        }
    }

    if removed_counter > 0 {
        println!("Removed a total of {removed_counter} due to unit missmatches");
    }

    for (item_code, enrichment) in results.iter(){
        let name = format!("e{item_code}.bincode");
        let buf = create_buf(name);
        bincode::serialize_into(buf, enrichment)
            .unwrap();
    }

}

pub fn parse_extra<P>(in_file: P, target_item_code: &Option<String>) -> EnrichmentInfos
where P: AsRef<Path>
{
    let path = in_file.as_ref();
    println!("PARSING EXTRA");
    {
        let check_item_code = |item_codes: &[String]|
        {
            assert_eq!(1, item_codes.len());
            if let Some(t_item_code) = target_item_code{
                assert_eq!(
                    t_item_code, 
                    &item_codes[0],
                    "Missmatch in Item code between Request and Savefile"
                );
            } 
        };

        if path.extension().is_some_and(|ext| ext == "bincode"){
            let buf = open_bufreader(path);
            if let Ok(r) = bincode::deserialize_from::<_, EnrichmentInfos>(buf){
                check_item_code(&r.sorted_item_codes);
                return r;
            }
        }
        
        if path.extension().is_some_and(|ext| ext == "json") {
            let buf = open_bufreader(path);
            if let Ok(r) = serde_json::from_reader::<_, EnrichmentInfos>(buf){
                check_item_code(&r.sorted_item_codes);
                return r;
            }
        }

    }
    let target_item_code: &str = target_item_code
        .as_ref()
        .expect("Cannot parse as Json or Bincode -> item code required");
    let map = crate::network::enriched_digraph::ExtraInfoMap::new();

    let buf = open_bufreader(path);
    let mut lines = buf.lines()
        .map(|l| l.expect("read error"));
    let first_line = lines.next()
        .unwrap();
    let header = line_to_vec(&first_line);

    let mut header_map = BTreeMap::new();
    let mut start_year: Option<i32> = None;
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

    let mut enrichments = EnrichmentInfos::new(
        total, 
        start_year,
        target_item_code.to_owned()
    );
    let mut not_even_once = true;
    for l in lines{
        let v = line_to_str_vec(&l);
        let item_code = v[item_code_id];
        if item_code == target_item_code{
            let unit = v[unit_id];
            let info_type = v[info_id];
            let entry_id = map.get(info_type);
            let country = v[country_id];
            not_even_once = false;
            
            for (year_idx, amount_str) in v[start_year_id..].iter().enumerate(){
                if amount_str.is_empty(){
                    continue;
                }
                let amount: f64 = amount_str.parse()
                    .expect("Error in parsing amount as float");
                let extra = Extra{unit: unit.to_owned(), amount};
                let country_info = enrichments.get_mut_inserting(year_idx, country);
                country_info.push(entry_id, extra);
            }
            
        }
    }
    if not_even_once{
        dbg!(&enrichments);
        panic!("Item code is not contained within the specified data set!");
    }
    println!("DONE PARSING EXTRA");
    enrichments
}


pub fn parse_beef_network<P>(
    input: P
)   -> Vec<Network>
    where P: AsRef<Path>
{
    let path = input.as_ref();
    let lines = open_as_unwrapped_lines_filter_comments(path);
    // Let us first check which nodes we have over all.
    let mut countries = BTreeSet::new();

    let beef_id = "Beef";

    for line in lines{
        let mut iter = LineIter::new(&line);
        let importer = iter.next().unwrap();
        let exporter = iter.next().unwrap();
        let item = iter.next().unwrap();
        if item != beef_id
        {
            continue;
        }
        countries.insert(importer.to_owned());
        countries.insert(exporter.to_owned());
    }

    let all_nodes = countries
        .into_iter()
        .map(Node::new)
        .collect_vec();

    let node_map: HashMap<_, _> = all_nodes.iter()
        .map(|n| n.identifier.as_str())
        .zip(0_usize..)
        .collect();
        


    let lines = open_as_unwrapped_lines_filter_comments(path);
    let mut year_map = BTreeMap::new();

    for line in lines {
        let mut iter = LineIter::new(&line);
        let exporter = iter.next().unwrap();
        let importer = iter.next().unwrap();
        let item = iter.next().unwrap();
        if item != beef_id
        {
            continue;
        }
        let year_str = iter.next().unwrap();
        let year: i32 = year_str.parse().unwrap();
        let quantity_str = iter.next().unwrap();
        let quantity: f64 = quantity_str.parse().unwrap();
        if quantity == 0.0 {
            continue;
        }
        let _value: &str = iter.into();

        let network = year_map.entry(year)
            .or_insert_with(
                || 
                {
                    Network{
                        nodes: all_nodes.clone(), 
                        direction: Direction::ExportTo,
                        data_origin: ReadType::Beef,
                        year,
                        unit: "Unknown".to_string(),
                        sorted_item_codes: vec!["Beef".to_owned()]
                    }
                }
            );
        let import_index = *node_map.get(importer).unwrap();
        let export_index = *node_map.get(exporter).unwrap();

        let edge = Edge{
            amount: quantity,
            index: import_index
        };

        network.nodes[export_index].adj.push(edge);
    }
    year_map.into_values().collect()
}

pub fn parse_all_networks(
    file_name: &str,
    read_type: ReadType
)-> anyhow::Result<BTreeMap<String, Vec<Network>>>
{
    let unit_tester = UNIT_TESTER.deref();
    let direction = read_type.get_direction();
    let wanted_transaction_type = read_type.get_str();

    let file = File::open(file_name)
        .unwrap();
    let reader = BufReader::with_capacity(64 * 1024, file);

    let mut lines = reader.lines();
    let header = lines.next().unwrap().unwrap(); 
    let entry_iter = header.split(',');

    let entry_names: Vec<_> = entry_iter.collect();

    for (idx, &entry) in entry_names.iter().enumerate() {
        println!("Entry {idx} is {entry}");
    }

    let mut map = BTreeMap::new();
    let mut years: Vec<(i32, usize)> = Vec::new();
    for (idx, &entry) in entry_names.iter().enumerate()
    {
        if let Some(number) = entry.strip_prefix('Y'){
            if let Ok(y) = number.parse(){
                years.push((y, idx));
            }
        }
        map.entry(entry)
            .or_insert(idx);
    }

    let item_id = map["Item Code"];
    let reporter_country_id = map["Reporter Country Code"];
    let partner_country_id = map["Partner Country Code"];
    let unit_id = map["Unit"];
    let transaction_type = map["Element"];

    let mut countries: BTreeMap<String, (String, BTreeSet<String>, bool)> = BTreeMap::new();

    let mut unit_errors = Vec::new();
    
    let line_len = map.len();
    let line_iter = lines
        .map(|line| {
            let line = line.unwrap();
            let line_v = line_to_vec(&line);
            assert_eq!(line_v.len(), line_len);
            line_v
        });
    for line_vec in line_iter{
    
        if line_vec[transaction_type] != wanted_transaction_type{
            continue;
        }
        if line_vec.len() != line_len{
            return Err(anyhow::anyhow!("Line error! old_len {line_len} new {}", line_vec.len()));
        }
        let current_item_code = &line_vec[item_id];
        let unit = &line_vec[unit_id];

        let (other_units, c_set, unit_error) = 
            countries
                .entry(current_item_code.clone())
                .or_insert_with(|| (unit.clone(), BTreeSet::new(), false));    

        if *unit_error{
            continue;
        }
        if !unit_tester.is_equiv(unit, other_units){
            *unit_error = true;
            unit_errors.push(format!("Item: {} Unit1: {} Unit2: {}", current_item_code, unit, other_units));
            continue;
        }

        let rep_c = line_vec.get(reporter_country_id).unwrap();
        c_set.insert(rep_c.clone());
        let part_c = line_vec.get(partner_country_id).unwrap();
        c_set.insert(part_c.clone());
        
    }

    if !unit_errors.is_empty()
    {
        println!("Encountered {} unit errors", unit_errors.len());
        for error in unit_errors{
            println!("{error}");
        }
    }

    let mut network_map: BTreeMap<String, NetworkParsingHelper> = BTreeMap::new();
    for (item_code, (unit, c_set, unit_error)) in countries.into_iter(){
        if unit_error{
            continue;
        }
        
        let all: Vec<_> = c_set
            .into_iter()
            .map(Node::new)
            .collect();

        let mut id_map = BTreeMap::new();

        all.iter()
            .enumerate()
            .for_each(
                |(id, item)|
                {
                    let code = &item.identifier;
                    id_map.insert(code.to_owned(), id);
                }
            );
        let all_networks: Vec<_> = years
            .iter()
            .map(|(year, _)| 
                {
                    Network{
                        nodes: all.clone(), 
                        direction,
                        data_origin: read_type,
                        year: *year,
                        unit: unit.clone(),
                        sorted_item_codes: vec![item_code.clone()]
                    }
                }
            ).collect();
        let helper = NetworkParsingHelper{
            id_map,
            networks: all_networks
        };
        network_map.insert(item_code, helper);
    }
    

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
                item[transaction_type] == wanted_transaction_type
            });

    for line in iter{
        let entry = match network_map.get_mut(line[item_id].as_str()){
            None => {
                // unit error in this item code 
                continue;
            },
            Some(e) =>{
                e
            }
        };
        let rep_c = &line[reporter_country_id];
        let part_c = &line[partner_country_id];

        let rep_id = entry.id_map[rep_c];
        let part_id = entry.id_map[part_c];

        (years.iter())
            .zip(entry.networks.iter_mut())
            .for_each(
                |((_, idx), network)|
                {
                    let amount_entry = &line[*idx];
                    if !amount_entry.is_empty(){
                        let amount: f64 = amount_entry.parse().unwrap();
                        if amount > 0.0{
                            let node = network.nodes.get_mut(rep_id).unwrap();
                            let edge = Edge{
                                amount,
                                index: part_id
                            };
                            node.adj.push(edge);
                        }

                    }

                }
            )
    }

    Ok(
        network_map.into_iter()
        .map(
            |(key, helper)|
            {
                (key, helper.networks)
            }
        ).collect()
    )
    
}

struct NetworkParsingHelper{
    id_map: BTreeMap<String, usize>,
    networks: Vec<Network>
}

pub fn network_parser(
    file_name: &str, 
    item_code: &str, 
    silent: bool,
    read_type: ReadType
) -> anyhow::Result<Vec<Network>>
{

    let direction = read_type.get_direction();
    let wanted_transaction_type = read_type.get_str();

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
    let mut years: Vec<(i32, usize)> = Vec::new();
    for (idx, &entry) in entry_names.iter().enumerate()
    {
        if let Some(number) = entry.strip_prefix('Y'){
            if let Ok(y) = number.parse(){
                years.push((y, idx));
            }
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

    let line_len = map.len();


    let line_iter = lines
        .map(|line| {
            let line = line.unwrap();
            let line_v = line_to_vec(&line);
            debug_assert_eq!(line_v.len(), line_len);
            line_v
        })
        .filter(
            |item| 
            {
                item[item_id] == item_code 
            }
        );

    for line_vec in line_iter{
       
        if line_vec[transaction_type] != wanted_transaction_type{
            continue;
        }
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

    

    let mut all_networks: Vec<_> = years
        .iter()
        .map(|(year, _)| 
            {
                Network{
                    nodes: all.clone(), 
                    direction,
                    data_origin: read_type,
                    year: *year,
                    unit: glob_unit.as_ref().unwrap().clone(),
                    sorted_item_codes: vec![item_code.to_string()]
                }
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

            (years.iter())
                .zip(all_networks.iter_mut())
                .for_each(
                    |((_, idx), network)|
                    {
                        let amount_entry = &line[*idx];
                        if !amount_entry.is_empty(){
                            let amount: f64 = amount_entry.parse().unwrap();
                            if amount > 0.0{
                                let node = network.nodes.get_mut(rep_id).unwrap();
                                let edge = Edge{
                                    amount,
                                    index: part_id
                                };
                                node.adj.push(edge);
                            }
                            
                        }
                        
                    }
                )
        }
    );

    Ok(all_networks)
}


pub fn country_map<P>(code_file: P) -> BTreeMap<String, String>
where P: AsRef<Path>
{
    let lines = open_as_unwrapped_lines(code_file)
        .skip(1);

    let mut code_country_map: BTreeMap<_,_> = BTreeMap::new();

    for line in lines {
        let mut s_iter = LineIter{line: &line};
        let code = s_iter.next().unwrap();
        let name = s_iter.nth(1).unwrap();

        code_country_map.insert(code.to_owned(), name.to_owned());
    }
    code_country_map

}



#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    pub fn random_read_checks()
    {
        // check last line that contains wheat to see if it is parsed correctly
        let networks = read_networks("15.bincode");

        let years = [
            Some(11045.0),
            None,
            None,
            None,
            Some(66347.0),
            Some(5961.0),
            None,
            Some(68932.0),
            Some(54723.0),
            None,
            Some(96688.0),
            Some(20201.0),
            None,
            None,
            None,
            Some(5442.0),
            None,
            None,
            Some(15723.0),
            Some(10815.0),
            Some(4035.0),
            None,
            Some(381.0),
            None,
            Some(1560.0),
            Some(14865.0),
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            Some(5288.0),
            None,
            None
        ];

        for (n, amount) in networks.iter().zip(years)
        {
            let z = n.get_index("181");
            let u = n.get_index("231");
            match amount{
                None => {
                    if let Some(zim) = z {
                        let node = &n.nodes[zim];
                        if let Some(usa) = u{
                            assert!(node.adj.iter().all(|e| e.index != usa));
                        }
                    }
                },
                Some(val) =>{
                    let zim = z.unwrap();
                    let usa = u.unwrap();
                    let node = &n.nodes[zim];
                    let e = node.adj.iter().find(|e| e.index == usa).unwrap();
                    assert_eq!(e.amount, val);
                }
            }
        }
    }
}