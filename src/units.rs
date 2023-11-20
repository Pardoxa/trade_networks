use std::collections::*;
use lazy_static::lazy_static;

lazy_static! {
    pub static ref UNIT_TESTER: EquivalenceTester = {
        EquivalenceTester::default()
    };
}


#[allow(dead_code)]
pub enum Equivalence{
    Equivalent,
    ConversionPossible,
    Incompatible
}

#[allow(dead_code)]
pub struct EquivalenceTester{
    equivalent: Vec<BTreeSet<&'static str>>,
    conversion_possible: Vec<BTreeSet<&'static str>>,
    equiv_map: BTreeMap<&'static str, usize>,
    conversion_map: BTreeMap<&'static str, usize>
}

impl EquivalenceTester{
    pub fn is_equiv(&self, a: &str, b: &str) -> bool 
    {
        if a == b {
            true
        } else if let Some(i) = self.equiv_map.get(a){
            self.equivalent[*i].contains(b)
        } else {
            false
        }
    }
}

impl Default for EquivalenceTester{
    fn default() -> Self
    {
        let equivs = [["tonnes", "t"]];
        let equivalent: Vec<BTreeSet<&str>> = equivs.iter()
            .map(
                |list|
                {
                    list.iter().copied().collect()
                }
            ).collect();

        let mut equiv_map = BTreeMap::new();
        
        for (i, set) in equivalent.iter().enumerate()
        {
            for &item in set.iter(){
                let r = equiv_map.insert(item, i);
                assert!(r.is_none());
            }
        }


        Self { 
            equivalent, 
            conversion_possible: Vec::new(), 
            equiv_map, 
            conversion_map: BTreeMap::new() 
        }
    }
}