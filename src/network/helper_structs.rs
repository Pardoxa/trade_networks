use serde::{Serialize, Deserialize};
use std::num::*;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TarjanNumberHelper{
    NotVisited,
    Visited(NonZeroU32)
}

impl TarjanNumberHelper {
    pub fn is_not_visited(&self) -> bool 
    {
        matches!(self, TarjanNumberHelper::NotVisited)
    }

    #[inline]
    pub fn get_num(&self) -> NonZeroU32
    {
        match self {
            Self::NotVisited => unreachable!(),
            Self::Visited(num) => *num    
        }
    }

    pub fn min(&self, other: Self) -> Self
    {
        TarjanNumberHelper::Visited(self.get_num().min(other.get_num()))
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

#[allow(dead_code)]
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
    pub id: String
}
