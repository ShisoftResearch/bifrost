use std::ops::{Sub, Add, Div};

pub fn min<T>(nums: &Vec<T>) -> Option<T> where T: Ord + Copy {
    nums.iter().fold(None, |min, x| match min {
        None => Some(*x),
        Some(y) => Some(if *x < y { *x } else { y }),
    })
}
pub fn max<T>(nums: &Vec<T>) -> Option<T> where T: Ord + Copy {
    nums.iter().fold(None, |max, x| match max {
        None => Some(*x),
        Some(y) => Some(if *x > y { *x } else { y }),
    })
}
pub fn avg_scale(nums: &Vec<u64>) -> Option<u64> {
    if nums.len() > 0 {
        let count = nums.len() as u64;
        let max_num = max(nums).unwrap();
        let min_num = min(nums).unwrap();
        let sum: u64 = nums.iter().sum();
        let mid_abs = (sum - (min_num * count)) / count;
        return Some(min_num + mid_abs)
    }
    return None;
}