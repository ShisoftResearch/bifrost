use bifrost::utils::math;

#[test]
fn max () {
    assert_eq!(math::max(&vec!(1, 2, 3, 4, 5)).unwrap(), 5);
    assert_eq!(math::max(&vec!(1, 2, 9, 4, 5)).unwrap(), 9);
    assert_eq!(math::max(&Vec::<u64>::new()), None);
}

#[test]
fn min () {
    assert_eq!(math::min(&vec!(1, 2, 3, 4, 5)).unwrap(), 1);
    assert_eq!(math::min(&vec!(1, 2, -10, 4, 5)).unwrap(), -10);
    assert_eq!(math::min(&Vec::<u64>::new()), None);
}