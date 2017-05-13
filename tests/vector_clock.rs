use bifrost::vector_clock::StandardVectorClock;

#[test]
fn test() {
    let mut clock = StandardVectorClock::new();
    let blank_clock = StandardVectorClock::new();
    clock.inc(1);
    println!("{:?}", clock.relation(&blank_clock));
    assert!(clock > blank_clock);
    assert!(blank_clock < clock);
    assert!(blank_clock != clock);
}