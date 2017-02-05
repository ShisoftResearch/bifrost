#[derive(Serialize, Deserialize, Debug)]
pub struct Member {
    pub id: u64,
    pub address: String,
    pub online: bool,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct Group {
    pub id: u64,
    pub name: String,
    pub members: u64,
}

