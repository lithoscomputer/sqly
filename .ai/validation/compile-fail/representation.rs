use sqly_validation::{Encode,Result};
struct Custom;
struct Unsupported;
impl Encode for Custom {
    type Repr = Unsupported;
    fn encode(&self) -> Result<Unsupported> { Ok(Unsupported) }
}
fn main() {}
