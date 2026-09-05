use sqly_validation::Transaction;
fn main() {
    let mut tx = Transaction;
    let rows = tx.query("SELECT 1").fetch();
    let _other = tx.query("SELECT 2");
    drop(rows);
}
