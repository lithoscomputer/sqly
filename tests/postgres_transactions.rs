#![cfg(feature = "postgres")]
use std::error::Error as _;
use std::io::{self, Read as _, Write as _};
use std::net::{Shutdown, TcpListener, TcpStream};
use std::sync::mpsc;
use std::time::Duration;
use std::{env, thread};

use sqly::{Database, Error, Lock, Result};
use tokio::sync::oneshot;
use tokio::task;
use tokio::time::timeout;
use url::Url;

fn url() -> String {
    env::var("SQLY_TEST_POSTGRES_URL").expect("PostgreSQL fixture")
}

#[tokio::test]
async fn dependent_read_sees_the_revision_committed_by_the_lock_holder() -> Result<()> {
    let db = Database::connect(url().as_str()).await?;
    db.query("DROP TABLE IF EXISTS sqly_tx_revision")
        .execute()
        .await?;
    db.query("DROP TABLE IF EXISTS sqly_tx_owner")
        .execute()
        .await?;
    db.query("CREATE TABLE sqly_tx_owner (id BIGINT PRIMARY KEY)")
        .execute()
        .await?;
    db.query("CREATE TABLE sqly_tx_revision (owner BIGINT PRIMARY KEY REFERENCES sqly_tx_owner(id), revision BIGINT NOT NULL)").execute().await?;
    db.query("INSERT INTO sqly_tx_owner VALUES (1)")
        .execute()
        .await?;
    db.query("INSERT INTO sqly_tx_revision VALUES (1, 1)")
        .execute()
        .await?;
    let mut first = db.begin_write().await?;
    assert!(
        first
            .lock(Lock::row("sqly_tx_owner").key("id", 1_i64))
            .await?
    );
    first
        .query("UPDATE sqly_tx_revision SET revision = 2 WHERE owner = 1")
        .execute()
        .await?;
    assert_eq!(
        db.query("SELECT revision FROM sqly_tx_revision WHERE owner = 1")
            .fetch_one()
            .await?
            .try_get::<i64>("revision")?,
        1
    );
    let mut second = db.begin_write().await?;
    let pid: i32 = second
        .query("SELECT pg_backend_pid() AS pid")
        .fetch_one()
        .await?
        .try_get("pid")?;
    let waiter = task::spawn(async move {
        assert!(
            second
                .lock(Lock::row("sqly_tx_owner").key("id", 1_i64))
                .await?
        );
        let revision: i64 = second
            .query("SELECT revision FROM sqly_tx_revision WHERE owner = 1")
            .fetch_one()
            .await?
            .try_get("revision")?;
        second.commit().await?;
        Ok::<_, Error>(revision)
    });
    timeout(Duration::from_secs(5), async {
        loop {
            let blocked: bool = db.query("SELECT wait_event_type = 'Lock' AS blocked FROM pg_stat_activity WHERE pid = $1").bind(pid).fetch_one().await?.try_get::<Option<bool>>("blocked")?.unwrap_or(false);
            if blocked { break; }
            task::yield_now().await;
        }
        Ok::<_, Error>(())
    }).await.expect("second transaction waits on the row lock")?;
    first.commit().await?;
    assert_eq!(
        timeout(Duration::from_secs(5), waiter)
            .await
            .expect("lock released")
            .expect("waiter")?,
        2
    );
    db.close().await;
    Ok(())
}

// A protocol proxy drops the server's COMMIT acknowledgement, after the server
// has sent CommandComplete. This proves a reported failure can accompany a
// committed write, without timing guesses or modifying production code.
fn lose_commit_ack(
    listener: &TcpListener,
    address: String,
    reached: oneshot::Sender<()>,
    resume: &mpsc::Receiver<()>,
) -> io::Result<()> {
    let (mut client, _) = listener.accept()?;
    let mut server = TcpStream::connect(address)?;
    client.set_read_timeout(Some(Duration::from_secs(15)))?;
    server.set_read_timeout(Some(Duration::from_secs(15)))?;
    let mut client_read = client.try_clone()?;
    let mut server_write = server.try_clone()?;
    let upstream = thread::spawn(move || {
        let _ = io::copy(&mut client_read, &mut server_write);
    });
    let result = (|| {
        loop {
            let mut header = [0; 5];
            server.read_exact(&mut header)?;
            let length = u32::from_be_bytes(header[1..].try_into().expect("four bytes"));
            if !(4..=16_777_216).contains(&length) {
                return Err(io::Error::other("invalid protocol length"));
            }
            let mut body = vec![0; (length - 4) as usize];
            server.read_exact(&mut body)?;
            if header[0] == b'C' && body == b"COMMIT\0" {
                reached.send(()).expect("commit observer");
                resume
                    .recv_timeout(Duration::from_secs(5))
                    .expect("resume proxy");
                return Ok(());
            }
            client.write_all(&header)?;
            client.write_all(&body)?;
        }
    })();
    let _ = client.shutdown(Shutdown::Both);
    let _ = server.shutdown(Shutdown::Both);
    upstream.join().expect("upstream proxy thread");
    result
}
#[tokio::test]
async fn lost_acknowledgement_and_cancelled_commit_can_accompany_committed_writes() -> Result<()> {
    let observer = Database::connect(url().as_str()).await?;
    observer
        .query("DROP TABLE IF EXISTS sqly_tx_commit_unknown")
        .execute()
        .await?;
    observer
        .query("CREATE TABLE sqly_tx_commit_unknown (id BIGINT PRIMARY KEY)")
        .execute()
        .await?;
    for (id, cancel) in [(1_i64, false), (2_i64, true)] {
        let mut proxy_url = Url::parse(&url()).expect("fixture URL");
        let address = format!(
            "{}:{}",
            proxy_url.host_str().expect("host"),
            proxy_url.port().unwrap_or(5432)
        );
        let listener = TcpListener::bind("127.0.0.1:0").expect("proxy listener");
        proxy_url.set_host(Some("127.0.0.1")).expect("proxy host");
        proxy_url
            .set_port(Some(listener.local_addr().expect("proxy address").port()))
            .expect("proxy port");
        let (reached, observed) = oneshot::channel();
        let (release, resume) = mpsc::channel();
        let proxy = thread::spawn(move || lose_commit_ack(&listener, address, reached, &resume));
        let db = Database::builder()
            .max_connections(1)
            .connect(proxy_url.as_str())
            .await?;
        let mut tx = db.begin_write().await?;
        tx.query("INSERT INTO sqly_tx_commit_unknown VALUES ($1)")
            .bind(id)
            .execute()
            .await?;
        let commit = task::spawn(tx.commit());
        timeout(Duration::from_secs(5), observed)
            .await
            .expect("server commits")
            .expect("commit signal");
        if cancel {
            commit.abort();
            assert!(commit.await.expect_err("cancelled commit").is_cancelled());
            release.send(()).expect("release proxy");
        } else {
            release.send(()).expect("release proxy");
            let error = timeout(Duration::from_secs(5), commit)
                .await
                .expect("lost acknowledgement")
                .expect("commit task")
                .expect_err("unknown commit");
            assert!(matches!(error, Error::CommitUnknown { .. }), "{error:?}");
            assert!(error.source().is_some());
        }
        proxy
            .join()
            .expect("proxy thread")
            .expect("discarded COMMIT acknowledgement");
        assert_eq!(
            observer
                .query("SELECT count(*) AS n FROM sqly_tx_commit_unknown WHERE id = $1")
                .bind(id)
                .fetch_one()
                .await?
                .try_get::<i64>("n")?,
            1
        );
        db.close().await;
    }
    observer.close().await;
    Ok(())
}

#[cfg(unix)]
#[tokio::test]
async fn percent_encoded_socket_directory_connects_to_postgres() -> Result<()> {
    use std::os::unix::net::UnixListener;
    use std::{fs, process};

    let directory = env::temp_dir().join(format!("sqly-pg-socket-{}", process::id()));
    fs::create_dir(&directory).expect("socket directory");
    let listener = UnixListener::bind(directory.join(".s.PGSQL.5432")).expect("Unix listener");
    let mut options = Url::parse(&url()).expect("fixture URL");
    let address = format!(
        "{}:{}",
        options.host_str().expect("host"),
        options.port().unwrap_or(5432)
    );
    let encoded = percent_encoding::utf8_percent_encode(
        directory.to_str().expect("UTF-8 directory"),
        percent_encoding::NON_ALPHANUMERIC,
    )
    .to_string();
    options
        .set_host(Some(&encoded))
        .expect("encoded Unix socket host");
    options.set_port(None).expect("socket port");
    let proxy = thread::spawn(move || -> io::Result<()> {
        let (mut client, _) = listener.accept()?;
        let mut server = TcpStream::connect(address)?;
        client.set_read_timeout(Some(Duration::from_secs(5)))?;
        server.set_read_timeout(Some(Duration::from_secs(5)))?;
        let mut client_read = client.try_clone()?;
        let mut server_write = server.try_clone()?;
        let upstream = thread::spawn(move || {
            let result = io::copy(&mut client_read, &mut server_write);
            let _ = server_write.shutdown(Shutdown::Write);
            result
        });
        let result = io::copy(&mut server, &mut client);
        let _ = client.shutdown(Shutdown::Both);
        let _ = server.shutdown(Shutdown::Both);
        upstream.join().expect("socket upstream")?;
        result.map(|_| ())
    });
    let db = Database::builder()
        .max_connections(1)
        .connect(options.as_str())
        .await?;
    assert_eq!(
        db.query("SELECT 42 AS value")
            .fetch_one()
            .await?
            .try_get::<i64>("value")?,
        42
    );
    db.close().await;
    proxy
        .join()
        .expect("socket proxy")
        .expect("Unix socket transport");
    fs::remove_dir_all(directory).expect("remove socket directory");
    Ok(())
}
