// This module proves an ownership strategy with real driver transactions.
// It is deliberately a small probe, not a complete sqly ambient implementation.
use std::{future::Future, pin::Pin, sync::{Arc, Mutex, atomic::{AtomicBool, Ordering}}, task::{Context, Poll}};
use futures_core::Stream;
#[derive(Debug,PartialEq)]
enum ScopeError { NoScope, Nested, Mismatch, ActiveStream, Aborted, Database(String) }
impl From<sqlx::Error> for ScopeError { fn from(e:sqlx::Error)->Self { Self::Database(e.to_string()) } }
type ScopeResult<T> = Result<T,ScopeError>;
type Cursor = Pin<Box<dyn Stream<Item=ScopeResult<i64>> + Send>>;
struct ScopeState {
    identity:u64,
    tx:Arc<Mutex<Option<sqlx::Transaction<'static,Backend>>>>,
    cursor:Mutex<Option<Cursor>>,
    closed:AtomicBool,
    aborted:AtomicBool,
}
tokio::task_local! { static ACTIVE: Arc<ScopeState>; }
struct ScopeOwner(Arc<ScopeState>);
impl Drop for ScopeOwner {
    fn drop(&mut self) {
        self.0.closed.store(true,Ordering::SeqCst);
        self.0.cursor.lock().unwrap().take();
        self.0.tx.lock().unwrap().take();
    }
}
fn active(identity:u64)->ScopeResult<Arc<ScopeState>> {
    let state=ACTIVE.try_with(Arc::clone).map_err(|_|ScopeError::NoScope)?;
    if state.identity!=identity || state.closed.load(Ordering::SeqCst) { return Err(ScopeError::Mismatch); }
    Ok(state)
}
async fn write_scope<T,F,Fut>(pool:&sqlx::Pool<Backend>, identity:u64, f:F)->ScopeResult<T>
where F:FnOnce()->Fut, Fut:Future<Output=ScopeResult<T>> {
    if ACTIVE.try_with(|_|()).is_ok() { return Err(ScopeError::Nested); }
    let tx=pool.begin().await?;
    let state=Arc::new(ScopeState { identity,tx:Arc::new(Mutex::new(Some(tx))),cursor:Mutex::new(None),closed:AtomicBool::new(false),aborted:AtomicBool::new(false) });
    let owner=ScopeOwner(state.clone());
    let result=ACTIVE.scope(state.clone(),f()).await;
    state.closed.store(true,Ordering::SeqCst);
    if state.cursor.lock().unwrap().is_some() {
        drop(owner); // drops the cursor, whose generator owns the transaction
        return match result { Err(e)=>Err(e), Ok(_)=>Err(ScopeError::ActiveStream) };
    }
    let tx=state.tx.lock().unwrap().take();
    let Some(tx)=tx else { return Err(ScopeError::Aborted); };
    match result {
        Ok(value) if !state.aborted.load(Ordering::SeqCst) => { tx.commit().await?; Ok(value) }
        Ok(_) => { tx.rollback().await?; Err(ScopeError::Aborted) }
        Err(error) => { tx.rollback().await?; Err(error) }
    }
}
#[derive(Clone)]
struct ProbeStore { identity:u64 }
impl ProbeStore {
    async fn insert(&self, id:i64)->ScopeResult<()> {
        let state=active(self.identity)?;
        if state.cursor.lock().unwrap().is_some() { return Err(ScopeError::ActiveStream); }
        // Buffered concurrency queuing is outside this small ownership probe.
        let mut tx=state.tx.lock().unwrap().take().ok_or(ScopeError::Aborted)?;
        let result=sqlx::query("INSERT INTO ambient_probe VALUES ($1)").bind(id).execute(&mut *tx).await;
        if result.is_err() { state.aborted.store(true,Ordering::SeqCst); }
        *state.tx.lock().unwrap()=Some(tx);
        result.map(|_|()).map_err(Into::into)
    }
    fn rows(&self)->ScopedRows { ScopedRows { identity:self.identity,state:None,done:false } }
}
struct ScopedRows { identity:u64,state:Option<Arc<ScopeState>>,done:bool }
impl Stream for ScopedRows {
    type Item=ScopeResult<i64>;
    fn poll_next(mut self:Pin<&mut Self>,cx:&mut Context<'_>)->Poll<Option<Self::Item>> {
        if self.done { return Poll::Ready(None); }
        let current=match active(self.identity) { Ok(s)=>s,Err(e)=>{self.done=true;return Poll::Ready(Some(Err(e)));} };
        if let Some(owned)=&self.state {
            if !Arc::ptr_eq(owned,&current) { self.done=true;return Poll::Ready(Some(Err(ScopeError::Mismatch))); }
        } else {
            if current.cursor.lock().unwrap().is_some() { self.done=true;return Poll::Ready(Some(Err(ScopeError::ActiveStream))); }
            let mut tx=current.tx.lock().unwrap().take().expect("scope owns transaction");
            let slot=current.tx.clone();
            let cursor=async_stream::try_stream! {
                {
                    let mut rows=sqlx::query_scalar::<_,i64>("SELECT id FROM ambient_probe ORDER BY id").fetch(&mut *tx);
                    while let Some(row)=rows.try_next().await? { yield row; }
                }
                *slot.lock().unwrap()=Some(tx);
            };
            *current.cursor.lock().unwrap()=Some(Box::pin(cursor));
            self.state=Some(current.clone());
        }
        let result={
            let mut slot=current.cursor.lock().unwrap();
            slot.as_mut().expect("active cursor").as_mut().poll_next(cx)
        };
        if matches!(result,Poll::Ready(None)|Poll::Ready(Some(Err(_)))) {
            if matches!(result,Poll::Ready(Some(Err(_)))) { current.aborted.store(true,Ordering::SeqCst); }
            current.cursor.lock().unwrap().take();
            self.done=true;
        }
        result
    }
}
impl Drop for ScopedRows {
    fn drop(&mut self) {
        if !self.done {
            if let Some(state)=&self.state {
                state.aborted.store(true,Ordering::SeqCst);
                state.cursor.lock().unwrap().take(); // generator drop rolls back
            }
        }
    }
}
async fn ambient_pool()->sqlx::Pool<Backend> {
    let pool=pool().await;
    pool.execute("CREATE TEMP TABLE ambient_probe (id BIGINT)").await.unwrap();
    pool
}
#[tokio::test]
async fn ambient_cross_store_calls_commit_and_reject_unscoped_nested_and_spawned_writes() {
    let pool=ambient_pool().await;
    let a=ProbeStore{identity:1};let b=a.clone();
    assert_eq!(a.insert(0).await,Err(ScopeError::NoScope));
    write_scope(&pool,1,||async {
        a.insert(1).await?; b.insert(2).await?;
        assert_eq!(write_scope(&pool,1,||async{Ok(())}).await,Err(ScopeError::Nested));
        assert_eq!(ProbeStore{identity:2}.insert(3).await,Err(ScopeError::Mismatch));
        let spawned=b.clone();
        assert_eq!(tokio::spawn(async move{spawned.insert(4).await}).await.unwrap(),Err(ScopeError::NoScope));
        Ok(())
    }).await.unwrap();
    let n:i64=sqlx::query_scalar("SELECT count(*) FROM ambient_probe").fetch_one(&pool).await.unwrap();
    assert_eq!(n,2);pool.close().await;
}
#[tokio::test]
async fn ambient_stream_rejects_competitors_and_releases_transaction_on_exhaustion() {
    let pool=ambient_pool().await;let store=ProbeStore{identity:1};
    write_scope(&pool,1,||async {
        store.insert(1).await?;store.insert(2).await?;
        let mut rows=store.rows();assert_eq!(rows.try_next().await?,Some(1));
        assert_eq!(store.insert(3).await,Err(ScopeError::ActiveStream));
        let mut competing=store.rows();assert_eq!(competing.try_next().await,Err(ScopeError::ActiveStream));
        assert_eq!(rows.try_next().await?,Some(2));assert_eq!(rows.try_next().await?,None);
        store.insert(3).await?;Ok(())
    }).await.unwrap();
    let n:i64=sqlx::query_scalar("SELECT count(*) FROM ambient_probe").fetch_one(&pool).await.unwrap();
    assert_eq!(n,3);pool.close().await;
}
#[tokio::test]
async fn ambient_escaped_stream_is_invalidated_and_scope_rolls_back_without_waiting() {
    let pool=ambient_pool().await;let store=ProbeStore{identity:1};
    let result=write_scope(&pool,1,||async {
        store.insert(1).await?;let mut rows=store.rows();assert_eq!(rows.try_next().await?,Some(1));Ok(rows)
    }).await;
    assert!(matches!(result,Err(ScopeError::ActiveStream)));
    let n:i64=tokio::time::timeout(Duration::from_secs(5),sqlx::query_scalar("SELECT count(*) FROM ambient_probe").fetch_one(&pool)).await.unwrap().unwrap();
    assert_eq!(n,0);pool.close().await;
}
#[tokio::test]
async fn ambient_scope_cancellation_drops_a_live_cursor_and_rolls_back() {
    let pool=ambient_pool().await;let store=ProbeStore{identity:1};
    let (started_tx,started_rx)=tokio::sync::oneshot::channel();
    let task_pool=pool.clone();
    let task=tokio::spawn(async move {
        write_scope(&task_pool,1,||async {
            store.insert(1).await?;
            let mut rows=store.rows(); assert_eq!(rows.try_next().await?,Some(1));
            started_tx.send(()).unwrap();
            std::future::pending::<()>().await;
            drop(rows);Ok(())
        }).await
    });
    started_rx.await.unwrap(); task.abort(); assert!(task.await.unwrap_err().is_cancelled());
    let n:i64=tokio::time::timeout(Duration::from_secs(5),sqlx::query_scalar("SELECT count(*) FROM ambient_probe").fetch_one(&pool)).await.unwrap().unwrap();
    assert_eq!(n,0);pool.close().await;
}
