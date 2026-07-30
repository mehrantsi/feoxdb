use super::*;
use std::sync::mpsc::sync_channel;

#[test]
fn stop_detaches_when_called_by_the_worker() {
    let (handle_tx, handle_rx) = sync_channel(1);
    let (done_tx, done_rx) = sync_channel(1);
    let handle = thread::spawn(move || {
        let handle = handle_rx.recv().unwrap();
        let mut sweeper = TtlSweeper {
            store: Weak::new(),
            config: TtlConfig::default(),
            shutdown: Arc::new(AtomicBool::new(false)),
            handle: Some(handle),
            stats: TtlSweeperStats::new(),
        };
        sweeper.stop();
        done_tx.send(()).unwrap();
    });

    handle_tx.send(handle).unwrap();
    done_rx.recv_timeout(Duration::from_secs(5)).unwrap();
}
