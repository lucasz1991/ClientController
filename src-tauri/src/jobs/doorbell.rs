use std::sync::mpsc::{self, Receiver, SyncSender, TrySendError};
use std::time::Duration;

pub(crate) const WATCHDOG_INTERVAL: Duration = Duration::from_secs(30);

#[derive(Clone)]
pub(crate) struct JobPollDoorbell {
    sender: SyncSender<()>,
}

impl JobPollDoorbell {
    pub(crate) fn signal(&self) -> Result<bool, String> {
        match self.sender.try_send(()) {
            Ok(()) => Ok(true),
            Err(TrySendError::Full(())) => Ok(false),
            Err(TrySendError::Disconnected(())) => {
                Err("job polling worker is not available".to_string())
            }
        }
    }
}

pub(crate) fn channel() -> (JobPollDoorbell, Receiver<()>) {
    let (sender, receiver) = mpsc::sync_channel(1);
    (JobPollDoorbell { sender }, receiver)
}

#[cfg(test)]
mod tests {
    use super::channel;
    use std::time::Duration;

    #[test]
    fn pending_signals_are_coalesced() {
        let (doorbell, receiver) = channel();

        assert!(doorbell.signal().expect("first signal should enqueue"));
        assert!(!doorbell.signal().expect("duplicate signal should coalesce"));
        receiver
            .recv_timeout(Duration::from_millis(50))
            .expect("queued signal should be available");
        assert!(doorbell
            .signal()
            .expect("signal should enqueue after drain"));
    }
}
