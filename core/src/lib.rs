use std::{
    panic,
    panic::PanicInfo,
    process,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::Duration,
};

use log::*;

mod fetch_stage;
pub mod ofac_stage;
mod staked_nodes_updater_service;
pub mod tpu;

/// Returns an exit boolean to let other threads gracefully shut down
pub fn graceful_panic(callback: Option<fn(&PanicInfo)>) -> Arc<AtomicBool> {
    let exit = Arc::new(AtomicBool::new(false));
    // Fail fast!
    let panic_hook = panic::take_hook();
    {
        let exit = exit.clone();
        panic::set_hook(Box::new(move |panic_info| {
            if let Some(f) = callback {
                f(panic_info);
            }
            exit.store(true, Ordering::Relaxed);
            error!("exiting process");
            // let other loops finish up
            std::thread::sleep(Duration::from_secs(5));
            // invoke the default handler and exit the process
            panic_hook(panic_info); // print the panic backtrace. default exit code is 101

            process::exit(1); // bail us out if thread blocks/refuses to join main thread
        }));
    }
    exit
}
