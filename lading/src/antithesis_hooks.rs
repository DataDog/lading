//! Antithesis system-under-test bootstrap for the lading binary.
//!
//! Compiled only under the `antithesis` feature. Initializes the Antithesis SDK
//! and installs a panic hook that reports any panic as an Antithesis
//! `unreachable!` before the process aborts. All SDK access goes through the
//! `lading_antithesis` facade, never `antithesis_sdk` directly.

/// Initialize the Antithesis SDK and install a panic-reporting hook.
///
/// Call once, as early in startup as possible, before any panic can occur. The
/// installed hook forwards to the previous hook after reporting, so normal panic
/// output is preserved.
pub fn init() {
    lading_antithesis::init();

    let default_hook = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |info| {
        let location = info
            .location()
            .map_or_else(String::new, ToString::to_string);
        let payload = info.payload();
        let message = payload
            .downcast_ref::<&str>()
            .map(|s| (*s).to_string())
            .or_else(|| payload.downcast_ref::<String>().cloned())
            .unwrap_or_else(|| "<non-string panic payload>".to_string());
        lading_antithesis::unreachable!(
            "lading panicked",
            { "message": message, "location": location }
        );
        default_hook(info);
    }));
}
